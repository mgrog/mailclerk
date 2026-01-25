use std::{
    collections::HashSet,
    sync::{atomic::AtomicI64, Arc},
};

use anyhow::{anyhow, Context};
use chrono::{NaiveDateTime, Utc};
use derive_more::Display;
use entity::{email_training, prelude::*, processed_email};
use indexmap::IndexSet;
use num_traits::FromPrimitive;
use sea_orm::{
    entity::*, prelude::Decimal, sea_query::OnConflict, ActiveValue, DatabaseConnection,
    EntityTrait, FromQueryResult, QueryFilter, QuerySelect,
};
use std::sync::atomic::Ordering::Relaxed;
use tokio::sync::watch;

use crate::{
    email::{
        client::{EmailClient, MessageListOptions},
        rules::UserEmailRules,
        simplified_message::SimplifiedMessage,
    },
    embed::chunker::chunk_email,
    prompt::task_extraction::ExtractedTask,
};
use crate::{
    error::{extract_database_error_code, AppError, AppResult, DatabaseErrorCode},
    model::{
        labels::UtilityLabels, processed_email::ProcessedEmailCtrl,
        user::UserWithAccountAccessAndUsage, user_token_usage::UserTokenUsageStatsCtrl,
    },
    prompt::{
        mistral::{self, CategoryPromptResponse},
        task_extraction, Priority, QueueEntry, TaskData, TaskQueue,
    },
    rate_limiters::RateLimiters,
    server_config::cfg,
    HttpClient, ServerState,
};

use crate::email::rules::{EmailRule, HEURISTIC_EMAIL_RULES};
use crate::embed::should_embed_email;

lazy_static::lazy_static!(
    static ref UNKNOWN_RULE: EmailRule = EmailRule {
            prompt_content: "Unknown".to_string(),
            mail_label: UtilityLabels::Uncategorized.as_str().to_string(),
            extract_tasks: true,
            priority: 2,
        };
    static ref EXCEPTION_RULES: &'static [&'static str] = &["Terms of Service Update", "Verification Code", "Security Alert"];
);

#[derive(Clone)]
/// EmailProcessor processes emails for a single user
pub struct EmailProcessor {
    pub user_id: i32,
    pub user_account_access_id: i32,
    pub email_address: String,
    pub created_at: chrono::DateTime<Utc>,
    daily_token_limit: i64,
    processed_email_count: Arc<AtomicI64>,
    failed_email_count: Arc<AtomicI64>,
    email_client: Arc<EmailClient>,
    token_count: Arc<AtomicI64>,
    http_client: HttpClient,
    conn: DatabaseConnection,
    rate_limiters: RateLimiters,
    task_queue: TaskQueue,
    user_email_rules: Arc<UserEmailRules>,
    interrupt_channel: (
        tokio::sync::watch::Sender<InterruptSignal>,
        tokio::sync::watch::Receiver<InterruptSignal>,
    ),
}

impl EmailProcessor {
    pub async fn new(
        server_state: ServerState,
        user: UserWithAccountAccessAndUsage,
    ) -> AppResult<Self> {
        let user_id = user.id;
        let user_account_access_id = user.user_account_access_id;
        let email_address = user.email.clone();
        let quota_used = user.tokens_consumed;
        let daily_token_limit = user.daily_token_limit;
        let conn = server_state.conn.clone();
        let http_client = server_state.http_client.clone();
        let rate_limiters = server_state.rate_limiters.clone();
        let task_queue = server_state.task_queue.clone();

        let email_client = EmailClient::new(http_client.clone(), conn.clone(), user)
            .await
            .map_err(|e| {
                AppError::Internal(anyhow!(
                    "Could not create email client for: {}, error: {}",
                    email_address,
                    e.to_string()
                ))
            })?;

        tracing::info!("Email client created successfully for {}", email_address);

        // -- DEBUG
        // println!("User's current usage: {}", quota_used);
        // println!("User's remaining quota: {}", remaining_quota);
        // -- DEBUG

        let user_email_rules = UserEmailRules::from_user(&conn, user_id).await?;
        let interrupt_channel = watch::channel(InterruptSignal::Run);

        let processor = EmailProcessor {
            user_id,
            user_account_access_id,
            email_address,
            created_at: chrono::Utc::now(),
            daily_token_limit,
            processed_email_count: Arc::new(AtomicI64::new(0)),
            failed_email_count: Arc::new(AtomicI64::new(0)),
            email_client: Arc::new(email_client),
            token_count: Arc::new(AtomicI64::new(quota_used)),
            http_client,
            conn,
            rate_limiters,
            task_queue,
            user_email_rules: Arc::new(user_email_rules),
            interrupt_channel,
        };

        tracing::info!("Email processor created for {}", processor.email_address);

        Ok(processor)
    }

    /// Queue emails for processing. Returns the number of emails queued.
    /// Queues recent (high priority) emails first, then older (low priority) if under quota.
    pub async fn queue_emails(&self) -> AppResult<i32> {
        if self.has_stopped_queueing() {
            return Ok(0);
        }

        // Only queue if no high priority emails are pending
        let queue_count = self.task_queue.queue_count(&self.email_address);
        if queue_count.high > 0 {
            return Ok(0);
        }

        self.queue_recent_emails().await.inspect_err(|e| {
            tracing::error!("Error queuing emails for {}: {:?}", self.email_address, e);
            self.fail();
        })
    }

    async fn fetch_email_ids(
        &self,
        options: Option<FetchOptions>,
    ) -> anyhow::Result<IndexSet<String>> {
        #[derive(FromQueryResult)]
        struct ProcessedEmailId {
            id: String,
        }

        let query = processed_email::Entity::find()
            .filter(processed_email::Column::UserId.eq(self.user_id))
            .select_only()
            .column(processed_email::Column::Id)
            .into_model::<ProcessedEmailId>()
            .all(&self.conn);

        let already_processed_ids = query
            .await?
            .into_iter()
            .map(|e| e.id)
            .collect::<HashSet<String>>();

        let mut message_ids_to_process = IndexSet::new();
        let load_page = |next_page_token: Option<String>| async {
            let resp = match self
                .email_client
                .get_message_list(MessageListOptions {
                    page_token: next_page_token,
                    more_recent_than: options.as_ref().and_then(|o| o.more_recent_than),

                    ..Default::default()
                })
                .await
                .context("Error loading next email page")
            {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::error!("Error loading next email page: {:?}", e);
                    return Err::<_, anyhow::Error>(e);
                }
            };

            Ok(resp)
        };

        // Collect at least 500 unprocessed emails or until there are no more emails
        let mut next_page_token = None;
        while let Ok(resp) = load_page(next_page_token.clone()).await {
            next_page_token = resp.next_page_token.clone();

            for id in resp
                .messages
                .unwrap_or_default()
                .into_iter()
                .filter_map(|m| m.id)
                .filter(|id| !already_processed_ids.contains(id))
            {
                if message_ids_to_process.len() >= 500 {
                    break;
                }

                message_ids_to_process.insert(id);
            }

            if next_page_token.is_none() || message_ids_to_process.len() >= 500 {
                break;
            }
        }

        Ok(message_ids_to_process)
    }

    async fn queue_recent_emails(&self) -> AppResult<i32> {
        let new_email_ids = self
            .fetch_email_ids(Some(FetchOptions {
                more_recent_than: Some(chrono::Duration::days(14)),
                ..Default::default()
            }))
            .await?;

        if new_email_ids.is_empty() {
            return Ok(0);
        }

        let mut num_added = 0;
        for email_id in &new_email_ids {
            let entry = QueueEntry {
                user_email: self.email_address.clone(),
                user_id: self.user_id,
                email_id: email_id.clone(),
                priority: Priority::High,
                task: TaskData::Categorization,
            };
            if self.task_queue.push(entry) {
                num_added += 1;
            }
        }

        Ok(num_added)
    }

    async fn queue_older_emails(&self) -> AppResult<i32> {
        let new_email_ids = self.fetch_email_ids(None).await?;

        if new_email_ids.is_empty() {
            return Ok(0);
        }

        let mut num_added = 0;
        for email_id in &new_email_ids {
            let entry = QueueEntry {
                user_email: self.email_address.clone(),
                user_id: self.user_id,
                email_id: email_id.clone(),
                priority: Priority::Low,
                task: TaskData::Categorization,
            };
            if self.task_queue.push(entry) {
                num_added += 1;
            }
        }

        Ok(num_added)
    }

    pub fn reset_quota(&self) {
        self.token_count.store(0, Relaxed);
    }

    async fn parse_and_prompt_email(
        &self,
        email_message: &SimplifiedMessage,
    ) -> anyhow::Result<PromptReturnData> {
        let CategoryPromptResponse {
            category: ai_answer,
            confidence,
            token_usage,
        } = mistral::send_category_prompt(
            &self.http_client,
            &self.rate_limiters,
            email_message,
            &self.user_email_rules,
        )
        .await
        .map_err(|e| anyhow!("Error sending prompt: {e}"))?;

        let mut selected_email_rule = self
            .user_email_rules
            .data()
            .iter()
            .find(|c| c.prompt_content.eq_ignore_ascii_case(&ai_answer))
            .unwrap_or(&UNKNOWN_RULE);

        let mut heuristics_used = false;
        if let Some(from) = email_message.from.as_deref() {
            if confidence < cfg.model.email_confidence_threshold
                && !EXCEPTION_RULES.contains(&selected_email_rule.prompt_content.as_str())
            {
                if let Some(rule) = HEURISTIC_EMAIL_RULES
                    .iter()
                    .find(|c| from.contains(&c.prompt_content))
                {
                    selected_email_rule = rule;
                    heuristics_used = true;
                }
            }
        }

        if confidence < cfg.model.email_confidence_threshold && !heuristics_used {
            selected_email_rule = &UNKNOWN_RULE;
        }

        Ok(PromptReturnData {
            email_rule: selected_email_rule.clone(),
            ai_answer,
            ai_confidence: confidence,
            heuristics_used,
            token_usage,
        })
    }

    // TODO: Remove this
    async fn record_email_for_training(
        &self,
        email_message: &SimplifiedMessage,
        parse_and_prompt_return: &PromptReturnData,
    ) -> anyhow::Result<()> {
        let PromptReturnData {
            ai_answer,
            ai_confidence,
            heuristics_used,
            ..
        } = parse_and_prompt_return;

        let email_training = email_training::ActiveModel {
            id: ActiveValue::NotSet,
            user_email: ActiveValue::Set(self.email_address.clone()),
            email_id: ActiveValue::Set(email_message.id.clone()),
            from: ActiveValue::Set(email_message.from.clone().unwrap_or_default()),
            subject: ActiveValue::Set(email_message.subject.clone().unwrap_or_default()),
            body: ActiveValue::Set(email_message.body.clone().unwrap_or_default()),
            ai_answer: ActiveValue::Set(ai_answer.clone()),
            confidence: ActiveValue::Set(*ai_confidence),
            heuristics_used: ActiveValue::Set(*heuristics_used),
        };

        EmailTraining::insert(email_training)
            .on_conflict(
                OnConflict::column(email_training::Column::EmailId)
                    .update_columns([
                        email_training::Column::AiAnswer,
                        email_training::Column::Confidence,
                        email_training::Column::HeuristicsUsed,
                    ])
                    .to_owned(),
            )
            .exec(&self.conn)
            .await
            .context("Error inserting email training data")?;

        Ok(())
    }

    /// Check thread status: unread replies, user replies, and multiple messages.
    /// Returns (has_unread_reply, user_has_replied, is_thread)
    async fn check_thread_status(
        &self,
        thread_id: &str,
        current_message_id: &str,
    ) -> anyhow::Result<(bool, bool, bool)> {
        let thread = self.email_client.get_thread_by_id(thread_id).await?;
        let messages = thread.messages.unwrap_or_default();

        let is_thread = messages.len() > 1;

        let has_unread_reply = messages.iter().any(|msg| {
            let msg_id = msg.id.as_deref().unwrap_or_default();
            let is_different_message = msg_id != current_message_id;
            let is_unread = msg
                .label_ids
                .as_ref()
                .map(|labels| labels.contains(&"UNREAD".to_string()))
                .unwrap_or(false);
            is_different_message && is_unread
        });

        let user_has_replied = messages.iter().any(|msg| {
            let msg_id = msg.id.as_deref().unwrap_or_default();
            let is_different_message = msg_id != current_message_id;
            let is_from_user = msg
                .payload
                .as_ref()
                .and_then(|p| p.headers.as_ref())
                .and_then(|headers| {
                    headers.iter().find_map(|h| {
                        if h.name.as_deref()?.eq_ignore_ascii_case("from") {
                            h.value.clone()
                        } else {
                            None
                        }
                    })
                })
                .map(|from| from.contains(&self.email_address))
                .unwrap_or(false);
            is_different_message && is_from_user
        });

        Ok((has_unread_reply, user_has_replied, is_thread))
    }

    async fn record_processed_email(
        &self,
        ProcessedEmailRecordData {
            email_message,
            ai_data: data,
        }: ProcessedEmailRecordData,
    ) -> anyhow::Result<()> {
        let closest_due_data = data
            .extracted_tasks
            .iter()
            .filter_map(|t| {
                t.due_date
                    .as_ref()
                    .and_then(|d| d.parse::<NaiveDateTime>().ok())
            })
            .min();

        let extracted_tasks = if data.extracted_tasks.is_empty() {
            ActiveValue::NotSet
        } else {
            let json_tasks: Vec<sea_orm::JsonValue> = data
                .extracted_tasks
                .into_iter()
                .map(|t| serde_json::json!(t))
                .collect();
            ActiveValue::Set(Some(json_tasks))
        };

        // Check thread status: unread replies, user replies, and if it's a thread
        let (has_new_reply, user_has_replied, is_thread) = match &email_message.thread_id {
            Some(thread_id) => self
                .check_thread_status(thread_id, &email_message.id)
                .await
                .unwrap_or((false, false, false)),
            None => (false, false, false),
        };

        match ProcessedEmailCtrl::insert(
            &self.conn,
            processed_email::ActiveModel {
                id: ActiveValue::Set(email_message.id.clone()),
                thread_id: ActiveValue::Set(email_message.thread_id),
                user_id: ActiveValue::Set(self.user_id),
                category: ActiveValue::Set(data.prompt_return_data.email_rule.mail_label),
                ai_answer: ActiveValue::Set(data.prompt_return_data.ai_answer),
                ai_confidence: ActiveValue::Set(data.prompt_return_data.ai_confidence.to_string()),
                processed_at: ActiveValue::NotSet,
                due_date: ActiveValue::Set(closest_due_data),
                extracted_tasks,
                history_id: ActiveValue::Set(Decimal::from(email_message.history_id)),
                is_read: ActiveValue::Set(!email_message.label_ids.contains(&"UNREAD".to_string())),
                tasks_done: ActiveValue::Set(false),
                has_new_reply: ActiveValue::Set(has_new_reply && user_has_replied),
                is_thread: ActiveValue::Set(is_thread),
                from: ActiveValue::Set(email_message.from),
                subject: ActiveValue::Set(email_message.subject),
                snippet: ActiveValue::Set(email_message.snippet),
                internal_date: ActiveValue::Set(email_message.internal_date),
            },
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(err) => match extract_database_error_code(&err) {
                Some(code)
                    if DatabaseErrorCode::from_u32(code)
                        .is_some_and(|c| c == DatabaseErrorCode::UniqueViolation) =>
                {
                    tracing::warn!("Email {} already processed", email_message.id);
                    Ok(())
                }
                _ => Err(anyhow!("Error inserting processed email: {:?}", err)),
            },
        }
    }

    async fn run_processor_pipeline(&self, email_id: String) -> anyhow::Result<()> {
        let email_message = self
            .email_client
            .get_message_by_id(&email_id)
            .await
            .context("Failed to fetch email")?;
        let simplified = SimplifiedMessage::from_gmail_message(&email_message).context(format!(
            "Failed to simplify email: {}, user: {}",
            email_id, self.user_id
        ))?;

        // Estimate token usage and check against remaining quota (allow up to 1% over)
        let email_text = simplified.to_string();
        let estimated_tokens = tokenizer::token_count(&email_text).unwrap_or(0) as i64;
        let quota_buffer = self.daily_token_limit / 100; // 1% buffer
        if estimated_tokens > self.quota_remaining() + quota_buffer {
            tracing::info!(
                "Estimated token usage ({}) exceeds remaining quota ({}) by more than 1% for {}. Cancelling processor.",
                estimated_tokens,
                self.quota_remaining(),
                self.email_address
            );
            self.cancel();
            return Ok(());
        }

        self.rate_limiters.acquire_one().await;

        let prompt_return_data = self.parse_and_prompt_email(&simplified).await?;

        let mut token_usage = prompt_return_data.token_usage;
        let mut extracted_tasks = Vec::new();

        // Extract tasks if the matched rule has extract_tasks enabled
        if prompt_return_data.email_rule.extract_tasks {
            self.rate_limiters.acquire_one().await;
            match task_extraction::extract_tasks_from_email(
                &self.http_client,
                &self.rate_limiters,
                &simplified,
            )
            .await
            {
                Ok(task_result) => {
                    token_usage += task_result.token_usage;
                    extracted_tasks = task_result.tasks;
                }
                Err(e) => {
                    tracing::warn!("Error extracting tasks from email: {:?}", e);
                }
            }
        }

        // Queue embedding task for batch processing (deferred, Background priority)
        if should_embed_email(prompt_return_data.email_rule.priority) {
            let chunks = chunk_email(&simplified);

            let entry = QueueEntry {
                user_email: self.email_address.clone(),
                user_id: self.user_id,
                email_id: simplified.id.clone(),
                priority: Priority::Background,
                task: TaskData::Embedding { chunks },
            };
            if self.task_queue.push(entry) {
                tracing::debug!(
                    "Queued embedding task for email {} (label: {})",
                    simplified.id,
                    prompt_return_data.email_rule.mail_label
                );
            }
        }

        match self
            .record_processed_email(ProcessedEmailRecordData {
                email_message: EmailData::from_message(email_message, simplified)?,
                ai_data: AiEmailData {
                    prompt_return_data,
                    extracted_tasks,
                },
            })
            .await
        {
            Ok(_) => {
                self.fetch_add_total_emails_processed(1);
                self.fetch_add_token_count(token_usage);
                self.add_tally_to_user_daily_quota(token_usage).await?;
                Ok(())
            }
            Err(e) => {
                self.fetch_add_total_emails_failed(1);
                Err(e)
            }
        }
    }

    pub async fn process_email(&self, email_id: String, priority: Priority) {
        if self.is_cancelled() || self.is_quota_reached() || self.is_failed() {
            // Do not process email if processor is failed, cancelled or quota is reached
            return;
        }

        let low_priority_cutoff = self.daily_token_limit / 2;
        if self.current_token_usage() > low_priority_cutoff && priority == Priority::Low {
            // Do not process low priority emails if quota is almost reached
            return;
        }

        match self.run_processor_pipeline(email_id.clone()).await {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Error processing email {}: {:?}", email_id, e);
            }
        }
    }

    async fn add_tally_to_user_daily_quota(&self, tokens: i64) -> anyhow::Result<()> {
        if tokens == 0 {
            return Ok(());
        }

        let updated_tally =
            UserTokenUsageStatsCtrl::add_to_daily_quota(&self.conn, &self.email_address, tokens)
                .await
                .map_err(|e| anyhow!("Error updating daily quota: {e}"))?;

        self.set_token_count(updated_tally);

        Ok(())
    }

    fn fail(&self) {
        let (tx, _) = &self.interrupt_channel;
        tx.send(InterruptSignal::Fail).unwrap();
    }

    pub fn cancel(&self) {
        let (tx, _) = &self.interrupt_channel;
        tx.send(InterruptSignal::Cancel).unwrap();
    }

    fn set_token_count(&self, tokens: i64) {
        self.token_count.store(tokens, Relaxed);
    }

    fn fetch_add_token_count(&self, tokens: i64) -> i64 {
        self.token_count.fetch_add(tokens, Relaxed)
    }

    pub fn is_quota_reached(&self) -> bool {
        let result = self.token_count.load(Relaxed) >= self.daily_token_limit;
        if result {
            let (tx, _) = &self.interrupt_channel;
            tx.send(InterruptSignal::Quota).unwrap();
        }
        result
    }

    pub fn quota_remaining(&self) -> i64 {
        self.daily_token_limit - self.current_token_usage()
    }

    pub fn current_token_usage(&self) -> i64 {
        self.token_count.load(Relaxed)
    }

    pub fn total_emails_processed(&self) -> i64 {
        self.processed_email_count.load(Relaxed)
    }

    pub fn fetch_add_total_emails_processed(&self, count: i64) -> i64 {
        self.processed_email_count.fetch_add(count, Relaxed)
    }

    pub fn total_emails_failed(&self) -> i64 {
        self.failed_email_count.load(Relaxed)
    }

    pub fn fetch_add_total_emails_failed(&self, count: i64) -> i64 {
        self.failed_email_count.fetch_add(count, Relaxed)
    }

    pub fn is_cancelled(&self) -> bool {
        let (_, rx) = &self.interrupt_channel;
        matches!(*rx.borrow(), InterruptSignal::Cancel)
    }

    pub fn is_failed(&self) -> bool {
        let (_, rx) = &self.interrupt_channel;
        matches!(*rx.borrow(), InterruptSignal::Fail)
    }

    pub fn emails_remaining(&self) -> i64 {
        self.task_queue
            .queue_count(&self.email_address)
            .categorization() as i64
    }

    pub fn status(&self) -> ProcessorStatus {
        let queue_count = self.task_queue.queue_count(&self.email_address);
        match true {
            _ if self.is_cancelled() => ProcessorStatus::Cancelled,
            _ if self.is_quota_reached() => ProcessorStatus::QuotaExceeded,
            _ if self.is_failed() => ProcessorStatus::Failed,
            _ if queue_count.high > 0 => ProcessorStatus::ProcessingHP,
            _ if queue_count.low > 0 => ProcessorStatus::ProcessingLP,
            _ => ProcessorStatus::Queueing,
        }
    }

    pub fn has_stopped_queueing(&self) -> bool {
        self.is_cancelled() || self.is_quota_reached() || self.is_failed()
    }

    pub fn get_current_state(&self) -> EmailProcessorStatusUpdate {
        let status = self.status();
        let queue_count = self.task_queue.queue_count(&self.email_address);

        EmailProcessorStatusUpdate {
            status,
            emails_processed: self.total_emails_processed(),
            emails_failed: self.total_emails_failed(),
            emails_remaining: self.emails_remaining(),
            total_emails: queue_count.categorization(),
            hp_emails: queue_count.high,
            lp_emails: queue_count.low,
            tokens_consumed: self.current_token_usage(),
            quota_remaining: self.daily_token_limit - self.current_token_usage(),
        }
    }
}

#[derive(Display, Debug)]
pub enum ProcessorStatus {
    ProcessingHP,
    ProcessingLP,
    Queueing,
    Cancelled,
    QuotaExceeded,
    Failed,
}

enum InterruptSignal {
    Run,
    Cancel,
    Quota,
    Fail,
}

#[derive(Debug, Clone, Default)]
struct FetchOptions {
    more_recent_than: Option<chrono::Duration>,
    categories: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct EmailProcessorStatusUpdate {
    pub status: ProcessorStatus,
    pub emails_processed: i64,
    pub emails_failed: i64,
    pub emails_remaining: i64,
    pub total_emails: usize,
    pub hp_emails: usize,
    pub lp_emails: usize,
    pub tokens_consumed: i64,
    pub quota_remaining: i64,
}

#[derive(Debug)]
pub struct PromptReturnData {
    pub email_rule: EmailRule,
    pub ai_answer: String,
    pub ai_confidence: f32,
    pub heuristics_used: bool,
    pub token_usage: i64,
}

#[derive(Debug)]
pub struct AiEmailData {
    pub prompt_return_data: PromptReturnData,
    pub extracted_tasks: Vec<ExtractedTask>,
}

#[derive(Debug)]
pub struct EmailData {
    pub id: String,
    pub label_ids: Vec<String>,
    pub thread_id: Option<String>,
    pub history_id: u64,
    pub internal_date: i64,
    pub from: Option<String>,
    pub subject: Option<String>,
    pub snippet: Option<String>,
}

impl EmailData {
    pub fn from_message(
        msg: google_gmail1::api::Message,
        simplified: SimplifiedMessage,
    ) -> anyhow::Result<Self> {
        let id = msg.id.ok_or_else(|| anyhow!("Missing email id"))?;

        Ok(Self {
            id,
            label_ids: msg.label_ids.unwrap_or_default(),
            thread_id: msg.thread_id,
            history_id: msg.history_id.unwrap_or_default(),
            internal_date: msg.internal_date.unwrap_or_default(),
            from: simplified.from,
            subject: simplified.subject,
            snippet: msg.snippet,
        })
    }
}

#[derive(Debug)]
pub struct ProcessedEmailRecordData {
    pub email_message: EmailData,
    pub ai_data: AiEmailData,
}
