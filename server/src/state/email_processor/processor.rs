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
        priority_queue::{Priority, PromptPriorityQueue},
        task_extraction,
    },
    rate_limiters::RateLimiters,
    server_config::cfg,
    HttpClient, ServerState,
};

use crate::email::rules::{EmailRule, HEURISTIC_EMAIL_RULES};

lazy_static::lazy_static!(
    static ref DAILY_QUOTA: i64 = cfg.api.token_limits.daily_user_quota as i64;
    static ref LOW_PRIORITY_CUTOFF: i64 = *DAILY_QUOTA / 2;
    static ref UNKNOWN_RULE: EmailRule = EmailRule {
            prompt_content: "Unknown".to_string(),
            mail_label: UtilityLabels::Uncategorized.as_str().to_string(),
            extract_tasks: true,
        };
    static ref EXCEPTION_RULES: &'static [&'static str] = &["Terms of Service Update", "Verification Code", "Security Alert"];
);

#[derive(Clone)]
// EmailProcessor processes emails for a single user
pub struct EmailProcessor {
    pub user_id: i32,
    pub user_account_access_id: i32,
    pub email_address: String,
    pub created_at: chrono::DateTime<Utc>,
    processed_email_count: Arc<AtomicI64>,
    failed_email_count: Arc<AtomicI64>,
    email_client: Arc<EmailClient>,
    token_count: Arc<AtomicI64>,
    http_client: HttpClient,
    conn: DatabaseConnection,
    rate_limiters: RateLimiters,
    priority_queue: PromptPriorityQueue,
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
        let conn = server_state.conn.clone();
        let http_client = server_state.http_client.clone();
        let rate_limiters = server_state.rate_limiters.clone();
        let priority_queue = server_state.priority_queue.clone();

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
            processed_email_count: Arc::new(AtomicI64::new(0)),
            failed_email_count: Arc::new(AtomicI64::new(0)),
            email_client: Arc::new(email_client),
            token_count: Arc::new(AtomicI64::new(quota_used)),
            http_client,
            conn,
            rate_limiters,
            priority_queue,
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
        if self
            .priority_queue
            .num_high_priority_in_queue(&self.email_address)
            > 0
        {
            return Ok(0);
        }

        // Try to queue recent emails first
        match self.queue_recent_emails().await {
            Ok(n) if n > 0 => return Ok(n),
            // Ok(_) if self.current_token_usage() < *LOW_PRIORITY_CUTOFF => {
            //     // No recent emails and under quota, try older emails
            //     return self.queue_older_emails().await;
            // }
            Err(e) => {
                tracing::error!("Error queuing emails for {}: {:?}", self.email_address, e);
                self.fail();
                return Err(e);
            }
            _ => {}
        }

        Ok(0)
    }

    async fn fetch_email_ids(
        &self,
        options: Option<FetchOptions>,
    ) -> anyhow::Result<IndexSet<u128>> {
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
            .map(|e| parse_id_to_int(e.id))
            .collect::<HashSet<u128>>();

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
                .filter_map(|m| m.id.map(parse_id_to_int))
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
            if self
                .priority_queue
                .push(self.email_address.clone(), *email_id, Priority::High)
            {
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
            if self
                .priority_queue
                .push(self.email_address.clone(), *email_id, Priority::Low)
            {
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
        email_message: &SimplifiedMessage,
        data: EmailProcessingData,
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
        let (has_new_reply, user_has_replied, is_thread) = self
            .check_thread_status(&email_message.thread_id, &email_message.id)
            .await
            .unwrap_or((false, false, false));

        match ProcessedEmailCtrl::insert(
            &self.conn,
            processed_email::ActiveModel {
                id: ActiveValue::Set(email_message.id.clone()),
                thread_id: ActiveValue::Set(email_message.thread_id.clone()),
                user_id: ActiveValue::Set(self.user_id),
                category: ActiveValue::Set(data.prompt_return_data.email_rule.mail_label.clone()),
                ai_answer: ActiveValue::Set(data.prompt_return_data.ai_answer.clone()),
                ai_confidence: ActiveValue::Set(data.prompt_return_data.ai_confidence.to_string()),
                processed_at: ActiveValue::NotSet,
                due_date: ActiveValue::Set(closest_due_data),
                extracted_tasks,
                history_id: ActiveValue::Set(Decimal::from(email_message.history_id)),
                is_read: ActiveValue::Set(!email_message.label_ids.contains(&"UNREAD".to_string())),
                tasks_done: ActiveValue::Set(false),
                has_new_reply: ActiveValue::Set(has_new_reply && user_has_replied),
                is_thread: ActiveValue::Set(is_thread),
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

    async fn run_processor_pipeline(&self, id: u128) -> anyhow::Result<()> {
        let email_id = parse_int_to_id(id);

        let email_message = self
            .email_client
            .get_simplified_message(&email_id)
            .await
            .context("Failed to fetch email")?;

        // Estimate token usage and check against remaining quota (allow up to 1% over)
        let email_text = email_message.to_string();
        let estimated_tokens = tokenizer::token_count(&email_text).unwrap_or(0) as i64;
        let quota_buffer = *DAILY_QUOTA / 100; // 1% buffer
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

        let result = self.parse_and_prompt_email(&email_message).await?;

        if cfg.settings.training_mode {
            match self
                .record_email_for_training(&email_message, &result)
                .await
            {
                Ok(_) => {
                    tracing::info!("Recorded message {} for training", email_message.id);
                }
                Err(e) => {
                    // This is a non-critical error, so we log it and continue
                    tracing::error!("Error recording email for training: {:?}", e);
                }
            };
        };

        let mut token_usage = result.token_usage;
        let mut extracted_tasks = Vec::new();

        // Extract tasks if the matched rule has extract_tasks enabled
        if result.email_rule.extract_tasks {
            self.rate_limiters.acquire_one().await;
            match task_extraction::extract_tasks_from_email(
                &self.http_client,
                &self.rate_limiters,
                &email_message,
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

        match self
            .record_processed_email(
                &email_message,
                EmailProcessingData {
                    prompt_return_data: result,
                    extracted_tasks,
                },
            )
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

    pub async fn process_email(&self, id: u128, priority: Priority) {
        if self.is_cancelled() || self.is_quota_reached() || self.is_failed() {
            // Do not process email if processor is failed, cancelled or quota is reached
            return;
        }

        if self.current_token_usage() > *LOW_PRIORITY_CUTOFF && priority == Priority::Low {
            // Do not process low priority emails if quota is almost reached
            return;
        }

        match self.run_processor_pipeline(id).await {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Error processing email {}: {:?}", id, e);
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
        let result = self.token_count.load(Relaxed) >= *DAILY_QUOTA;
        if result {
            let (tx, _) = &self.interrupt_channel;
            tx.send(InterruptSignal::Quota).unwrap();
        }
        result
    }

    pub fn quota_remaining(&self) -> i64 {
        *DAILY_QUOTA - self.current_token_usage()
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
        self.priority_queue.num_in_queue(&self.email_address) as i64
    }

    pub fn status(&self) -> ProcessorStatus {
        match true {
            _ if self.is_cancelled() => ProcessorStatus::Cancelled,
            _ if self.is_quota_reached() => ProcessorStatus::QuotaExceeded,
            _ if self.is_failed() => ProcessorStatus::Failed,
            _ if self
                .priority_queue
                .num_high_priority_in_queue(&self.email_address)
                > 0 =>
            {
                ProcessorStatus::ProcessingHP
            }
            _ if self
                .priority_queue
                .num_low_priority_in_queue(&self.email_address)
                > 0 =>
            {
                ProcessorStatus::ProcessingLP
            }
            _ => ProcessorStatus::Queueing,
        }
    }

    pub fn has_stopped_queueing(&self) -> bool {
        self.is_cancelled() || self.is_quota_reached() || self.is_failed()
    }

    pub fn get_current_state(&self) -> EmailProcessorStatusUpdate {
        let status = self.status();

        let num_processing_hp = self
            .priority_queue
            .num_high_priority_in_queue(&self.email_address);

        let num_processing_lp = self
            .priority_queue
            .num_low_priority_in_queue(&self.email_address);

        EmailProcessorStatusUpdate {
            status,
            emails_processed: self.total_emails_processed(),
            emails_failed: self.total_emails_failed(),
            emails_remaining: self.emails_remaining(),
            total_emails: num_processing_lp + num_processing_hp,
            hp_emails: num_processing_hp,
            lp_emails: num_processing_lp,
            tokens_consumed: self.current_token_usage(),
            quota_remaining: *DAILY_QUOTA - self.current_token_usage(),
        }
    }
}

// Helper functions
pub fn parse_id_to_int(id: impl Into<String>) -> u128 {
    let id = id.into();
    u128::from_str_radix(&id, 16).expect("Could not parse email id to integer")
}

pub fn parse_int_to_id(id: u128) -> String {
    format!("{:x}", id)
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
pub struct EmailProcessingData {
    pub prompt_return_data: PromptReturnData,
    pub extracted_tasks: Vec<ExtractedTask>,
}
