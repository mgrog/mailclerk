use std::{
    collections::HashSet,
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};

use anyhow::{anyhow, Context};
use chrono::Utc;
use derive_more::Display;
use entity::{email_training, prelude::*, processed_email};
use indexmap::IndexSet;
use num_traits::FromPrimitive;
use sea_orm::{
    entity::*, query::*, sea_query::OnConflict, ActiveValue, DatabaseConnection, EntityTrait,
    FromQueryResult,
};
use std::sync::atomic::Ordering::Relaxed;
use tokio::sync::watch;

use crate::{
    email::{
        client::{EmailClient, MessageListOptions},
        simplified_message::SimplifiedMessage,
        rules::UserEmailRules,
    },
    error::{extract_database_error_code, AppError, AppResult, DatabaseErrorCode},
    model::{
        labels::UtilityLabels, processed_email::ProcessedEmailCtrl, response::LabelUpdate,
        user::UserWithAccountAccessAndUsage, user_token_usage::UserTokenUsageStatsCtrl,
    },
    prompt::{
        mistral::{self, CategoryPromptResponse},
        priority_queue::{Priority, PromptPriorityQueue},
    },
    rate_limiters::RateLimiters,
    server_config::cfg,
    HttpClient, ServerState,
};

use super::rules::{EmailRule, HEURISTIC_EMAIL_RULES};

lazy_static::lazy_static!(
    static ref DAILY_QUOTA: i64 = cfg.api.token_limits.daily_user_quota as i64;
    static ref LOW_PRIORITY_CUTOFF: i64 = *DAILY_QUOTA / 2;
    static ref UNKNOWN_RULE: EmailRule = EmailRule {
            prompt_content: "Unknown".to_string(),
            mail_label: UtilityLabels::Uncategorized.as_str().to_string(),
            associated_email_client_category: None,
        };
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

        let clone = processor.clone();
        tokio::spawn(async move {
            clone.run().await.unwrap_or_else(|e| {
                tracing::error!(
                    "Error running email processor for {}: {:?}",
                    clone.email_address,
                    e
                );
            });
        });

        Ok(processor)
    }

    async fn run(&self) -> AppResult<()> {
        tracing::info!("Starting email processor for {}", self.email_address);

        match self.configure_user_labels().await {
            Ok(true) => {
                tracing::info!("Labels configured successfully for {}", self.email_address);
            }
            Ok(false) => {
                tracing::info!("Labels already configured for {}", self.email_address);
            }
            Err(e) => {
                tracing::error!(
                    "Error configuring labels for {}: {:?}",
                    self.email_address,
                    e
                );
                self.fail();
                return Err(e.into());
            }
        };
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        let mut rx = self.interrupt_channel.1.clone();
        loop {
            tokio::select! {
              _ = interval.tick() => {},
              _ = rx.wait_for(|v| matches!(v, InterruptSignal::Cancel | InterruptSignal::Fail | InterruptSignal::Quota)) => {
                tracing::info!("Interrupt signal received for {}", self.email_address);
                break;
              }
            }

            if self
                .priority_queue
                .num_high_priority_in_queue(&self.email_address)
                == 0
            {
                match self.queue_recent_emails().await {
                    Ok(n) if n > 0 => {}
                    // If there are no recent emails and sufficient quota remaining, queue older emails in low priority
                    Ok(_) if self.current_token_usage() < *LOW_PRIORITY_CUTOFF => {
                        match self.queue_older_emails().await {
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!(
                                    "Error queuing older emails for {}: {:?}",
                                    self.email_address,
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "Error queuing recent emails for {}: {:?}",
                            self.email_address,
                            e
                        );
                        self.fail();
                        return Err(e);
                    }
                    _ => {}
                }
            }
        }

        tracing::info!(
            "Email processor for {} finished with status: {}",
            self.email_address,
            self.status()
        );

        Ok(())
    }

    async fn fetch_email_ids(
        &self,
        options: Option<FetchOptions>,
    ) -> anyhow::Result<IndexSet<u128>> {
        #[derive(FromQueryResult)]
        struct ProcessedEmailId {
            id: String,
        }

        let already_processed_ids = processed_email::Entity::find()
            .filter(processed_email::Column::UserId.eq(self.user_id))
            .select_only()
            .column(processed_email::Column::Id)
            .into_model::<ProcessedEmailId>()
            .all(&self.conn)
            .await?
            .into_iter()
            .map(|e| parse_id_to_int(e.id))
            .collect::<HashSet<_>>();

        let mut message_ids_to_process = IndexSet::new();
        let load_page = |next_page_token: Option<String>| async {
            let resp = match self
                .email_client
                .get_message_list(MessageListOptions {
                    page_token: next_page_token,
                    more_recent_than: options.as_ref().and_then(|o| o.more_recent_than),
                    categories: options.as_ref().and_then(|o| o.categories.clone()),
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
            .find(|c| c.prompt_content == ai_answer)
            .unwrap_or(&UNKNOWN_RULE);

        if let Some(from) = email_message.from.as_ref() {
            if selected_email_rule.prompt_content != "Terms of Service Update"
                && selected_email_rule.prompt_content != "Verification Code"
                && selected_email_rule.prompt_content != "Security Alert"
                && cfg.heuristics.iter().any(|h| from.contains(&h.from))
            {
                selected_email_rule = HEURISTIC_EMAIL_RULES
                    .iter()
                    .find(|c| from.contains(&c.prompt_content))
                    .unwrap();
            }
        }

        let heuristics_used = selected_email_rule.prompt_content != ai_answer;

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

    async fn categorize_email_in_client(
        &self,
        email_message: &SimplifiedMessage,
        email_rule: EmailRule,
    ) -> anyhow::Result<LabelUpdate> {
        let label_update = match self
            .email_client
            .label_email(
                email_message.id.clone(),
                email_message.label_ids.clone(),
                email_rule.clone(),
            )
            .await
        {
            Ok(label_update) => label_update,
            Err(e) => {
                tracing::error!("Error labeling email {}: {:?}", email_message.id, e);
                // If labeling fails, try to fix labels
                match self.configure_user_labels().await {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!("Could not fix labels for {}: {:?}", self.email_address, e);
                        self.fail();
                    }
                }
                // We allow email to be queued again later if labeling fails
                // As we will not record as successfully processed
                return Err(e);
            }
        };

        Ok(label_update)
    }

    async fn record_processed_email(
        &self,
        email_message: &SimplifiedMessage,
        data: EmailProcessingData,
    ) -> anyhow::Result<()> {
        match ProcessedEmailCtrl::insert(
            &self.conn,
            processed_email::ActiveModel {
                id: ActiveValue::Set(email_message.id.clone()),
                user_id: ActiveValue::Set(self.user_id),
                labels_applied: ActiveValue::Set(data.label_update.added),
                labels_removed: ActiveValue::Set(data.label_update.removed),
                category: ActiveValue::Set(data.prompt_return_data.email_rule.mail_label.clone()),
                ai_answer: ActiveValue::Set(data.prompt_return_data.ai_answer.clone()),
                processed_at: ActiveValue::NotSet,
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

        self.rate_limiters.acquire_one().await;

        let result = self.parse_and_prompt_email(&email_message).await?;
        if cfg.settings.training_mode {
            match self
                .record_email_for_training(&email_message, &result)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    // This is a non-critical error, so we log it and continue
                    tracing::error!("Error recording email for training: {:?}", e);
                }
            };
        };
        let label_update = self
            .categorize_email_in_client(&email_message, result.email_rule.clone())
            .await?;

        let token_usage = result.token_usage;

        match self
            .record_processed_email(
                &email_message,
                EmailProcessingData {
                    prompt_return_data: result,
                    label_update,
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

    async fn configure_user_labels(&self) -> anyhow::Result<bool> {
        let user_custom_labels = self.user_email_rules.get_custom_labels();
        self.email_client
            .configure_labels_if_needed(user_custom_labels)
            .await
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
    pub label_update: LabelUpdate,
}
