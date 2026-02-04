//! Message Fetcher
//!
//! Fetches full message content for pending email IDs and adds them to the categorization queue.

use std::sync::Arc;

use anyhow::Context;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

use crate::{
    email::{
        client::{EmailClient, MessageFormat},
        simplified_message::SimplifiedMessage,
    },
    model::user::{UserCtrl, UserWithAccountAccessAndUsage},
    observability::PipelineTracker,
    ServerState,
};

use super::{get_email_client, queues::PipelineQueues, types::PipelineItem};

/// Fetches full message content for pending email IDs
#[derive(Clone)]
pub struct MessageFetcher {
    server_state: ServerState,
    queues: Arc<PipelineQueues>,
    tracker: PipelineTracker,
}

impl MessageFetcher {
    pub fn new(
        server_state: ServerState,
        queues: Arc<PipelineQueues>,
        tracker: PipelineTracker,
    ) -> Self {
        Self {
            server_state,
            queues,
            tracker,
        }
    }

    /// Main fetching loop - runs continuously
    pub async fn run(&self, shutdown: CancellationToken) {
        tracing::info!("Message fetcher started");

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("Message fetcher shutting down");
                    break;
                }
                _ = self.fetch_next_batch() => {}
            }
        }
    }

    /// Fetch messages for users with pending email IDs
    async fn fetch_next_batch(&self) {
        // Get users with pending emails
        let user_ids = self.queues.pending_user_ids();

        if user_ids.is_empty() {
            // No pending emails, wait a bit before checking again
            sleep(Duration::from_millis(100)).await;
            return;
        }

        // Process each user
        for user_id in user_ids {
            if let Err(e) = self.fetch_for_user(user_id).await {
                tracing::warn!("Error fetching messages for user {}: {}", user_id, e);
            }
        }
    }

    /// Fetch messages for a specific user
    async fn fetch_for_user(&self, user_id: i32) -> anyhow::Result<()> {
        // Take all pending email IDs for this user
        let email_ids = self.queues.take_pending_for_user(user_id);

        if email_ids.is_empty() {
            return Ok(());
        }

        let total_to_fetch = email_ids.len();

        // Get user info
        let user = UserCtrl::get_with_account_access_and_usage_by_id(&self.server_state.conn, user_id)
            .await
            .context("Failed to get user")?;

        // Track fetch start
        self.tracker.user_fetch_start(
            user_id,
            user.email.clone(),
            total_to_fetch,
            user.tokens_consumed,
            user.daily_token_limit,
        );

        // Get email client for user (uses cache)
        let email_client = get_email_client(&self.server_state, user.clone())
            .await
            .context("Failed to get email client")?;

        // Create track_fn that updates progress and records fetched count
        let tracker = self.tracker.clone();
        let prev_count = std::sync::atomic::AtomicUsize::new(0);
        let track_fn = move |current: usize| {
            let prev = prev_count.swap(current, std::sync::atomic::Ordering::Relaxed);
            let delta = current.saturating_sub(prev);
            tracker.user_fetch_progress(user_id, current);
            tracker.increment_fetched(delta as u64);
        };

        // Fetch messages in batches (get_messages_by_ids handles rate limiting)
        let messages = email_client
            .get_messages_by_ids(&email_ids, MessageFormat::Raw, Some(&track_fn))
            .await
            .context("Failed to fetch messages")?;

        tracing::debug!(
            "Fetched {} messages for user {} (user_id: {})",
            messages.len(),
            user.email,
            user_id
        );

        // Process each message
        for msg in &messages {
            if let Err(e) = self.process_message(msg, &user, &email_client).await {
                tracing::warn!(
                    "Error processing message {:?}: {}",
                    msg.id.as_deref().unwrap_or("unknown"),
                    e
                );
                // Remove from pipeline since we failed to process
                if let Some(id) = &msg.id {
                    self.queues.remove_from_pipeline(id);
                }
            }
        }

        // Track fetch complete
        self.tracker.user_fetch_complete(user_id, messages.len(), total_to_fetch);

        Ok(())
    }

    /// Process a single message into a PipelineItem
    async fn process_message(
        &self,
        msg: &google_gmail1::api::Message,
        user: &UserWithAccountAccessAndUsage,
        email_client: &EmailClient,
    ) -> anyhow::Result<()> {
        let email_id = msg
            .id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Message has no ID"))?;

        // Parse to SimplifiedMessage
        let simplified = SimplifiedMessage::from_gmail_message(msg)
            .context("Failed to simplify message")?;

        // Check if email is read (UNREAD label not present)
        let label_ids = msg.label_ids.clone().unwrap_or_default();
        let is_read = !label_ids.contains(&"UNREAD".to_string());

        // Check thread status for unread emails
        let has_new_reply = if !is_read {
            self.check_thread_status(email_client, &simplified, &user.email)
                .await
                .unwrap_or(false)
        } else {
            false
        };

        // Estimate content tokens (email content only, system prompts added per-stage)
        let estimated_content_tokens = estimate_content_tokens(&simplified);

        // Create PipelineItem
        let item = PipelineItem {
            user_id: user.id,
            user_email: user.email.clone(),
            email_id: email_id.clone(),
            thread_id: simplified.thread_id.clone(),
            is_read,
            has_new_reply,
            label_ids,
            history_id: msg.history_id.unwrap_or_default(),
            internal_date: msg.internal_date.unwrap_or_default(),
            simplified_message: simplified,
            first_pass_result: None,
            second_pass_result: None,
            extracted_tasks: Vec::new(),
            estimated_content_tokens,
            actual_tokens: 0,
        };

        // Add to main categorization queue
        self.queues.push_to_main_categorization_queue(item);

        Ok(())
    }

    /// Check thread status for an email (has new reply from someone other than user)
    async fn check_thread_status(
        &self,
        email_client: &EmailClient,
        message: &SimplifiedMessage,
        user_email: &str,
    ) -> anyhow::Result<bool> {
        // Get thread to check for replies
        let thread = email_client
            .get_thread_by_id(&message.thread_id)
            .await
            .context("Failed to get thread")?;

        // Check if thread has multiple messages
        let messages = thread.messages.unwrap_or_default();
        if messages.len() <= 1 {
            return Ok(false);
        }

        // Check for unread messages from someone other than the user
        for msg in &messages {
            // Skip the current message
            if msg.id.as_deref() == Some(&message.id) {
                continue;
            }

            // Check if message is unread
            let labels = msg.label_ids.as_ref();
            let is_unread = labels.map(|l| l.contains(&"UNREAD".to_string())).unwrap_or(false);

            if !is_unread {
                continue;
            }

            // Check if from someone other than the user
            if let Some(payload) = &msg.payload {
                if let Some(headers) = &payload.headers {
                    for header in headers {
                        if header.name.as_deref() == Some("From") {
                            if let Some(from) = &header.value {
                                if !from.contains(user_email) {
                                    return Ok(true);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(false)
    }
}

/// Estimate content token count for an email (without system prompt)
fn estimate_content_tokens(simplified: &SimplifiedMessage) -> i64 {
    let email_text = simplified.to_string();
    tokenizer::token_count(&email_text).unwrap_or(0) as i64
}
