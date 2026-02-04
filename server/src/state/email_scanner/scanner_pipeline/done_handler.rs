//! Done Handler
//!
//! Handles completed items from the done_queue:
//! - Updates user token quotas using actual API token usage
//! - Inserts processed emails into the database
//! - Marks items as complete in the pipeline

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use futures::stream::{self, StreamExt};
use sea_orm::DatabaseConnection;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

use crate::{
    model::user_token_usage::UserTokenUsageStatsCtrl,
    observability::PipelineTracker,
    state::email_scanner::{
        batch_processor::batch_insert_processed_emails,
        shared::{EmailScanData, ProcessedEmailData},
    },
};

use super::{queues::PipelineQueues, types::PipelineItem};

/// Maximum concurrent user token quota updates
const MAX_CONCURRENT_QUOTA_UPDATES: usize = 1000;

/// Handles completed items from the done_queue
#[derive(Clone)]
pub struct DoneHandler {
    conn: DatabaseConnection,
    queues: Arc<PipelineQueues>,
    tracker: PipelineTracker,
}

impl DoneHandler {
    pub fn new(
        conn: DatabaseConnection,
        queues: Arc<PipelineQueues>,
        tracker: PipelineTracker,
    ) -> Self {
        Self {
            conn,
            queues,
            tracker,
        }
    }

    /// Main processing loop - continuously processes completed items
    pub async fn run(&self, shutdown: CancellationToken) {
        tracing::info!("Done handler started");

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("Done handler shutting down");
                    // Process any remaining items before shutdown
                    self.process_done_items().await;
                    break;
                }
                _ = self.process_done_items() => {
                    // Small sleep to avoid busy-waiting when queue is empty
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// Process all items currently in the done queue
    async fn process_done_items(&self) {
        let items = self.queues.drain_done_queue();
        if items.is_empty() {
            return;
        }

        tracing::debug!("Processing {} completed items", items.len());

        // 1. Update user token quotas using actual API token usage (concurrently)
        if let Err(e) = self.update_user_token_quotas(&items).await {
            tracing::error!("Failed to update user token quotas: {}", e);
            // Continue with DB insert even if quota update fails
        }

        // 2. Convert to ProcessedEmailData and insert into database
        if let Err(e) = self.insert_processed_emails(&items).await {
            tracing::error!("Failed to insert processed emails: {}", e);
            // Continue with cleanup even if DB insert fails
        }

        // 3. Mark items as complete and record metrics
        let item_count = items.len();
        for item in &items {
            self.queues.mark_complete(&item.email_id);
        }
        self.tracker.increment_inserted(item_count as u64);

        // 4. Record per-user 24hr email stats
        let mut user_email_counts: HashMap<i32, u64> = HashMap::new();
        for item in &items {
            *user_email_counts.entry(item.user_id).or_default() += 1;
        }
        for (user_id, count) in user_email_counts {
            self.tracker.record_user_processed(user_id, count);
        }

        tracing::info!(
            "Completed processing {} items (inserted to DB, updated quotas)",
            item_count
        );
    }

    /// Update user token quotas using actual API token usage (concurrently, limited to 1000 at a time)
    async fn update_user_token_quotas(&self, items: &[PipelineItem]) -> anyhow::Result<()> {
        // Group items by user_email and sum their actual_tokens
        let mut user_tokens: HashMap<String, i64> = HashMap::new();
        for item in items {
            *user_tokens.entry(item.user_email.clone()).or_default() += item.actual_tokens;
        }

        // Filter out zero-token entries
        let user_tokens: Vec<(String, i64)> = user_tokens
            .into_iter()
            .filter(|(_, tokens)| *tokens > 0)
            .collect();

        if user_tokens.is_empty() {
            return Ok(());
        }

        // Log token usage summary
        let total_tokens: i64 = user_tokens.iter().map(|(_, t)| t).sum();
        tracing::debug!(
            "Updating token quotas for {} users, total: {} tokens",
            user_tokens.len(),
            total_tokens
        );

        // Update all users' quotas concurrently, limited to MAX_CONCURRENT_QUOTA_UPDATES at a time
        let conn = &self.conn;
        stream::iter(user_tokens)
            .for_each_concurrent(MAX_CONCURRENT_QUOTA_UPDATES, |(user_email, tokens)| async move {
                match UserTokenUsageStatsCtrl::add_to_daily_quota(conn, &user_email, tokens).await {
                    Ok(new_total) => {
                        tracing::debug!(
                            "Updated quota for {}: +{} tokens (new total: {})",
                            user_email,
                            tokens,
                            new_total
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to update quota for {}: {} (+{} tokens)",
                            user_email,
                            e,
                            tokens
                        );
                    }
                }
            })
            .await;

        Ok(())
    }

    /// Convert PipelineItems to ProcessedEmailData and insert into database
    async fn insert_processed_emails(&self, items: &[PipelineItem]) -> anyhow::Result<()> {
        let processed_emails: Vec<ProcessedEmailData> = items
            .iter()
            .map(|item| {
                // Get final categorization result
                let (category, ai_answer, ai_confidence) = item
                    .final_result()
                    .map(|r| (r.category.clone(), r.ai_answer.clone(), r.ai_confidence))
                    .unwrap_or_else(|| {
                        (
                            "uncategorized".to_string(),
                            "Unknown".to_string(),
                            0.0,
                        )
                    });

                ProcessedEmailData {
                    user_id: item.user_id,
                    email_data: EmailScanData {
                        id: item.email_id.clone(),
                        thread_id: Some(item.thread_id.clone()),
                        label_ids: item.label_ids.clone(),
                        history_id: item.history_id,
                        internal_date: item.internal_date,
                        from: item.simplified_message.from.clone(),
                        subject: item.simplified_message.subject.clone(),
                        snippet: item.simplified_message.snippet.clone(),
                        body: item.simplified_message.body.clone(),
                    },
                    category,
                    ai_answer,
                    ai_confidence,
                    extracted_tasks: item.extracted_tasks.clone(),
                }
            })
            .collect();

        let count = processed_emails.len();
        let inserted = batch_insert_processed_emails(&self.conn, processed_emails)
            .await
            .context("Failed to batch insert processed emails")?;

        tracing::debug!("Inserted {} / {} processed emails", inserted, count);

        Ok(())
    }
}
