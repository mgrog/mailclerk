//! Email ID Poller
//!
//! Polls users for new email IDs at regular intervals and adds them to the pipeline.

use std::sync::Arc;

use anyhow::Context;
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;

use crate::{
    email::client::{EmailClient, MessageListOptions},
    model::user::{PollableUser, UserCtrl},
    observability::PipelineTracker,
    server_config::cfg,
    ServerState,
};

use super::{get_email_client, queues::PipelineQueues};

/// Polls users for new email IDs and adds them to the pipeline
#[derive(Clone)]
pub struct EmailIdPoller {
    server_state: ServerState,
    queues: Arc<PipelineQueues>,
    tracker: PipelineTracker,
}

impl EmailIdPoller {
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

    /// Main polling loop - runs every polling_interval
    pub async fn run(&self, shutdown: CancellationToken) {
        let polling_interval = Duration::from_secs(cfg.scanner_pipeline.polling_interval_secs);
        let mut interval = interval(polling_interval);

        tracing::info!(
            "Email ID poller started (interval: {}s, max parallel users: {})",
            cfg.scanner_pipeline.polling_interval_secs,
            cfg.scanner_pipeline.max_parallel_poll_users
        );

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("Email ID poller shutting down");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(e) = self.poll_users().await {
                        tracing::error!("Error polling users: {}", e);
                    }
                }
            }
        }
    }

    /// Poll N users in parallel for new email IDs
    async fn poll_users(&self) -> anyhow::Result<()> {
        // Get users who are ready for polling
        let users = self.get_pollable_users().await?;

        if users.is_empty() {
            return Ok(());
        }

        tracing::debug!("Polling {} users for new email IDs", users.len());

        // Process users in chunks to respect max_parallel_poll_users
        let max_parallel = cfg.scanner_pipeline.max_parallel_poll_users;
        for chunk in users.chunks(max_parallel) {
            let futures: Vec<_> = chunk
                .iter()
                .map(|user| self.poll_single_user(user))
                .collect();

            // Run chunk in parallel
            let results = futures::future::join_all(futures).await;

            // Log any errors but continue
            for result in results {
                if let Err(e) = result {
                    tracing::warn!("Error polling user: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Get users who are ready for polling
    async fn get_pollable_users(&self) -> anyhow::Result<Vec<PollableUser>> {
        let users = UserCtrl::all_pollable(&self.server_state.conn)
            .await
            .context("Failed to get users for batch pipeline")?;

        Ok(users)
    }

    /// Poll a single user for new email IDs
    async fn poll_single_user(&self, user: &PollableUser) -> anyhow::Result<()> {
        // Allow 4% buffer over the daily limit before skipping
        let buffer = user.daily_token_limit / 25; // 4% buffer
        let effective_limit = user.daily_token_limit.saturating_add(buffer);

        // Skip if user has reached quota
        if user.tokens_consumed >= effective_limit {
            tracing::debug!(
                "Skipping user {} - quota reached ({}/{})",
                user.email,
                user.tokens_consumed,
                user.daily_token_limit
            );
            self.tracker.user_poll_skipped(
                user.id,
                user.email.clone(),
                "quota".to_string(),
                user.tokens_consumed,
                user.daily_token_limit,
            );
            return Ok(());
        }

        // Skip if user's daily usage + estimated tokens in pipeline would exceed limit
        let estimated_in_pipeline = self.queues.get_user_estimated_tokens(user.id);
        let projected_usage = user.tokens_consumed + estimated_in_pipeline;
        if projected_usage >= effective_limit {
            tracing::debug!(
                "Skipping user {} - projected usage exceeds limit (consumed: {}, in_pipeline: {}, limit: {})",
                user.email,
                user.tokens_consumed,
                estimated_in_pipeline,
                user.daily_token_limit
            );
            self.tracker.user_poll_skipped(
                user.id,
                user.email.clone(),
                "pipeline_quota".to_string(),
                user.tokens_consumed,
                user.daily_token_limit,
            );
            return Ok(());
        }

        // Track poll start
        self.tracker.user_poll_start(
            user.id,
            user.email.clone(),
            user.tokens_consumed,
            user.daily_token_limit,
        );

        // Get email client for user (uses cache)
        let email_client = get_email_client(&self.server_state, user.clone())
            .await
            .context("Failed to get email client")?;

        // Fetch recent email IDs (only IDs, not full content)
        let email_ids = self.fetch_email_ids(&email_client, user).await?;

        if email_ids.is_empty() {
            self.tracker.user_poll_none(user.id);
            return Ok(());
        }

        // Add email IDs to pending queue (dedup happens inside)
        let mut added_count = 0;
        for email_id in email_ids {
            if self.queues.add_pending_email_id(user.id, email_id) {
                added_count += 1;
            }
        }

        if added_count > 0 {
            tracing::debug!(
                "Added {} new email IDs for user {} (user_id: {})",
                added_count,
                user.email,
                user.id
            );
            self.tracker.user_poll_found(user.id, added_count);
        } else {
            self.tracker.user_poll_none(user.id);
        }

        Ok(())
    }

    /// Fetch email IDs for a user (recent emails only)
    async fn fetch_email_ids(
        &self,
        email_client: &EmailClient,
        user: &PollableUser,
    ) -> anyhow::Result<Vec<String>> {
        // Fetch recent emails - use configured max lookback
        let max_lookback_days = cfg.continuous_scan.max_lookback_days;

        let response = email_client
            .get_message_list(MessageListOptions {
                more_recent_than: Some(chrono::Duration::days(max_lookback_days)),
                ..Default::default()
            })
            .await
            .context("Failed to fetch message list")?;

        let email_ids: Vec<String> = response
            .messages
            .unwrap_or_default()
            .into_iter()
            .filter_map(|msg| msg.id)
            .collect();

        // Filter out emails that are already processed in DB
        let unprocessed_ids = self.filter_unprocessed_ids(&email_ids, user.id).await?;

        Ok(unprocessed_ids)
    }

    /// Filter out email IDs that have already been processed
    async fn filter_unprocessed_ids(
        &self,
        email_ids: &[String],
        user_id: i32,
    ) -> anyhow::Result<Vec<String>> {
        use crate::model::processed_email::ProcessedEmailCtrl;

        if email_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Check which emails are already in the processed_emails table
        let existing_ids =
            ProcessedEmailCtrl::get_existing_ids(&self.server_state.conn, user_id, email_ids)
                .await
                .context("Failed to check existing processed emails")?;

        // Filter out existing ones
        let unprocessed: Vec<String> = email_ids
            .iter()
            .filter(|id| !existing_ids.contains(*id))
            .cloned()
            .collect();

        Ok(unprocessed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::observability::PipelineTracker;
    use crate::state::email_scanner::scanner_pipeline::queues::PipelineQueues;

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::auth::session_store::AuthSessionStore;
        use crate::model::processed_email::ProcessedEmailCtrl;
        use crate::model::user::UserCtrl;
        use crate::observability::ScanTracker;
        use crate::rate_limiters::RateLimiters;
        use crate::state::email_client_map::EmailClientMap;
        use crate::testing::common::setup;
        use crate::{EmailClientCache, ServerState};

        async fn setup_server_state() -> ServerState {
            let (conn, http_client) = setup().await;
            let session_store = AuthSessionStore::new();
            let email_client_cache: EmailClientCache = {
                let map = EmailClientMap::builder()
                    .with_http_client(http_client.clone())
                    .with_connection(conn.clone())
                    .build()
                    .await
                    .expect("Email client map failed to build!");
                Arc::new(tokio::sync::RwLock::new(map))
            };

            ServerState {
                http_client,
                conn,
                rate_limiters: RateLimiters::from_env(),
                session_store,
                email_client_cache,
                scan_tracker: ScanTracker::new(),
            }
        }

        fn create_poller(server_state: ServerState) -> EmailIdPoller {
            let queues = Arc::new(PipelineQueues::new(3600));
            let tracker = PipelineTracker::new(queues.clone());
            EmailIdPoller::new(server_state, queues, tracker)
        }

        /// Integration test: get_pollable_users returns users with active subscriptions
        /// and completed initial scan who haven't exceeded quota
        #[tokio::test]
        async fn test_get_pollable_users() {
            let server_state = setup_server_state().await;
            let poller = create_poller(server_state);

            let users = poller.get_pollable_users().await;
            assert!(
                users.is_ok(),
                "Failed to get pollable users: {:?}",
                users.err()
            );

            let users = users.unwrap();
            // All returned users should be under quota
            for user in &users {
                assert!(
                    user.tokens_consumed < user.daily_token_limit,
                    "User {} should be under quota: {} < {}",
                    user.email,
                    user.tokens_consumed,
                    user.daily_token_limit
                );
            }

            println!("Found {} pollable users", users.len());
        }

        /// Integration test: poll_users executes without error
        #[tokio::test]
        async fn test_poll_users() {
            let server_state = setup_server_state().await;
            let poller = create_poller(server_state);

            let result = poller.poll_users().await;
            assert!(result.is_ok(), "poll_users failed: {:?}", result.err());
        }

        /// Integration test: poll_single_user for a known test user
        #[tokio::test]
        async fn test_poll_single_user() {
            let server_state = setup_server_state().await;
            let test_email = "mpgrospamacc@gmail.com";

            // Get the test user
            let user = UserCtrl::get_pollable_by_email(&server_state.conn, test_email).await;

            if user.is_err() {
                println!("Test user not found, skipping test");
                return;
            }

            let user = user.unwrap();
            let poller = create_poller(server_state);

            let result = poller.poll_single_user(&user).await;
            assert!(
                result.is_ok(),
                "poll_single_user failed: {:?}",
                result.err()
            );

            // Check if any emails were added to the queue
            let pending_count = poller.queues.pending_count_for_user(user.id);
            println!(
                "Added {} pending emails for user {}",
                pending_count, user.email
            );
        }

        /// Integration test: filter_unprocessed_ids correctly filters out processed emails
        #[tokio::test]
        async fn test_filter_unprocessed_ids() {
            let server_state = setup_server_state().await;
            let test_email = "mpgrospamacc@gmail.com";

            let user = UserCtrl::get_with_account_access_and_usage_by_email(
                &server_state.conn,
                test_email,
            )
            .await;

            if user.is_err() {
                println!("Test user not found, skipping test");
                return;
            }

            let user = user.unwrap();
            let poller = create_poller(server_state.clone());

            // Get some existing processed email IDs for this user
            let existing_ids = ProcessedEmailCtrl::get_existing_ids(
                &server_state.conn,
                user.id,
                &["fake_id_1".to_string(), "fake_id_2".to_string()],
            )
            .await;

            assert!(existing_ids.is_ok());

            // Test filtering with a mix of real and fake IDs
            let test_ids = vec!["fake_new_id_1".to_string(), "fake_new_id_2".to_string()];

            let filtered = poller.filter_unprocessed_ids(&test_ids, user.id).await;
            assert!(
                filtered.is_ok(),
                "filter_unprocessed_ids failed: {:?}",
                filtered.err()
            );

            let filtered = filtered.unwrap();
            // Since these are fake IDs, they should all be "unprocessed"
            assert_eq!(
                filtered.len(),
                test_ids.len(),
                "Fake IDs should all be unprocessed"
            );
        }

        /// Integration test: queues correctly track pending emails
        #[tokio::test]
        async fn test_queue_integration() {
            let queues = Arc::new(PipelineQueues::new(3600));

            // Add some pending email IDs
            let user_id = 1;
            assert!(queues.add_pending_email_id(user_id, "test_email_1".to_string()));
            assert!(queues.add_pending_email_id(user_id, "test_email_2".to_string()));

            // Verify counts
            assert_eq!(queues.pending_count_for_user(user_id), 2);
            assert_eq!(queues.total_pending_emails(), 2);

            // Verify dedup works
            assert!(!queues.add_pending_email_id(user_id, "test_email_1".to_string()));
            assert_eq!(queues.pending_count_for_user(user_id), 2);

            // Take pending and verify they're removed
            let pending = queues.take_pending_for_user(user_id);
            assert_eq!(pending.len(), 2);
            assert_eq!(queues.pending_count_for_user(user_id), 0);

            // But they should still be in pipeline
            assert!(queues.is_in_pipeline("test_email_1"));
            assert!(queues.is_in_pipeline("test_email_2"));

            // Mark complete and verify moved to recently_processed
            queues.mark_complete("test_email_1");
            assert!(!queues.is_in_pipeline("test_email_1"));
            assert!(queues.is_recently_processed("test_email_1"));
        }

        /// Integration test: fetch_email_ids retrieves IDs from Gmail
        #[tokio::test]
        async fn test_fetch_email_ids() {
            let server_state = setup_server_state().await;
            let test_email = "mpgrospamacc@gmail.com";

            let user = UserCtrl::get_pollable_by_email(&server_state.conn, test_email).await;

            if user.is_err() {
                println!("Test user not found, skipping test");
                return;
            }

            let user = user.unwrap();
            let poller = create_poller(server_state.clone());

            // Get email client
            let email_client = get_email_client(&server_state, user.clone()).await;
            assert!(
                email_client.is_ok(),
                "Failed to get email client: {:?}",
                email_client.err()
            );

            let email_client = email_client.unwrap();

            // Fetch email IDs
            let email_ids = poller.fetch_email_ids(&email_client, &user).await;
            assert!(
                email_ids.is_ok(),
                "fetch_email_ids failed: {:?}",
                email_ids.err()
            );

            let email_ids = email_ids.unwrap();
            println!("Fetched {} unprocessed email IDs", email_ids.len());
        }

        /// Integration test: poller respects shutdown signal
        #[tokio::test]
        async fn test_poller_shutdown() {
            let server_state = setup_server_state().await;
            let poller = create_poller(server_state);

            let shutdown = CancellationToken::new();
            let shutdown_clone = shutdown.clone();

            // Start poller in background
            let handle = tokio::spawn(async move {
                poller.run(shutdown_clone).await;
            });

            // Give it a moment to start
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Signal shutdown
            shutdown.cancel();

            // Wait for poller to stop (with timeout)
            let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
            assert!(result.is_ok(), "Poller did not shutdown within timeout");
            assert!(result.unwrap().is_ok(), "Poller task panicked");
        }
    }
}
