//! Scanner Pipeline
//!
//! A pipeline for processing emails using Mistral's batch API.
//! This module replaces the on-demand queue processor for continuous email scanning.
//!
//! ## Architecture
//!
//! The pipeline consists of several components:
//!
//! - **Poller**: Polls users for new email IDs at regular intervals
//! - **Fetcher**: Fetches full message content for pending emails
//! - **Stages**: Runs batch categorization and task extraction jobs
//! - **Done Handler**: Persists processed emails to the database
//! - **Orchestrator**: Coordinates all components
//!
//! ## Queues
//!
//! Emails flow through the following queues:
//!
//! 1. `pending_email_ids` - Email IDs waiting to be fetched
//! 2. `main_categorization_queue` - Items ready for system rules categorization
//! 3. `user_defined_categorization_queue` - Items needing user rules categorization
//! 4. `task_extraction_queue` - Items needing task extraction
//! 5. `done_queue` - Completed items ready for DB insert
//!
//! //! ## Notes
//! This currently does not persist stages on shutdown.
//! We may need to add ability to resume stages if batch jobs are abandoned too often.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use crate::state::email_scanner::scanner_pipeline::ScannerPipeline;
//!
//! let pipeline = ScannerPipeline::new(server_state);
//! pipeline.start();
//!
//! // To shutdown gracefully
//! pipeline.shutdown();
//! ```

use crate::{
    email::client::EmailClient,
    model::user::{AccountAccess, EmailAddress, Id},
    util::check_expired,
    ServerState,
};

mod done_handler;
mod fetcher;
mod orchestrator;
mod poller;
mod prompt_stages;
pub mod queues;
pub mod types;

/// Get or create an EmailClient for a user, using the cache when possible.
pub async fn get_email_client(
    server_state: &ServerState,
    user: impl AccountAccess + Id + EmailAddress,
) -> anyhow::Result<EmailClient> {
    let user_email = user.email();

    // Try to get from cache with read lock - touch() is atomic so no write lock needed
    {
        let cache = server_state.email_client_cache.read().await;
        if let Some(client) = cache.get(user_email) {
            if !check_expired(client.expires_at) {
                client.touch();
                return Ok(client.clone());
            }
        }
    }

    // Client missing or expired - create new one and insert with write lock
    let client = EmailClient::new(
        server_state.http_client.clone(),
        server_state.conn.clone(),
        user,
    )
    .await?;

    let mut cache = server_state.email_client_cache.write().await;
    cache.insert(client.email_address.clone(), client.clone());

    Ok(client)
}

// Re-export the main orchestrator
pub use orchestrator::ScannerPipeline;

#[cfg(test)]
mod tests {
    #[cfg(feature = "integration")]
    mod integration {
        use std::sync::Arc;
        use tokio::time::Duration;

        use crate::auth::session_store::AuthSessionStore;
        use crate::db_core::prelude::*;
        use crate::email::client::MessageListOptions;
        use crate::model::processed_email::ProcessedEmailCtrl;
        use crate::model::user::UserCtrl;
        use crate::observability::{PipelineTracker, ScanTracker};
        use crate::prompt::TaskQueue;
        use crate::rate_limiters::RateLimiters;
        use crate::server_config::cfg;
        use crate::state::email_client_map::EmailClientMap;
        use crate::state::email_scanner::scanner_pipeline::{
            get_email_client, orchestrator::ScannerPipeline, queues::PipelineQueues,
        };
        use crate::testing::common::setup;
        use crate::{EmailClientCache, ServerState};

        const TEST_EMAIL: &str = "mpgrospamacc@gmail.com";

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

        /// Integration test: Full happy path - poll, fetch, categorize, insert, cleanup
        ///
        /// This test exercises the complete batch pipeline flow for a single email:
        /// 1. Get a test user and fetch a recent email ID
        /// 2. Add the email to the pipeline queues
        /// 3. Start the pipeline and let it process
        /// 4. Verify the email was inserted into processed_emails
        /// 5. Clean up by deleting the test record
        #[tokio::test]
        async fn test_scanner_pipeline_happy_path() {
            let server_state = setup_server_state().await;

            // 1. Get test user
            let user = UserCtrl::get_with_account_access_and_usage_by_email(
                &server_state.conn,
                TEST_EMAIL,
            )
            .await;

            if user.is_err() {
                println!("Test user {} not found, skipping test", TEST_EMAIL);
                return;
            }
            let user = user.unwrap();
            println!("Test user: {} (id: {})", user.email, user.id);

            // 2. Get email client and fetch a recent email ID
            let email_client = get_email_client(&server_state, user.clone())
                .await
                .expect("Failed to get email client");

            let max_lookback_days = cfg.continuous_scan.max_lookback_days;
            let response = email_client
                .get_message_list(MessageListOptions {
                    more_recent_than: Some(chrono::Duration::days(max_lookback_days)),
                    max_results: Some(10),
                    ..Default::default()
                })
                .await
                .expect("Failed to fetch message list");

            let email_ids: Vec<String> = response
                .messages
                .unwrap_or_default()
                .into_iter()
                .filter_map(|msg| msg.id)
                .collect();

            if email_ids.is_empty() {
                println!("No recent emails found, skipping test");
                return;
            }

            // Find an email that's NOT already processed
            let existing_ids =
                ProcessedEmailCtrl::get_existing_ids(&server_state.conn, user.id, &email_ids)
                    .await
                    .expect("Failed to check existing emails");

            let test_email_id = email_ids.into_iter().find(|id| !existing_ids.contains(id));

            let test_email_id = match test_email_id {
                Some(id) => id,
                None => {
                    println!("All recent emails already processed, skipping test");
                    return;
                }
            };

            println!("Test email ID: {}", test_email_id);

            // 3. Create pipeline and add the test email to pending queue
            let pipeline = ScannerPipeline::new(server_state.clone());
            let queues = pipeline.queues();

            // Manually add the test email ID to the pending queue
            let added = queues.add_pending_email_id(user.id, test_email_id.clone());
            assert!(added, "Failed to add email to pending queue");
            println!("Added email to pending queue");

            // 4. Start the pipeline
            pipeline.start();
            println!("Pipeline started");

            // 5. Wait for the email to be processed (with timeout)
            let timeout = Duration::from_secs(120); // 2 minutes max
            let poll_interval = Duration::from_millis(500);
            let start = std::time::Instant::now();

            let mut processed = false;
            while start.elapsed() < timeout {
                // Check if email is in recently_processed (means it completed)
                if queues.is_recently_processed(&test_email_id) {
                    processed = true;
                    break;
                }

                // Also check if it's no longer in pipeline at all
                if !queues.is_in_pipeline(&test_email_id)
                    && !queues.is_recently_processed(&test_email_id)
                {
                    // Check if it made it to the database
                    let existing = ProcessedEmailCtrl::get_existing_ids(
                        &server_state.conn,
                        user.id,
                        std::slice::from_ref(&test_email_id),
                    )
                    .await
                    .unwrap_or_default();

                    if existing.contains(&test_email_id) {
                        processed = true;
                        break;
                    }
                }

                // Print progress
                if start.elapsed().as_secs().is_multiple_of(5) {
                    println!("{}", pipeline.get_status_table());
                }

                tokio::time::sleep(poll_interval).await;
            }

            // 6. Shutdown the pipeline
            pipeline.shutdown();
            println!("Pipeline shutdown");

            // Give it a moment to complete shutdown
            tokio::time::sleep(Duration::from_millis(500)).await;

            // 7. Verify the email was processed
            let existing = ProcessedEmailCtrl::get_existing_ids(
                &server_state.conn,
                user.id,
                std::slice::from_ref(&test_email_id),
            )
            .await
            .expect("Failed to check processed email");

            if !processed || !existing.contains(&test_email_id) {
                // Print final stats for debugging
                println!("Final pipeline status:\n{}", pipeline.get_status_table());
                panic!(
                    "Email {} was not processed within timeout. processed={}, in_db={}",
                    test_email_id,
                    processed,
                    existing.contains(&test_email_id)
                );
            }

            println!("Email {} successfully processed!", test_email_id);
            println!("{}", pipeline.get_status_table());

            // 8. Clean up - delete the test processed email
            let delete_result = ProcessedEmail::delete_by_id(test_email_id.clone())
                .exec(&server_state.conn)
                .await;

            match delete_result {
                Ok(result) => {
                    println!(
                        "Cleanup: deleted {} processed email record(s)",
                        result.rows_affected
                    );
                }
                Err(e) => {
                    println!("Warning: cleanup failed: {}", e);
                }
            }

            // Verify cleanup
            let remaining = ProcessedEmailCtrl::get_existing_ids(
                &server_state.conn,
                user.id,
                std::slice::from_ref(&test_email_id),
            )
            .await
            .expect("Failed to verify cleanup");

            assert!(
                !remaining.contains(&test_email_id),
                "Cleanup failed: email {} still exists in database",
                test_email_id
            );

            println!("Test completed successfully!");
        }

        /// Integration test: Pipeline handles empty queue gracefully
        #[tokio::test]
        async fn test_pipeline_empty_queue() {
            let server_state = setup_server_state().await;

            let pipeline = ScannerPipeline::new(server_state);

            // Start with empty queues
            pipeline.start();

            // Let it run briefly
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Should still be healthy
            let stats = pipeline.get_stats();
            assert_eq!(stats.total_in_pipeline, 0);
            assert_eq!(stats.failed_queue, 0);

            // Shutdown
            pipeline.shutdown();
            tokio::time::sleep(Duration::from_millis(500)).await;

            println!("Empty queue test passed");
        }

        /// Integration test: Pipeline queues operations
        #[tokio::test]
        async fn test_pipeline_queues() {
            let queues = Arc::new(PipelineQueues::new(3600));
            let tracker = PipelineTracker::new(queues.clone());

            // Test pending queue operations
            let user_id = 999;
            assert!(queues.add_pending_email_id(user_id, "test1".to_string()));
            assert!(queues.add_pending_email_id(user_id, "test2".to_string()));
            assert!(!queues.add_pending_email_id(user_id, "test1".to_string())); // Duplicate

            assert_eq!(queues.pending_count_for_user(user_id), 2);
            assert_eq!(queues.total_pending_emails(), 2);
            assert!(queues.is_in_pipeline("test1"));

            // Take pending
            let taken = queues.take_pending_for_user(user_id);
            assert_eq!(taken.len(), 2);
            assert_eq!(queues.pending_count_for_user(user_id), 0);

            // Still in pipeline until marked complete
            assert!(queues.is_in_pipeline("test1"));
            assert!(!queues.is_recently_processed("test1"));

            // Mark complete
            queues.mark_complete("test1");
            assert!(!queues.is_in_pipeline("test1"));
            assert!(queues.is_recently_processed("test1"));

            // Can't add again while recently processed
            assert!(!queues.add_pending_email_id(user_id, "test1".to_string()));

            // Test tracker records
            tracker.increment_fetched(2);
            tracker.increment_processed(1);
            tracker.increment_inserted(1);

            let stats = tracker.get_stats();
            assert!(stats.emails_fetched_per_sec >= 0.0);

            println!("Queue operations test passed");
        }
    }
}
