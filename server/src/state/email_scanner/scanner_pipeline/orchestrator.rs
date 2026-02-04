//! Scanner Pipeline Orchestrator
//!
//! Main entry point for the email scanner pipeline.
//! Initializes and runs all pipeline components:
//! - EmailIdPoller: Polls users for new email IDs
//! - MessageFetcher: Fetches full message content
//! - StageRunner: Runs categorization and task extraction batches
//! - DoneHandler: Handles DB inserts and token quota updates

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::{observability::PipelineTracker, server_config::cfg, ServerState};

use super::{
    done_handler::DoneHandler, fetcher::MessageFetcher, poller::EmailIdPoller,
    prompt_stages::StageRunner, queues::PipelineQueues,
};

/// Main orchestrator for the email scanner pipeline
pub struct ScannerPipeline {
    queues: Arc<PipelineQueues>,
    tracker: PipelineTracker,
    poller: EmailIdPoller,
    fetcher: MessageFetcher,
    stage_runner: StageRunner,
    done_handler: DoneHandler,
    shutdown: CancellationToken,
}

impl ScannerPipeline {
    /// Create a new scanner pipeline
    pub fn new(server_state: ServerState) -> Self {
        let recently_processed_ttl = cfg.scanner_pipeline.recently_processed_ttl_secs;
        let queues = Arc::new(PipelineQueues::new(recently_processed_ttl));
        let tracker = PipelineTracker::new(queues.clone());

        let poller = EmailIdPoller::new(server_state.clone(), queues.clone(), tracker.clone());

        let fetcher = MessageFetcher::new(server_state.clone(), queues.clone(), tracker.clone());

        let stage_runner = StageRunner::new(
            server_state.http_client.clone(),
            server_state.conn.clone(),
            queues.clone(),
            tracker.clone(),
        );

        let done_handler =
            DoneHandler::new(server_state.conn.clone(), queues.clone(), tracker.clone());

        Self {
            queues,
            tracker,
            poller,
            fetcher,
            stage_runner,
            done_handler,
            shutdown: CancellationToken::new(),
        }
    }

    /// Start the pipeline
    ///
    /// This spawns all worker tasks and returns immediately.
    /// Use `shutdown()` to stop the pipeline gracefully.
    pub fn start(&self) {
        let shutdown = self.shutdown.clone();

        // Spawn poller task
        let poller = self.poller.clone();
        let poller_shutdown = shutdown.clone();
        tokio::spawn(async move {
            poller.run(poller_shutdown).await;
        });

        // Spawn fetcher task
        let fetcher = self.fetcher.clone();
        let fetcher_shutdown = shutdown.clone();
        tokio::spawn(async move {
            fetcher.run(fetcher_shutdown).await;
        });

        // Spawn stage runner task
        let stage_runner = self.stage_runner.clone();
        let stage_runner_shutdown = shutdown.clone();
        tokio::spawn(async move {
            stage_runner.run(stage_runner_shutdown).await;
        });

        // Spawn done handler task
        let done_handler = self.done_handler.clone();
        let done_handler_shutdown = shutdown.clone();
        tokio::spawn(async move {
            done_handler.run(done_handler_shutdown).await;
        });

        tracing::info!("Scanner pipeline started");
    }

    /// Shutdown the pipeline gracefully
    pub fn shutdown(&self) {
        tracing::info!("Shutting down scanner pipeline...");
        self.shutdown.cancel();
    }

    /// Get the cancellation token for external shutdown coordination
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Get current pipeline stats
    pub fn get_stats(&self) -> crate::observability::PipelineStats {
        self.tracker.get_stats()
    }

    /// Get formatted status table for logging
    pub fn get_status_table(&self) -> String {
        self.tracker.get_status_table()
    }

    /// Get reference to the tracker
    pub fn tracker(&self) -> &PipelineTracker {
        &self.tracker
    }

    /// Get reference to the queues (for monitoring)
    pub fn queues(&self) -> &Arc<PipelineQueues> {
        &self.queues
    }
}
