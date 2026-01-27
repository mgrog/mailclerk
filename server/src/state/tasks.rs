use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use entity::processed_email;
use entity::sea_orm_active_enums::CleanupAction;
use futures::stream::{self, StreamExt};
use futures::FutureExt;
use google_gmail1::api::ListMessagesResponse;
use sea_orm::DatabaseConnection;
use tokio::task::JoinHandle;
use tokio::time::interval;

use crate::model::auto_cleanup_setting::AutoCleanupSettingCtrl;
use crate::model::processed_email::ProcessedEmailCtrl;
use crate::model::user::UserCtrl;
use crate::observability::ScanTracker;
use crate::prompt::{TaskData, TaskQueue};
use crate::rate_limiters::RateLimiters;
use crate::server_config::cfg;
use crate::HttpClient;
use crate::{error::AppResult, ServerState};

use super::email_scanner::ActiveEmailProcessorMap;
use crate::email::client::{EmailClient, MessageListOptions};

pub async fn add_users_to_processing(
    state: ServerState,
    email_processor_map: ActiveEmailProcessorMap,
) -> AppResult<()> {
    let conn = &state.conn;
    let user_accounts = UserCtrl::all_available_for_processing(conn).await?;

    // Stop users who shouldn't be processed
    let valid_emails: HashSet<String> = user_accounts.iter().map(|u| u.email.clone()).collect();

    for (email, proc) in email_processor_map.entries() {
        if !valid_emails.contains(&email) {
            proc.cancel();
        }
    }
    tracing::info!("Adding {} users to processing", user_accounts.len());

    for user in user_accounts {
        match email_processor_map.insert_processor(user).await {
            Ok(_) => {}
            Err(err) => {
                tracing::error!("Error adding user to processing: {:?}", err);
            }
        }
    }

    Ok(())
}

pub async fn sweep_for_cancelled_subscriptions(
    state: &ServerState,
    email_processor_map: ActiveEmailProcessorMap,
) -> AppResult<()> {
    let conn = &state.conn;
    let user_accounts = UserCtrl::all_with_cancelled_subscriptions(conn).await?;

    for user in user_accounts {
        email_processor_map.cancel_processor(&user.email);
    }

    Ok(())
}

/// Task that triggers each processor to queue new emails concurrently.
/// Runs on a 60-second interval, iterating through all active processors.
/// Also cleans up processors that have stopped queueing and have no items in the task queue.
pub fn run_email_queueing_loop(
    task_queue: TaskQueue,
    email_processor_map: ActiveEmailProcessorMap,
) -> JoinHandle<()> {
    let mut interval = interval(Duration::from_secs(60));
    tracing::info!("Starting email queueing loop...");

    tokio::spawn(async move {
        loop {
            interval.tick().await;

            let queue = task_queue.clone();
            let map = email_processor_map.clone();

            let result = AssertUnwindSafe(async {
                let processors = map.entries();
                if processors.is_empty() {
                    return;
                }

                // Separate stopped processors (for cleanup) from active ones (for queueing)
                let (stopped, active): (Vec<_>, Vec<_>) = processors
                    .into_iter()
                    .partition(|(_, processor)| processor.has_stopped_queueing());

                // Only cleanup stopped processors that have nothing left in the task queue
                let processors_for_cleanup: HashSet<_> = stopped
                    .into_iter()
                    .filter(|(email, _)| queue.queue_count(email).total() == 0)
                    .map(|(email, _)| email)
                    .collect();

                // Queue emails concurrently, max 10 at a time
                stream::iter(active)
                    .for_each_concurrent(10, |(email, processor)| async move {
                        match processor.queue_emails().await {
                            Ok(n) if n > 0 => {
                                tracing::debug!("Queued {} emails for {}", n, email);
                            }
                            Err(e) => {
                                tracing::error!("Error queueing emails for {}: {:?}", email, e);
                            }
                            _ => {}
                        }
                    })
                    .await;

                // Cleanup stopped processors
                if !processors_for_cleanup.is_empty() {
                    map.cleanup_processors(processors_for_cleanup);
                }
            })
            .catch_unwind()
            .await;

            if let Err(panic) = result {
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "Unknown panic".to_string());
                tracing::error!("Email queueing loop panicked, recovering: {}", msg);
            }
        }
    })
}

/// This function pulls categorization tasks from the task queue (High and Low priority)
/// and sends them to the appropriate email processor for processing.
pub fn run_categorization_loop(
    task_queue: TaskQueue,
    email_processor_map: ActiveEmailProcessorMap,
) -> JoinHandle<()> {
    let max_workers = cfg.api.prompt_limits.rate_limit_per_sec * 2 - 1;
    tracing::info!(
        "Starting categorization loop with {} workers...",
        max_workers
    );

    tokio::spawn(async move {
        stream::iter(0..max_workers)
            .for_each_concurrent(max_workers, |worker_id| {
                let queue = task_queue.clone();
                let map = email_processor_map.clone();
                async move {
                    loop {
                        let queue = queue.clone();
                        let map = map.clone();

                        let result = AssertUnwindSafe(async {
                            // Only pop categorization tasks (High/Low), skip Background
                            if let Some(entry) = queue.pop_categorization() {
                                let email_id = entry.email_id.clone();
                                // Create guard that will remove email from in_processing set when dropped
                                // This ensures cleanup even if process_email panics
                                let _guard = queue.create_processing_guard(email_id.clone());
                                if let Some(processor) = map.get(&entry.user_email) {
                                    processor.process_email(email_id, entry.priority).await;
                                }
                                // _guard dropped here, removing email from in_processing set
                                true
                            } else {
                                false
                            }
                        })
                        .catch_unwind()
                        .await;

                        match result {
                            Ok(false) => {
                                // No work available, sleep before checking again
                                tokio::time::sleep(Duration::from_millis(500)).await;
                            }
                            Err(panic) => {
                                let msg = panic
                                    .downcast_ref::<&str>()
                                    .map(|s| s.to_string())
                                    .or_else(|| panic.downcast_ref::<String>().cloned())
                                    .unwrap_or_else(|| "Unknown panic".to_string());
                                tracing::error!(
                                    "Categorization worker {} panicked, recovering: {}",
                                    worker_id,
                                    msg
                                );
                            }
                            Ok(true) => {}
                        }
                    }
                }
            })
            .await;
    })
}

/// This function pulls embedding tasks from the task queue (Background priority)
/// and processes them in batches using the Mistral embedding API.
pub fn run_embedding_loop(
    task_queue: TaskQueue,
    http_client: HttpClient,
    conn: DatabaseConnection,
    rate_limiters: RateLimiters,
) -> JoinHandle<()> {
    use crate::model::email_embedding::{EmailEmbeddingCtrl, EmbeddingInsert};

    let batch_size = cfg.embedding.batch_size;
    let batch_wait_ms = cfg.embedding.batch_wait_ms;

    tracing::info!(
        "Starting embedding loop (batch_size={}, batch_wait_ms={})...",
        batch_size,
        batch_wait_ms
    );

    tokio::spawn(async move {
        loop {
            // Wait for categorization work to clear, or timeout
            let mut waited = 0u64;
            while task_queue.has_categorization_work() && waited < batch_wait_ms {
                tokio::time::sleep(Duration::from_millis(100)).await;
                waited += 100;
            }

            // Try to get a batch of embedding tasks
            let batch = task_queue.pop_background_batch(batch_size);

            if batch.is_empty() {
                // No embedding work, sleep and try again
                tokio::time::sleep(Duration::from_millis(1000)).await;
                continue;
            }

            tracing::debug!("Processing embedding batch of {} tasks", batch.len());

            // Acquire rate limit before making API call
            rate_limiters.acquire_one().await;

            // Collect chunks with their parent entry info for batch embedding
            // Each chunk needs to track: email_id, user_id, chunk_index, text
            struct ChunkInfo {
                email_id: String,
                user_id: i32,
                chunk_index: i32,
                text: String,
            }

            let mut chunk_infos: Vec<ChunkInfo> = Vec::new();
            let mut processed_email_ids: Vec<String> = Vec::new();

            for entry in batch {
                if let TaskData::Embedding { chunks } = &entry.task {
                    for chunk in chunks {
                        chunk_infos.push(ChunkInfo {
                            email_id: entry.email_id.clone(),
                            user_id: entry.user_id,
                            chunk_index: chunk.index as i32,
                            text: chunk.text.clone(),
                        });
                    }
                    processed_email_ids.push(entry.email_id.clone());
                }
            }

            if chunk_infos.is_empty() {
                continue;
            }

            let texts: Vec<String> = chunk_infos.iter().map(|c| c.text.clone()).collect();

            // Call Mistral batch embedding API
            match crate::embed::service::embed_batch(&http_client, &texts).await {
                Ok(embeddings) => {
                    // Build batch insert entries
                    let inserts: Vec<EmbeddingInsert> = chunk_infos
                        .into_iter()
                        .zip(embeddings)
                        .map(|(chunk_info, embedding)| EmbeddingInsert {
                            email_id: chunk_info.email_id,
                            user_id: chunk_info.user_id,
                            chunk_index: chunk_info.chunk_index,
                            embedding,
                            chunk_text: if cfg.settings.training_mode {
                                Some(chunk_info.text)
                            } else {
                                None
                            },
                        })
                        .collect();

                    let count = inserts.len();
                    if let Err(e) = EmailEmbeddingCtrl::batch_insert(&conn, inserts).await {
                        tracing::error!("Failed to insert embeddings batch: {:?}", e);
                    } else {
                        tracing::debug!("Successfully embedded {} chunks", count);
                    }
                }
                Err(e) => {
                    tracing::error!("Batch embedding failed: {:?}", e);
                }
            }

            // Mark all as done
            for email_id in processed_email_ids {
                task_queue.mark_done(&email_id);
            }
        }
    })
}

pub async fn cleanup_email(
    email_client: EmailClient,
    processed_email: processed_email::Model,
    action: CleanupAction,
) -> anyhow::Result<()> {
    match action {
        CleanupAction::Nothing => {}
        CleanupAction::Delete => {
            email_client.trash_email(&processed_email.id).await?;
        }
        CleanupAction::Archive => {
            email_client.archive_email(&processed_email.id).await?;
        }
    }

    Ok(())
}

async fn get_email_client(
    http_client: HttpClient,
    conn: DatabaseConnection,
    user_id: i32,
) -> anyhow::Result<EmailClient> {
    let user = UserCtrl::get_with_account_access_by_id(&conn, user_id).await?;
    let client = EmailClient::new(http_client, conn, user).await?;
    Ok(client)
}

async fn get_all_message_ids_with_keep_label(
    email_client: EmailClient,
) -> anyhow::Result<HashSet<String>> {
    let mut message_ids_with_keep_label = HashSet::new();
    let load_page = |next_page_token: Option<String>| async {
        let resp = email_client
            .get_message_list(MessageListOptions {
                page_token: next_page_token,
                label_filter: Some(
                    [
                        "label:inbox".to_string(),
                        "label:Mailclerk/keep".to_string(),
                    ]
                    .join(" AND "),
                ),
                ..Default::default()
            })
            .await
            .context("Error loading next keep label email page")?;

        Ok::<_, anyhow::Error>(resp)
    };

    let mut next_page_token = None;
    while let ListMessagesResponse {
        messages: Some(messages),
        next_page_token: next_token_resp,
        ..
    } = load_page(next_page_token.clone()).await?
    {
        next_page_token = next_token_resp;

        for message in messages {
            if let Some(id) = message.id {
                message_ids_with_keep_label.insert(id);
            }
        }

        if next_page_token.is_none() {
            break;
        }
    }

    Ok(message_ids_with_keep_label)
}

pub async fn run_auto_email_cleanup(
    http_client: HttpClient,
    conn: DatabaseConnection,
) -> anyhow::Result<()> {
    let active_user_cleanup_settings =
        AutoCleanupSettingCtrl::all_active_user_cleanup_settings(&conn)
            .await
            .context("Failed to fetch cleanup settings in auto cleanup")?;

    let user_to_cleanup_settings = active_user_cleanup_settings.into_iter().fold(
        HashMap::<_, Vec<_>>::new(),
        |mut acc, setting| {
            acc.entry(setting.user_id).or_default().push(setting);
            acc
        },
    );

    for (user_id, settings) in user_to_cleanup_settings {
        let email_client = match get_email_client(http_client.clone(), conn.clone(), user_id).await
        {
            Ok(client) => client,
            Err(err) => {
                tracing::error!(
                    "Failed to create email client for user {}: {:?}",
                    user_id,
                    err
                );
                // TODO: Notify user if access issue
                continue;
            }
        };

        let keep_ids = Arc::new(get_all_message_ids_with_keep_label(email_client.clone()).await?);

        for setting in settings {
            if let Ok(emails_to_cleanup) =
                ProcessedEmailCtrl::get_users_processed_emails_for_cleanup(&conn, &setting).await
            {
                if emails_to_cleanup.is_empty() {
                    continue;
                }

                tracing::info!(
                    "Cleaning up {} emails for user {} according to setting:\n{:?}",
                    emails_to_cleanup.len(),
                    email_client.email_address,
                    setting
                );

                let queue = Arc::new(Mutex::new(
                    emails_to_cleanup
                        .into_iter()
                        .filter(|email| !keep_ids.contains(&email.id))
                        .collect::<Vec<_>>(),
                ));

                let threads = (0..5)
                    .map(|_| {
                        let queue = queue.clone();
                        let email_client = email_client.clone();
                        let setting = setting.clone();
                        tokio::spawn(async move {
                            let next = || queue.lock().unwrap().pop();
                            while let Some(email) = next() {
                                match setting.cleanup_action {
                                    CleanupAction::Nothing => {}
                                    CleanupAction::Delete => {
                                        // -- DEBUG
                                        // println!("Trashing email {:?}", email);
                                        // tokio::time::sleep(Duration::from_millis(10)).await;
                                        // -- DEBUG
                                        email_client.trash_email(&email.id).await.unwrap_or_else(
                                            |e| {
                                                tracing::error!(
                                                    "Failed to trash email {} for user {}: {:?}",
                                                    email.id,
                                                    email_client.email_address,
                                                    e
                                                )
                                            },
                                        );
                                    }
                                    CleanupAction::Archive => {
                                        // -- DEBUG
                                        // println!("Archiving email: {:?}", email);
                                        // tokio::time::sleep(Duration::from_millis(10)).await;
                                        // -- DEBUG
                                        email_client.archive_email(&email.id).await.unwrap_or_else(
                                            |e| {
                                                tracing::error!(
                                                    "Failed to archive email {} for user {}: {:?}",
                                                    email.id,
                                                    email_client.email_address,
                                                    e
                                                )
                                            },
                                        );
                                    }
                                }
                            }
                        })
                    })
                    .collect::<tokio::task::JoinSet<_>>();

                threads.join_all().await;
            }
        }
    }
    Ok(())
}

pub fn watch(
    task_queue: TaskQueue,
    email_processor_map: ActiveEmailProcessorMap,
    rate_limiters: RateLimiters,
    scan_tracker: ScanTracker,
) -> JoinHandle<()> {
    let mut interval = interval(Duration::from_secs(5));
    let mut now = std::time::Instant::now();
    let mut last_recorded = 0;
    tokio::spawn(async move {
        loop {
            interval.tick().await;
            let diff = email_processor_map.total_emails_processed() - last_recorded;
            let emails_per_second = diff as f64 / now.elapsed().as_secs_f64();
            now = std::time::Instant::now();
            last_recorded = email_processor_map.total_emails_processed();
            let limiter_status = rate_limiters.get_status();
            let queue_count = task_queue.total_count();
            let in_processing = task_queue.num_in_processing();

            // Update processor stats in scan_tracker for each active processor
            for (user_email, processor) in email_processor_map.entries() {
                let status = processor.status();
                let processed = processor.total_emails_processed() as u64;
                let user_queue_count = task_queue.queue_count(&user_email).total();
                scan_tracker.update_processor(&user_email, status, processed, user_queue_count);
            }

            // Get combined status tables from scan_tracker
            let status_update = scan_tracker.get_current_state();

            // Only log if there's something to report
            if status_update.is_some() || email_processor_map.len() > 0 {
                let mut status_msg = format!(
                    "Status Update:\n{email_per_second:.2} emails/s | Bucket {limiter_status} | Processing {in_processing} | Queue: H={high} L={low} BG={background}",
                    email_per_second = emails_per_second,
                    limiter_status = limiter_status,
                    in_processing = in_processing,
                    high = queue_count.high,
                    low = queue_count.low,
                    background = queue_count.background,
                );

                if let Some(state) = status_update {
                    status_msg.push_str(&format!("\n{}", state));
                }

                tracing::info!("{}", status_msg);
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::common::setup;

    #[tokio::test]
    async fn test_auto_cleanup() {
        let (conn, http_client) = setup().await;
        run_auto_email_cleanup(http_client, conn).await.unwrap();
    }
}
