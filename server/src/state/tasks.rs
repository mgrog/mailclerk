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
use crate::prompt::priority_queue::PromptPriorityQueue;
use crate::rate_limiters::RateLimiters;
use crate::server_config::cfg;
use crate::HttpClient;
use crate::{error::AppResult, ServerState};

use super::email_processor::ActiveEmailProcessorMap;
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
/// Also cleans up processors that have stopped queueing and have no items in the priority queue.
pub fn run_email_queueing_loop(
    prompt_priority_queue: PromptPriorityQueue,
    email_processor_map: ActiveEmailProcessorMap,
) -> JoinHandle<()> {
    let mut interval = interval(Duration::from_secs(60));
    tracing::info!("Starting email queueing loop...");

    tokio::spawn(async move {
        loop {
            interval.tick().await;

            let queue = prompt_priority_queue.clone();
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

                // Only cleanup stopped processors that have nothing left in the priority queue
                let processors_for_cleanup: HashSet<_> = stopped
                    .into_iter()
                    .filter(|(email, _)| queue.num_in_queue(email) == 0)
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

/// This function pulls emails from the prompt priority queue and sends them to the
/// appropriate email processor for processing.
pub fn run_email_processing_loop(
    prompt_priority_queue: PromptPriorityQueue,
    email_processor_map: ActiveEmailProcessorMap,
) -> JoinHandle<()> {
    let max_workers = cfg.api.prompt_limits.rate_limit_per_sec * 2 - 1;
    tracing::info!(
        "Starting email processing loop with {} workers...",
        max_workers
    );

    tokio::spawn(async move {
        stream::iter(0..max_workers)
            .for_each_concurrent(max_workers, |worker_id| {
                let queue = prompt_priority_queue.clone();
                let map = email_processor_map.clone();
                async move {
                    loop {
                        let queue = queue.clone();
                        let map = map.clone();

                        let result = AssertUnwindSafe(async {
                            if let Some(entry) = queue.pop() {
                                let email_id = entry.email_id;
                                // Create guard that will remove email from in_processing set when dropped
                                // This ensures cleanup even if process_email panics
                                let _guard = queue.create_processing_guard(email_id);
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
                                    "Email processing worker {} panicked, recovering: {}",
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
    prompt_priority_queue: PromptPriorityQueue,
    email_processor_map: ActiveEmailProcessorMap,
    rate_limiters: RateLimiters,
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
            let in_processing = prompt_priority_queue.num_in_processing();
            if let Some(update) = email_processor_map.get_current_state() {
                tracing::info!(
                        "Processor Status Update:\n{email_per_second:.2} emails/s Bucket {limiter_status} Processing {in_processing}\n{update}",
                        email_per_second = emails_per_second,
                        limiter_status = limiter_status,
                        in_processing = in_processing,
                        update = update
                    );
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
