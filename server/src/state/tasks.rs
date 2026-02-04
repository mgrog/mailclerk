//! Background tasks for the email system
//!
//! This module contains background tasks that run on a schedule,
//! such as auto email cleanup.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::Context;
use entity::sea_orm_active_enums::CleanupAction;
use google_gmail1::api::ListMessagesResponse;
use sea_orm::DatabaseConnection;

use crate::model::auto_cleanup_setting::AutoCleanupSettingCtrl;
use crate::model::processed_email::ProcessedEmailCtrl;
use crate::model::user::UserCtrl;
use crate::HttpClient;

use crate::email::client::{EmailClient, MessageListOptions};

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

/// Run automatic email cleanup for all users with active cleanup settings.
/// This function is called by the scheduler on a cron schedule.
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
