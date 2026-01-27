//! Initial Scan Module
//!
//! Processes a user's emails from the past N days using Mistral's batch inference API.
//! This is used when a user's `is_initial_scan_complete` flag is false.

use std::collections::HashMap;

use anyhow::Context;
use chrono::Duration;

use crate::{
    email::{
        client::{EmailClient, MessageFormat, MessageListOptions},
        rules::{EmailRule, UserEmailRules},
        simplified_message::SimplifiedMessage,
    },
    model::user::{UserCtrl, UserWithAccountAccessAndUsage},
    prompt::task_extraction::ExtractedTask,
    server_config::cfg,
    state::email_scanner::{
        batch_processor::{
            batch_insert_processed_emails, run_categorization_batch, run_task_extraction_batch,
        },
        shared::{find_matching_rule, EmailScanData, ProcessedEmailData},
    },
    ServerState,
};

/// Run initial scan for a single user.
/// This function:
/// 1. Fetches emails from the past N days (configured via initial_scan.lookback_days)
/// 2. Runs batch categorization
/// 3. Runs batch task extraction for relevant emails
/// 4. Stores results in the database (in chunks of 1000)
/// 5. Sets is_initial_scan_complete = true
pub async fn run_initial_scan_for_user(
    state: ServerState,
    user: UserWithAccountAccessAndUsage,
) -> anyhow::Result<()> {
    let user_id = user.id;
    let user_email = user.email.clone();
    let conn = state.conn.clone();
    let http_client = state.http_client.clone();

    let max_emails = cfg.initial_scan.max_emails;
    let lookback_days = cfg.initial_scan.lookback_days;

    tracing::info!(
        "Starting initial scan for user {} ({}) - max {} emails, {} days lookback",
        user_id,
        user_email,
        max_emails,
        lookback_days
    );

    // Create email client
    let email_client = EmailClient::new(http_client.clone(), conn.clone(), user.clone())
        .await
        .context("Failed to create email client for initial scan")?;

    // Load user's email rules
    let user_email_rules = UserEmailRules::from_user(&conn, user_id)
        .await
        .context("Failed to load user email rules")?;

    // Fetch emails from past N days
    let emails = fetch_emails_for_scan(&email_client, lookback_days, max_emails).await?;

    if emails.is_empty() {
        tracing::info!("No emails to process for user {}", user_email);
        UserCtrl::set_initial_scan_complete(&conn, user_id).await?;
        return Ok(());
    }

    tracing::info!(
        "Fetched {} emails for initial scan of user {}",
        emails.len(),
        user_email
    );

    // Phase 1: Batch categorization
    let category_results = run_categorization_batch(
        &http_client,
        &emails,
        &user_email_rules,
        "initial_scan_categorization",
    )
    .await?;

    tracing::info!(
        "Categorization complete for user {}: {} results",
        user_email,
        category_results.len()
    );

    // Build map of email_id -> email from
    let email_from_map: HashMap<String, String> = emails
        .iter()
        .filter_map(|e| e.from.as_ref().map(|from| (e.id.clone(), from.clone())))
        .collect();

    // Build map of email_id -> (category, confidence, email_rule)
    let mut categorization_map: HashMap<String, (String, f32, EmailRule)> = HashMap::new();
    for result in category_results {
        let (email_rule, _) = find_matching_rule(
            &result.category,
            result.confidence,
            &user_email_rules,
            email_from_map.get(&result.email_id),
        );
        categorization_map.insert(
            result.email_id,
            (result.category, result.confidence, email_rule),
        );
    }

    // Phase 2: Batch task extraction for emails that need it
    let emails_needing_tasks: Vec<&EmailScanData> = emails
        .iter()
        .filter(|e| {
            categorization_map
                .get(&e.id)
                .map(|(_, _, rule)| rule.extract_tasks)
                .unwrap_or(false)
        })
        .collect();

    let task_results = if !emails_needing_tasks.is_empty() {
        tracing::info!(
            "Running task extraction for {} emails",
            emails_needing_tasks.len()
        );
        run_task_extraction_batch(
            &http_client,
            &emails_needing_tasks,
            "initial_scan_task_extraction",
        )
        .await?
    } else {
        Vec::new()
    };

    // Build map of email_id -> tasks
    let task_map: HashMap<String, Vec<ExtractedTask>> = task_results
        .into_iter()
        .map(|r| (r.email_id, r.tasks))
        .collect();

    // Phase 3: Build processed email data
    let processed_emails: Vec<ProcessedEmailData> = emails
        .into_iter()
        .filter_map(|email| {
            let (ai_answer, ai_confidence, rule) = categorization_map.remove(&email.id)?;
            let category = rule.mail_label.clone();
            let extracted_tasks = task_map.get(&email.id).cloned().unwrap_or_default();

            Some(ProcessedEmailData {
                email_data: email,
                category,
                ai_answer,
                ai_confidence,
                extracted_tasks,
            })
        })
        .collect();

    // Phase 4: Batch insert in chunks
    let stored_count = batch_insert_processed_emails(&conn, user_id, processed_emails).await?;

    tracing::info!(
        "Initial scan complete for user {}: {} emails stored",
        user_email,
        stored_count
    );

    // Set is_initial_scan_complete = true
    UserCtrl::set_initial_scan_complete(&conn, user_id).await?;

    Ok(())
}

/// Fetch emails from the past N days for initial scan
async fn fetch_emails_for_scan(
    email_client: &EmailClient,
    lookback_days: i64,
    max_emails: usize,
) -> anyhow::Result<Vec<EmailScanData>> {
    let mut all_message_ids = Vec::new();
    let mut next_page_token = None;

    // Fetch message IDs from past N days
    loop {
        let response = email_client
            .get_message_list(MessageListOptions {
                page_token: next_page_token,
                more_recent_than: Some(Duration::days(lookback_days)),
                ..Default::default()
            })
            .await
            .context("Failed to fetch message list")?;

        if let Some(messages) = response.messages {
            for msg in messages {
                if let Some(id) = msg.id {
                    all_message_ids.push(id);
                    if all_message_ids.len() >= max_emails {
                        break;
                    }
                }
            }
        }

        if all_message_ids.len() >= max_emails {
            break;
        }

        next_page_token = response.next_page_token;
        if next_page_token.is_none() {
            break;
        }
    }

    tracing::debug!(
        "Found {} message IDs for initial scan",
        all_message_ids.len()
    );

    if all_message_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Batch fetch message content
    let messages = email_client
        .get_messages_by_ids(&all_message_ids, MessageFormat::Raw)
        .await
        .context("Failed to batch fetch messages")?;

    // Convert to EmailScanData
    let mut email_data = Vec::with_capacity(messages.len());
    for msg in messages {
        let simplified = match SimplifiedMessage::from_gmail_message(&msg) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("Failed to simplify message: {:?}", e);
                continue;
            }
        };

        email_data.push(EmailScanData {
            id: simplified.id,
            thread_id: msg.thread_id,
            label_ids: msg.label_ids.unwrap_or_default(),
            history_id: msg.history_id.unwrap_or_default(),
            internal_date: msg.internal_date.unwrap_or_default(),
            from: simplified.from,
            subject: simplified.subject,
            snippet: msg.snippet,
            body: simplified.body,
        });
    }

    Ok(email_data)
}

#[cfg(test)]
mod tests {
    use crate::{
        email::rules::EmailRule,
        model::labels::UtilityLabels,
        state::email_scanner::shared::find_matching_rule,
    };

    use super::*;

    #[test]
    fn test_find_matching_rule_high_confidence() {
        let rules = UserEmailRules::new(vec![EmailRule {
            prompt_content: "Newsletter".to_string(),
            mail_label: "newsletter".to_string(),
            extract_tasks: false,
            priority: 3,
        }]);

        let (rule, _) = find_matching_rule("Newsletter", 0.95, &rules, Some("Someone"));
        assert_eq!(rule.mail_label, "newsletter");
    }

    #[test]
    fn test_find_matching_rule_low_confidence() {
        let rules = UserEmailRules::new(vec![EmailRule {
            prompt_content: "Newsletter".to_string(),
            mail_label: "newsletter".to_string(),
            extract_tasks: false,
            priority: 3,
        }]);

        let (rule, _) = find_matching_rule("Newsletter", 0.3, &rules, Some("Someone"));
        assert_eq!(rule.mail_label, UtilityLabels::Uncategorized.as_str());
    }

    #[test]
    fn test_find_matching_rule_unknown_category() {
        let rules = UserEmailRules::new(vec![EmailRule {
            prompt_content: "Newsletter".to_string(),
            mail_label: "newsletter".to_string(),
            extract_tasks: false,
            priority: 3,
        }]);

        let (rule, _) = find_matching_rule("SomethingElse", 0.95, &rules, Some("Someone"));
        assert_eq!(rule.mail_label, UtilityLabels::Uncategorized.as_str());
    }
}
