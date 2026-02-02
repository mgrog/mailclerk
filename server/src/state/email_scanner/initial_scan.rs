//! Initial Scan Module
//!
//! Processes a user's emails from the past N days using Mistral's batch inference API.
//! This is used when a user's `is_initial_scan_complete` flag is false.

use std::collections::HashMap;

use anyhow::{bail, Context};
use chrono::Duration;

use crate::{
    email::{
        client::{EmailClient, MessageFormat, MessageListOptions},
        rules::SystemEmailRules,
        simplified_message::SimplifiedMessage,
    },
    model::user::{UserCtrl, UserWithAccountAccessAndUsage},
    observability::{ScanPhase, ScanTracker, ScanType},
    prompt::{mistral::categorization_user_prompt, task_extraction::ExtractedTask},
    server_config::cfg,
    state::email_scanner::{
        batch_processor::{
            batch_insert_processed_emails, run_categorization_batch, run_task_extraction_batch,
        },
        shared::{
            find_matching_rule_system, CategorizationResult, CategorizationSource, EmailScanData,
            ProcessedEmailData,
        },
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
    scan_tracker: ScanTracker,
) -> anyhow::Result<()> {
    let user_id = user.id;
    let user_email = user.email.clone();
    let conn = state.conn.clone();
    let http_client = state.http_client.clone();

    let max_emails = cfg.initial_scan.max_emails;
    let lookback_days = cfg.initial_scan.lookback_days;

    // Register the scan with the tracker
    scan_tracker.register_scan(ScanType::InitialScan, user_email.clone(), user_id);

    tracing::info!(
        "Starting initial scan for user {} ({}) - max {} emails, {} days lookback",
        user_id,
        user_email,
        max_emails,
        lookback_days
    );

    // Helper to handle errors and update tracker
    let run_scan = async {
        // Create email client
        let email_client = EmailClient::new(http_client.clone(), conn.clone(), user.clone())
            .await
            .context("Failed to create email client for initial scan")?;

        // Load system rules (initial scan uses system rules only - users don't have custom rules yet)
        let system_rules = SystemEmailRules::from_db(&conn)
            .await
            .context("Failed to load system email rules")?;

        // Phase: Fetching emails from past N days
        scan_tracker.set_phase(user_id, ScanPhase::Fetching);
        let emails = fetch_emails_for_scan(
            &email_client,
            lookback_days,
            max_emails,
            user_id,
            &scan_tracker,
        )
        .await?;

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

        // Prune emails if token usage would exceed the batch limit
        let token_limit = cfg.initial_scan.batch_token_limit;
        let emails = prune_emails_to_token_limit(emails, token_limit)?;

        // Update tracker with total email count (after pruning)
        scan_tracker.set_total_emails(user_id, emails.len());

        // Build map of email_id -> email from (used for heuristics)
        let email_from_map: HashMap<String, String> = emails
            .iter()
            .filter_map(|e| e.from.as_ref().map(|from| (e.id.clone(), from.clone())))
            .collect();

        // Phase 1: Batch categorization with system rules
        scan_tracker.set_phase(user_id, ScanPhase::Categorizing { job_id: None });
        let track_fn = |completed: u64| {
            scan_tracker.set_processed_emails(user_id, completed as usize);
        };
        let cat_batch = run_categorization_batch(
            &http_client,
            &emails,
            &system_rules,
            &format!("initial_scan_cat_{}", user_id),
            Some(&track_fn),
        )
        .await?;

        // Update phase with job ID
        if !cat_batch.job_id.is_empty() {
            scan_tracker.set_phase(
                user_id,
                ScanPhase::Categorizing {
                    job_id: Some(cat_batch.job_id.clone()),
                },
            );
        }

        tracing::info!(
            "Categorization complete for user {}: {} results (job: {})",
            user_email,
            cat_batch.results.len(),
            cat_batch.job_id
        );

        // Build categorization results map (with heuristics)
        let categorization_map: HashMap<String, CategorizationResult> = cat_batch
            .results
            .into_iter()
            .map(|r| {
                let (email_rule, _) = find_matching_rule_system(
                    r.general_category.as_deref(),
                    &r.specific_category,
                    r.confidence,
                    &system_rules,
                    email_from_map.get(&r.email_id),
                );
                let result = CategorizationResult {
                    email_id: r.email_id.clone(),
                    category: email_rule.prompt_content.clone(),
                    confidence: r.confidence,
                    label: email_rule.mail_label.clone(),
                    email_rule,
                    source: CategorizationSource::System,
                };
                (r.email_id, result)
            })
            .collect();

        // Phase 2: Batch task extraction for emails that need it
        let emails_needing_tasks: Vec<&EmailScanData> = emails
            .iter()
            .filter(|e| {
                categorization_map
                    .get(&e.id)
                    .map(|r| r.email_rule.extract_tasks)
                    .unwrap_or(false)
            })
            .collect();

        let task_batch_results = if !emails_needing_tasks.is_empty() {
            scan_tracker.set_phase(user_id, ScanPhase::ExtractingTasks { job_id: None });
            tracing::info!(
                "Running task extraction for {} emails",
                emails_needing_tasks.len()
            );
            let task_batch = run_task_extraction_batch(
                &http_client,
                &emails_needing_tasks,
                &format!("initial_scan_task_{}", user_id),
                None,
            )
            .await?;

            // Update phase with job ID
            if !task_batch.job_id.is_empty() {
                scan_tracker.set_phase(
                    user_id,
                    ScanPhase::ExtractingTasks {
                        job_id: Some(task_batch.job_id),
                    },
                );
            }

            task_batch.results
        } else {
            Vec::new()
        };

        // Build map of email_id -> tasks
        let task_map: HashMap<String, Vec<ExtractedTask>> = task_batch_results
            .into_iter()
            .map(|r| (r.email_id, r.tasks))
            .collect();

        // Phase 3: Build processed email data
        let mut categorization_map = categorization_map;
        let processed_emails: Vec<ProcessedEmailData> = emails
            .into_iter()
            .filter_map(|email| {
                let result = categorization_map.remove(&email.id)?;
                let extracted_tasks = task_map.get(&email.id).cloned().unwrap_or_default();

                Some(ProcessedEmailData {
                    email_data: email,
                    category: result.label,
                    ai_answer: result.category,
                    ai_confidence: result.confidence,
                    extracted_tasks,
                })
            })
            .collect();

        // Phase 4: Batch insert in chunks
        scan_tracker.set_phase(user_id, ScanPhase::Inserting);
        let stored_count = batch_insert_processed_emails(&conn, user_id, processed_emails).await?;
        scan_tracker.set_processed_emails(user_id, stored_count);

        tracing::info!(
            "Initial scan complete for user {}: {} emails stored",
            user_email,
            stored_count
        );

        // Set is_initial_scan_complete = true
        UserCtrl::set_initial_scan_complete(&conn, user_id).await?;

        Ok::<(), anyhow::Error>(())
    };

    // Run the scan and handle errors
    match run_scan.await {
        Ok(()) => {
            scan_tracker.complete_scan(user_id);
            Ok(())
        }
        Err(e) => {
            scan_tracker.fail_scan(user_id, e.to_string());
            Err(e)
        }
    }
}

/// Estimate the token usage for a single email in the categorization batch.
fn estimate_email_tokens(email: &EmailScanData) -> usize {
    use crate::prompt::mistral::SYSTEM_PROMPT_TOKEN_ESTIMATE;

    let user_content = categorization_user_prompt(
        email.subject.as_deref().unwrap_or(""),
        email.from.as_deref().unwrap_or(""),
        email.body.as_deref().unwrap_or(""),
    );

    // Use the tokenizer to count user content tokens
    let user_tokens = tokenizer::token_count(&user_content).unwrap_or({
        // Fallback: rough estimate of 1 token per 4 characters
        user_content.len() / 4
    });

    // Add system prompt token estimate
    let system_tokens = *SYSTEM_PROMPT_TOKEN_ESTIMATE as usize;

    user_tokens + system_tokens
}

/// Estimate total token usage for all emails and prune if necessary.
/// Drops oldest emails first (by internal_date) until within the token limit.
/// Returns the pruned list of emails that fit within the token limit.
fn prune_emails_to_token_limit(
    emails: Vec<EmailScanData>,
    token_limit: usize,
) -> anyhow::Result<Vec<EmailScanData>> {
    // Calculate token usage for each email
    let mut emails_with_tokens: Vec<(EmailScanData, usize)> = emails
        .into_iter()
        .map(|e| {
            let tokens = estimate_email_tokens(&e);
            (e, tokens)
        })
        .collect();

    let total_tokens: usize = emails_with_tokens.iter().map(|(_, t)| *t).sum();

    if total_tokens <= token_limit {
        tracing::info!(
            "Estimated token usage: {} (limit: {}), no pruning needed",
            total_tokens,
            token_limit
        );
        return Ok(emails_with_tokens.into_iter().map(|(e, _)| e).collect());
    }

    tracing::warn!(
        "Estimated token usage {} exceeds limit {}, pruning emails",
        total_tokens,
        token_limit
    );

    // Sort by internal_date ascending (oldest first) so we remove oldest emails first
    emails_with_tokens.sort_by(|a, b| a.0.internal_date.cmp(&b.0.internal_date));

    let mut current_total = total_tokens;
    let mut pruned_count = 0;

    // Remove oldest emails until we're under the limit
    while current_total > token_limit && !emails_with_tokens.is_empty() {
        if let Some((_, tokens)) = emails_with_tokens.first() {
            current_total -= tokens;
            emails_with_tokens.remove(0);
            pruned_count += 1;
        }
    }

    if emails_with_tokens.is_empty() {
        bail!(
            "Cannot reduce token usage below limit {}. \
             All {} emails would need to be removed.",
            token_limit,
            pruned_count
        );
    }

    tracing::info!(
        "Pruned {} oldest emails to fit within token limit. \
         Remaining: {} emails, {} tokens",
        pruned_count,
        emails_with_tokens.len(),
        current_total
    );

    Ok(emails_with_tokens.into_iter().map(|(e, _)| e).collect())
}

/// Fetch emails from the past N days for initial scan
async fn fetch_emails_for_scan(
    email_client: &EmailClient,
    lookback_days: i64,
    max_emails: usize,
    user_id: i32,
    scan_tracker: &ScanTracker,
) -> anyhow::Result<Vec<EmailScanData>> {
    let mut all_message_ids = Vec::new();
    let mut next_page_token = None;
    let start_time = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(120);

    // Fetch message IDs from past N days
    loop {
        // Check for timeout
        if start_time.elapsed() >= timeout {
            tracing::warn!(
                "Fetch timeout reached after 2 minutes, proceeding with {} emails",
                all_message_ids.len()
            );
            break;
        }
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

        // Update scan tracker with current fetched count
        scan_tracker.set_fetch_total(user_id, all_message_ids.len());
        scan_tracker.set_total_emails(user_id, all_message_ids.len());

        if all_message_ids.len() >= max_emails {
            break;
        }

        next_page_token = response.next_page_token;
        if next_page_token.is_none() {
            tracing::debug!("No next page token, ending fetch loop");
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
    let track = |count: usize| {
        scan_tracker.increment_fetched(user_id, count);
    };
    let messages = email_client
        .get_messages_by_ids(&all_message_ids, MessageFormat::Raw, Some(&track))
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
        email::rules::EmailRule, model::labels::UtilityLabels,
        state::email_scanner::shared::find_matching_rule_system,
    };

    use super::*;

    #[test]
    fn test_find_matching_rule_high_confidence() {
        let rules = SystemEmailRules::new(vec![EmailRule {
            prompt_content: "Newsletter".to_string(),
            mail_label: "newsletter".to_string(),
            extract_tasks: false,
            priority: 3,
        }]);

        let (rule, _) = find_matching_rule_system(
            Some("newsletter"),
            "Newsletter",
            0.95,
            &rules,
            Some("Someone"),
        );
        assert_eq!(rule.mail_label, "newsletter");
    }

    #[test]
    fn test_find_matching_rule_low_confidence() {
        let rules = SystemEmailRules::new(vec![EmailRule {
            prompt_content: "Newsletter".to_string(),
            mail_label: "newsletter".to_string(),
            extract_tasks: false,
            priority: 3,
        }]);

        let (rule, _) = find_matching_rule_system(
            Some("newsletter"),
            "Newsletter",
            0.3,
            &rules,
            Some("Someone"),
        );
        assert_eq!(rule.mail_label, UtilityLabels::Uncategorized.as_str());
    }

    #[test]
    fn test_find_matching_rule_unknown_category() {
        let rules = SystemEmailRules::new(vec![EmailRule {
            prompt_content: "Newsletter".to_string(),
            mail_label: "newsletter".to_string(),
            extract_tasks: false,
            priority: 3,
        }]);

        let (rule, _) = find_matching_rule_system(
            Some("Something"),
            "SomethingElse",
            0.95,
            &rules,
            Some("Someone"),
        );
        assert_eq!(rule.mail_label, UtilityLabels::Uncategorized.as_str());
    }
}
