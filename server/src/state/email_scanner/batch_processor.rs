//! Batch Processing Module
//!
//! Provides reusable functions for batch processing emails using Mistral's batch inference API.
//! This module can be used by initial scan, re-categorization, and other mass scanning operations.

use chrono::NaiveDateTime;
use entity::processed_email;
use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait};

use crate::{
    db_core::prelude::*,
    email::rules::UserEmailRules,
    prompt::{mistral, mistral_batch, task_extraction},
    state::email_scanner::shared::{EmailScanData, ProcessedEmailData},
    HttpClient,
};

pub use crate::prompt::mistral::categorization_user_prompt;
pub use crate::prompt::task_extraction::task_extraction_user_prompt;

/// Result from a categorization batch including job ID
pub struct CategorizationBatchResult {
    pub job_id: String,
    pub results: Vec<mistral_batch::CategoryResult>,
}

/// Result from a task extraction batch including job ID
pub struct TaskExtractionBatchResult {
    pub job_id: String,
    pub results: Vec<mistral_batch::TaskExtractionResult>,
}

/// Chunk size for batch database inserts
pub const DB_INSERT_CHUNK_SIZE: usize = 1000;

/// Run batch categorization for a collection of emails.
///
/// This function creates batch requests for each email and submits them
/// to Mistral's batch API for categorization.
///
/// # Arguments
/// * `http_client` - HTTP client for API requests
/// * `emails` - Slice of emails to categorize
/// * `user_email_rules` - User's email rules for generating the system prompt
/// * `job_name` - Name for the batch job (for logging/tracking)
///
/// # Returns
/// Categorization batch result containing job_id and results with email_id, category, and confidence
pub async fn run_categorization_batch(
    http_client: &HttpClient,
    emails: &[EmailScanData],
    user_email_rules: &UserEmailRules,
    job_name: &str,
) -> anyhow::Result<CategorizationBatchResult> {
    let system_prompt = mistral::system_prompt(user_email_rules.get_prompt_categories());

    let requests: Vec<mistral_batch::BatchRequest> = emails
        .iter()
        .map(|email| {
            let user_content = categorization_user_prompt(
                email.subject.as_deref().unwrap_or(""),
                email.body.as_deref().unwrap_or(""),
            );

            mistral_batch::BatchRequest::for_categorization(
                email.id.clone(),
                system_prompt.clone(),
                user_content,
            )
        })
        .collect();

    let batch_result = mistral_batch::run_batch_job(http_client, requests, job_name).await?;

    Ok(CategorizationBatchResult {
        job_id: batch_result.job_id,
        results: mistral_batch::parse_categorization_results(batch_result.results),
    })
}

/// Run batch task extraction for a collection of emails.
///
/// This function creates batch requests for task extraction and submits them
/// to Mistral's batch API.
///
/// # Arguments
/// * `http_client` - HTTP client for API requests
/// * `emails` - Slice of email references to extract tasks from
/// * `job_name` - Name for the batch job (for logging/tracking)
///
/// # Returns
/// Task extraction batch result containing job_id and results with email_id and extracted tasks
pub async fn run_task_extraction_batch(
    http_client: &HttpClient,
    emails: &[&EmailScanData],
    job_name: &str,
) -> anyhow::Result<TaskExtractionBatchResult> {
    let system_prompt = task_extraction::system_prompt();

    let requests: Vec<mistral_batch::BatchRequest> = emails
        .iter()
        .map(|email| {
            let user_content = task_extraction_user_prompt(
                email.subject.as_deref().unwrap_or(""),
                email.body.as_deref().unwrap_or(""),
            );

            mistral_batch::BatchRequest::for_task_extraction(
                email.id.clone(),
                system_prompt.clone(),
                user_content,
            )
        })
        .collect();

    let batch_result = mistral_batch::run_batch_job(http_client, requests, job_name).await?;

    Ok(TaskExtractionBatchResult {
        job_id: batch_result.job_id,
        results: mistral_batch::parse_task_extraction_results(batch_result.results),
    })
}

/// Batch insert processed emails into the database in chunks.
///
/// This function inserts emails in chunks of `DB_INSERT_CHUNK_SIZE` to avoid
/// memory issues with large batches. Duplicates are skipped using ON CONFLICT.
///
/// # Arguments
/// * `conn` - Database connection
/// * `user_id` - ID of the user who owns these emails
/// * `processed_emails` - Vector of processed email data to insert
///
/// # Returns
/// Total number of emails inserted
pub async fn batch_insert_processed_emails(
    conn: &DatabaseConnection,
    user_id: i32,
    processed_emails: Vec<ProcessedEmailData>,
) -> anyhow::Result<usize> {
    let mut total_inserted = 0;

    for chunk in processed_emails.chunks(DB_INSERT_CHUNK_SIZE) {
        let active_models: Vec<processed_email::ActiveModel> = chunk
            .iter()
            .map(|data| build_active_model(user_id, data))
            .collect();

        let count = active_models.len();

        // Use insert_many with on_conflict to skip duplicates
        match ProcessedEmail::insert_many(active_models)
            .on_conflict(
                sea_orm::sea_query::OnConflict::column(processed_email::Column::Id)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(conn)
            .await
        {
            Ok(_) => {
                total_inserted += count;
                tracing::debug!("Inserted chunk of {} emails", count);
            }
            Err(e) => {
                // Log but continue - some might have been duplicates
                tracing::warn!("Error inserting chunk: {:?}", e);
            }
        }
    }

    Ok(total_inserted)
}

/// Build an ActiveModel for a processed email.
///
/// This function converts `ProcessedEmailData` into a SeaORM `ActiveModel`
/// ready for database insertion.
///
/// # Arguments
/// * `user_id` - ID of the user who owns this email
/// * `data` - Processed email data
///
/// # Returns
/// ActiveModel ready for insertion
pub fn build_active_model(user_id: i32, data: &ProcessedEmailData) -> processed_email::ActiveModel {
    let closest_due_date = data
        .extracted_tasks
        .iter()
        .filter_map(|t| {
            t.due_date
                .as_ref()
                .and_then(|d| d.parse::<NaiveDateTime>().ok())
        })
        .min();

    let extracted_tasks = if data.extracted_tasks.is_empty() {
        ActiveValue::NotSet
    } else {
        let json_tasks: Vec<sea_orm::JsonValue> = data
            .extracted_tasks
            .iter()
            .map(|t| serde_json::json!(t))
            .collect();
        ActiveValue::Set(Some(json_tasks))
    };

    let is_read = !data.email_data.label_ids.contains(&"UNREAD".to_string());

    processed_email::ActiveModel {
        id: ActiveValue::Set(data.email_data.id.clone()),
        thread_id: ActiveValue::Set(data.email_data.thread_id.clone()),
        user_id: ActiveValue::Set(user_id),
        category: ActiveValue::Set(data.category.clone()),
        ai_answer: ActiveValue::Set(data.ai_answer.clone()),
        ai_confidence: ActiveValue::Set(data.ai_confidence.to_string()),
        processed_at: ActiveValue::NotSet,
        due_date: ActiveValue::Set(closest_due_date),
        extracted_tasks,
        history_id: ActiveValue::Set(sea_orm::prelude::Decimal::from(data.email_data.history_id)),
        is_read: ActiveValue::Set(is_read),
        tasks_done: ActiveValue::Set(false),
        has_new_reply: ActiveValue::Set(false),
        is_thread: ActiveValue::Set(false),
        from: ActiveValue::Set(data.email_data.from.clone()),
        subject: ActiveValue::Set(data.email_data.subject.clone()),
        snippet: ActiveValue::Set(data.email_data.snippet.clone()),
        internal_date: ActiveValue::Set(data.email_data.internal_date),
    }
}
