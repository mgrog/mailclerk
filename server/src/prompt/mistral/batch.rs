//! Mistral Batch Inference API Client
//!
//! This module provides functionality to process large batches of emails
//! using Mistral's batch inference API for cost-effective processing.
//! Uses file upload for batch requests.

use anyhow::{anyhow, Context};
use reqwest::multipart;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use thiserror::Error;

use crate::{server_config::cfg, HttpClient};

use super::parse_category_answer;

// ============================================================================
// Error Types
// ============================================================================

/// Structured error response from Mistral API
#[derive(Debug, Clone, Deserialize)]
pub struct MistralApiError {
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: Option<String>,
    pub param: Option<String>,
    pub code: Option<String>,
}

/// Errors that can occur when creating an inline batch job
#[derive(Debug, Error)]
pub enum CreateBatchJobError {
    #[error("Request failed: {0}")]
    RequestFailed(#[from] reqwest::Error),

    #[error("Invalid request: {0}")]
    InvalidRequest(MistralApiError),

    #[error("Authentication failed: {0}")]
    AuthenticationError(MistralApiError),

    #[error("Rate limited: {0}")]
    RateLimited(MistralApiError),

    #[error("Server error: {0}")]
    ServerError(MistralApiError),

    #[error("API error (HTTP {status}): {message}")]
    ApiError { status: u16, message: String },

    #[error("Failed to parse response: {0}")]
    ParseError(#[from] serde_json::Error),
}

impl std::fmt::Display for MistralApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)?;
        if let Some(ref param) = self.param {
            write!(f, " (param: {})", param)?;
        }
        if let Some(ref code) = self.code {
            write!(f, " [code: {}]", code)?;
        }
        Ok(())
    }
}

const BATCH_JOBS_ENDPOINT: &str = "https://api.mistral.ai/v1/batch/jobs";
const FILES_ENDPOINT: &str = "https://api.mistral.ai/v1/files";

/// Maximum time to wait for a batch job to complete (30 minutes)
const JOB_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// Polling interval for job status
const POLL_INTERVAL: Duration = Duration::from_secs(10);

// ============================================================================
// Request Types
// ============================================================================

/// A single message in a chat completion request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

/// The body of a chat completion request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ResponseFormat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseFormat {
    #[serde(rename = "type")]
    pub format_type: String,
}

/// A single batch request for inline batching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRequest {
    pub custom_id: String,
    pub body: ChatRequestBody,
}

impl BatchRequest {
    /// Create a new batch request for email categorization
    pub fn for_categorization(
        email_id: String,
        system_prompt: String,
        user_content: String,
    ) -> Self {
        Self {
            custom_id: email_id,
            body: ChatRequestBody {
                model: None, // Model is specified at job level
                messages: vec![
                    ChatMessage {
                        role: "system".to_string(),
                        content: system_prompt,
                    },
                    ChatMessage {
                        role: "user".to_string(),
                        content: user_content,
                    },
                ],
                temperature: Some(cfg.model.temperature),
                max_tokens: Some(100),
                response_format: Some(ResponseFormat {
                    format_type: "json_object".to_string(),
                }),
            },
        }
    }

    /// Create a new batch request for task extraction
    pub fn for_task_extraction(
        email_id: String,
        system_prompt: String,
        user_content: String,
    ) -> Self {
        Self {
            custom_id: email_id,
            body: ChatRequestBody {
                model: None,
                messages: vec![
                    ChatMessage {
                        role: "system".to_string(),
                        content: system_prompt,
                    },
                    ChatMessage {
                        role: "user".to_string(),
                        content: user_content,
                    },
                ],
                temperature: Some(cfg.model.temperature),
                max_tokens: Some(500), // Task extraction may need more tokens
                response_format: Some(ResponseFormat {
                    format_type: "json_object".to_string(),
                }),
            },
        }
    }
}

// ============================================================================
// Response Types
// ============================================================================

/// Batch job status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BatchJobStatus {
    Queued,
    Running,
    Success,
    Failed,
    TimeoutExceeded,
    CancellationRequested,
    Cancelled,
}

impl BatchJobStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            BatchJobStatus::Success
                | BatchJobStatus::Failed
                | BatchJobStatus::TimeoutExceeded
                | BatchJobStatus::Cancelled
        )
    }

    pub fn is_success(&self) -> bool {
        matches!(self, BatchJobStatus::Success)
    }
}

/// Batch job details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchJob {
    pub id: String,
    pub status: BatchJobStatus,
    #[serde(default)]
    pub input_files: Vec<String>,
    pub output_file: Option<String>,
    pub error_file: Option<String>,
    pub model: String,
    pub endpoint: String,
    #[serde(default)]
    pub total_requests: u64,
    #[serde(default)]
    pub completed_requests: u64,
    #[serde(default)]
    pub failed_requests: u64,
}

/// A single result from the batch output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResultLine {
    pub id: String,
    pub custom_id: String,
    pub response: BatchResultResponse,
    pub error: Option<BatchResultError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResultResponse {
    pub status_code: u16,
    pub body: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResultError {
    pub message: String,
}

/// Parsed categorization result
#[derive(Debug, Clone)]
pub struct CategoryResult {
    pub email_id: String,
    pub general_category: Option<String>,
    pub specific_category: String,
    pub confidence: f32,
    pub token_usage: i64,
}

/// Parsed task extraction result
#[derive(Debug, Clone)]
pub struct TaskExtractionResult {
    pub email_id: String,
    pub tasks: Vec<super::super::task_extraction::ExtractedTask>,
    pub token_usage: i64,
}

// ============================================================================
// API Client Functions
// ============================================================================

/// Response from file upload endpoint
#[derive(Debug, Clone, Deserialize)]
struct FileUploadResponse {
    id: String,
}

/// Create JSONL content from batch requests (in memory)
fn create_jsonl_content(requests: &[BatchRequest]) -> Result<Vec<u8>, serde_json::Error> {
    let mut content = Vec::new();
    for request in requests {
        let line = serde_json::to_string(request)?;
        content.extend_from_slice(line.as_bytes());
        content.push(b'\n');
    }
    Ok(content)
}

/// Upload JSONL content to Mistral's files API
async fn upload_batch_file(
    http_client: &HttpClient,
    jsonl_content: Vec<u8>,
) -> Result<String, CreateBatchJobError> {
    let file_part = multipart::Part::bytes(jsonl_content)
        .file_name("batch.jsonl")
        .mime_str("application/jsonl")
        .map_err(|e| CreateBatchJobError::ApiError {
            status: 0,
            message: format!("Failed to create multipart: {}", e),
        })?;

    let form = multipart::Form::new()
        .text("purpose", "batch")
        .part("file", file_part);

    let resp = http_client
        .post(FILES_ENDPOINT)
        .bearer_auth(&cfg.api.key)
        .multipart(form)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let error_body = resp.text().await.unwrap_or_default();

        if let Ok(api_error) = serde_json::from_str::<MistralApiError>(&error_body) {
            return Err(match status {
                400 => CreateBatchJobError::InvalidRequest(api_error),
                401 => CreateBatchJobError::AuthenticationError(api_error),
                429 => CreateBatchJobError::RateLimited(api_error),
                500..=599 => CreateBatchJobError::ServerError(api_error),
                _ => CreateBatchJobError::ApiError {
                    status,
                    message: api_error.message,
                },
            });
        }

        return Err(CreateBatchJobError::ApiError {
            status,
            message: error_body,
        });
    }

    let upload_response: FileUploadResponse = resp.json().await?;
    Ok(upload_response.id)
}

/// Create a batch job by uploading a JSONL file
pub async fn create_batch_job(
    http_client: &HttpClient,
    requests: Vec<BatchRequest>,
) -> Result<BatchJob, CreateBatchJobError> {
    // Create JSONL content in memory
    let jsonl_content = create_jsonl_content(&requests)?;

    // Upload to Mistral
    let file_id = upload_batch_file(http_client, jsonl_content).await?;
    tracing::debug!("Uploaded batch file with id: {}", file_id);

    // Create batch job with the uploaded file
    let resp = http_client
        .post(BATCH_JOBS_ENDPOINT)
        .bearer_auth(&cfg.api.key)
        .header("Accept", "application/json")
        .json(&json!({
            "input_files": [file_id],
            "model": &cfg.model.id,
            "endpoint": "/v1/chat/completions",
            "metadata": {
                "purpose": "initial_scan"
            }
        }))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let error_body = resp.text().await.unwrap_or_default();

        // Try to parse as JSON error from Mistral API
        if let Ok(api_error) = serde_json::from_str::<MistralApiError>(&error_body) {
            return Err(match status {
                400 => CreateBatchJobError::InvalidRequest(api_error),
                401 => CreateBatchJobError::AuthenticationError(api_error),
                429 => CreateBatchJobError::RateLimited(api_error),
                500..=599 => CreateBatchJobError::ServerError(api_error),
                _ => CreateBatchJobError::ApiError {
                    status,
                    message: api_error.message,
                },
            });
        }

        return Err(CreateBatchJobError::ApiError {
            status,
            message: error_body,
        });
    }

    Ok(resp.json::<BatchJob>().await?)
}

/// Get the current status of a batch job
pub async fn get_batch_job(http_client: &HttpClient, job_id: &str) -> anyhow::Result<BatchJob> {
    let url = format!("{}/{}", BATCH_JOBS_ENDPOINT, job_id);

    let resp = http_client
        .get(&url)
        .bearer_auth(&cfg.api.key)
        .send()
        .await
        .context("Failed to get batch job status")?;

    if !resp.status().is_success() {
        let error_body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("Failed to get batch job: {}", error_body));
    }

    resp.json::<BatchJob>()
        .await
        .context("Failed to parse batch job response")
}

/// Wait for a batch job to complete, polling at regular intervals
///
/// Optionally accepts a tracking function that will be called with the number
/// of completed requests on each poll.
pub async fn wait_for_job(
    http_client: &HttpClient,
    job_id: &str,
    track_fn: Option<&(dyn Fn(u64) + Send + Sync)>,
) -> anyhow::Result<BatchJob> {
    let start = std::time::Instant::now();

    loop {
        let job = get_batch_job(http_client, job_id).await?;

        tracing::debug!(
            "Batch job {} status: {:?} ({}/{} completed)",
            job_id,
            job.status,
            job.completed_requests,
            job.total_requests
        );

        if let Some(f) = track_fn {
            f(job.completed_requests);
        }

        if job.status.is_terminal() {
            return Ok(job);
        }

        if start.elapsed() > JOB_TIMEOUT {
            return Err(anyhow!(
                "Batch job {} timed out after {:?}",
                job_id,
                JOB_TIMEOUT
            ));
        }

        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Download the results from a completed batch job
pub async fn download_results(
    http_client: &HttpClient,
    file_id: &str,
) -> anyhow::Result<Vec<BatchResultLine>> {
    let url = format!("{}/{}/content", FILES_ENDPOINT, file_id);

    let resp = http_client
        .get(&url)
        .bearer_auth(&cfg.api.key)
        .send()
        .await
        .context("Failed to download results file")?;

    if !resp.status().is_success() {
        let error_body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("Failed to download results: {}", error_body));
    }

    let content = resp
        .text()
        .await
        .context("Failed to read results content")?;

    // Parse JSONL
    let results: Vec<BatchResultLine> = content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
        .filter_map(|(i, line)| {
            serde_json::from_str(line)
                .map_err(|e| {
                    tracing::warn!("Failed to parse result line {}: {:?}", i, e);
                    e
                })
                .ok()
        })
        .collect();

    Ok(results)
}

/// Delete a file from Mistral's files API
async fn delete_file(http_client: &HttpClient, file_id: &str) {
    let url = format!("{}/{}", FILES_ENDPOINT, file_id);

    match http_client
        .delete(&url)
        .bearer_auth(&cfg.api.key)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            tracing::debug!("Deleted file: {}", file_id);
        }
        Ok(resp) => {
            tracing::warn!("Failed to delete file {}: HTTP {}", file_id, resp.status());
        }
        Err(e) => {
            tracing::warn!("Failed to delete file {}: {}", file_id, e);
        }
    }
}

/// Parse categorization results from batch output
pub fn parse_categorization_results(results: Vec<BatchResultLine>) -> Vec<CategoryResult> {
    results
        .into_iter()
        .filter_map(|result| {
            if result.error.is_some() || result.response.status_code != 200 {
                tracing::warn!(
                    "Skipping failed result for email {}: {:?}",
                    result.custom_id,
                    result.error
                );
                return None;
            }

            let body = &result.response.body;
            let choices = body.get("choices")?.as_array()?;
            let choice = choices.first()?;
            let content = choice.get("message")?.get("content")?.as_str()?;
            let usage = body.get("usage")?;
            let total_tokens = usage.get("total_tokens")?.as_i64()?;

            // Parse the JSON response content
            let answer = parse_category_answer(content)?;

            Some(CategoryResult {
                email_id: result.custom_id,
                general_category: answer.general_category,
                specific_category: answer.specific_category,
                confidence: answer.confidence,
                token_usage: total_tokens,
            })
        })
        .collect()
}

/// Parse task extraction results from batch output
pub fn parse_task_extraction_results(results: Vec<BatchResultLine>) -> Vec<TaskExtractionResult> {
    use super::super::task_extraction::ExtractedTask;

    results
        .into_iter()
        .filter_map(|result| {
            if result.error.is_some() || result.response.status_code != 200 {
                tracing::warn!(
                    "Skipping failed task extraction for email {}: {:?}",
                    result.custom_id,
                    result.error
                );
                return None;
            }

            let body = &result.response.body;
            let choices = body.get("choices")?.as_array()?;
            let choice = choices.first()?;
            let content = choice.get("message")?.get("content")?.as_str()?;
            let usage = body.get("usage")?;
            let total_tokens = usage.get("total_tokens")?.as_i64()?;

            // Parse the JSON response content
            #[derive(Deserialize)]
            struct TasksJson {
                tasks: Vec<ExtractedTask>,
            }

            let parsed: TasksJson = serde_json::from_str(content).ok()?;

            Some(TaskExtractionResult {
                email_id: result.custom_id,
                tasks: parsed.tasks,
                token_usage: total_tokens,
            })
        })
        .collect()
}

// ============================================================================
// High-Level Batch Processing
// ============================================================================

/// Result from a batch job including job metadata
pub struct BatchJobResult {
    pub job_id: String,
    pub results: Vec<BatchResultLine>,
}

/// Run a complete batch job: upload file, create job, wait, download results, cleanup
///
/// Optionally accepts a tracking function that will be called with the number
/// of completed requests on each poll while waiting for the job.
pub async fn run_batch_job(
    http_client: &HttpClient,
    requests: Vec<BatchRequest>,
    job_name: &str,
    track_fn: Option<&(dyn Fn(u64) + Send + Sync)>,
) -> anyhow::Result<BatchJobResult> {
    if requests.is_empty() {
        return Ok(BatchJobResult {
            job_id: String::new(),
            results: Vec::new(),
        });
    }

    tracing::info!(
        "Starting batch job '{}' with {} requests",
        job_name,
        requests.len()
    );

    // Create job with file upload
    let job = create_batch_job(http_client, requests).await?;
    let job_id = job.id.clone();
    let input_files = job.input_files.clone();
    tracing::info!("Created batch job: {} (status: {:?})", job.id, job.status);

    // Wait for completion
    let completed_job = wait_for_job(http_client, &job.id, track_fn).await?;

    if !completed_job.status.is_success() {
        // Clean up input files even on failure
        let deletions = input_files.iter().map(|id| delete_file(http_client, id));
        futures::future::join_all(deletions).await;
        return Err(anyhow!(
            "Batch job {} failed with status: {:?}",
            job.id,
            completed_job.status
        ));
    }

    tracing::info!(
        "Batch job {} completed: {}/{} succeeded",
        job.id,
        completed_job.completed_requests,
        completed_job.total_requests
    );

    // Download results
    let output_file_id = completed_job
        .output_file
        .ok_or_else(|| anyhow!("Completed job has no output file"))?;

    let results = download_results(http_client, &output_file_id).await?;
    tracing::info!("Downloaded {} results", results.len());

    // Clean up files (input and output)
    let deletions = input_files
        .iter()
        .map(|id| id.as_str())
        .chain(std::iter::once(output_file_id.as_str()))
        .map(|id| delete_file(http_client, id));
    futures::future::join_all(deletions).await;

    Ok(BatchJobResult { job_id, results })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_request_serialization() {
        let request = BatchRequest::for_categorization(
            "email123".to_string(),
            "System prompt".to_string(),
            "User content".to_string(),
        );

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("email123"));
        assert!(json.contains("System prompt"));
        assert!(json.contains("User content"));
    }

    #[test]
    fn test_batch_job_status() {
        assert!(BatchJobStatus::Success.is_terminal());
        assert!(BatchJobStatus::Failed.is_terminal());
        assert!(!BatchJobStatus::Running.is_terminal());
        assert!(!BatchJobStatus::Queued.is_terminal());
    }

    #[test]
    fn test_parse_categorization_result() {
        let result_json = r#"{
            "id": "result123",
            "custom_id": "email456",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{"message": {"content": "{\"general_category\": \"newsletter\", \"specific_category\": \"Newsletter\", \"confidence\": 0.95}"}}],
                    "usage": {"total_tokens": 100}
                }
            },
            "error": null
        }"#;

        let result: BatchResultLine = serde_json::from_str(result_json).unwrap();
        let parsed = parse_categorization_results(vec![result]);

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].email_id, "email456");
        assert_eq!(parsed[0].general_category.as_ref().unwrap(), "newsletter");
        assert_eq!(parsed[0].specific_category, "Newsletter");
        assert!((parsed[0].confidence - 0.95).abs() < 0.01);
    }
}
