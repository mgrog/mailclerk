//! Mistral Batch Inference API Client
//!
//! This module provides functionality to process large batches of emails
//! using Mistral's batch inference API for cost-effective processing.
//! Uses inline batching (for under 10k requests) rather than file uploads.

use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;

use crate::{server_config::cfg, HttpClient};

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
    pub category: String,
    pub confidence: f32,
    pub token_usage: i64,
}

/// Parsed task extraction result
#[derive(Debug, Clone)]
pub struct TaskExtractionResult {
    pub email_id: String,
    pub tasks: Vec<super::task_extraction::ExtractedTask>,
    pub token_usage: i64,
}

// ============================================================================
// API Client Functions
// ============================================================================

/// Create a batch job with inline requests (no file upload needed)
pub async fn create_inline_batch_job(
    http_client: &HttpClient,
    requests: Vec<BatchRequest>,
) -> anyhow::Result<BatchJob> {
    let resp = http_client
        .post(BATCH_JOBS_ENDPOINT)
        .bearer_auth(&cfg.api.key)
        .json(&json!({
            "requests": requests,
            "model": &cfg.model.id,
            "endpoint": "/v1/chat/completions",
            "metadata": {
                "purpose": "initial_scan"
            }
        }))
        .send()
        .await
        .context("Failed to create batch job")?;

    if !resp.status().is_success() {
        let error_body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("Batch job creation failed: {}", error_body));
    }

    resp.json::<BatchJob>()
        .await
        .context("Failed to parse batch job response")
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
pub async fn wait_for_job(http_client: &HttpClient, job_id: &str) -> anyhow::Result<BatchJob> {
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

    let content = resp.text().await.context("Failed to read results content")?;

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
            let parsed: serde_json::Value = serde_json::from_str(content).ok()?;
            let category = parsed.get("category")?.as_str()?.to_string();
            let confidence = parsed.get("confidence")?.as_f64()? as f32;

            Some(CategoryResult {
                email_id: result.custom_id,
                category,
                confidence,
                token_usage: total_tokens,
            })
        })
        .collect()
}

/// Parse task extraction results from batch output
pub fn parse_task_extraction_results(results: Vec<BatchResultLine>) -> Vec<TaskExtractionResult> {
    use super::task_extraction::ExtractedTask;

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

/// Run a complete batch job: create with inline requests, wait, download results
pub async fn run_batch_job(
    http_client: &HttpClient,
    requests: Vec<BatchRequest>,
    job_name: &str,
) -> anyhow::Result<Vec<BatchResultLine>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }

    tracing::info!(
        "Starting batch job '{}' with {} requests",
        job_name,
        requests.len()
    );

    // Create job with inline requests
    let job = create_inline_batch_job(http_client, requests).await?;
    tracing::info!("Created batch job: {} (status: {:?})", job.id, job.status);

    // Wait for completion
    let completed_job = wait_for_job(http_client, &job.id).await?;

    if !completed_job.status.is_success() {
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

    Ok(results)
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
                    "choices": [{"message": {"content": "{\"category\": \"Newsletter\", \"confidence\": 0.95}"}}],
                    "usage": {"total_tokens": 100}
                }
            },
            "error": null
        }"#;

        let result: BatchResultLine = serde_json::from_str(result_json).unwrap();
        let parsed = parse_categorization_results(vec![result]);

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].email_id, "email456");
        assert_eq!(parsed[0].category, "Newsletter");
        assert!((parsed[0].confidence - 0.95).abs() < 0.01);
    }
}
