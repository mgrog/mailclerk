use anyhow::{anyhow, Context};
use indoc::formatdoc;
use reqwest::StatusCode;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::email::simplified_message::SimplifiedMessage;
use crate::rate_limiters;
use crate::HttpClient;
use crate::{
    error::{AppError, AppResult},
    server_config::cfg,
};

use super::mistral::ChatApiResponseOrError;

const AI_ENDPOINT: &str = "https://api.mistral.ai/v1/chat/completions";

pub fn system_prompt() -> String {
    formatdoc! {r#"
        You are a helpful assistant that extracts actionable tasks from emails.
        Analyze the email content and identify any tasks, action items, or to-dos that the recipient should complete.

        For each task found, extract:
        - title: A brief, actionable title for the task
        - description: Additional context or details (optional)
        - due_date: Any mentioned deadline in ISO 8601 format YYYY-MM-DD (optional)
        - priority: "high", "medium", or "low" based on urgency indicators (optional)

        If no actionable tasks are found, return an empty array.
        Respond only with a JSON object containing a "tasks" array. Do not provide explanations."#}
}

/// Build the user prompt for task extraction.
/// This is the prompt template used in both batch and real-time task extraction.
pub fn task_extraction_user_prompt(subject: &str, body: &str) -> String {
    format!(
        r#"Extract any actionable tasks from the following email.
                <subject>{}</subject>
                <body>{}</body>"#,
        subject, body
    )
}

pub async fn extract_tasks_from_email(
    http_client: &HttpClient,
    rate_limiters: &rate_limiters::RateLimiters,
    email_message: &SimplifiedMessage,
) -> AppResult<TaskExtractionResponse> {
    let subject = email_message.subject.as_ref().map_or("", |s| s.as_str());
    let body = email_message.body.as_ref().map_or("", |s| s.as_str());
    let user_content = task_extraction_user_prompt(subject, body);

    let resp = http_client
        .post(AI_ENDPOINT)
        .bearer_auth(&cfg.api.key)
        .json(&json!(
          {
            "model": &cfg.model.id,
            "temperature": cfg.model.temperature,
            "messages": [
              {
                "role": "system",
                "content": system_prompt()
              },
              {
                "role": "user",
                "content": user_content
              }
            ],
            "response_format": { "type": "json_object" }
          }
        ))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| {
            if let Some(status) = e.status() {
                match status {
                    StatusCode::BAD_REQUEST => AppError::BadRequest(e.to_string()),
                    StatusCode::REQUEST_TIMEOUT => AppError::RequestTimeout,
                    StatusCode::TOO_MANY_REQUESTS => AppError::TooManyRequests,
                    _ => AppError::Internal(e.into()),
                }
            } else {
                AppError::Internal(e.into())
            }
        })?;

    let parsed = serde_json::from_value::<ChatApiResponseOrError>(resp.clone())
        .context(format!("Could not parse chat response: {}", resp))?;

    let parsed = match parsed {
        ChatApiResponseOrError::Error(error) => {
            if error.message == "Requests rate limit exceeded" {
                rate_limiters.trigger_backoff();
            }
            return Err(anyhow!("Chat API error: {:?}", error).into());
        }
        ChatApiResponseOrError::Response(parsed) => parsed,
    };

    let (tasks, usage) = {
        let choice = parsed.choices.first().context("No choices in response")?;
        let usage = parsed.usage;
        match serde_json::from_str::<TasksJson>(&choice.message.content) {
            Ok(TasksJson { tasks }) => Ok::<_, AppError>((tasks, usage)),
            Err(e) => {
                tracing::warn!("Could not parse task extraction JSON response: {:?}", e);
                Ok((vec![], usage))
            }
        }
    }?;

    Ok(TaskExtractionResponse {
        tasks,
        token_usage: usage.total_tokens,
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskExtractionResponse {
    pub tasks: Vec<ExtractedTask>,
    pub token_usage: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(rename_all = "camelCase")]
pub struct ExtractedTask {
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TasksJson {
    tasks: Vec<ExtractedTask>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_prompt() {
        let prompt = system_prompt();
        assert!(prompt.contains("actionable tasks"));
        assert!(prompt.contains("title"));
        assert!(prompt.contains("due_date"));
        assert!(prompt.contains("priority"));
    }

    #[test]
    fn test_parse_tasks_json() {
        let json_str = r#"{"tasks": [{"title": "Review document", "description": "Review the Q4 report", "due_date": "2024-01-15", "priority": "high"}]}"#;
        let parsed: TasksJson = serde_json::from_str(json_str).unwrap();
        assert_eq!(parsed.tasks.len(), 1);
        assert_eq!(parsed.tasks[0].title, "Review document");
        assert_eq!(parsed.tasks[0].priority, Some("high".to_string()));
    }

    #[test]
    fn test_parse_empty_tasks() {
        let json_str = r#"{"tasks": []}"#;
        let parsed: TasksJson = serde_json::from_str(json_str).unwrap();
        assert!(parsed.tasks.is_empty());
    }
}
