extern crate google_gmail1 as gmail1;

use anyhow::{anyhow, Context};
use chrono::{FixedOffset, Utc};
use futures::future::join_all;
use google_gmail1::api::{
    Label, ListLabelsResponse, ListMessagesResponse, ListThreadsResponse, Message, Profile, Thread,
    WatchResponse,
};
use lazy_static::lazy_static;
use leaky_bucket::RateLimiter;
use lib_email_clients::gmail::api_quota::{GMAIL_API_QUOTA, GMAIL_QUOTA_PER_SECOND};
use lib_email_clients::gmail::label_colors::GmailLabelColorMap;
use once_cell::sync::Lazy;
use serde_json::json;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::{collections::HashSet, time::Duration};
use strum::IntoEnumIterator;
use uuid::Uuid;

use crate::model::labels::UtilityLabels;
use crate::model::user::UserAccessCtrl;
use crate::{
    db_core::prelude::*,
    model::{
        labels,
        user::{AccountAccess, EmailAddress, Id},
    },
    server_config::cfg,
    HttpClient,
};

use super::simplified_message::SimplifiedMessage;

/// Gmail API error response structure
#[derive(Debug, Clone, serde::Deserialize)]
pub struct GmailApiError {
    pub error: GmailApiErrorDetail,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct GmailApiErrorDetail {
    pub code: u16,
    pub message: String,
    #[serde(default)]
    pub errors: Vec<GmailApiErrorItem>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct GmailApiErrorItem {
    pub message: String,
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Result of a batch message fetch - either a successful message or an error
#[derive(Debug)]
pub enum BatchMessageResult {
    Success(Message),
    Error {
        message_id: String,
        error: GmailApiError,
    },
}

#[derive(Default)]
/// Filter and paging options for message list
pub struct MessageListOptions {
    /// Default is any non-mailclerk label
    pub label_filter: Option<String>,
    /// Messages more recent than this duration will be returned
    pub more_recent_than: Option<chrono::Duration>,
    pub older_than: Option<chrono::Duration>,
    pub categories: Option<Vec<String>>,
    pub page_token: Option<String>,
    pub max_results: Option<u32>,
}

#[derive(Default)]
/// Filter and paging options for thread list
pub struct ThreadListOptions {
    /// Gmail search query (e.g., "is:unread", "from:example@gmail.com")
    pub query: Option<String>,
    /// Only return threads with labels that match all of the specified label IDs
    pub label_ids: Option<Vec<String>>,
    /// Page token for pagination
    pub page_token: Option<String>,
    /// Maximum number of threads to return (default: 100)
    pub max_results: Option<u32>,
}

/// Format parameter for Gmail API message requests
#[derive(Debug, Clone, Copy, Default)]
pub enum MessageFormat {
    /// Returns the full email message data with body content parsed
    Full,
    /// Returns only email message IDs and labels
    Minimal,
    /// Returns email metadata (headers) without body
    Metadata,
    /// Returns the full email message in RFC 2822 format as a base64url encoded string
    #[default]
    Raw,
}

impl MessageFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageFormat::Full => "full",
            MessageFormat::Minimal => "minimal",
            MessageFormat::Metadata => "metadata",
            MessageFormat::Raw => "raw",
        }
    }
}

enum EmailClientError {
    RateLimitExceeded,
    Unauthorized,
    BadRequest,
    Unknown,
}

type EmailClientResult<T> = Result<T, EmailClientError>;

macro_rules! gmail_url {
    ($($params:expr),*) => {
        {
            const GMAIL_ENDPOINT: &str = "https://www.googleapis.com/gmail/v1/users/me";
            let list_params = vec![$($params),*];
            let path = list_params.join("/");
            format!("{}/{}", GMAIL_ENDPOINT, path)
        }
    };
}

pub const MAX_MESSAGES_PER_PAGE_DEFAULT: u32 = 500;

lazy_static! {
    static ref COLOR_MAP: Lazy<GmailLabelColorMap> = Lazy::new(GmailLabelColorMap::new);
}

#[derive(Debug, Clone)]
pub struct EmailClient {
    http_client: HttpClient,
    access_token: String,
    rate_limiter: Arc<RateLimiter>,
    pub email_address: String,
    pub expires_at: DateTimeWithTimeZone,
    /// Unix timestamp (seconds) of last cache access - atomic for lock-free updates
    last_used: Arc<AtomicI64>,
}

impl EmailClient {
    /// Update the last_used timestamp to now
    pub fn touch(&self) {
        self.last_used
            .store(Utc::now().timestamp(), Ordering::Relaxed);
    }

    /// Get the last_used timestamp
    pub fn last_used(&self) -> chrono::DateTime<Utc> {
        chrono::DateTime::from_timestamp(self.last_used.load(Ordering::Relaxed), 0)
            .unwrap_or_else(Utc::now)
    }
}

// TODO: Migrate Gmail specific parts to libs/email_clients/gmail
impl EmailClient {
    pub async fn new(
        http_client: reqwest::Client,
        conn: DatabaseConnection,
        mut user: impl AccountAccess + Id + EmailAddress,
    ) -> anyhow::Result<EmailClient> {
        let rate_limiter = Arc::new(
            RateLimiter::builder()
                .initial(GMAIL_QUOTA_PER_SECOND)
                .interval(Duration::from_secs(1))
                .refill(GMAIL_QUOTA_PER_SECOND)
                .build(),
        );

        let access_token =
            match UserAccessCtrl::get_refreshed_token(&http_client, &conn, &mut user).await {
                Ok(token) => token,
                Err(e) => {
                    tracing::error!("Error getting token: {:?}", e);
                    return Err(anyhow!("Unknown error getting access token: {:?}", e));
                }
            };

        // let user_configured_settings = UserInboxSettingsCtrl::get(&conn, user.id())
        //     .await
        //     .context("Could not retrieve inbox settings")?;

        // let mut category_inbox_settings = UserInboxSettingsCtrl::default();
        // category_inbox_settings.extend(user_configured_settings);

        // This will be a map from Mailclerk/* -> inbox_settings
        // let category_inbox_settings = category_inbox_settings
        //     .into_iter()
        //     .map(|(category, setting)| (format!("Mailclerk/{}", category), setting))
        //     .collect::<HashMap<_, _>>();

        Ok(EmailClient {
            http_client,
            access_token,
            rate_limiter,
            email_address: user.email().to_string(),
            expires_at: user.get_expires_at(),
            last_used: Arc::new(AtomicI64::new(Utc::now().timestamp())),
        })
    }

    // This is only used to test a new client on authentication
    pub fn from_access_code(http_client: reqwest::Client, access_token: String) -> EmailClient {
        let rate_limiter = Arc::new(
            RateLimiter::builder()
                .initial(GMAIL_QUOTA_PER_SECOND)
                .interval(Duration::from_secs(1))
                .refill(GMAIL_QUOTA_PER_SECOND)
                .build(),
        );

        EmailClient {
            http_client,
            access_token,
            rate_limiter,
            email_address: "test".to_string(),
            expires_at: chrono::Utc::now().with_timezone(&FixedOffset::east_opt(0).unwrap())
                + chrono::Duration::seconds(15),
            last_used: Arc::new(AtomicI64::new(Utc::now().timestamp())),
        }
    }

    pub async fn watch_mailbox(&self) -> anyhow::Result<WatchResponse> {
        self.rate_limiter.acquire(GMAIL_API_QUOTA.watch).await;
        const TOPIC_NAME: &str = "projects/mail-assist-434915/topics/mailclerk-user-inboxes";
        let resp = self
            .http_client
            .post(gmail_url!("watch"))
            .bearer_auth(&self.access_token)
            .json(&json!({
                "topicName": TOPIC_NAME,
                "labelIds": ["INBOX"],
                "labelFilterBehavior": "INCLUDE",
            }));

        let data = resp.send().await?;

        if !data.status().is_success() {
            let json = data.json::<serde_json::Value>().await?;
            return Err(anyhow!("Error watching mailbox: {:?}", json));
        }

        let json = data.json::<WatchResponse>().await?;

        Ok(json)
    }

    pub async fn get_message_list(
        &self,
        options: MessageListOptions,
    ) -> anyhow::Result<ListMessagesResponse> {
        self.rate_limiter
            .acquire(GMAIL_API_QUOTA.messages_list)
            .await;

        let time_filter = options
            .more_recent_than
            .map(|duration| format!("after:{}", (Utc::now() - duration).timestamp()));

        let mut filters = vec![];
        if let Some(time_filter) = time_filter {
            filters.push(time_filter);
        }
        let max_results = options.max_results.unwrap_or(MAX_MESSAGES_PER_PAGE_DEFAULT);

        let mut query = vec![
            ("q".to_string(), filters.join(" ")),
            ("maxResults".to_string(), max_results.to_string()),
        ];

        if let Some(token) = options.page_token {
            query.push(("pageToken".to_string(), token));
        }
        let resp = self
            .http_client
            .get(gmail_url!("messages"))
            .query(&query)
            .bearer_auth(&self.access_token)
            .send()
            .await?;

        let data = resp.json::<ListMessagesResponse>().await?;

        Ok(data)
    }

    pub async fn get_message_by_id(&self, message_id: &str) -> anyhow::Result<Message> {
        self.rate_limiter
            .acquire(GMAIL_API_QUOTA.messages_get)
            .await;
        let id = message_id;
        let req = self
            .http_client
            .get(gmail_url!("messages", id))
            .bearer_auth(&self.access_token)
            .query(&[("format", "RAW")])
            .send()
            .await?;

        req.json::<Message>().await.context("Error getting message")
    }

    /// Batch fetch multiple messages using Gmail's batch API
    /// Maximum 100 requests per batch (Gmail API limit)
    pub async fn get_messages_by_ids(
        &self,
        message_ids: &[impl AsRef<str>],
        format: MessageFormat,
        track: Option<&(dyn Fn(usize) + Send + Sync)>,
    ) -> anyhow::Result<Vec<Message>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Gmail batch API limit is 100 requests per batch
        // API quota is 250 units per sec
        // messages.get costs 5 units each
        // Limit is ~50 messages/second max per user
        // We go less than this for margin
        const BATCH_SIZE: usize = 15;
        const MAX_RETRIES: u32 = 3;
        let mut all_messages = Vec::with_capacity(message_ids.len());

        for chunk in message_ids.chunks(BATCH_SIZE) {
            // Acquire rate limit tokens for all requests in batch
            for _ in 0..chunk.len() {
                self.rate_limiter
                    .acquire(GMAIL_API_QUOTA.messages_get)
                    .await;
            }

            let mut pending_ids: Vec<String> =
                chunk.iter().map(|id| id.as_ref().to_string()).collect();
            let mut retry_count = 0;

            while !pending_ids.is_empty() && retry_count <= MAX_RETRIES {
                if retry_count > 0 {
                    // Exponential backoff: 1s, 2s, 4s
                    let delay = Duration::from_secs(1 << (retry_count - 1));
                    tracing::info!(
                        "Retrying {} messages after {:?} (attempt {}/{})",
                        pending_ids.len(),
                        delay,
                        retry_count,
                        MAX_RETRIES
                    );
                    tokio::time::sleep(delay).await;
                }

                let results = self.batch_get_messages(&pending_ids, format).await?;
                let mut successes = 0;
                let mut failed_ids = Vec::new();

                for result in results {
                    match result {
                        BatchMessageResult::Success(message) => {
                            successes += 1;
                            all_messages.push(message)
                        }

                        BatchMessageResult::Error { message_id, error } => {
                            // Retry on rate limit errors (429)
                            if error.error.code == 429 {
                                failed_ids.push(message_id);
                            } else {
                                tracing::warn!(
                                    "Skipping message {} due to non-retryable error: {} (code: {})",
                                    message_id,
                                    error.error.message,
                                    error.error.code
                                );
                            }
                        }
                    }
                }

                if let Some(track) = track {
                    track(successes);
                }

                pending_ids = failed_ids;
                retry_count += 1;
            }

            if !pending_ids.is_empty() {
                tracing::error!(
                    "Failed to fetch {} messages after {} retries: {:?}",
                    pending_ids.len(),
                    MAX_RETRIES,
                    pending_ids
                );
            }
        }

        Ok(all_messages)
    }

    async fn batch_get_messages(
        &self,
        message_ids: &[impl AsRef<str>],
        format: MessageFormat,
    ) -> anyhow::Result<Vec<BatchMessageResult>> {
        let boundary = format!("batch_{}", Uuid::new_v4());

        // Build multipart body
        let mut body = String::new();
        for (i, message_id) in message_ids.iter().enumerate() {
            body.push_str(&format!("--{}\r\n", boundary));
            body.push_str("Content-Type: application/http\r\n");
            body.push_str(&format!("Content-ID: <item{}>\r\n\r\n", i));
            body.push_str(&format!(
                "GET /gmail/v1/users/me/messages/{}?format={}\r\n\r\n",
                message_id.as_ref(),
                format.as_str().to_uppercase()
            ));
        }
        body.push_str(&format!("--{}--", boundary));

        let resp = self
            .http_client
            .post("https://www.googleapis.com/batch/gmail/v1")
            .bearer_auth(&self.access_token)
            .header(
                "Content-Type",
                format!("multipart/mixed; boundary={}", boundary),
            )
            .body(body)
            .send()
            .await?;

        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .context("Missing content-type header")?
            .to_string();

        let response_body = resp.text().await?;

        #[cfg(test)]
        crate::testing::common::dump_to_file("batch_response", &response_body);

        // Parse response boundary from content-type
        let response_boundary = content_type
            .split("boundary=")
            .nth(1)
            .context("Missing boundary in response")?;

        // Parse multipart response
        let mut results = Vec::new();
        let parts: Vec<&str> = response_body
            .split(&format!("--{}", response_boundary))
            .filter(|p| !p.trim().is_empty() && !p.trim().starts_with("--"))
            .collect();

        for part in parts {
            // Extract Content-ID to map back to message_id
            // Format: Content-ID: <response-item{i}>
            let message_id = part
                .lines()
                .find(|line| line.to_lowercase().starts_with("content-id:"))
                .and_then(|line| {
                    // Extract index from <response-item{i}> or <item{i}>
                    line.split("item")
                        .nth(1)
                        .and_then(|s| s.trim_end_matches('>').parse::<usize>().ok())
                })
                .and_then(|idx| message_ids.get(idx))
                .map(|id| id.as_ref().to_string())
                .unwrap_or_default();

            // Each part has HTTP headers, then a blank line, then the JSON body
            // Find the JSON body (after the HTTP response line and headers)
            if let Some(json_start) = part.find("\r\n\r\n") {
                let after_http_headers = &part[json_start + 4..];
                // There might be another set of headers (HTTP response), find JSON after that
                if let Some(json_start2) = after_http_headers.find("\r\n\r\n") {
                    let json_body = after_http_headers[json_start2 + 4..].trim();
                    if !json_body.is_empty() && json_body.starts_with('{') {
                        // Try parsing as error first
                        if let Ok(error) = serde_json::from_str::<GmailApiError>(json_body) {
                            tracing::warn!(
                                "Batch request error for message {}: {} (code: {})",
                                message_id,
                                error.error.message,
                                error.error.code
                            );
                            results.push(BatchMessageResult::Error { message_id, error });
                        } else {
                            // Try parsing as message
                            match serde_json::from_str::<Message>(json_body) {
                                Ok(message) => results.push(BatchMessageResult::Success(message)),
                                Err(e) => {
                                    tracing::warn!(
                                        "Failed to parse message {} from batch response: {}",
                                        message_id,
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    pub async fn get_simplified_message(
        &self,
        message_id: &str,
    ) -> anyhow::Result<SimplifiedMessage> {
        let message = self.get_message_by_id(message_id).await?;
        SimplifiedMessage::from_gmail_message(&message)
    }

    pub async fn get_threads(
        &self,
        options: ThreadListOptions,
    ) -> anyhow::Result<ListThreadsResponse> {
        self.rate_limiter
            .acquire(GMAIL_API_QUOTA.threads_list)
            .await;

        let mut query: Vec<(String, String)> = Vec::new();

        if let Some(q) = options.query {
            query.push(("q".to_string(), q));
        }

        if let Some(label_ids) = options.label_ids {
            for label_id in label_ids {
                query.push(("labelIds".to_string(), label_id));
            }
        }

        let max_results = options.max_results.unwrap_or(100);
        query.push(("maxResults".to_string(), max_results.to_string()));

        if let Some(token) = options.page_token {
            query.push(("pageToken".to_string(), token));
        }

        let resp = self
            .http_client
            .get(gmail_url!("threads"))
            .query(&query)
            .bearer_auth(&self.access_token)
            .send()
            .await?;

        let data = resp.json::<ListThreadsResponse>().await?;

        Ok(data)
    }

    pub async fn get_thread_by_id(&self, thread_id: &str) -> anyhow::Result<Thread> {
        self.rate_limiter.acquire(GMAIL_API_QUOTA.threads_get).await;

        let resp = self
            .http_client
            .get(gmail_url!("threads", thread_id))
            .bearer_auth(&self.access_token)
            .send()
            .await?;

        resp.json::<Thread>().await.context("Error getting thread")
    }

    /// Batch fetch multiple threads using Gmail's batch API                               
    /// Maximum 100 requests per batch (Gmail API limit)                                   
    pub async fn get_threads_by_ids(&self, thread_ids: &[String]) -> anyhow::Result<Vec<Thread>> {
        if thread_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Gmail batch API limit is 100 requests per batch
        const BATCH_SIZE: usize = 100;
        let mut all_threads = Vec::with_capacity(thread_ids.len());

        for chunk in thread_ids.chunks(BATCH_SIZE) {
            // Acquire rate limit tokens for all requests in batch
            for _ in 0..chunk.len() {
                self.rate_limiter.acquire(GMAIL_API_QUOTA.threads_get).await;
            }

            let threads = self.batch_get_threads(chunk).await?;
            all_threads.extend(threads);
        }

        Ok(all_threads)
    }

    async fn batch_get_threads(&self, thread_ids: &[String]) -> anyhow::Result<Vec<Thread>> {
        let boundary = format!("batch_{}", Uuid::new_v4());

        // Build multipart body
        let mut body = String::new();
        for (i, thread_id) in thread_ids.iter().enumerate() {
            body.push_str(&format!("--{}\r\n", boundary));
            body.push_str("Content-Type: application/http\r\n");
            body.push_str(&format!("Content-ID: <item{}>\r\n\r\n", i));
            body.push_str(&format!(
                "GET /gmail/v1/users/me/threads/{}?format=full\r\n\r\n",
                thread_id
            ));
        }
        body.push_str(&format!("--{}--", boundary));

        let resp = self
            .http_client
            .post("https://www.googleapis.com/batch/gmail/v1")
            .bearer_auth(&self.access_token)
            .header(
                "Content-Type",
                format!("multipart/mixed; boundary={}", boundary),
            )
            .body(body)
            .send()
            .await?;

        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .context("Missing content-type header")?
            .to_string();

        let response_body = resp.text().await?;

        // Parse response boundary from content-type
        let response_boundary = content_type
            .split("boundary=")
            .nth(1)
            .context("Missing boundary in response")?;

        // Parse multipart response
        let mut threads = Vec::new();
        let parts: Vec<&str> = response_body
            .split(&format!("--{}", response_boundary))
            .filter(|p| !p.trim().is_empty() && !p.trim().starts_with("--"))
            .collect();

        for part in parts {
            // Each part has HTTP headers, then a blank line, then the JSON body
            // Find the JSON body (after the HTTP response line and headers)
            if let Some(json_start) = part.find("\r\n\r\n") {
                let after_http_headers = &part[json_start + 4..];
                // There might be another set of headers (HTTP response), find JSON after that
                if let Some(json_start2) = after_http_headers.find("\r\n\r\n") {
                    let json_body = after_http_headers[json_start2 + 4..].trim();
                    if !json_body.is_empty() && json_body.starts_with('{') {
                        match serde_json::from_str::<Thread>(json_body) {
                            Ok(thread) => threads.push(thread),
                            Err(e) => {
                                tracing::warn!("Failed to parse thread from batch response: {}", e);
                            }
                        }
                    }
                }
            }
        }

        Ok(threads)
    }

    pub async fn get_labels(&self) -> anyhow::Result<Vec<Label>> {
        self.rate_limiter.acquire(GMAIL_API_QUOTA.labels_list).await;
        let resp = self
            .http_client
            .get(gmail_url!("labels"))
            .bearer_auth(&self.access_token)
            .send()
            .await?;
        let data = resp.json::<ListLabelsResponse>().await?;

        data.labels.context("No labels found")
    }

    pub async fn create_label(&self, label: Label) -> anyhow::Result<Label> {
        self.rate_limiter
            .acquire(GMAIL_API_QUOTA.labels_create)
            .await;

        let resp = self
            .http_client
            .post(gmail_url!("labels"))
            .bearer_auth(&self.access_token)
            .json(&label)
            .send()
            .await?;
        let data = resp.json::<serde_json::Value>().await?;
        if let Some(error) = data.get("error") {
            if error.get("code").is_some_and(|x| x.as_i64() == Some(409)) {
                // Label already exists
                return Ok(label);
            }
            return Err(anyhow::anyhow!(
                "Error creating label: {:?} Error: {:?}",
                label,
                data
            ));
        }

        Ok(serde_json::from_value(data)?)
    }

    pub async fn delete_label(&self, label_id: String) -> anyhow::Result<()> {
        self.rate_limiter
            .acquire(GMAIL_API_QUOTA.labels_delete)
            .await;
        let resp = self
            .http_client
            .delete(gmail_url!("labels", &label_id))
            .bearer_auth(&self.access_token)
            .send()
            .await?;
        match resp.json::<serde_json::Value>().await {
            Ok(data) if data.get("error").is_some() => {
                Err(anyhow::anyhow!("Error deleting label: {:?}", data))
            }
            Ok(unknown) => Err(anyhow::anyhow!("Unknown response: {:?}", unknown)),
            Err(_) => {
                // An empty response is expected if the label was deleted successfully
                Ok(())
            }
        }
    }

    pub async fn configure_labels_if_needed(
        &self,
        user_custom_labels: Vec<String>,
    ) -> anyhow::Result<bool> {
        let current_labels = self.get_labels().await?;

        let parent_label_exists = current_labels
            .iter()
            .any(|l| l.name.as_ref().is_some_and(|n| n == "Mailclerk"));

        let existing_labels = current_labels
            .iter()
            .filter(|l| l.name.as_ref().is_some_and(|n| n.contains("Mailclerk/")))
            .cloned()
            .collect::<Vec<_>>();

        // -- DEBUG
        // println!("Existing labels: {:?}", existing_labels);
        // -- DEBUG

        // Configure labels if they need it
        let required_labels = cfg
            .categories
            .iter()
            .map(|c| c.mail_label.as_str())
            .chain(cfg.heuristics.iter().map(|c| c.mail_label.as_str()))
            .chain(labels::UtilityLabels::iter().map(|c| c.as_str()))
            .chain(user_custom_labels.iter().map(|s| s.as_str()))
            .map(|mail_label| format!("Mailclerk/{}", mail_label))
            .collect::<HashSet<_>>();

        let existing_label_names = existing_labels
            .iter()
            .map(|l| l.name.clone().unwrap_or_default())
            .collect::<HashSet<_>>();

        let missing_labels = required_labels
            .difference(&existing_label_names)
            .cloned()
            .collect::<Vec<_>>();

        // -- DEBUG
        // println!("Missing labels: {:?}", existing_labels);
        // -- DEBUG

        let unneeded_labels = {
            let unneeded = existing_label_names
                .difference(&required_labels)
                .cloned()
                .collect::<HashSet<_>>();

            existing_labels
                .iter()
                .filter(|l| l.name.as_ref().is_some_and(|n| unneeded.contains(n)))
                .cloned()
                .collect::<Vec<_>>()
        };

        if parent_label_exists && missing_labels.is_empty() && unneeded_labels.is_empty() {
            // Labels are already configured
            return Ok(false);
        }

        if !parent_label_exists {
            let label = Label {
                id: None,
                type_: Some("user".to_string()),
                color: Some(COLOR_MAP.get("blue-600")),
                name: Some("Mailclerk".to_string()),
                messages_total: None,
                messages_unread: None,
                threads_total: None,
                threads_unread: None,
                message_list_visibility: Some("show".to_string()),
                label_list_visibility: Some("labelShow".to_string()),
            };
            self.create_label(label)
                .await
                .context("Could not create parent label")?;
        }

        let labels_to_add = missing_labels.into_iter().collect::<Vec<_>>();

        // Add mailclerk labels
        let add_label_tasks = labels_to_add.into_iter().map(|label| {
            let (message_list_visibility, label_list_visibility) =
                if label == UtilityLabels::Uncategorized.as_str() {
                    (Some("hide".to_string()), Some("labelHide".to_string()))
                } else {
                    (
                        Some("show".to_string()),
                        Some("labelShowIfUnread".to_string()),
                    )
                };
            let label_name = label.split("Mailclerk/").nth(1).unwrap_or("");
            let color = COLOR_MAP.get(label_name);
            let label = Label {
                id: None,
                type_: Some("user".to_string()),
                color: Some(color),
                name: Some(label.clone()),
                messages_total: None,
                messages_unread: None,
                threads_total: None,
                threads_unread: None,
                message_list_visibility,
                label_list_visibility,
            };
            async { self.create_label(label).await }
        });

        // Reset mailclerk labels
        //? Maybe remove this in the future?
        //? Probably needs to migrate existing mails to new labels
        // let remove_label_tasks = existing_labels.into_iter().map(|label| async {
        //     let id = label.id.context("Label id not provided")?;
        //     self.delete_label(id).await
        // });

        // let results = join_all(remove_label_tasks).await;
        // for result in results {
        //     match result {
        //         Ok(_) => {}
        //         Err(e) => {
        //             tracing::error!("{e}");
        //             return Err(e);
        //         }
        //     }
        // }

        let results = join_all(add_label_tasks).await;
        for result in results {
            result.context("Could not create label")?;
        }

        Ok(true)
    }

    // pub async fn label_email(
    //     &self,
    //     email_id: String,
    //     current_labels: Vec<String>,
    //     email_rule: EmailRule,
    // ) -> anyhow::Result<LabelUpdate> {
    //     let user_labels = self.get_labels().await?;
    //     self.rate_limiter
    //         .acquire(GMAIL_API_QUOTA.messages_modify)
    //         .await;
    //     let (json_body, update) = build_label_update(user_labels, current_labels, email_rule)?;
    //     let resp = self
    //         .http_client
    //         .post(gmail_url!("messages", &email_id, "modify"))
    //         .bearer_auth(&self.access_token)
    //         .json(&json_body)
    //         .send()
    //         .await?;
    //     let data = resp.json::<serde_json::Value>().await?;

    //     if data.get("error").is_some() {
    //         return Err(anyhow::anyhow!("Error labelling email: {:?}", data));
    //     }

    //     Ok(update)
    // }

    pub async fn get_profile(&self) -> anyhow::Result<Profile> {
        self.rate_limiter.acquire(GMAIL_API_QUOTA.get_profile).await;
        let resp = self
            .http_client
            .get("https://www.googleapis.com/gmail/v1/users/me/profile")
            .bearer_auth(&self.access_token)
            .send()
            .await?;

        Ok(resp.json::<Profile>().await?)
    }

    pub async fn insert_email(&self, message: Message) -> anyhow::Result<()> {
        self.rate_limiter
            .acquire(GMAIL_API_QUOTA.messages_insert)
            .await;
        self.http_client
            .post(gmail_url!("messages"))
            .bearer_auth(&self.access_token)
            .json(&message)
            .send()
            .await?;

        Ok(())
    }

    pub async fn trash_email(&self, message_id: &str) -> anyhow::Result<()> {
        self.rate_limiter
            .acquire(GMAIL_API_QUOTA.messages_trash)
            .await;
        self.http_client
            .post(gmail_url!("messages", message_id, "trash"))
            .bearer_auth(&self.access_token)
            .send()
            .await?;

        Ok(())
    }

    pub async fn archive_email(&self, message_id: &str) -> anyhow::Result<()> {
        self.rate_limiter
            .acquire(GMAIL_API_QUOTA.messages_modify)
            .await;
        self.http_client
            .post(gmail_url!("messages", message_id, "modify"))
            .bearer_auth(&self.access_token)
            .json(&json!({
                "removeLabelIds": ["INBOX", "UNREAD"],
                "addLabelIds": []
            }))
            .send()
            .await?;

        Ok(())
    }

    pub async fn mark_email_as_read(&self, message_id: &str) -> anyhow::Result<()> {
        self.rate_limiter
            .acquire(GMAIL_API_QUOTA.messages_modify)
            .await;
        self.http_client
            .post(gmail_url!("messages", message_id, "modify"))
            .bearer_auth(&self.access_token)
            .json(&json!({
                "removeLabelIds": ["UNREAD"],
                "addLabelIds": []
            }))
            .send()
            .await?;

        Ok(())
    }

    pub async fn mark_email_as_unread(&self, message_id: &str) -> anyhow::Result<()> {
        self.rate_limiter
            .acquire(GMAIL_API_QUOTA.messages_modify)
            .await;
        self.http_client
            .post(gmail_url!("messages", message_id, "modify"))
            .bearer_auth(&self.access_token)
            .json(&json!({
                "removeLabelIds": [],
                "addLabelIds": ["UNREAD"]
            }))
            .send()
            .await?;

        Ok(())
    }

    /// Get a message by ID with FULL format (includes headers in payload)
    /// Use this when you need access to headers like Message-ID, In-Reply-To, References
    pub async fn get_message_with_headers(&self, message_id: &str) -> anyhow::Result<Message> {
        self.rate_limiter
            .acquire(GMAIL_API_QUOTA.messages_get)
            .await;
        let req = self
            .http_client
            .get(gmail_url!("messages", message_id))
            .bearer_auth(&self.access_token)
            .query(&[("format", "FULL")])
            .send()
            .await?;

        req.json::<Message>()
            .await
            .context("Error getting message with headers")
    }

    /// Send an email using the Gmail API
    /// The raw_message should be a base64url-encoded RFC 2822 MIME message
    /// If thread_id is provided, the message will be added to that thread (for replies)
    pub async fn send_message(
        &self,
        raw_message: &str,
        thread_id: Option<&str>,
    ) -> anyhow::Result<Message> {
        self.rate_limiter
            .acquire(GMAIL_API_QUOTA.messages_send)
            .await;

        let mut body = json!({
            "raw": raw_message
        });

        if let Some(tid) = thread_id {
            body["threadId"] = json!(tid);
        }

        let resp = self
            .http_client
            .post(gmail_url!("messages", "send"))
            .bearer_auth(&self.access_token)
            .json(&body)
            .send()
            .await?;

        let data = resp.json::<serde_json::Value>().await?;

        if let Some(error) = data.get("error") {
            return Err(anyhow!("Error sending message: {:?}", error));
        }

        serde_json::from_value(data).context("Failed to parse send response")
    }
}

// fn build_label_update(
//     user_labels: Vec<Label>,
//     current_labels: Vec<String>,
//     email_rule: EmailRule,
// ) -> anyhow::Result<(serde_json::Value, LabelUpdate)> {
//     static RE_CATEGORY_LABEL: Lazy<Regex> = Lazy::new(|| Regex::new(r"CATEGORY_+").unwrap());

//     let current_categories = current_labels
//         .iter()
//         .filter(|c| RE_CATEGORY_LABEL.is_match(c))
//         .cloned()
//         .collect::<Vec<_>>();

//     let categories_to_add = email_rule
//         .associated_email_client_category
//         .clone()
//         .map_or(Vec::new(), |c| vec![c.to_value()]);

//     // Only remove categories if you have a different category to add
//     let categories_to_remove = if categories_to_add.is_empty() {
//         None
//     } else {
//         Some(
//             current_categories
//                 .iter()
//                 .filter(|c| !categories_to_add.contains(c))
//                 .cloned()
//                 .collect::<Vec<_>>(),
//         )
//     };

//     let label_id = user_labels
//         .iter()
//         .find(|l| l.name.as_ref() == Some(&format!("Mailclerk/{}", email_rule.mail_label)))
//         .map(|l| l.id.clone().unwrap_or_default())
//         .context(format!("Could not find {}!", email_rule.mail_label))?;

//     let (label_ids_to_add, label_names_applied) = {
//         let mut label_ids = categories_to_add.clone();
//         label_ids.push(label_id);
//         let mut label_names = categories_to_add;
//         label_names.push(email_rule.mail_label);

//         (
//             label_ids.into_iter().collect::<Vec<_>>(),
//             label_names.into_iter().collect::<Vec<_>>(),
//         )
//     };

//     Ok((
//         json!(
//             {
//                 "addLabelIds": label_ids_to_add,
//                 "removeLabelIds": categories_to_remove.clone().unwrap_or_default()
//             }
//         ),
//         LabelUpdate {
//             added: Some(label_names_applied),
//             removed: categories_to_remove,
//         },
//     ))
// }

fn get_required_labels() -> HashSet<String> {
    cfg.categories
        .iter()
        .map(|c| c.mail_label.as_str())
        .chain(cfg.heuristics.iter().map(|c| c.mail_label.as_str()))
        .chain(labels::UtilityLabels::iter().map(|c| c.as_str()))
        .map(|mail_label| format!("Mailclerk/{}", mail_label))
        .collect::<HashSet<_>>()
}

fn format_filter(label: &str) -> String {
    let label = label.replace(" ", "-");
    format!("label:Mailclerk/{}", label)
}

fn default_mailclerk_label_filter() -> String {
    // Add mailclerk labels to filter
    let label_set = cfg
        .categories
        .iter()
        .map(|c| c.mail_label.as_str())
        .chain(cfg.heuristics.iter().map(|c| c.mail_label.as_str()))
        .chain(labels::UtilityLabels::iter().map(|l| l.as_str()))
        .map(format_filter)
        .collect::<HashSet<String>>();

    let labels = vec!["label:inbox".to_string()]
        .into_iter()
        .chain(label_set)
        .collect::<Vec<_>>();

    labels.join(" AND NOT ")
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "integration")]
    use std::collections::HashSet;
    #[cfg(feature = "integration")]
    use strum::IntoEnumIterator;

    #[cfg(feature = "integration")]
    use google_gmail1::api::Label;

    #[cfg(feature = "integration")]
    use super::*;
    #[cfg(feature = "integration")]
    use crate::{
        email::client::{format_filter, get_required_labels},
        model::labels,
        server_config::cfg,
        testing::common::setup_email_client,
    };

    #[test]
    fn test_gmail_url() {
        let url = gmail_url!("messages");
        assert_eq!(url, "https://www.googleapis.com/gmail/v1/users/me/messages");
        let url = gmail_url!("messages", "123");
        assert_eq!(
            url,
            "https://www.googleapis.com/gmail/v1/users/me/messages/123"
        );
    }

    #[test]
    #[ignore = "expected body output is outdated"]
    fn test_sanitize_message() {
        use super::*;
        use google_gmail1::api::Message;
        use std::fs;

        let root = env!("CARGO_MANIFEST_DIR");

        let path = format!("{root}/src/testing/data/jobot_message.json");
        let json = fs::read_to_string(path).expect("Unable to read file");

        let message = serde_json::from_str::<Message>(&json).expect("Unable to parse json");

        let sanitized =
            SimplifiedMessage::from_gmail_message(&message).expect("Unable to parse message");
        let test = SimplifiedMessage {
                    id: "1921e8debe9a2256".to_string(),
        label_ids: vec![
            "Label_29".to_string(),
            "Label_5887327980780978551".to_string(),
            "CATEGORY_UPDATES".to_string(),
            "INBOX".to_string(),
        ],
        thread_id: "1921e8debe9a2256".to_string(),
        history_id: 12323499,
        internal_date: 1727089470000,
        from: Some(
            "jobs@alerts.jobot.com".to_string(),
        ),
        subject: Some(
            "Remote Sr. JavaScript Engineer openings are available. Apply Now.".to_string(),
        ),
        snippet: sanitized.snippet.clone(),
        body: Some(
            concat!(
                "Apply Now, Rachel and Charles are hiring for Remote Sr. JavaScript Engineer and Software Engineer roles! [Jobot logo] ",
            "12 New Jobs for your Job Search Recommended Apply -- Based on your resume Remote Sr. JavaScript Engineer [[LINK]] Growing health-tech startup seeks a Remote Sr. JavaScriptEngineer to join their team! [] REMOTE [] Washington, DC [] $130,000 - $155,000 1-Click Apply [[LINK]] [Rachel Hilton Berry] Rachel ", 
            "Also Consider -- Based on your resume Software Engineer [[LINK]] build out modern web applications and automated deployment pipelines 100% from home [] REMOTE [] McLean, VA [] $100,000 - $140,000 1-Click Apply [[LINK]] [Charles Simmons] Charles ", 
            "AlsoConsider -- Based on your resume Sr. Software Engineer [[LINK]] 100% Remote Role, Innovative Legal Software Company [] REMOTE [] Oklahoma City, OK [] $140,000 - $160,000 1-Click Apply [[LINK]] [Duran Workman] Duran ", 
            "Also Consider -- Based on your resume Senior Software Engineer [[LINK]] [] REMOTE [] Oklahoma City, OK +1 [] $115,000 - $155,000 1-Click Apply [[LINK]] [Dan Dungy] Dan ", 
            "AlsoConsider -- Based on your resume Frontend Developer - Remote [[LINK]] Growing tech company in the supply chain space is hiring for a Frontend Software Developer! [] REMOTE [] Chicago, IL [] $90,000 - $115,000 1-Click Apply [[LINK]] [Sydney Weaver] Sydney ",
            "Also Consider -- Based on your resume Flutter and Dart Engineer [[LINK]] 100% remote - Contract to Hire - Native Development [] REMOTE[] Cincinnati, OH +2 [] $45 - $55 1-Click Apply [[LINK]] [Chuck Wirtz] Chuck ", 
            "Also Consider -- Based on your resume Mobile Developer - Specializing in NFC Tech [[LINK]] [] REMOTE [] Austin, TX [] $50 - $80 1-Click Apply [[LINK]] [Ashley Elm] Ashley ", 
            "Also Consider -- Based on your resume Senior Software Engineer (Swift Integrations) [[LINK]] Remote Opportunity/AI Start Up/ Blockchain []REMOTE [] San Jose, CA [] $170,000 - $210,000 1-Click Apply [[LINK]] [Heather Burnach] Heather ", 
            "Also Consider -- Based on your resume Lead Growth Engineer [[LINK]] Lead Growth Engineer (PST, Remote) with scaling health/wellness startup- $90M, Series B [] REMOTE [] West Hollywood, CA [] $165,000 - $215,000 1-Click Apply [[LINK]] [Oliver Belkin] Oliver ",
            "Also Consider -- Based on your resumeSenior Software Engineer-(PHP, TypeScript, Node, AWS) [[LINK]] Senior Software Engineer-CONTRACT-REMOTE [] REMOTE [] Charlotte, NC [] $70 - $90 1-Click Apply [[LINK]] [Chris Chomic] Chris ",
            "Also Consider -- Based on your resume Senior React Native Developer [[LINK]] [] REMOTE [] San Francisco, CA [] $160,000 - $180,000 1-Click Apply [[LINK]] [Joe Lynch] Joe ",
            "Also Consider -- Based on yourresume Senior Backend Engineer [[LINK]] Build out modern platforms supporting the short term rental SaaS space 100% Remote [] REMOTE [] Philadelphia, PA +1 [] $130,000 - $175,000 1-Click Apply [[LINK]] [Charles Simmons] Charles [LinkedIn logo] [[LINK]][Instagram logo] [[LINK]] Jobot.com [[LINK]] | Unsubscribe [[LINK]] Copyright Jobot, LLC, All rights reserved. 3101 West Pacific Coast Hwy,Newport Beach, CA 92663"
        ).to_string())
        };
        assert_eq!(sanitized, test);
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn test_trash_email() {
        let client = setup_email_client("mpgrospamacc@gmail.com").await;
        client.trash_email("193936af5309bb57").await.unwrap();
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn test_archive_email() {
        let client = setup_email_client("mpgrospamacc@gmail.com").await;
        client.archive_email("193936af5309bb57").await.unwrap();
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn test_batch_get_messages_raw_format() {
        let client = setup_email_client("mpgrospamacc@gmail.com").await;

        // First get a list of messages to have valid IDs
        let list_response = client
            .get_message_list(super::MessageListOptions {
                max_results: Some(200),
                ..Default::default()
            })
            .await
            .expect("Failed to get message list");

        let message_ids: Vec<String> = list_response
            .messages
            .unwrap_or_default()
            .into_iter()
            .filter_map(|m| m.id)
            .collect();

        assert!(
            message_ids.len() >= 2,
            "Need at least 2 messages to test batch fetch"
        );

        let messages = client
            .get_messages_by_ids(&message_ids, super::MessageFormat::Raw, None)
            .await
            .expect("Failed to batch get messages");

        assert_eq!(
            messages.len(),
            message_ids.len(),
            "Should return same number of messages as requested"
        );

        for (i, msg) in messages.iter().enumerate() {
            assert!(msg.id.is_some(), "Message {} should have an id", i);
            assert!(
                msg.raw.is_some(),
                "Message {} should have raw content with RAW format",
                i
            );
            println!("ID: <{}>", msg.id.as_ref().unwrap());

            // Test that each message can be converted to a SimplifiedMessage
            let simplified = super::SimplifiedMessage::from_gmail_message(msg).expect(&format!(
                "Message {} should convert to SimplifiedMessage",
                i
            ));
            println!(
                "ID: <{}> Subject: <{}>",
                simplified.id,
                simplified.subject.unwrap()
            );

            assert!(
                !simplified.id.is_empty(),
                "SimplifiedMessage {} should have an id",
                i
            );
        }
    }
}
