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
use regex::Regex;
use serde_json::json;
use std::sync::Arc;
use std::{collections::HashSet, time::Duration};
use strum::IntoEnumIterator;
use uuid::Uuid;

use crate::model::labels::UtilityLabels;
use crate::model::user::UserAccessCtrl;
use crate::{
    db_core::prelude::*,
    model::response::LabelUpdate,
    model::{
        labels,
        user::{AccountAccess, EmailAddress, Id},
    },
    server_config::{cfg, DAILY_SUMMARY_CATEGORY},
    HttpClient,
};

use super::rules::EmailRule;
use super::simplified_message::SimplifiedMessage;

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

const MAX_RESULTS_DEFAULT: u32 = 500;

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

        let label_filter = options
            .label_filter
            .unwrap_or(default_mailclerk_label_filter());

        let mut filters = vec![label_filter];
        if let Some(time_filter) = time_filter {
            filters.push(time_filter);
        }
        let max_results = options.max_results.unwrap_or(MAX_RESULTS_DEFAULT);

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
        message_ids: &[String],
    ) -> anyhow::Result<Vec<Message>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Gmail batch API limit is 100 requests per batch
        const BATCH_SIZE: usize = 100;
        let mut all_messages = Vec::with_capacity(message_ids.len());

        for chunk in message_ids.chunks(BATCH_SIZE) {
            // Acquire rate limit tokens for all requests in batch
            for _ in 0..chunk.len() {
                self.rate_limiter
                    .acquire(GMAIL_API_QUOTA.messages_get)
                    .await;
            }

            let messages = self.batch_get_messages(chunk).await?;
            all_messages.extend(messages);
        }

        Ok(all_messages)
    }

    async fn batch_get_messages(&self, message_ids: &[String]) -> anyhow::Result<Vec<Message>> {
        let boundary = format!("batch_{}", Uuid::new_v4());

        // Build multipart body
        let mut body = String::new();
        for (i, message_id) in message_ids.iter().enumerate() {
            body.push_str(&format!("--{}\r\n", boundary));
            body.push_str("Content-Type: application/http\r\n");
            body.push_str(&format!("Content-ID: <item{}>\r\n\r\n", i));
            body.push_str(&format!(
                "GET /gmail/v1/users/me/messages/{}?format=RAW\r\n\r\n",
                message_id
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
        let mut messages = Vec::new();
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
                        match serde_json::from_str::<Message>(json_body) {
                            Ok(message) => messages.push(message),
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to parse message from batch response: {}",
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(messages)
    }

    pub async fn get_simplified_message(
        &self,
        message_id: &str,
    ) -> anyhow::Result<SimplifiedMessage> {
        let message = self.get_message_by_id(message_id).await?;
        SimplifiedMessage::from_gmail_message(message)
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

    /// Gets the label id for the mailclerk daily summary label, if doesn't exist, creates it
    pub async fn get_daily_summary_label_id(&self) -> anyhow::Result<String> {
        let existing_labels = self.get_labels().await?;
        let daily_summary_label_name =
            format!("Mailclerk/{}", DAILY_SUMMARY_CATEGORY.mail_label.clone());
        if let Some(label) = existing_labels.iter().find(|l| {
            l.name
                .as_ref()
                .is_some_and(|n| n.as_str() == daily_summary_label_name.as_str())
        }) {
            Ok(label.id.clone().context("Label id not provided")?)
        } else {
            let label = self
                .create_label(Label {
                    id: None,
                    type_: Some("user".to_string()),
                    color: Some(COLOR_MAP.get("white")),
                    name: Some(daily_summary_label_name.clone()),
                    messages_total: None,
                    messages_unread: None,
                    threads_total: None,
                    threads_unread: None,
                    message_list_visibility: Some("show".to_string()),
                    label_list_visibility: Some("labelShowIfUnread".to_string()),
                })
                .await?;

            Ok(label.id.context("Label id not provided")?)
        }
    }

    pub async fn label_email(
        &self,
        email_id: String,
        current_labels: Vec<String>,
        email_rule: EmailRule,
    ) -> anyhow::Result<LabelUpdate> {
        let user_labels = self.get_labels().await?;
        self.rate_limiter
            .acquire(GMAIL_API_QUOTA.messages_modify)
            .await;
        let (json_body, update) = build_label_update(user_labels, current_labels, email_rule)?;
        let resp = self
            .http_client
            .post(gmail_url!("messages", &email_id, "modify"))
            .bearer_auth(&self.access_token)
            .json(&json_body)
            .send()
            .await?;
        let data = resp.json::<serde_json::Value>().await?;

        if data.get("error").is_some() {
            return Err(anyhow::anyhow!("Error labelling email: {:?}", data));
        }

        Ok(update)
    }

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

fn build_label_update(
    user_labels: Vec<Label>,
    current_labels: Vec<String>,
    email_rule: EmailRule,
) -> anyhow::Result<(serde_json::Value, LabelUpdate)> {
    static RE_CATEGORY_LABEL: Lazy<Regex> = Lazy::new(|| Regex::new(r"CATEGORY_+").unwrap());

    let current_categories = current_labels
        .iter()
        .filter(|c| RE_CATEGORY_LABEL.is_match(c))
        .cloned()
        .collect::<Vec<_>>();

    let categories_to_add = email_rule
        .associated_email_client_category
        .clone()
        .map_or(Vec::new(), |c| vec![c.to_value()]);

    // Only remove categories if you have a different category to add
    let categories_to_remove = if categories_to_add.is_empty() {
        None
    } else {
        Some(
            current_categories
                .iter()
                .filter(|c| !categories_to_add.contains(c))
                .cloned()
                .collect::<Vec<_>>(),
        )
    };

    let label_id = user_labels
        .iter()
        .find(|l| l.name.as_ref() == Some(&format!("Mailclerk/{}", email_rule.mail_label)))
        .map(|l| l.id.clone().unwrap_or_default())
        .context(format!("Could not find {}!", email_rule.mail_label))?;

    let (label_ids_to_add, label_names_applied) = {
        let mut label_ids = categories_to_add.clone();
        label_ids.push(label_id);
        let mut label_names = categories_to_add;
        label_names.push(email_rule.mail_label);

        (
            label_ids.into_iter().collect::<Vec<_>>(),
            label_names.into_iter().collect::<Vec<_>>(),
        )
    };

    Ok((
        json!(
            {
                "addLabelIds": label_ids_to_add,
                "removeLabelIds": categories_to_remove.clone().unwrap_or_default()
            }
        ),
        LabelUpdate {
            added: Some(label_names_applied),
            removed: categories_to_remove,
        },
    ))
}

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

enum EmailClientError {
    RateLimitExceeded,
    Unauthorized,
    BadRequest,
    Unknown,
}

type EmailClientResult<T> = Result<T, EmailClientError>;

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use strum::IntoEnumIterator;

    use google_gmail1::api::Label;

    use super::*;
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
    fn test_build_label_update() {
        let user_labels = vec![Label {
            id: Some("Label_10".to_string()),
            name: Some("mailclerk:ads".to_string()),
            ..Label::default()
        }];
        match super::build_label_update(
            user_labels,
            ["CATEGORY_SOCIAL".to_string()].to_vec(),
            super::EmailRule {
                prompt_content: "Advertisment".to_string(),
                mail_label: "mailclerk:ads".to_string(),
                associated_email_client_category: Some(
                    AssociatedEmailClientCategory::CategoryPromotions,
                ),
            },
        ) {
            Ok((json_body, update)) => {
                assert_eq!(
                    json_body,
                    serde_json::json!({
                        "addLabelIds": ["CATEGORY_PROMOTIONS", "Label_10"],
                        "removeLabelIds": ["CATEGORY_SOCIAL"]
                    })
                );
                assert_eq!(
                    update,
                    super::LabelUpdate {
                        added: Some(vec![
                            "CATEGORY_PROMOTIONS".to_string(),
                            "mailclerk:ads".to_string()
                        ]),
                        removed: Some(vec!["CATEGORY_SOCIAL".to_string()])
                    }
                );
            }
            Err(e) => panic!("Error: {:?}", e),
        }
    }

    #[test]
    fn test_sanitize_message() {
        use super::*;
        use google_gmail1::api::Message;
        use std::fs;

        let root = env!("CARGO_MANIFEST_DIR");

        let path = format!("{root}/src/testing/data/jobot_message.json");
        let json = fs::read_to_string(path).expect("Unable to read file");

        let message = serde_json::from_str::<Message>(&json).expect("Unable to parse json");

        let sanitized =
            SimplifiedMessage::from_gmail_message(message).expect("Unable to parse message");
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

    #[test]
    fn test_get_required_labels() {
        dotenvy::from_filename(".env.integration").unwrap();
        let required_labels = get_required_labels();
        dbg!(&required_labels);
        assert_eq!(true, false)
    }

    #[test]
    fn test_build_mailclerk_label_filter() {
        dotenvy::from_filename(".env.integration").unwrap();
        let filter = super::default_mailclerk_label_filter();

        let expected = &cfg
            .categories
            .iter()
            .map(|c| c.mail_label.as_str())
            .chain(cfg.heuristics.iter().map(|c| c.mail_label.as_str()))
            .chain(labels::UtilityLabels::iter().map(|l| l.as_str()))
            .map(format_filter)
            .chain(std::iter::once("label:inbox".to_string()))
            .collect::<HashSet<_>>();

        let actual = filter
            .split(" AND NOT ")
            .map(|x| x.to_string())
            .collect::<HashSet<_>>();

        dbg!(&expected, "len: ", expected.len());
        dbg!(&actual, "len: ", actual.len());

        assert!(expected.is_subset(&actual) && actual.is_subset(expected));
    }

    #[tokio::test]
    async fn test_trash_email() {
        let client = setup_email_client("mpgrospamacc@gmail.com").await;
        client.trash_email("193936af5309bb57").await.unwrap();
    }

    #[tokio::test]
    async fn test_archive_email() {
        let client = setup_email_client("mpgrospamacc@gmail.com").await;
        client.archive_email("193936af5309bb57").await.unwrap();
    }
}
