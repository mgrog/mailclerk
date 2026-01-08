use std::collections::HashMap;

use anyhow::Context;
use axum::{extract::Multipart, extract::Query, extract::State, Json};
use lettre::{
    message::{
        header::ContentType, Attachment as LettreAttachment, MultiPart as LettreMultiPart,
        SinglePart as LettreSinglePart,
    },
    Message,
};
use serde::{Deserialize, Serialize};

use crate::{
    auth::jwt::Claims,
    email::{
        client::{EmailClient, ThreadListOptions},
        sanitized_message::{parsed_and_sanitized_gmail_thread, SanitizedThread},
    },
    error::{AppError, AppJsonResult},
    model::user::UserCtrl,
    util::check_expired,
    ServerState,
};

async fn fetch_email_client(
    ServerState {
        email_client_cache,
        http_client,
        conn,
        ..
    }: ServerState,
    user_email: String,
) -> anyhow::Result<EmailClient> {
    // First, try to get from cache with read lock
    {
        let cache = email_client_cache.read().await;
        if let Some(client) = cache.get(&user_email) {
            if !check_expired(client.expires_at) {
                return Ok(client.clone());
            }
        }
    }

    // Client missing or expired - create new one and insert with write lock
    let user = UserCtrl::get_with_account_access_by_email(&conn, &user_email).await?;
    let client = EmailClient::new(http_client, conn, user).await?;

    let mut cache = email_client_cache.write().await;
    cache.insert(user_email, client.clone());

    Ok(client)
}

const DEFAULT_PAGE_SIZE: u32 = 20;
const MAX_PAGE_SIZE: u32 = 100;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetAllEmailsQuery {
    /// Cursor for pagination (opaque string from previous response)
    pub cursor: Option<String>,
    /// Number of items to return (defaults to 20, max 100)
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailSummary {
    pub id: String,
    pub thread_id: Option<String>,
    pub subject: Option<String>,
    pub from: Option<String>,
    pub date: Option<String>,
    pub snippet: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetAllEmailsResponse {
    pub threads: Vec<SanitizedThread>,
    /// Cursor for the next page, None if no more results
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

/// # GET /email
///
/// Query parameters:
/// - `cursor`: Optional cursor for pagination (from previous response)
/// - `limit`: Optional number of items to return (default: 20, max: 100)
pub async fn get_all(
    claims: Claims,
    State(state): State<ServerState>,
    Query(query): Query<GetAllEmailsQuery>,
) -> AppJsonResult<GetAllEmailsResponse> {
    let user_email = &claims.email;
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE).min(MAX_PAGE_SIZE);
    let cursor = query.cursor;

    // Get the user's email client from the cache
    let email_client = fetch_email_client(state.clone(), user_email.clone()).await?;

    let options = ThreadListOptions {
        page_token: cursor,
        max_results: Some(limit + 1),
        ..Default::default()
    };
    let response = email_client.get_threads(options).await?;
    let list = response.threads.unwrap_or_default();

    // Get thread IDs for batching
    let ids: Vec<String> = list.iter().filter_map(|t| t.id.clone()).collect();

    // Batch fetch full threads with messages
    let threads = email_client.get_threads_by_ids(&ids).await?;

    let has_more = threads.len() > limit as usize;
    let threads_to_return: Vec<google_gmail1::api::Thread> = if has_more {
        threads[..limit as usize].to_vec()
    } else {
        threads
    };
    let next_cursor = if has_more {
        threads_to_return.last().and_then(|m| m.id.clone())
    } else {
        None
    };

    // Collect all message IDs from threads
    // let message_ids: Vec<String> = threads_to_return
    //     .iter()
    //     .flat_map(|t| t.messages.clone().unwrap_or_default())
    //     .filter_map(|m| m.id)
    //     .collect();

    // // Batch fetch full messages
    // let full_messages = email_client.get_messages_by_ids(&message_ids).await?;

    // // Build a map from message ID to full message for quick lookup
    // let message_map: HashMap<String, google_gmail1::api::Message> = full_messages
    //     .into_iter()
    //     .filter_map(|m| m.id.clone().map(|id| (id, m)))
    //     .collect();

    // // Insert full messages back into each thread
    // let threads_with_messages: Vec<google_gmail1::api::Thread> = threads_to_return
    //     .into_iter()
    //     .map(|mut thread| {
    //         if let Some(messages) = thread.messages.as_mut() {
    //             *messages = messages
    //                 .iter()
    //                 .filter_map(|m| m.id.as_ref().and_then(|id| message_map.get(id).cloned()))
    //                 .collect();
    //         }
    //         thread
    //     })
    //     .collect();

    let threads: Vec<_> = threads_to_return
        .into_iter()
        .map(|t| parsed_and_sanitized_gmail_thread(t).ok())
        .flatten()
        .collect();

    Ok(Json(GetAllEmailsResponse {
        threads,
        next_cursor,
        has_more,
    }))
}

#[derive(Debug)]
pub struct Attachment {
    pub filename: String,
    pub content: Vec<u8>,
    pub content_type: String,
}

#[derive(Debug)]
pub struct SendEmailData {
    pub to: Vec<String>,
    pub cc: Option<Vec<String>>,
    pub bcc: Option<Vec<String>>,
    pub subject: String,
    pub body: String,
    pub is_html: bool,
    pub attachments: Vec<Attachment>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SendEmailResponse {
    pub message_id: String,
    pub thread_id: Option<String>,
}

/// # POST /email/send
///
/// Multipart form fields:
/// - `to`: recipient email(s), comma-separated
/// - `cc`: cc email(s), comma-separated (optional)
/// - `bcc`: bcc email(s), comma-separated (optional)
/// - `subject`: email subject
/// - `body`: email body text
/// - `isHtml`: "true" or "false" (optional, defaults to false)
/// - `attachments`: file field(s), can be multiple
pub async fn send(
    claims: Claims,
    State(state): State<ServerState>,
    multipart: Multipart,
) -> AppJsonResult<SendEmailResponse> {
    let _user_id = claims.sub;
    let _email_data = parse_multipart(multipart).await?;
    let _message = build_email(&_email_data)?;

    unimplemented!()
}

async fn parse_multipart(
    mut multipart: Multipart,
) -> Result<SendEmailData, crate::error::AppError> {
    let mut to: Option<Vec<String>> = None;
    let mut cc: Option<Vec<String>> = None;
    let mut bcc: Option<Vec<String>> = None;
    let mut subject: Option<String> = None;
    let mut body: Option<String> = None;
    let mut is_html = false;
    let mut attachments: Vec<Attachment> = Vec::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError::BadRequest(format!("Failed to read multipart field: {}", e)))?
    {
        let name = field.name().unwrap_or_default().to_string();

        match name.as_str() {
            "to" => {
                let value = field.text().await.map_err(|e| {
                    AppError::BadRequest(format!("Failed to read 'to' field: {}", e))
                })?;
                to = Some(value.split(',').map(|s| s.trim().to_string()).collect());
            }
            "cc" => {
                let value = field.text().await.map_err(|e| {
                    AppError::BadRequest(format!("Failed to read 'cc' field: {}", e))
                })?;
                cc = Some(value.split(',').map(|s| s.trim().to_string()).collect());
            }
            "bcc" => {
                let value = field.text().await.map_err(|e| {
                    AppError::BadRequest(format!("Failed to read 'bcc' field: {}", e))
                })?;
                bcc = Some(value.split(',').map(|s| s.trim().to_string()).collect());
            }
            "subject" => {
                subject = Some(field.text().await.map_err(|e| {
                    AppError::BadRequest(format!("Failed to read 'subject' field: {}", e))
                })?);
            }
            "body" => {
                body = Some(field.text().await.map_err(|e| {
                    AppError::BadRequest(format!("Failed to read 'body' field: {}", e))
                })?);
            }
            "isHtml" => {
                let value = field.text().await.map_err(|e| {
                    AppError::BadRequest(format!("Failed to read 'isHtml' field: {}", e))
                })?;
                is_html = value.to_lowercase() == "true";
            }
            "attachments" => {
                let filename = field.file_name().unwrap_or("attachment").to_string();
                let content_type = field
                    .content_type()
                    .unwrap_or("application/octet-stream")
                    .to_string();
                let content = field
                    .bytes()
                    .await
                    .map_err(|e| AppError::BadRequest(format!("Failed to read attachment: {}", e)))?
                    .to_vec();

                attachments.push(Attachment {
                    filename,
                    content,
                    content_type,
                });
            }
            _ => {}
        }
    }

    Ok(SendEmailData {
        to: to.ok_or_else(|| AppError::BadRequest("Missing 'to' field".into()))?,
        cc,
        bcc,
        subject: subject.ok_or_else(|| AppError::BadRequest("Missing 'subject' field".into()))?,
        body: body.ok_or_else(|| AppError::BadRequest("Missing 'body' field".into()))?,
        is_html,
        attachments,
    })
}

fn build_email(data: &SendEmailData) -> Result<Message, crate::error::AppError> {
    let mut builder = Message::builder().subject(&data.subject);

    // Add recipients
    for recipient in &data.to {
        builder = builder.to(recipient.parse().map_err(|e| {
            AppError::BadRequest(format!("Invalid 'to' address '{}': {}", recipient, e))
        })?);
    }

    if let Some(cc_list) = &data.cc {
        for recipient in cc_list {
            builder = builder.cc(recipient.parse().map_err(|e| {
                AppError::BadRequest(format!("Invalid 'cc' address '{}': {}", recipient, e))
            })?);
        }
    }

    if let Some(bcc_list) = &data.bcc {
        for recipient in bcc_list {
            builder = builder.bcc(recipient.parse().map_err(|e| {
                AppError::BadRequest(format!("Invalid 'bcc' address '{}': {}", recipient, e))
            })?);
        }
    }

    // Build body part
    let body_part = if data.is_html {
        LettreSinglePart::builder()
            .header(ContentType::TEXT_HTML)
            .body(data.body.clone())
    } else {
        LettreSinglePart::builder()
            .header(ContentType::TEXT_PLAIN)
            .body(data.body.clone())
    };

    // Build message with or without attachments
    let message = if data.attachments.is_empty() {
        builder
            .body(data.body.clone())
            .map_err(|e| AppError::BadRequest(format!("Failed to build email: {}", e)))?
    } else {
        let mut multipart = LettreMultiPart::mixed().singlepart(body_part);

        for attachment in &data.attachments {
            let content_type: ContentType = attachment
                .content_type
                .parse()
                .unwrap_or(ContentType::TEXT_PLAIN);

            let lettre_attachment = LettreAttachment::new(attachment.filename.clone())
                .body(attachment.content.clone(), content_type);

            multipart = multipart.singlepart(lettre_attachment);
        }

        builder
            .multipart(multipart)
            .map_err(|e| AppError::BadRequest(format!("Failed to build email: {}", e)))?
    };

    Ok(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::FromRequest;
    use axum::http::Request;

    fn create_multipart_body(
        boundary: &str,
        fields: Vec<(&str, &str, Option<&str>, &[u8])>,
    ) -> Vec<u8> {
        let mut body = Vec::new();

        for (name, content_type, filename, data) in fields {
            body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());

            if let Some(fname) = filename {
                body.extend_from_slice(
                    format!(
                        "Content-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\n",
                        name, fname
                    )
                    .as_bytes(),
                );
            } else {
                body.extend_from_slice(
                    format!("Content-Disposition: form-data; name=\"{}\"\r\n", name).as_bytes(),
                );
            }

            body.extend_from_slice(format!("Content-Type: {}\r\n\r\n", content_type).as_bytes());
            body.extend_from_slice(data);
            body.extend_from_slice(b"\r\n");
        }

        body.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());
        body
    }

    async fn create_multipart_from_body(boundary: &str, body: Vec<u8>) -> Multipart {
        let request = Request::builder()
            .header(
                "content-type",
                format!("multipart/form-data; boundary={}", boundary),
            )
            .body(Body::from(body))
            .unwrap();

        Multipart::from_request(request, &()).await.unwrap()
    }

    #[tokio::test]
    async fn test_parse_multipart_basic_email() {
        let boundary = "----TestBoundary1234";
        let body = create_multipart_body(
            boundary,
            vec![
                ("to", "text/plain", None, b"test@example.com"),
                ("subject", "text/plain", None, b"Test Subject"),
                ("body", "text/plain", None, b"Hello, this is a test email."),
            ],
        );

        let multipart = create_multipart_from_body(boundary, body).await;
        let result = parse_multipart(multipart).await.unwrap();

        assert_eq!(result.to, vec!["test@example.com"]);
        assert_eq!(result.subject, "Test Subject");
        assert_eq!(result.body, "Hello, this is a test email.");
        assert!(!result.is_html);
        assert!(result.cc.is_none());
        assert!(result.bcc.is_none());
        assert!(result.attachments.is_empty());
    }

    #[tokio::test]
    async fn test_parse_multipart_multiple_recipients() {
        let boundary = "----TestBoundary5678";
        let body = create_multipart_body(
            boundary,
            vec![
                (
                    "to",
                    "text/plain",
                    None,
                    b"alice@example.com, bob@example.com",
                ),
                ("cc", "text/plain", None, b"charlie@example.com"),
                (
                    "bcc",
                    "text/plain",
                    None,
                    b"dave@example.com, eve@example.com",
                ),
                ("subject", "text/plain", None, b"Group Email"),
                ("body", "text/plain", None, b"Hello everyone!"),
            ],
        );

        let multipart = create_multipart_from_body(boundary, body).await;
        let result = parse_multipart(multipart).await.unwrap();

        assert_eq!(result.to, vec!["alice@example.com", "bob@example.com"]);
        assert_eq!(result.cc, Some(vec!["charlie@example.com".to_string()]));
        assert_eq!(
            result.bcc,
            Some(vec![
                "dave@example.com".to_string(),
                "eve@example.com".to_string()
            ])
        );
    }

    #[tokio::test]
    async fn test_parse_multipart_html_email() {
        let boundary = "----TestBoundaryHTML";
        let body = create_multipart_body(
            boundary,
            vec![
                ("to", "text/plain", None, b"test@example.com"),
                ("subject", "text/plain", None, b"HTML Email"),
                (
                    "body",
                    "text/plain",
                    None,
                    b"<h1>Hello</h1><p>This is HTML</p>",
                ),
                ("isHtml", "text/plain", None, b"true"),
            ],
        );

        let multipart = create_multipart_from_body(boundary, body).await;
        let result = parse_multipart(multipart).await.unwrap();

        assert!(result.is_html);
        assert_eq!(result.body, "<h1>Hello</h1><p>This is HTML</p>");
    }

    #[tokio::test]
    async fn test_parse_multipart_with_attachment() {
        let boundary = "----TestBoundaryAttach";
        let attachment_content = b"This is the content of the attachment.";

        let body = create_multipart_body(
            boundary,
            vec![
                ("to", "text/plain", None, b"test@example.com"),
                ("subject", "text/plain", None, b"Email with Attachment"),
                ("body", "text/plain", None, b"Please see attached."),
                (
                    "attachments",
                    "text/plain",
                    Some("document.txt"),
                    attachment_content,
                ),
            ],
        );

        let multipart = create_multipart_from_body(boundary, body).await;
        let result = parse_multipart(multipart).await.unwrap();

        assert_eq!(result.attachments.len(), 1);
        assert_eq!(result.attachments[0].filename, "document.txt");
        assert_eq!(result.attachments[0].content_type, "text/plain");
        assert_eq!(result.attachments[0].content, attachment_content.to_vec());
    }

    #[tokio::test]
    async fn test_parse_multipart_with_multiple_attachments() {
        let boundary = "----TestBoundaryMultiAttach";

        let body = create_multipart_body(
            boundary,
            vec![
                ("to", "text/plain", None, b"test@example.com"),
                ("subject", "text/plain", None, b"Multiple Attachments"),
                ("body", "text/plain", None, b"Multiple files attached."),
                ("attachments", "text/plain", Some("file1.txt"), b"Content 1"),
                (
                    "attachments",
                    "application/pdf",
                    Some("file2.pdf"),
                    b"PDF content",
                ),
                (
                    "attachments",
                    "image/png",
                    Some("image.png"),
                    b"\x89PNG\r\n\x1a\n",
                ),
            ],
        );

        let multipart = create_multipart_from_body(boundary, body).await;
        let result = parse_multipart(multipart).await.unwrap();

        assert_eq!(result.attachments.len(), 3);

        assert_eq!(result.attachments[0].filename, "file1.txt");
        assert_eq!(result.attachments[0].content_type, "text/plain");

        assert_eq!(result.attachments[1].filename, "file2.pdf");
        assert_eq!(result.attachments[1].content_type, "application/pdf");

        assert_eq!(result.attachments[2].filename, "image.png");
        assert_eq!(result.attachments[2].content_type, "image/png");
    }

    #[tokio::test]
    async fn test_parse_multipart_missing_to_field() {
        let boundary = "----TestBoundaryMissingTo";
        let body = create_multipart_body(
            boundary,
            vec![
                ("subject", "text/plain", None, b"No recipient"),
                ("body", "text/plain", None, b"Missing to field"),
            ],
        );

        let multipart = create_multipart_from_body(boundary, body).await;
        let result = parse_multipart(multipart).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("'to'")));
    }

    #[tokio::test]
    async fn test_parse_multipart_missing_subject_field() {
        let boundary = "----TestBoundaryMissingSubject";
        let body = create_multipart_body(
            boundary,
            vec![
                ("to", "text/plain", None, b"test@example.com"),
                ("body", "text/plain", None, b"Missing subject"),
            ],
        );

        let multipart = create_multipart_from_body(boundary, body).await;
        let result = parse_multipart(multipart).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("'subject'")));
    }

    #[tokio::test]
    async fn test_parse_multipart_missing_body_field() {
        let boundary = "----TestBoundaryMissingBody";
        let body = create_multipart_body(
            boundary,
            vec![
                ("to", "text/plain", None, b"test@example.com"),
                ("subject", "text/plain", None, b"No body"),
            ],
        );

        let multipart = create_multipart_from_body(boundary, body).await;
        let result = parse_multipart(multipart).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("'body'")));
    }
}
