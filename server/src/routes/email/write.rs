use anyhow::anyhow;
use axum::{
    extract::{Multipart, State},
    Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use indoc::formatdoc;
use lettre::{
    message::{
        header::ContentType, Attachment as LettreAttachment, Mailbox, MultiPart as LettreMultiPart,
        SinglePart as LettreSinglePart,
    },
    Message,
};
use serde::Serialize;

use crate::{
    auth::jwt::Claims,
    error::{AppError, AppJsonResult},
    util::html_escape,
    ServerState,
};

use super::shared::{extract_body_from_payload, fetch_email_client, get_message_header};

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
    /// Message ID to reply to (for threading)
    pub reply_to_message_id: Option<String>,
    /// Message ID to forward
    pub forward_message_id: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SendEmailResponse {
    pub message_id: String,
    pub thread_id: Option<String>,
}

/// Context for building a reply email
#[derive(Debug, Default)]
struct ReplyContext {
    /// The Message-ID header of the original message
    in_reply_to: Option<String>,
    /// The References header chain (includes original Message-ID)
    references: Option<String>,
    /// The thread ID to keep the reply in the same thread
    thread_id: Option<String>,
}

/// Metadata extracted from the original message for forwarding
#[derive(Debug)]
struct ForwardMetadata {
    from: Option<String>,
    to: Option<String>,
    date: Option<String>,
    subject: Option<String>,
    body_text: Option<String>,
    body_html: Option<String>,
}

/// Extract reply context from an original message
fn extract_reply_context(message: &google_gmail1::api::Message) -> ReplyContext {
    let message_id = get_message_header(message, "Message-ID");
    let existing_references = get_message_header(message, "References");

    // Build References header: existing references + original Message-ID
    let references = match (&existing_references, &message_id) {
        (Some(refs), Some(mid)) => Some(format!("{} {}", refs, mid)),
        (None, Some(mid)) => Some(mid.clone()),
        (Some(refs), None) => Some(refs.clone()),
        (None, None) => None,
    };

    ReplyContext {
        in_reply_to: message_id,
        references,
        thread_id: message.thread_id.clone(),
    }
}

/// Extract metadata from the original message for forwarding
fn extract_forward_metadata(message: &google_gmail1::api::Message) -> ForwardMetadata {
    let (body_text, body_html) = message
        .payload
        .as_ref()
        .map(extract_body_from_payload)
        .unwrap_or((None, None));

    ForwardMetadata {
        from: get_message_header(message, "From"),
        to: get_message_header(message, "To"),
        date: get_message_header(message, "Date"),
        subject: get_message_header(message, "Subject"),
        body_text,
        body_html,
    }
}

/// Format the forwarded message body (plain text version)
fn format_forward_body_text(user_body: &str, metadata: &ForwardMetadata) -> String {
    let original_body = metadata.body_text.as_deref().unwrap_or("");
    format!(
        "{}\n\n---------- Forwarded message ---------\nFrom: {}\nDate: {}\nSubject: {}\nTo: {}\n\n{}",
        user_body,
        metadata.from.as_deref().unwrap_or(""),
        metadata.date.as_deref().unwrap_or(""),
        metadata.subject.as_deref().unwrap_or(""),
        metadata.to.as_deref().unwrap_or(""),
        original_body
    )
}

/// Format the forwarded message body (HTML version)
fn format_forward_body_html(user_body: &str, metadata: &ForwardMetadata) -> String {
    let original_body = metadata
        .body_html
        .as_deref()
        .or(metadata.body_text.as_deref())
        .unwrap_or("");

    formatdoc! {r#"
        {user_body}<br><br>
        <div style="border-left: 1px solid #ccc; padding-left: 12px; margin-left: 0;">
            <p>
                <b>---------- Forwarded message ---------</b><br>
                <b>From:</b> {from}<br>
                <b>Date:</b> {date}<br>
                <b>Subject:</b> {subject}<br>
                <b>To:</b> {to}
            </p>
            {original_body}
        </div>"#,
        user_body = user_body,
        from = html_escape(metadata.from.as_deref().unwrap_or("")),
        date = html_escape(metadata.date.as_deref().unwrap_or("")),
        subject = html_escape(metadata.subject.as_deref().unwrap_or("")),
        to = html_escape(metadata.to.as_deref().unwrap_or("")),
        original_body = original_body,
    }
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
/// - `replyToMessageId`: Gmail message ID to reply to (optional)
/// - `forwardMessageId`: Gmail message ID to forward (optional)
pub async fn send(
    claims: Claims,
    State(state): State<ServerState>,
    multipart: Multipart,
) -> AppJsonResult<SendEmailResponse> {
    let user_email = &claims.email;
    let mut email_data = parse_multipart(multipart).await?;

    // Get the user's email client
    let email_client = fetch_email_client(state.clone(), user_email.clone()).await?;

    // Determine if this is a reply, forward, or new message
    let mut reply_context = ReplyContext::default();

    // Handle reply
    if let Some(ref reply_msg_id) = email_data.reply_to_message_id {
        let original_message = email_client
            .get_message_with_headers(reply_msg_id)
            .await
            .map_err(|e| {
                AppError::BadRequest(format!("Failed to fetch original message for reply: {}", e))
            })?;

        reply_context = extract_reply_context(&original_message);

        // Auto-prefix subject with "Re:" if not already present
        if !email_data.subject.to_lowercase().starts_with("re:") {
            email_data.subject = format!("Re: {}", email_data.subject);
        }
    }

    // Handle forward
    if let Some(ref forward_msg_id) = email_data.forward_message_id {
        let original_message = email_client
            .get_message_with_headers(forward_msg_id)
            .await
            .map_err(|e| {
                AppError::BadRequest(format!(
                    "Failed to fetch original message for forward: {}",
                    e
                ))
            })?;

        let forward_metadata = extract_forward_metadata(&original_message);

        // Format body with forwarded content
        if email_data.is_html {
            email_data.body = format_forward_body_html(&email_data.body, &forward_metadata);
        } else {
            email_data.body = format_forward_body_text(&email_data.body, &forward_metadata);
        }

        // Auto-prefix subject with "Fwd:" if not already present
        if !email_data.subject.to_lowercase().starts_with("fwd:") {
            email_data.subject = format!("Fwd: {}", email_data.subject);
        }
    }

    // Build the email message
    let message = build_email(&email_data, user_email, &reply_context)?;

    // Convert to base64url-encoded raw format for Gmail API
    let raw_message = URL_SAFE_NO_PAD.encode(message.formatted());

    // Send via Gmail API
    let sent_message = email_client
        .send_message(&raw_message, reply_context.thread_id.as_deref())
        .await
        .map_err(|e| AppError::Internal(anyhow!("Failed to send email: {}", e)))?;

    Ok(Json(SendEmailResponse {
        message_id: sent_message.id.unwrap_or_default(),
        thread_id: sent_message.thread_id,
    }))
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
    let mut reply_to_message_id: Option<String> = None;
    let mut forward_message_id: Option<String> = None;

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
            "replyToMessageId" => {
                let value = field.text().await.map_err(|e| {
                    AppError::BadRequest(format!("Failed to read 'replyToMessageId' field: {}", e))
                })?;
                if !value.is_empty() {
                    reply_to_message_id = Some(value);
                }
            }
            "forwardMessageId" => {
                let value = field.text().await.map_err(|e| {
                    AppError::BadRequest(format!("Failed to read 'forwardMessageId' field: {}", e))
                })?;
                if !value.is_empty() {
                    forward_message_id = Some(value);
                }
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
        reply_to_message_id,
        forward_message_id,
    })
}

fn build_email(
    data: &SendEmailData,
    from_email: &str,
    reply_context: &ReplyContext,
) -> Result<Message, crate::error::AppError> {
    // Parse the from address
    let from_mailbox: Mailbox = from_email.parse().map_err(|e| {
        AppError::BadRequest(format!("Invalid 'from' address '{}': {}", from_email, e))
    })?;

    let mut builder = Message::builder().from(from_mailbox).subject(&data.subject);

    // Add In-Reply-To header for replies
    if let Some(ref in_reply_to) = reply_context.in_reply_to {
        builder = builder.in_reply_to(in_reply_to.clone());
    }

    // Add References header for replies (helps email clients thread properly)
    if let Some(ref references) = reply_context.references {
        builder = builder.references(references.clone());
    }

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
    use crate::util::html_escape;
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

    #[tokio::test]
    async fn test_parse_multipart_with_reply_to_message_id() {
        let boundary = "----TestBoundaryReply";
        let body = create_multipart_body(
            boundary,
            vec![
                ("to", "text/plain", None, b"test@example.com"),
                ("subject", "text/plain", None, b"Re: Original Subject"),
                ("body", "text/plain", None, b"This is my reply."),
                ("replyToMessageId", "text/plain", None, b"abc123def456"),
            ],
        );

        let multipart = create_multipart_from_body(boundary, body).await;
        let result = parse_multipart(multipart).await.unwrap();

        assert_eq!(result.reply_to_message_id, Some("abc123def456".to_string()));
        assert!(result.forward_message_id.is_none());
    }

    #[tokio::test]
    async fn test_parse_multipart_with_forward_message_id() {
        let boundary = "----TestBoundaryForward";
        let body = create_multipart_body(
            boundary,
            vec![
                ("to", "text/plain", None, b"test@example.com"),
                ("subject", "text/plain", None, b"Fwd: Original Subject"),
                ("body", "text/plain", None, b"FYI - see below."),
                ("forwardMessageId", "text/plain", None, b"xyz789ghi012"),
            ],
        );

        let multipart = create_multipart_from_body(boundary, body).await;
        let result = parse_multipart(multipart).await.unwrap();

        assert_eq!(result.forward_message_id, Some("xyz789ghi012".to_string()));
        assert!(result.reply_to_message_id.is_none());
    }

    #[tokio::test]
    async fn test_parse_multipart_empty_reply_forward_fields() {
        let boundary = "----TestBoundaryEmptyFields";
        let body = create_multipart_body(
            boundary,
            vec![
                ("to", "text/plain", None, b"test@example.com"),
                ("subject", "text/plain", None, b"Test Subject"),
                ("body", "text/plain", None, b"Test body"),
                ("replyToMessageId", "text/plain", None, b""),
                ("forwardMessageId", "text/plain", None, b""),
            ],
        );

        let multipart = create_multipart_from_body(boundary, body).await;
        let result = parse_multipart(multipart).await.unwrap();

        // Empty strings should result in None
        assert!(result.reply_to_message_id.is_none());
        assert!(result.forward_message_id.is_none());
    }

    #[test]
    fn test_extract_reply_context_with_message_id() {
        let message = google_gmail1::api::Message {
            thread_id: Some("thread123".to_string()),
            payload: Some(google_gmail1::api::MessagePart {
                headers: Some(vec![google_gmail1::api::MessagePartHeader {
                    name: Some("Message-ID".to_string()),
                    value: Some("<original@example.com>".to_string()),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let context = extract_reply_context(&message);

        assert_eq!(
            context.in_reply_to,
            Some("<original@example.com>".to_string())
        );
        assert_eq!(
            context.references,
            Some("<original@example.com>".to_string())
        );
        assert_eq!(context.thread_id, Some("thread123".to_string()));
    }

    #[test]
    fn test_extract_reply_context_with_existing_references() {
        let message = google_gmail1::api::Message {
            thread_id: Some("thread456".to_string()),
            payload: Some(google_gmail1::api::MessagePart {
                headers: Some(vec![
                    google_gmail1::api::MessagePartHeader {
                        name: Some("Message-ID".to_string()),
                        value: Some("<current@example.com>".to_string()),
                    },
                    google_gmail1::api::MessagePartHeader {
                        name: Some("References".to_string()),
                        value: Some("<first@example.com> <second@example.com>".to_string()),
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let context = extract_reply_context(&message);

        assert_eq!(
            context.in_reply_to,
            Some("<current@example.com>".to_string())
        );
        assert_eq!(
            context.references,
            Some("<first@example.com> <second@example.com> <current@example.com>".to_string())
        );
        assert_eq!(context.thread_id, Some("thread456".to_string()));
    }

    #[test]
    fn test_extract_reply_context_no_headers() {
        let message = google_gmail1::api::Message::default();

        let context = extract_reply_context(&message);

        assert!(context.in_reply_to.is_none());
        assert!(context.references.is_none());
        assert!(context.thread_id.is_none());
    }

    #[test]
    fn test_get_message_header() {
        use super::super::shared::get_message_header;

        let message = google_gmail1::api::Message {
            payload: Some(google_gmail1::api::MessagePart {
                headers: Some(vec![
                    google_gmail1::api::MessagePartHeader {
                        name: Some("From".to_string()),
                        value: Some("sender@example.com".to_string()),
                    },
                    google_gmail1::api::MessagePartHeader {
                        name: Some("Subject".to_string()),
                        value: Some("Test Subject".to_string()),
                    },
                    google_gmail1::api::MessagePartHeader {
                        name: Some("Date".to_string()),
                        value: Some("Mon, 1 Jan 2024 12:00:00 +0000".to_string()),
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            get_message_header(&message, "From"),
            Some("sender@example.com".to_string())
        );
        assert_eq!(
            get_message_header(&message, "subject"), // Case insensitive
            Some("Test Subject".to_string())
        );
        assert_eq!(get_message_header(&message, "NonExistent"), None);
    }

    #[test]
    fn test_extract_forward_metadata() {
        let message = google_gmail1::api::Message {
            payload: Some(google_gmail1::api::MessagePart {
                mime_type: Some("text/plain".to_string()),
                headers: Some(vec![
                    google_gmail1::api::MessagePartHeader {
                        name: Some("From".to_string()),
                        value: Some("original-sender@example.com".to_string()),
                    },
                    google_gmail1::api::MessagePartHeader {
                        name: Some("To".to_string()),
                        value: Some("original-recipient@example.com".to_string()),
                    },
                    google_gmail1::api::MessagePartHeader {
                        name: Some("Date".to_string()),
                        value: Some("Mon, 1 Jan 2024 12:00:00 +0000".to_string()),
                    },
                    google_gmail1::api::MessagePartHeader {
                        name: Some("Subject".to_string()),
                        value: Some("Original Subject".to_string()),
                    },
                ]),
                body: Some(google_gmail1::api::MessagePartBody {
                    data: Some("Original email body content".as_bytes().to_vec()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let metadata = extract_forward_metadata(&message);

        assert_eq!(
            metadata.from,
            Some("original-sender@example.com".to_string())
        );
        assert_eq!(
            metadata.to,
            Some("original-recipient@example.com".to_string())
        );
        assert_eq!(
            metadata.date,
            Some("Mon, 1 Jan 2024 12:00:00 +0000".to_string())
        );
        assert_eq!(metadata.subject, Some("Original Subject".to_string()));
        assert_eq!(
            metadata.body_text,
            Some("Original email body content".to_string())
        );
    }

    #[test]
    fn test_format_forward_body_text() {
        let metadata = ForwardMetadata {
            from: Some("sender@example.com".to_string()),
            to: Some("recipient@example.com".to_string()),
            date: Some("Mon, 1 Jan 2024 12:00:00 +0000".to_string()),
            subject: Some("Original Subject".to_string()),
            body_text: Some("This is the original message.".to_string()),
            body_html: None,
        };

        let result = format_forward_body_text("My comment before forwarding.", &metadata);

        assert!(result.contains("My comment before forwarding."));
        assert!(result.contains("---------- Forwarded message ---------"));
        assert!(result.contains("From: sender@example.com"));
        assert!(result.contains("Date: Mon, 1 Jan 2024 12:00:00 +0000"));
        assert!(result.contains("Subject: Original Subject"));
        assert!(result.contains("To: recipient@example.com"));
        assert!(result.contains("This is the original message."));
    }

    #[test]
    fn test_format_forward_body_html() {
        let metadata = ForwardMetadata {
            from: Some("sender@example.com".to_string()),
            to: Some("recipient@example.com".to_string()),
            date: Some("Mon, 1 Jan 2024 12:00:00 +0000".to_string()),
            subject: Some("Original Subject".to_string()),
            body_text: None,
            body_html: Some("<p>This is the <b>original</b> message.</p>".to_string()),
        };

        let result = format_forward_body_html("<p>My comment.</p>", &metadata);

        assert!(result.contains("<p>My comment.</p>"));
        assert!(result.contains("Forwarded message"));
        assert!(result.contains("sender@example.com"));
        assert!(result.contains("recipient@example.com"));
        assert!(result.contains("<p>This is the <b>original</b> message.</p>"));
    }

    #[test]
    fn test_format_forward_body_html_escapes_metadata() {
        let metadata = ForwardMetadata {
            from: Some("sender <test@example.com>".to_string()),
            to: Some("recipient@example.com".to_string()),
            date: Some("Mon, 1 Jan 2024".to_string()),
            subject: Some("Test <script>alert('xss')</script>".to_string()),
            body_text: Some("Safe content".to_string()),
            body_html: None,
        };

        let result = format_forward_body_html("My comment", &metadata);

        // Metadata should be escaped
        assert!(result.contains("&lt;test@example.com&gt;"));
        assert!(result.contains("&lt;script&gt;"));
        assert!(!result.contains("<script>"));
    }

    #[test]
    fn test_html_escape() {
        assert_eq!(html_escape("<script>"), "&lt;script&gt;");
        assert_eq!(html_escape("a & b"), "a &amp; b");
        assert_eq!(html_escape("\"quoted\""), "&quot;quoted&quot;");
        assert_eq!(html_escape("normal text"), "normal text");
    }

    #[test]
    fn test_build_email_with_reply_context() {
        let data = SendEmailData {
            to: vec!["recipient@example.com".to_string()],
            cc: None,
            bcc: None,
            subject: "Re: Original Subject".to_string(),
            body: "This is my reply.".to_string(),
            is_html: false,
            attachments: vec![],
            reply_to_message_id: Some("abc123".to_string()),
            forward_message_id: None,
        };

        let reply_context = ReplyContext {
            in_reply_to: Some("<original@example.com>".to_string()),
            references: Some("<original@example.com>".to_string()),
            thread_id: Some("thread123".to_string()),
        };

        let message = build_email(&data, "sender@example.com", &reply_context).unwrap();
        let formatted_bytes = message.formatted();
        let formatted = String::from_utf8_lossy(&formatted_bytes);

        assert!(formatted.contains("From: sender@example.com"));
        assert!(formatted.contains("To: recipient@example.com"));
        assert!(formatted.contains("Subject: Re: Original Subject"));
        assert!(formatted.contains("In-Reply-To: <original@example.com>"));
        assert!(formatted.contains("References: <original@example.com>"));
        assert!(formatted.contains("This is my reply."));
    }

    #[test]
    fn test_build_email_without_reply_context() {
        let data = SendEmailData {
            to: vec!["recipient@example.com".to_string()],
            cc: None,
            bcc: None,
            subject: "New Email".to_string(),
            body: "This is a new email.".to_string(),
            is_html: false,
            attachments: vec![],
            reply_to_message_id: None,
            forward_message_id: None,
        };

        let reply_context = ReplyContext::default();

        let message = build_email(&data, "sender@example.com", &reply_context).unwrap();
        let formatted_bytes = message.formatted();
        let formatted = String::from_utf8_lossy(&formatted_bytes);

        assert!(formatted.contains("From: sender@example.com"));
        assert!(formatted.contains("To: recipient@example.com"));
        assert!(formatted.contains("Subject: New Email"));
        // Should not contain reply headers
        assert!(!formatted.contains("In-Reply-To:"));
        assert!(!formatted.contains("References:"));
    }

    #[test]
    fn test_build_email_html_body() {
        let data = SendEmailData {
            to: vec!["recipient@example.com".to_string()],
            cc: None,
            bcc: None,
            subject: "HTML Email".to_string(),
            body: "<h1>Hello</h1><p>This is HTML.</p>".to_string(),
            is_html: true,
            attachments: vec![],
            reply_to_message_id: None,
            forward_message_id: None,
        };

        let reply_context = ReplyContext::default();

        let message = build_email(&data, "sender@example.com", &reply_context).unwrap();
        let formatted_bytes = message.formatted();
        let formatted = String::from_utf8_lossy(&formatted_bytes);

        assert!(formatted.contains("<h1>Hello</h1>"));
    }
}
