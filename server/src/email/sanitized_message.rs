use ammonia::Builder;
use anyhow::Context;
use base64::{engine::general_purpose::STANDARD, Engine};
use google_gmail1::api::{Message, MessagePart};
use indoc::formatdoc;
use mail_parser::{MessageParser, MimeHeaders};
use once_cell::sync::Lazy;
use serde::Serialize;
use std::collections::{HashMap, HashSet};

/// HTML sanitizer configured for email content
/// Allows common email formatting tags while removing dangerous elements
static HTML_SANITIZER: Lazy<Builder<'static>> = Lazy::new(|| {
    let mut tags: HashSet<&'static str> = HashSet::new();
    // Common text formatting
    tags.extend([
        "a",
        "abbr",
        "acronym",
        "address",
        "b",
        "bdi",
        "bdo",
        "big",
        "blockquote",
        "br",
        "caption",
        "center",
        "cite",
        "code",
        "col",
        "colgroup",
        "dd",
        "del",
        "dfn",
        "div",
        "dl",
        "dt",
        "em",
        "figcaption",
        "figure",
        "font",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "hr",
        "i",
        "img",
        "ins",
        "kbd",
        "li",
        "mark",
        "ol",
        "p",
        "pre",
        "q",
        "rp",
        "rt",
        "ruby",
        "s",
        "samp",
        "small",
        "span",
        "strike",
        "strong",
        "sub",
        "sup",
        "table",
        "tbody",
        "td",
        "tfoot",
        "th",
        "thead",
        "tr",
        "tt",
        "u",
        "ul",
        "var",
        "wbr",
    ]);

    let mut tag_attributes: HashMap<&'static str, HashSet<&'static str>> = HashMap::new();

    // Allow common attributes on most tags
    let generic_attrs: HashSet<&'static str> = [
        "align", "class", "dir", "id", "lang", "style", "title", "valign",
    ]
    .into_iter()
    .collect();

    for tag in &tags {
        tag_attributes.insert(tag, generic_attrs.clone());
    }

    // Additional attributes for specific tags
    // Note: "rel" is excluded because link_rel() manages it automatically
    tag_attributes
        .get_mut("a")
        .unwrap()
        .extend(["href", "name", "target"]);
    tag_attributes
        .get_mut("img")
        .unwrap()
        .extend(["src", "alt", "width", "height", "border"]);
    tag_attributes.get_mut("table").unwrap().extend([
        "border",
        "cellpadding",
        "cellspacing",
        "width",
        "height",
        "bgcolor",
    ]);
    tag_attributes
        .get_mut("td")
        .unwrap()
        .extend(["colspan", "rowspan", "width", "height", "bgcolor", "nowrap"]);
    tag_attributes.get_mut("th").unwrap().extend([
        "colspan", "rowspan", "width", "height", "bgcolor", "nowrap", "scope",
    ]);
    tag_attributes
        .get_mut("col")
        .unwrap()
        .extend(["span", "width"]);
    tag_attributes
        .get_mut("colgroup")
        .unwrap()
        .extend(["span", "width"]);
    tag_attributes
        .get_mut("font")
        .unwrap()
        .extend(["color", "face", "size"]);
    tag_attributes
        .get_mut("hr")
        .unwrap()
        .extend(["size", "width", "noshade"]);
    tag_attributes
        .get_mut("ol")
        .unwrap()
        .extend(["start", "type"]);
    tag_attributes.get_mut("ul").unwrap().insert("type");
    tag_attributes.get_mut("li").unwrap().insert("value");
    tag_attributes.get_mut("blockquote").unwrap().insert("cite");
    tag_attributes.get_mut("q").unwrap().insert("cite");
    tag_attributes
        .get_mut("del")
        .unwrap()
        .extend(["cite", "datetime"]);
    tag_attributes
        .get_mut("ins")
        .unwrap()
        .extend(["cite", "datetime"]);

    // URL schemes allowed in href/src
    let mut url_schemes: HashSet<&'static str> = HashSet::new();
    url_schemes.extend(["http", "https", "mailto", "cid", "data"]);

    let mut builder = Builder::new();
    builder
        .tags(tags)
        .tag_attributes(tag_attributes)
        .url_schemes(url_schemes)
        .link_rel(Some("noopener noreferrer"))
        .strip_comments(true);
    builder
});

/// Sanitize HTML content to remove XSS vectors while preserving email formatting
pub fn sanitize_html(html: &str) -> String {
    HTML_SANITIZER.clean(html).to_string()
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SanitizedMessage {
    pub id: String,
    pub label_ids: Vec<String>,
    pub thread_id: String,
    pub history_id: u64,
    pub internal_date: i64,
    pub from: Option<String>,
    pub subject: Option<String>,
    pub body: Option<String>,
    pub webview: Option<String>,
}

impl SanitizedMessage {
    /// Create a SanitizedMessage from a Gmail API Message
    /// Supports both RAW format (with `raw` field) and FULL format (with `payload` field)
    pub fn from_gmail_message(msg: Message) -> anyhow::Result<Self> {
        let id = msg.id.context("Message missing id")?;
        let thread_id = msg.thread_id.context("Message missing thread_id")?;
        let label_ids = msg.label_ids.unwrap_or_default();
        let history_id = msg.history_id.unwrap_or(0);
        let internal_date = msg.internal_date.unwrap_or(0);

        // Try RAW format first (contains full MIME message)
        if let Some(raw) = &msg.raw {
            let parsed = MessageParser::default()
                .parse(raw)
                .context("Failed to parse MIME message")?;

            let from = parsed.from().and_then(|f| f.first()).map(|addr| {
                if let Some(name) = addr.name() {
                    format!("{} <{}>", name, addr.address().unwrap_or_default())
                } else {
                    addr.address().unwrap_or_default().to_string()
                }
            });

            let subject = parsed.subject().map(|s| s.to_string());

            // Extract plain text body for processing
            let body = parsed.body_text(0).map(|b| b.to_string());

            // Generate webview HTML
            let webview = parsed_message_to_webview_html(parsed).ok();

            return Ok(Self {
                id,
                label_ids,
                thread_id,
                history_id,
                internal_date,
                from,
                subject,
                body,
                webview,
            });
        }

        // Neither raw nor payload available
        Ok(Self {
            id,
            label_ids,
            thread_id,
            history_id,
            internal_date,
            from: None,
            subject: None,
            body: None,
            webview: None,
        })
    }
}

/// Input: Parsed Message
/// Output: full HTML document safe to drop into a WebView
pub fn parsed_message_to_webview_html(message: mail_parser::Message) -> anyhow::Result<String> {
    // Extract preferred body
    let mut html = message.body_html(0).map(|b| b.to_string());
    let text = message.body_text(0).map(|b| b.to_string());

    if html.is_none() {
        // Escape HTML entities
        let escaped = text
            .as_deref()
            .unwrap_or("")
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;");
        html = Some(format!("<pre>{}</pre>", escaped));
    }

    let mut html = html.unwrap();

    // 4. Collect inline images (Content-ID → data URL)
    let mut cid_map: HashMap<String, String> = HashMap::new();

    for part in message.attachments() {
        // Check if this is an inline attachment by looking at Content-Disposition
        let is_inline = part
            .content_disposition()
            .map(|cd| cd.ctype().eq_ignore_ascii_case("inline"))
            .unwrap_or(false);

        if is_inline || part.content_id().is_some() {
            if let Some(cid) = part.content_id() {
                if let Some(ct) = part.content_type() {
                    let data = part.contents();
                    if !data.is_empty() {
                        let mime = ct.ctype();
                        let subtype = ct.subtype().unwrap_or("octet-stream");
                        let encoded = STANDARD.encode(data);
                        cid_map.insert(
                            cid.trim_matches(&['<', '>'][..]).to_string(),
                            format!("data:{}/{};base64,{}", mime, subtype, encoded),
                        );
                    }
                }
            }
        }
    }

    // 5. Rewrite cid: URLs in HTML
    for (cid, data_url) in cid_map {
        html = html.replace(&format!("cid:{}", cid), &data_url);
    }

    // 6. Sanitize HTML to remove XSS vectors (scripts, event handlers, etc.)
    let html = sanitize_html(&html);

    // 7. Wrap with WebView-safe shell
    Ok(wrap_in_webview_shell(&html))
}

/// Decode body.data from Gmail API MessagePartBody
/// The google_gmail1 crate already decodes base64url, so data is raw bytes
fn decode_body_data(bytes: &[u8]) -> anyhow::Result<String> {
    String::from_utf8(bytes.to_vec()).map_err(Into::into)
}

/// Convert raw bytes to standard base64 string (for data URLs)
fn bytes_to_base64url_string(data: &[u8]) -> String {
    // Gmail API stores base64url data as bytes, convert to string for data URL
    String::from_utf8_lossy(data).to_string()
}

/// Extracted body content from a Gmail message payload
#[derive(Debug, Default)]
struct ExtractedBody {
    html: Option<String>,
    text: Option<String>,
    /// Map of Content-ID → data URL for inline images
    cid_map: HashMap<String, String>,
}

/// Parse Gmail API Message payload structure and convert to WebView-safe HTML
/// This works with the `payload` field from threads.get or messages.get (format=full)
pub fn gmail_payload_to_webview_html(message: &Message) -> anyhow::Result<String> {
    let payload = message.payload.as_ref().context("Message has no payload")?;

    let mut extracted = ExtractedBody::default();
    extract_body_from_part(payload, &mut extracted);

    // Use HTML if available, otherwise convert plain text
    let mut html = if let Some(html_body) = extracted.html {
        html_body
    } else if let Some(text_body) = extracted.text {
        // Escape HTML entities and wrap in <pre>
        let escaped = text_body
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;");
        format!("<pre>{}</pre>", escaped)
    } else {
        "<p>No content</p>".to_string()
    };

    // Rewrite cid: URLs to data URLs
    for (cid, data_url) in &extracted.cid_map {
        html = html.replace(&format!("cid:{}", cid), data_url);
    }

    // Sanitize HTML
    let html = sanitize_html(&html);

    // Wrap with WebView-safe shell
    Ok(wrap_in_webview_shell(&html))
}

#[derive(Debug, Clone, Serialize)]
pub struct ExtractedMessage {
    from: Option<String>,
    subject: Option<String>,
    date: Option<String>,
    body: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SanitizedThread {
    #[serde(flatten)]
    pub thread: google_gmail1::api::Thread,
    pub sanitized_messages: Vec<ExtractedMessage>,
}

/// Parse a Gmail Thread and convert all messages to a single WebView-safe HTML document
pub fn parsed_and_sanitized_gmail_thread(
    thread: google_gmail1::api::Thread,
) -> anyhow::Result<SanitizedThread> {
    let messages = thread.messages.as_ref().context("Thread has no messages")?;

    let mut sanitized_messages = Vec::with_capacity(messages.len());

    for message in messages {
        let payload = match message.payload.as_ref() {
            Some(p) => p,
            None => continue,
        };

        let mut extracted = ExtractedBody::default();
        extract_body_from_part(payload, &mut extracted);

        // Get message metadata from headers
        let (from, subject, date) = extract_headers(payload);

        // Use HTML if available, otherwise convert plain text
        let mut body_html = if let Some(html_body) = extracted.html {
            html_body
        } else if let Some(text_body) = extracted.text {
            let escaped = text_body
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
                .replace('"', "&quot;");
            format!("<pre>{}</pre>", escaped)
        } else {
            "<p>No content</p>".to_string()
        };

        // Rewrite cid: URLs
        for (cid, data_url) in &extracted.cid_map {
            body_html = body_html.replace(&format!("cid:{}", cid), data_url);
        }

        // Sanitize
        let body_html = sanitize_html(&body_html);

        // Format as thread message
        sanitized_messages.push(ExtractedMessage {
            from,
            subject,
            date,
            body: body_html,
        });
    }

    Ok(SanitizedThread {
        thread,
        sanitized_messages,
    })
}

/// Recursively extract body content from a MessagePart
fn extract_body_from_part(part: &MessagePart, extracted: &mut ExtractedBody) {
    let mime_type = part.mime_type.as_deref().unwrap_or("");
    println!("Extract body from part!");
    dbg!(&mime_type);
    match mime_type {
        "text/html" => {
            println!("Found html!");
            if let Some(data) = part.body.as_ref().and_then(|b| b.data.as_ref()) {
                if let Ok(html) = decode_body_data(data) {
                    extracted.html = Some(html);
                }
            }
        }
        "text/plain" => {
            if let Some(data) = part.body.as_ref().and_then(|b| b.data.as_ref()) {
                if let Ok(text) = decode_body_data(data) {
                    // Only set text if we don't have it yet (prefer first text/plain)
                    if extracted.text.is_none() {
                        extracted.text = Some(text);
                    }
                }
            }
        }
        mime if mime.starts_with("multipart/") => {
            // Recurse into parts
            if let Some(parts) = &part.parts {
                for sub_part in parts {
                    extract_body_from_part(sub_part, extracted);
                }
            }
        }
        mime if mime.starts_with("image/") => {
            // Check for inline image with Content-ID
            if let Some(cid) = get_header(part, "Content-ID") {
                if let Some(data) = part.body.as_ref().and_then(|b| b.data.as_ref()) {
                    let cid_clean = cid.trim_matches(&['<', '>'][..]).to_string();
                    // Data is already base64url encoded from Gmail API
                    let data_b64 = bytes_to_base64url_string(data);
                    let data_url = format!("data:{};base64,{}", mime_type, data_b64);
                    extracted.cid_map.insert(cid_clean, data_url);
                }
            }
        }
        _ => {
            // For other types, still recurse in case there are nested parts
            if let Some(parts) = &part.parts {
                for sub_part in parts {
                    extract_body_from_part(sub_part, extracted);
                }
            }
        }
    }
}

/// Extract common headers from a MessagePart
fn extract_headers(part: &MessagePart) -> (Option<String>, Option<String>, Option<String>) {
    let from = get_header(part, "From");
    let subject = get_header(part, "Subject");
    let date = get_header(part, "Date");
    (from, subject, date)
}

/// Get a header value from a MessagePart
fn get_header(part: &MessagePart, name: &str) -> Option<String> {
    part.headers.as_ref()?.iter().find_map(|h| {
        if h.name.as_deref()?.eq_ignore_ascii_case(name) {
            h.value.clone()
        } else {
            None
        }
    })
}

/// Wrap HTML body in a WebView-safe shell (single message)
fn wrap_in_webview_shell(body: &str) -> String {
    formatdoc! {r#"
        <!DOCTYPE html>
        <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1.0" />
            <style>
                img {{ max-width: 100%; height: auto; }}
                table {{ max-width: 100%; border-collapse: collapse; }}
                body {{ margin: 0; padding: 8px; font-family: -apple-system, BlinkMacSystemFont, sans-serif; }}
            </style>
        </head>
        <body>{body}</body>
        </html>"#,
        body = body
    }
}

/// Wrap HTML body in a WebView-safe shell (thread with multiple messages)
fn wrap_in_thread_webview_shell(body: &str) -> String {
    formatdoc! {r#"
        <!DOCTYPE html>
        <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1.0" />
            <style>
                img {{ max-width: 100%; height: auto; }}
                table {{ max-width: 100%; border-collapse: collapse; }}
                body {{ margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, sans-serif; }}
                .thread-message {{ border-bottom: 1px solid #e0e0e0; padding: 16px 8px; }}
                .thread-message:last-child {{ border-bottom: none; }}
                .message-header {{ margin-bottom: 12px; }}
                .message-from {{ font-weight: 600; color: #333; }}
                .message-meta {{ font-size: 12px; color: #666; margin-top: 2px; }}
                .message-body {{ overflow-wrap: break-word; }}
            </style>
        </head>
        <body>{body}</body>
        </html>"#,
        body = body
    }
}
