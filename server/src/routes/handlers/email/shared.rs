/// Extract a header value from a Gmail API Message payload
pub(super) fn get_message_header(message: &google_gmail1::api::Message, name: &str) -> Option<String> {
    message
        .payload
        .as_ref()?
        .headers
        .as_ref()?
        .iter()
        .find(|h| {
            h.name
                .as_deref()
                .map(|n| n.eq_ignore_ascii_case(name))
                .unwrap_or(false)
        })
        .and_then(|h| h.value.clone())
}

/// Extract body text from a Gmail message part recursively
pub(super) fn extract_body_from_payload(
    part: &google_gmail1::api::MessagePart,
) -> (Option<String>, Option<String>) {
    let mime_type = part.mime_type.as_deref().unwrap_or("");

    match mime_type {
        "text/plain" => {
            let text = part
                .body
                .as_ref()
                .and_then(|b| b.data.as_ref())
                .and_then(|d| String::from_utf8(d.clone()).ok());
            (text, None)
        }
        "text/html" => {
            let html = part
                .body
                .as_ref()
                .and_then(|b| b.data.as_ref())
                .and_then(|d| String::from_utf8(d.clone()).ok());
            (None, html)
        }
        mime if mime.starts_with("multipart/") => {
            let mut text = None;
            let mut html = None;
            if let Some(parts) = &part.parts {
                for sub_part in parts {
                    let (t, h) = extract_body_from_payload(sub_part);
                    if t.is_some() && text.is_none() {
                        text = t;
                    }
                    if h.is_some() && html.is_none() {
                        html = h;
                    }
                }
            }
            (text, html)
        }
        _ => {
            // Recurse into nested parts
            if let Some(parts) = &part.parts {
                let mut text = None;
                let mut html = None;
                for sub_part in parts {
                    let (t, h) = extract_body_from_payload(sub_part);
                    if t.is_some() && text.is_none() {
                        text = t;
                    }
                    if h.is_some() && html.is_none() {
                        html = h;
                    }
                }
                (text, html)
            } else {
                (None, None)
            }
        }
    }
}
