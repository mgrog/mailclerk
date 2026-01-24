use std::{borrow::Cow, fmt};

use anyhow::Context;
use mail_parser::MessageParser;
use regex::Regex;

use crate::email::sanitized_message::sanitize_html;

const RE_WHITESPACE_STR: &str = r"[\t]+";
const RE_NEWLINE_STR: &str = r"[\n\r]+";
const RE_LONG_SPACE_STR: &str = r" {2,}";
const RE_DIVIDERS_STR: &str = r"[-=_\*]{3,}";
const RE_HTTP_LINK_STR: &str = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)";
const RE_ORPHANED_PAREN_LINK_STR: &str = r"\(\[LINK\]";
const RE_IMG_TAG_STR: &str = r#"<img[^>]*alt=["']([^"']*)["'][^>]*/?>"#;
// Non-word patterns (hashes, UUIDs, tokens, etc.)
const RE_HEX_HASH_STR: &str = r"\b[a-fA-F0-9]{32,}\b";
const RE_UUID_STR: &str =
    r"\b[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}\b";
const RE_BASE64_LONG_STR: &str = r"[a-zA-Z0-9+/]{40,}={0,2}";
const RE_LONG_TOKEN_STR: &str = r"\b[a-zA-Z0-9_-]{30,}\b";
// Footer detection patterns (case-insensitive)
const RE_FOOTER_STR: &str = r"(?i)(^|\s)(unsubscribe|opt[- ]?out|manage\s+(your\s+)?(email\s+)?preferences|email\s+preferences|update\s+(your\s+)?preferences|sent\s+from\s+(my\s+)?(iphone|ipad|android|samsung|galaxy|mobile|outlook)|get\s+outlook\s+for|this\s+(email|message)\s+(is\s+)?(was\s+)?sent\s+(to|from)|confidential(ity)?(\s+notice)?|this\s+(e-?mail|message)\s+(and\s+any\s+attachments\s+)?(is|are|may\s+be)\s+(intended|privileged|confidential)|if\s+you\s+(are\s+not|have\s+received)\s+(the\s+intended|this\s+(e-?mail|message)\s+in\s+error)|please\s+(delete|disregard|notify)|privacy\s+policy|terms\s+(of\s+service|and\s+conditions)|all\s+rights\s+reserved|©\s*\d{4}|\d{4}\s*©|view\s+(this\s+)?(email\s+)?in\s+(your\s+)?browser|trouble\s+viewing|add\s+us\s+to\s+your\s+address\s+book|you('re|\s+are)\s+(receiving|getting)\s+this\s+(email|message|because))";

lazy_static::lazy_static!(
    static ref RE_WHITESPACE: Regex = Regex::new(RE_WHITESPACE_STR).unwrap();
    static ref RE_NEWLINE: Regex = Regex::new(RE_NEWLINE_STR).unwrap();
    static ref RE_LONG_SPACE: Regex = Regex::new(RE_LONG_SPACE_STR).unwrap();
    static ref RE_DIVIDERS: Regex = Regex::new(RE_DIVIDERS_STR).unwrap();
    static ref RE_HTTP_LINK: Regex = Regex::new(RE_HTTP_LINK_STR).unwrap();
    static ref RE_ORPHANED_PAREN_LINK: Regex = Regex::new(RE_ORPHANED_PAREN_LINK_STR).unwrap();
    static ref RE_IMG_TAG: Regex = Regex::new(RE_IMG_TAG_STR).unwrap();
    static ref RE_FOOTER: Regex = Regex::new(RE_FOOTER_STR).unwrap();
    static ref RE_HEX_HASH: Regex = Regex::new(RE_HEX_HASH_STR).unwrap();
    static ref RE_UUID: Regex = Regex::new(RE_UUID_STR).unwrap();
    static ref RE_BASE64_LONG: Regex = Regex::new(RE_BASE64_LONG_STR).unwrap();
    static ref RE_LONG_TOKEN: Regex = Regex::new(RE_LONG_TOKEN_STR).unwrap();
);

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SimplifiedMessage {
    pub id: String,
    pub label_ids: Vec<String>,
    pub thread_id: String,
    pub history_id: u64,
    pub internal_date: i64,
    pub from: Option<String>,
    pub subject: Option<String>,
    pub body: Option<String>,
}

impl SimplifiedMessage {
    pub fn from_gmail_message(msg: google_gmail1::api::Message) -> anyhow::Result<Self> {
        let id = msg.clone().id.unwrap_or_default();
        let label_ids = msg.clone().label_ids.unwrap_or_default();
        let thread_id = msg.thread_id.clone().unwrap_or_default();
        let history_id = msg.history_id.unwrap_or_default();
        let internal_date = msg.internal_date.unwrap_or_default();
        msg.raw
            .as_ref()
            .map(|input| {
                let msg = MessageParser::default().parse(input);
                let StrippedMessage {
                    from,
                    subject,
                    body,
                } = msg.map_or(StrippedMessage::default(), strip_formatting_and_links);

                SimplifiedMessage {
                    id,
                    from,
                    label_ids,
                    thread_id,
                    history_id,
                    internal_date,
                    subject,
                    body,
                }
            })
            .context(format!(
                "No raw message found in message response: {:?}",
                msg
            ))
    }

    pub fn from_string(email: String) -> Self {
        let b = email;
        let b = RE_HTTP_LINK.replace_all(&b, "[LINK]");
        let b = RE_ORPHANED_PAREN_LINK.replace_all(&b, "[LINK]");
        let b = strip_non_words(&b);
        let b = strip_non_text(&b);
        let b = strip_formatting(&b);
        let body = b.to_string();

        SimplifiedMessage {
            body: Some(body),
            ..Default::default()
        }
    }
}

impl fmt::Display for SimplifiedMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "<subject>{}</subject> <body>{}</body>",
            self.subject.as_deref().unwrap_or_default(),
            self.body.as_deref().unwrap_or_default()
        )
    }
}

/// Sanitize html, remove links,
/// And remove all non-signal from email
fn strip_formatting_and_links(msg: mail_parser::Message) -> StrippedMessage {
    let subject = msg.subject().map(|s| s.to_string());
    let body = msg.body_text(0).map(|b| b.to_string());
    let from = msg
        .from()
        .and_then(|f| f.first().and_then(|x| x.address().map(|a| a.to_string())));

    let subject = subject.map(|s| {
        let bytes = s.as_bytes();
        let s: String = html2text::from_read(bytes, 400);
        let s = RE_WHITESPACE.replace_all(&s, " ");
        let s = RE_LONG_SPACE.replace_all(&s, " ");
        s.to_string()
    });
    let body = body.map(|b| {
        let b = sanitize_html(&b);
        let b = replace_images(&b); // Replace img tags with descriptive text
        let bytes = b.as_bytes();
        let b: String = html2text::from_read(bytes, 400);
        let b = RE_HTTP_LINK.replace_all(&b, "[LINK]"); // Replace bare URLs after html2text preserves link text
        let b = RE_ORPHANED_PAREN_LINK.replace_all(&b, "[LINK]"); // Clean up orphaned opening parens
        let b = strip_non_words(&b); // Remove hashes, UUIDs, tokens, etc.
        let b = strip_non_text(&b); // Remove control chars, keep all languages + emojis
        let b = strip_formatting(&b);
        let b = strip_footer(&b); // Remove email footers
        b.to_string()
    });

    StrippedMessage {
        from,
        subject,
        body,
    }
}

fn replace_images<'h>(body: &'h str) -> Cow<'h, str> {
    RE_IMG_TAG.replace_all(body, "[An image of $1]")
}

fn strip_formatting(body: &str) -> Cow<'_, str> {
    let b = RE_WHITESPACE.replace_all(body, " ");
    let b = RE_NEWLINE.replace_all(&b, "\n");
    let b = RE_DIVIDERS.replace_all(&b, " ");
    let b = RE_LONG_SPACE.replace_all(&b, " ");
    b.into_owned().into()
}

/// Remove email footers (unsubscribe links, legal disclaimers, signatures, etc.)
fn strip_footer(body: &str) -> &str {
    if let Some(m) = RE_FOOTER.find(body) {
        body[..m.start()].trim_end()
    } else {
        body
    }
}

/// Remove non-word strings like hashes, UUIDs, base64 tokens, and long alphanumeric strings
fn strip_non_words(body: &str) -> Cow<'_, str> {
    let b = RE_HEX_HASH.replace_all(body, "");
    let b = RE_UUID.replace_all(&b, "");
    let b = RE_BASE64_LONG.replace_all(&b, "");
    RE_LONG_TOKEN.replace_all(&b, "").into_owned().into()
}

/// Remove non-text characters (control chars, private use, etc.) while keeping
/// letters from all languages, numbers, punctuation, and emojis
fn strip_non_text(body: &str) -> String {
    body.chars().filter(|c| is_valid_text_char(*c)).collect()
}

/// Check if a character is valid text (letters, numbers, punctuation, whitespace, emojis)
fn is_valid_text_char(c: char) -> bool {
    c.is_alphanumeric() || c.is_whitespace() || c.is_ascii_punctuation() || is_common_emoji(c)
}

/// Check if a character is a common emoji
fn is_common_emoji(c: char) -> bool {
    matches!(c,
        // Emoticons
        '\u{1F600}'..='\u{1F64F}' |
        // Misc symbols and pictographs
        '\u{1F300}'..='\u{1F5FF}' |
        // Transport and map symbols
        '\u{1F680}'..='\u{1F6FF}' |
        // Flags
        '\u{1F1E0}'..='\u{1F1FF}' |
        // Supplemental symbols and pictographs
        '\u{1F900}'..='\u{1F9FF}' |
        // Symbols and pictographs extended-A
        '\u{1FA00}'..='\u{1FA6F}' |
        // Chess, cards, misc
        '\u{2600}'..='\u{26FF}' |
        // Dingbats
        '\u{2700}'..='\u{27BF}' |
        // Enclosed alphanumeric supplement (circled letters, etc)
        '\u{1F100}'..='\u{1F1FF}'
    )
}

#[derive(Debug, Default)]
struct StrippedMessage {
    from: Option<String>,
    subject: Option<String>,
    body: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::fs;

    use google_gmail1::api::Message;

    use super::*;

    #[test]
    fn test_strip_formatting_and_links() {
        let root = env!("CARGO_MANIFEST_DIR");

        let path = format!("{root}/src/testing/data/message_with_divider.json");
        let json = fs::read_to_string(path).expect("Unable to read file");

        let message = serde_json::from_str::<Message>(&json).expect("Unable to parse json");

        let parsed =
            SimplifiedMessage::from_gmail_message(message).expect("Unable to parse message");

        dbg!(&parsed);

        let regexes = vec![
            RE_WHITESPACE_STR,
            RE_LONG_SPACE_STR,
            RE_DIVIDERS_STR,
            RE_HTTP_LINK_STR,
            RE_IMG_TAG_STR,
            RE_FOOTER_STR,
        ]
        .into_iter()
        .map(|r| Regex::new(r).unwrap());

        for regex in regexes {
            println!("Checking regex: {:?}", regex);
            assert!(!regex.is_match(parsed.subject.as_ref().unwrap()));
            assert!(!regex.is_match(parsed.body.as_ref().unwrap()));
        }
    }

    #[test]
    fn test_img_tag_alt_text_extraction() {
        // Test that img tags with alt text are replaced with descriptive text
        let img_with_alt = r#"<img src="test.png" alt="Company Logo">"#;
        let result = replace_images(img_with_alt);
        assert_eq!(result, "[An image of Company Logo]");

        // Test self-closing img tag
        let img_self_closing = r#"<img alt="Icon" src="icon.png" />"#;
        let result = replace_images(img_self_closing);
        assert_eq!(result, "[An image of Icon]");

        // Test with single quotes
        let img_single_quotes = r#"<img src='test.png' alt='Single Quote Alt'>"#;
        let result = replace_images(img_single_quotes);
        assert_eq!(result, "[An image of Single Quote Alt]");

        // Test multiple img tags
        let multiple_imgs = r#"<img alt="First"> and <img alt="Second">"#;
        let result = replace_images(multiple_imgs);
        assert_eq!(result, "[An image of First] and [An image of Second]");
    }

    #[test]
    fn test_http_link_replacement() {
        let text_with_link = "Visit https://example.com for more info";
        let result = RE_HTTP_LINK.replace_all(text_with_link, "[LINK]");
        assert_eq!(result, "Visit [LINK] for more info");

        let text_with_www = "Check out https://www.example.com/path?query=1";
        let result = RE_HTTP_LINK.replace_all(text_with_www, "[LINK]");
        assert_eq!(result, "Check out [LINK]");

        let multiple_links = "See https://a.com and http://b.org for details";
        let result = RE_HTTP_LINK.replace_all(multiple_links, "[LINK]");
        assert_eq!(result, "See [LINK] and [LINK] for details");
    }

    #[test]
    fn test_from_string() {
        let email = "Hello\n\nVisit https://example.com\n\n---\n\nThanks".to_string();
        let msg = SimplifiedMessage::from_string(email);

        assert!(msg.body.is_some());
        let body = msg.body.unwrap();

        // Should replace link with [LINK]
        assert!(body.contains("[LINK]"));
        assert!(!body.contains("https://"));

        // Should remove dividers
        assert!(!body.contains("---"));
    }

    #[test]
    fn test_display_implementation() {
        let msg = SimplifiedMessage {
            subject: Some("Test Subject".to_string()),
            body: Some("Test Body".to_string()),
            ..Default::default()
        };

        let display = format!("{}", msg);
        assert_eq!(
            display,
            "<subject>Test Subject</subject> <body>Test Body</body>"
        );
    }

    #[test]
    fn test_display_with_empty_fields() {
        let msg = SimplifiedMessage::default();

        let display = format!("{}", msg);
        assert_eq!(display, "<subject></subject> <body></body>");
    }

    #[test]
    fn test_whitespace_normalization() {
        let text = "Hello\t\tworld\n\ntest\r\rmore";
        let result = strip_formatting(text);
        assert_eq!(result, "Hello world\ntest\nmore");
    }

    #[test]
    fn test_long_space_normalization() {
        let text = "Hello     world  test";
        let result = RE_LONG_SPACE.replace_all(text, " ");
        assert_eq!(result, "Hello world test");
    }

    #[test]
    fn test_divider_removal() {
        let text = "Above --- Below";
        let result = RE_DIVIDERS.replace_all(text, " ");
        assert_eq!(result, "Above   Below");

        let text = "Above === Below";
        let result = RE_DIVIDERS.replace_all(text, " ");
        assert_eq!(result, "Above   Below");

        let text = "Above ___ Below";
        let result = RE_DIVIDERS.replace_all(text, " ");
        assert_eq!(result, "Above   Below");
    }

    #[test]
    fn test_strip_non_words() {
        // Test hex hash removal (MD5, SHA256, etc.)
        let text = "Your code is abc123def456abc123def456abc123def456 please verify";
        let result = strip_non_words(text);
        assert_eq!(result, "Your code is  please verify");

        // Test UUID removal
        let text = "Reference: 550e8400-e29b-41d4-a716-446655440000 for your records";
        let result = strip_non_words(text);
        assert_eq!(result, "Reference:  for your records");

        // Test base64-like string removal
        let text = "Token: aGVsbG8gd29ybGQgdGhpcyBpcyBhIGxvbmcgYmFzZTY0IHN0cmluZw== attached";
        let result = strip_non_words(text);
        assert_eq!(result, "Token:  attached");

        // Test long alphanumeric token removal
        let text = "Tracking: abc123xyz789def456ghi012jkl345 here";
        let result = strip_non_words(text);
        assert_eq!(result, "Tracking:  here");

        // Test normal text is preserved
        let text = "Hello world, this is a normal email message.";
        let result = strip_non_words(text);
        assert_eq!(result, "Hello world, this is a normal email message.");

        // Test short alphanumeric strings are preserved
        let text = "Order #12345 confirmed for ABC123";
        let result = strip_non_words(text);
        assert_eq!(result, "Order #12345 confirmed for ABC123");
    }

    #[test]
    fn test_strip_non_text() {
        // Normal ASCII preserved
        let text = "Hello world!";
        let result = strip_non_text(text);
        assert_eq!(result, "Hello world!");

        // Emojis preserved
        let text = "Great job! 🎉👍";
        let result = strip_non_text(text);
        assert_eq!(result, "Great job! 🎉👍");

        // Other language text preserved
        let text = "Héllo wörld café";
        let result = strip_non_text(text);
        assert_eq!(result, "Héllo wörld café");

        // Mixed: emojis and other languages kept
        let text = "Thanks! 😊 Merci für alles";
        let result = strip_non_text(text);
        assert_eq!(result, "Thanks! 😊 Merci für alles");

        // Control characters removed
        let text = "Hello\x00world\x1B\x07";
        let result = strip_non_text(text);
        assert_eq!(result, "Helloworld");
    }

    #[test]
    fn test_strip_footer() {
        // Test unsubscribe removal
        let text = "Important email content here. Click here to unsubscribe from our mailing list.";
        let result = strip_footer(text);
        assert_eq!(result, "Important email content here. Click here to");

        // Test opt-out removal
        let text = "Your order is confirmed. Opt out of marketing emails here.";
        let result = strip_footer(text);
        assert_eq!(result, "Your order is confirmed.");

        // Test sent from iPhone
        let text = "Thanks for the update! Sent from my iPhone";
        let result = strip_footer(text);
        assert_eq!(result, "Thanks for the update!");

        // Test legal disclaimer - matches "This email is confidential"
        let text = "Meeting confirmed for 3pm. This email is confidential and intended only for the recipient.";
        let result = strip_footer(text);
        assert_eq!(result, "Meeting confirmed for 3pm.");

        // Test privacy policy
        let text = "Welcome to our service! Privacy Policy | Terms of Service";
        let result = strip_footer(text);
        assert_eq!(result, "Welcome to our service!");

        // Test copyright
        let text = "Thanks for shopping with us. © 2024 Company Inc.";
        let result = strip_footer(text);
        assert_eq!(result, "Thanks for shopping with us.");

        // Test no footer - should return full text
        let text = "Just a regular email with no footer content.";
        let result = strip_footer(text);
        assert_eq!(result, "Just a regular email with no footer content.");

        // Test manage preferences
        let text = "Your weekly digest is ready. Manage your email preferences here.";
        let result = strip_footer(text);
        assert_eq!(result, "Your weekly digest is ready.");
    }
}
