use anyhow::Context;
use mail_parser::MessageParser;
use regex::Regex;

const RE_WHITESPACE_STR: &str = r"[\r\t\n]+";
const RE_LONG_SPACE_STR: &str = r" {2,}";
const RE_NON_ASCII_STR: &str = r"[^\x20-\x7E]";
const RE_DIVIDERS_STR: &str = r"[-=_]{3,}";
const RE_HTTP_LINK_STR: &str = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)";

lazy_static::lazy_static!(
    static ref RE_WHITESPACE: Regex = Regex::new(r"[\r\t\n]+").unwrap();
    static ref RE_LONG_SPACE: Regex = Regex::new(r" {2,}").unwrap();
    static ref RE_NON_ASCII: Regex = Regex::new(r"[^\x20-\x7E]").unwrap();
    static ref RE_DIVIDERS: Regex = Regex::new(r"[-=_]{3,}").unwrap();
    static ref RE_HTTP_LINK: Regex = Regex::new(r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)").unwrap();
);

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ParsedMessage {
    pub id: String,
    pub label_ids: Vec<String>,
    pub thread_id: String,
    pub history_id: u64,
    pub internal_date: i64,
    pub from: Option<String>,
    pub subject: Option<String>,
    pub body: Option<String>,
}

impl ParsedMessage {
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

                ParsedMessage {
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
        let b = RE_NON_ASCII.replace_all(&b, "");
        let b = RE_WHITESPACE.replace_all(&b, " ");
        let b = RE_DIVIDERS.replace_all(&b, " ");
        let b = RE_LONG_SPACE.replace_all(&b, " ");
        let body = b.to_string();

        ParsedMessage {
            body: Some(body),
            ..Default::default()
        }
    }
}

fn strip_formatting_and_links(msg: mail_parser::Message) -> StrippedMessage {
    let subject = msg.subject().map(|s| s.to_string());
    let body = msg.body_text(0).map(|b| b.to_string());
    let from = msg
        .from()
        .and_then(|f| f.first().and_then(|x| x.address().map(|a| a.to_string())));

    let subject = subject.map(|s| {
        let s = RE_NON_ASCII.replace_all(&s, "");
        let s = RE_WHITESPACE.replace_all(&s, " ");
        let s = RE_LONG_SPACE.replace_all(&s, " ");
        s.to_string()
    });
    let body = body.map(|b| {
        let b = RE_HTTP_LINK.replace_all(&b, "[LINK]");
        let bytes = b.as_bytes();
        let b: String = html2text::from_read(bytes, 400);
        let b = RE_NON_ASCII.replace_all(&b, "");
        let b = RE_WHITESPACE.replace_all(&b, " ");
        let b = RE_DIVIDERS.replace_all(&b, " ");
        let b = RE_LONG_SPACE.replace_all(&b, " ");
        b.to_string()
    });

    StrippedMessage {
        from,
        subject,
        body,
    }
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

        let parsed = ParsedMessage::from_gmail_message(message).expect("Unable to parse message");

        dbg!(&parsed);

        let regexes = vec![
            RE_WHITESPACE_STR,
            RE_LONG_SPACE_STR,
            RE_NON_ASCII_STR,
            RE_DIVIDERS_STR,
            RE_HTTP_LINK_STR,
        ]
        .into_iter()
        .map(|r| Regex::new(r).unwrap());

        for regex in regexes {
            println!("Checking regex: {:?}", regex);
            assert!(!regex.is_match(parsed.subject.as_ref().unwrap()));
            assert!(!regex.is_match(parsed.body.as_ref().unwrap()));
        }
    }
}
