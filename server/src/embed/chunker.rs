//! Sentence chunking for email embedding

use std::sync::LazyLock;

use regex::Regex;

use crate::email::simplified_message::SimplifiedMessage;

// Match sentence-ending punctuation followed by whitespace, or [LINK] placeholder
static CHUNK_BOUNDARY: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[.!?]\s+|\[LINK\]").unwrap());

/// Check if text has meaningful content (at least two consecutive letters)
fn has_content(text: &str) -> bool {
    let text = text.trim();
    if text == "[LINK]" {
        return false;
    }
    let mut prev_alpha = false;
    for c in text.chars() {
        if c.is_alphabetic() {
            if prev_alpha {
                return true;
            }
            prev_alpha = true;
        } else {
            prev_alpha = false;
        }
    }
    false
}

/// Maximum tokens per chunk (Mistral embed limit is ~8192)
const MAX_TOKENS_PER_CHUNK: usize = 8192;

/// Approximate characters per token (conservative estimate)
const CHARS_PER_TOKEN: usize = 4;

/// Maximum characters per chunk based on token limit
const MAX_CHARS_PER_CHUNK: usize = MAX_TOKENS_PER_CHUNK * CHARS_PER_TOKEN;

#[derive(Debug, Clone)]
pub struct Chunk {
    pub index: usize,
    pub text: String,
}

pub fn chunk_email(msg: &SimplifiedMessage) -> Vec<Chunk> {
    let mut chunks: Vec<Chunk> = vec![];

    if let Some(subject) = msg.subject.as_deref() {
        chunks.extend(chunk_email_text(subject, 0));
    }
    if let Some(body) = msg.body.as_deref() {
        chunks.extend(chunk_email_text(body, chunks.len()));
    }
    chunks
}

/// Split email text into sentence-based chunks.
///
/// Rules:
/// 1. Split on sentence boundaries (. ! ?)
/// 2. Truncate sentences longer than MAX_CHARS_PER_CHUNK
/// 3. Return empty vec if input is empty/whitespace only
fn chunk_email_text(text: &str, start_idx: usize) -> Vec<Chunk> {
    let text = text.trim();
    if text.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut last_end = 0;
    let mut idx = start_idx;

    for mat in CHUNK_BOUNDARY.find_iter(text) {
        let matched = mat.as_str();
        // For sentence endings, include the punctuation; for [LINK], exclude it
        let sentence_end = if matched == "[LINK]" {
            mat.start()
        } else {
            mat.start() + 1
        };
        let sentence = text[last_end..sentence_end].trim();

        if has_content(sentence) {
            let chunk_text = if sentence.len() > MAX_CHARS_PER_CHUNK {
                sentence[..MAX_CHARS_PER_CHUNK].to_string()
            } else {
                sentence.to_string()
            };
            chunks.push(Chunk {
                index: idx,
                text: chunk_text,
            });
            idx += 1;
        }

        last_end = mat.end();
    }

    // Handle remaining text (last sentence without trailing whitespace)
    let remaining = text[last_end..].trim();
    if has_content(remaining) {
        let chunk_text = if remaining.len() > MAX_CHARS_PER_CHUNK {
            remaining[..MAX_CHARS_PER_CHUNK].to_string()
        } else {
            remaining.to_string()
        };
        chunks.push(Chunk {
            index: idx,
            text: chunk_text,
        });
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_chunking() {
        let text = "Hello world. This is a test! How are you?";
        let chunks = chunk_email_text(text, 0);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].text, "Hello world.");
        assert_eq!(chunks[0].index, 0);
        assert_eq!(chunks[1].text, "This is a test!");
        assert_eq!(chunks[1].index, 1);
        assert_eq!(chunks[2].text, "How are you?");
        assert_eq!(chunks[2].index, 2);
    }

    #[test]
    fn test_empty_input() {
        assert!(chunk_email_text("", 0).is_empty());
        assert!(chunk_email_text("   ", 0).is_empty());
    }

    #[test]
    fn test_single_sentence() {
        let text = "Just one sentence here";
        let chunks = chunk_email_text(text, 0);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "Just one sentence here");
    }

    #[test]
    fn test_truncation() {
        let long_sentence = "a".repeat(MAX_CHARS_PER_CHUNK + 100);
        let chunks = chunk_email_text(&long_sentence, 0);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text.len(), MAX_CHARS_PER_CHUNK);
    }

    #[test]
    fn test_preserves_sentence_ending_punctuation() {
        let text = "First sentence. Second sentence! Third sentence?";
        let chunks = chunk_email_text(text, 0);
        assert_eq!(chunks.len(), 3);
        assert!(chunks[0].text.ends_with('.'));
        assert!(chunks[1].text.ends_with('!'));
        assert!(chunks[2].text.ends_with('?'));
    }

    #[test]
    fn test_handles_multiple_spaces() {
        let text = "First sentence.   Second sentence.";
        let chunks = chunk_email_text(text, 0);
        assert_eq!(chunks.len(), 2);
    }

    #[test]
    fn test_filters_empty_sentences() {
        let text = "First.  . Second.";
        let chunks = chunk_email_text(text, 0);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text, "First.");
        assert_eq!(chunks[1].text, "Second.");
    }

    #[test]
    fn test_splits_on_link_and_filters() {
        let text = "Check this out[LINK]and also this[LINK]final text";
        let chunks = chunk_email_text(text, 0);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].text, "Check this out");
        assert_eq!(chunks[1].text, "and also this");
        assert_eq!(chunks[2].text, "final text");
    }

    #[test]
    fn test_filters_link_only_chunks() {
        let text = "Hello. [LINK] World.";
        let chunks = chunk_email_text(text, 0);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text, "Hello.");
        assert_eq!(chunks[1].text, "World.");
    }

    #[test]
    fn test_filters_numbers_and_special_chars_only() {
        let text = "Hello. 12345. $#@!%. 2024-01-01. Real content here.";
        let chunks = chunk_email_text(text, 0);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text, "Hello.");
        assert_eq!(chunks[1].text, "Real content here.");
    }

    #[test]
    fn test_filters_single_letters() {
        let text = "Hello. A. B 1 C. Ok.";
        let chunks = chunk_email_text(text, 0);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text, "Hello.");
        assert_eq!(chunks[1].text, "Ok.");
    }
}
