//! Sentence chunking for email embedding

use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    // Match sentence-ending punctuation followed by whitespace
    static ref SENTENCE_END: Regex = Regex::new(r"[.!?]\s+").unwrap();
}

/// Check if text has meaningful content (not just punctuation/whitespace)
fn has_content(text: &str) -> bool {
    text.chars().any(|c| c.is_alphanumeric())
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

/// Split email text into sentence-based chunks.
///
/// Rules:
/// 1. Split on sentence boundaries (. ! ?)
/// 2. Truncate sentences longer than MAX_CHARS_PER_CHUNK
/// 3. Return empty vec if input is empty/whitespace only
pub fn chunk_email_text(text: &str) -> Vec<Chunk> {
    let text = text.trim();
    if text.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut last_end = 0;
    let mut idx = 0;

    for mat in SENTENCE_END.find_iter(text) {
        // Include the punctuation (but not the whitespace) in the sentence
        let sentence_end = mat.start() + 1;
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
        let chunks = chunk_email_text(text);
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
        assert!(chunk_email_text("").is_empty());
        assert!(chunk_email_text("   ").is_empty());
    }

    #[test]
    fn test_single_sentence() {
        let text = "Just one sentence here";
        let chunks = chunk_email_text(text);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "Just one sentence here");
    }

    #[test]
    fn test_truncation() {
        let long_sentence = "a".repeat(MAX_CHARS_PER_CHUNK + 100);
        let chunks = chunk_email_text(&long_sentence);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text.len(), MAX_CHARS_PER_CHUNK);
    }

    #[test]
    fn test_preserves_sentence_ending_punctuation() {
        let text = "First sentence. Second sentence! Third sentence?";
        let chunks = chunk_email_text(text);
        assert_eq!(chunks.len(), 3);
        assert!(chunks[0].text.ends_with('.'));
        assert!(chunks[1].text.ends_with('!'));
        assert!(chunks[2].text.ends_with('?'));
    }

    #[test]
    fn test_handles_multiple_spaces() {
        let text = "First sentence.   Second sentence.";
        let chunks = chunk_email_text(text);
        assert_eq!(chunks.len(), 2);
    }

    #[test]
    fn test_filters_empty_sentences() {
        let text = "First.  . Second.";
        let chunks = chunk_email_text(text);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text, "First.");
        assert_eq!(chunks[1].text, "Second.");
    }
}
