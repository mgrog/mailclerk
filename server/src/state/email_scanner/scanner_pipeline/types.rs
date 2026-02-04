//! Scanner Pipeline Types
//!
//! Core data structures used throughout the scanner pipeline.

use crate::email::simplified_message::SimplifiedMessage;
use crate::prompt::task_extraction::ExtractedTask;

/// Item flowing through the scanner pipeline
#[derive(Debug, Clone)]
pub struct PipelineItem {
    // Identity
    pub user_id: i32,
    pub user_email: String,
    pub email_id: String,
    pub thread_id: String,

    // Email metadata
    pub is_read: bool,
    pub has_new_reply: bool,
    pub label_ids: Vec<String>,
    pub history_id: u64,
    pub internal_date: i64,
    pub simplified_message: SimplifiedMessage,

    // Accumulated results (filled as pipeline progresses)
    pub first_pass_result: Option<CategorizationResult>,
    pub second_pass_result: Option<CategorizationResult>,
    pub extracted_tasks: Vec<ExtractedTask>,

    // Token tracking
    /// Estimated content tokens (email content only, calculated via tokenizer)
    pub estimated_content_tokens: i64,
    /// Actual tokens consumed (accumulated from API responses)
    pub actual_tokens: i64,
}

impl PipelineItem {
    /// Get the final categorization result (second pass if available, otherwise first pass)
    pub fn final_result(&self) -> Option<&CategorizationResult> {
        // If second pass exists and has high confidence, use it
        if let Some(ref second) = self.second_pass_result {
            if second.ai_confidence > 0.95 {
                return Some(second);
            }
        }
        // Otherwise use first pass
        self.first_pass_result.as_ref()
    }

    /// Get the final category string
    pub fn final_category(&self) -> Option<&str> {
        self.final_result().map(|r| r.category.as_str())
    }

    /// Get the final AI answer
    pub fn final_ai_answer(&self) -> Option<&str> {
        self.final_result().map(|r| r.ai_answer.as_str())
    }

    /// Get the final confidence
    pub fn final_confidence(&self) -> Option<f32> {
        self.final_result().map(|r| r.ai_confidence)
    }
}

/// Result from a categorization pass
#[derive(Debug, Clone)]
pub struct CategorizationResult {
    pub category: String,
    pub ai_answer: String,
    pub ai_confidence: f32,
}

/// A failed item waiting for retry
#[derive(Debug, Clone)]
pub struct FailedItem {
    pub item: PipelineItem,
    pub stage: PipelineStage,
    pub error: String,
    pub retry_count: u32,
}

/// Stage in the pipeline where failure occurred
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PipelineStage {
    MainCategorization,
    UserDefinedCategorization,
    TaskExtraction,
}

impl std::fmt::Display for PipelineStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineStage::MainCategorization => write!(f, "MainCategorization"),
            PipelineStage::UserDefinedCategorization => write!(f, "UserDefinedCategorization"),
            PipelineStage::TaskExtraction => write!(f, "TaskExtraction"),
        }
    }
}

/// Routing decision after first pass categorization
#[derive(Debug, Clone)]
pub struct FirstPassRouting {
    pub needs_second_pass: bool,
    pub needs_task_extraction: bool,
}

/// Custom ID helpers for batch requests
/// Format: "{user_id}_{email_id}"
pub fn make_custom_id(user_id: i32, email_id: &str) -> String {
    format!("{}_{}", user_id, email_id)
}

/// Parse a custom_id back into user_id and email_id
/// Returns None if the format is invalid
pub fn parse_custom_id(custom_id: &str) -> Option<(i32, String)> {
    let parts: Vec<&str> = custom_id.splitn(2, '_').collect();
    if parts.len() == 2 {
        let user_id = parts[0].parse().ok()?;
        let email_id = parts[1].to_string();
        Some((user_id, email_id))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_custom_id_roundtrip() {
        let user_id = 42;
        let email_id = "18abc123def456";

        let custom_id = make_custom_id(user_id, email_id);
        assert_eq!(custom_id, "42_18abc123def456");

        let (parsed_user_id, parsed_email_id) = parse_custom_id(&custom_id).unwrap();
        assert_eq!(parsed_user_id, user_id);
        assert_eq!(parsed_email_id, email_id);
    }

    #[test]
    fn test_custom_id_with_underscore_in_email() {
        let user_id = 1001;
        let email_id = "msg_id_xyz_123";

        let custom_id = make_custom_id(user_id, email_id);
        assert_eq!(custom_id, "1001_msg_id_xyz_123");

        let (parsed_user_id, parsed_email_id) = parse_custom_id(&custom_id).unwrap();
        assert_eq!(parsed_user_id, user_id);
        assert_eq!(parsed_email_id, email_id);
    }

    #[test]
    fn test_parse_invalid_custom_id() {
        assert!(parse_custom_id("invalid").is_none());
        assert!(parse_custom_id("not_a_number_123").is_none());
        assert!(parse_custom_id("").is_none());
    }
}
