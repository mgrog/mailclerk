pub mod batch;
pub mod on_demand;
pub mod task_extraction;

use std::collections::HashMap;
use std::sync::LazyLock;

use indoc::formatdoc;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::server_config::CATEGORIZATION_CONFIG;

#[derive(Debug, Serialize, Deserialize)]
pub struct CategoryChatResponse {
    pub general_category: Option<String>,
    pub specific_category: String,
    pub confidence: f32,
    pub token_usage: i64,
}

/// Parsed answer from the AI model's JSON response
#[derive(Debug, Clone)]
pub struct ParsedAnswer {
    pub general_category: Option<String>,
    pub specific_category: String,
    pub confidence: f32,
}

/// Parse the AI model's JSON response content into a structured answer.
/// Returns None if parsing fails or required fields are missing.
pub fn parse_category_answer(content: &str) -> Option<ParsedAnswer> {
    let parsed: serde_json::Value = serde_json::from_str(content).ok()?;
    let general_category = parsed
        .get("general_category")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let specific_category = parsed.get("specific_category")?.as_str()?.to_string();
    let confidence = parsed.get("confidence")?.as_f64()? as f32;

    Some(ParsedAnswer {
        general_category,
        specific_category,
        confidence,
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PromptUsage {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    Stop,
    Length,
    ModelLength,
    Error,
    ToolCalls,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatChoice {
    pub index: i32,
    pub message: ChatMessage,
    pub finish_reason: FinishReason,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatApiResponse {
    pub choices: Vec<ChatChoice>,
    pub usage: PromptUsage,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatApiError {
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChatApiResponseOrError {
    Response(ChatApiResponse),
    Error(ChatApiError),
}

// Not currently used, adds the token cost
pub static MAILCLERK_JSON_SCHEMA: LazyLock<serde_json::Value> = LazyLock::new(|| {
    json!({
        "type": "json_schema",
        "json_schema": {
            "name": "email_classification",
            "strict": true,
            "schema": {
                "type": "object",
                "properties": {
                    "general_category": {
                        "type": "string",
                        "title": "General Category"
                    },
                    "specific_category": {
                        "type": "string",
                        "title": "Specific Category"
                    },
                    "confidence": {
                        "type": "number",
                        "title": "Confidence"
                    }
                },
                "required": ["general_category", "specific_category", "confidence"],
                "additionalProperties": false
            }
        }
    })
});

static SYSTEM_TAXONOMY: LazyLock<String> = LazyLock::new(|| {
    let mut category_map: HashMap<String, Vec<String>> = HashMap::new();
    for item in &CATEGORIZATION_CONFIG.rules {
        category_map
            .entry(item.mail_label.clone())
            .and_modify(|bucket| bucket.push(item.semantic_key.clone()))
            .or_insert(vec![item.semantic_key.clone()]);
    }

    category_map
        .iter()
        .map(|(key, values)| {
            let value_lines = values
                .iter()
                .map(|v| format!("  • \"{}\"", v))
                .collect::<Vec<_>>()
                .join("\n");
            format!("• \"{}\"\n{}", key, value_lines)
        })
        .collect::<Vec<_>>()
        .join("\n")
});

pub static SYSTEM_PROMPT_TOKEN_ESTIMATE: LazyLock<i64> = LazyLock::new(|| {
    let prompt_text = system_prompt(SystemPromptInput::SystemDefined);
    tokenizer::token_count(&prompt_text).unwrap() as i64
});

pub enum SystemPromptInput {
    SystemDefined,
    UserDefined(Vec<String>),
}

pub fn system_prompt(input: SystemPromptInput) -> String {
    let (taxonomy, preamble, output_instruction) = match input {
        SystemPromptInput::SystemDefined => (
            SYSTEM_TAXONOMY.to_string(),
            "Your task is to categorize the given email into one general category and one specific category from the predefined taxonomy below.",
            "You will only respond with a JSON object with the keys general_category, specific_category, and confidence."
        ),
        SystemPromptInput::UserDefined(ref list) => {
            (list.iter().map(|c| format!("• \"{}\"", c)).join("\n"),
            "Your task is to categorize the given email into one specific category from the predefined taxonomy below.",
            "You will only respond with a JSON object with the keys specific_category, and confidence."
        )
        }
    };

    formatdoc! {r#"
        You are an email classification engine.
        {preamble}

        Instructions:
        Read the email content carefully (subject, sender, body).
        Determine the sender's intent, not the user's reaction.
        Choose the single best general category.
        Choose the most specific matching type within that category.
        If multiple categories apply, choose the dominant intent.
        Do not invent new categories or types.
        If the email does not clearly fit, write "Unknown" in the category fields.

        Taxonomy (authoritative):

        {taxonomy}

        {output_instruction}
        "confidence" is a float between 0 and 1 representing classification certainty.
        Do not provide explanations."#
    }
}

/// Build the user prompt for email categorization.
/// This is the prompt template used in both batch and real-time categorization.
pub fn categorization_user_prompt(subject: &str, sender: &str, body: &str) -> String {
    formatdoc!(
        r#"Categorize the following email based on subject, sender, and body.
            Only select a category when it is strongly correlated with the content. If you cannot select a category confidently, respond with "Unknown".
            Make a reasonable choice based on the intent, formatting, tone, and typical conventions.

            <subject>{}</subject>
            <sender>{}</sender>
            <body>{}</body>"#,
        subject,
        sender,
        body
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_prompt_token_estimate() {
        // Get the actual system prompt text
        let prompt_text = system_prompt(SystemPromptInput::SystemDefined);

        // Count the actual tokens in the prompt
        let actual_tokens = tokenizer::token_count(&prompt_text)
            .expect("Failed to count tokens in system prompt") as i64;

        // Get the pre-calculated estimate
        let estimated_tokens = *SYSTEM_PROMPT_TOKEN_ESTIMATE;
        println!("System prompt token usage: {}", estimated_tokens);

        // The estimate should match the actual count
        // (they use the same tokenizer, so they should be identical)
        assert_eq!(
            actual_tokens, estimated_tokens,
            "System prompt token estimate mismatch: estimated={}, actual={}",
            estimated_tokens, actual_tokens
        );
    }
}
