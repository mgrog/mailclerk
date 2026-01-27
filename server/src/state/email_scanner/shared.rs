use crate::{
    email::rules::{EmailRule, UserEmailRules, EXCEPTION_RULES, HEURISTIC_EMAIL_RULES, UNKNOWN_RULE},
    prompt::task_extraction::ExtractedTask,
    server_config::cfg,
};

#[derive(Debug)]
pub struct PromptReturnData {
    pub email_rule: EmailRule,
    pub ai_answer: String,
    pub ai_confidence: f32,
    pub heuristics_used: bool,
    pub token_usage: i64,
}

#[derive(Debug)]
pub struct AiEmailData {
    pub prompt_return_data: PromptReturnData,
    pub extracted_tasks: Vec<ExtractedTask>,
}

/// Data collected for a single email during initial scan
#[derive(Debug)]
pub struct EmailScanData {
    pub id: String,
    pub thread_id: Option<String>,
    pub label_ids: Vec<String>,
    pub history_id: u64,
    pub internal_date: i64,
    pub from: Option<String>,
    pub subject: Option<String>,
    pub snippet: Option<String>,
    pub body: Option<String>,
}

/// Result of processing a single email
#[derive(Debug)]
pub struct ProcessedEmailData {
    pub email_data: EmailScanData,
    pub category: String,
    pub ai_answer: String,
    pub ai_confidence: f32,
    pub extracted_tasks: Vec<ExtractedTask>,
}

/// Find the matching email rule based on category and confidence
pub fn find_matching_rule(
    category: &str,
    confidence: f32,
    user_email_rules: &UserEmailRules,
    // Used for heuristics
    email_from: Option<impl AsRef<str>>,
) -> (EmailRule, bool) {
    let mut selected_rule = user_email_rules
        .data()
        .iter()
        .find(|c| c.prompt_content.eq_ignore_ascii_case(category))
        .unwrap_or(&UNKNOWN_RULE);

    // Check if we should use heuristics
    let mut heuristics_used = false;
    if confidence < cfg.model.email_confidence_threshold
        && !EXCEPTION_RULES.contains(&selected_rule.prompt_content.as_str())
    {
        if let Some(rule) = HEURISTIC_EMAIL_RULES.iter().find(|c| {
            email_from
                .as_ref()
                .is_some_and(|f| f.as_ref().contains(&c.prompt_content))
        }) {
            selected_rule = rule;
            heuristics_used = true;
        }
    }

    // Check if we should use unknown due to low confidence and no heuristics available
    if confidence < cfg.model.email_confidence_threshold && !heuristics_used {
        selected_rule = &UNKNOWN_RULE;
    }

    (selected_rule.clone(), heuristics_used)
}
