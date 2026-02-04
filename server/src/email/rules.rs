use std::collections::HashSet;
use std::sync::LazyLock;

use crate::{
    db_core::prelude::*,
    model::{
        labels::UtilityLabels, system_email_rule::SystemEmailRuleCtrl,
        user_email_rule::UserEmailRuleCtrl,
    },
    prompt::mistral::SystemPromptInput,
};
use anyhow::Context;

/// The default rule used when no category matches or confidence is too low
pub static UNKNOWN_RULE: LazyLock<EmailRule> = LazyLock::new(|| EmailRule {
    prompt_content: "Unknown".to_string(),
    mail_label: UtilityLabels::Uncategorized.as_str().to_string(),
    extract_tasks: true,
    priority: 2,
});

/// Categories that should not fall back to unknown even with low confidence
pub static EXCEPTION_RULES: &[&str] = &[
    "Terms of Service Update",
    "Verification Code",
    "Security Alert",
];

pub static HEURISTIC_EMAIL_RULES: LazyLock<Vec<EmailRule>> = LazyLock::new(|| {
    use crate::server_config::cfg;
    cfg.heuristics
        .iter()
        .map(|h| EmailRule {
            prompt_content: h.from.clone(),
            mail_label: h.mail_label.clone(),
            extract_tasks: false,
            priority: h.priority,
        })
        .collect()
});

#[derive(Debug, Clone)]
pub struct EmailRule {
    pub prompt_content: String,
    pub mail_label: String,
    pub extract_tasks: bool,
    pub priority: i32,
}

/// Trait for email rule collections that can be used in categorization
pub trait EmailRules {
    fn data(&self) -> &[EmailRule];
    fn get_prompt_input(&self) -> SystemPromptInput;
}

/// Wrapper for system-wide email rules (first pass categorization)
pub struct SystemEmailRules {
    data: Vec<EmailRule>,
}

impl SystemEmailRules {
    pub fn new(data: Vec<EmailRule>) -> Self {
        Self { data }
    }

    pub async fn from_db(conn: &DatabaseConnection) -> anyhow::Result<Self> {
        let models = SystemEmailRuleCtrl::get_all(conn)
            .await
            .context("Failed to fetch system email rules")?;

        let data = models
            .into_iter()
            .map(|m| EmailRule {
                prompt_content: m.semantic_key,
                mail_label: m.mail_label,
                extract_tasks: m.extract_tasks,
                priority: m.priority,
            })
            .collect();

        Ok(Self::new(data))
    }

    pub fn data(&self) -> &[EmailRule] {
        &self.data
    }
}

impl EmailRules for SystemEmailRules {
    fn data(&self) -> &[EmailRule] {
        &self.data
    }

    fn get_prompt_input(&self) -> SystemPromptInput {
        SystemPromptInput::SystemDefined
    }
}

/// Wrapper for user-specific email rules (second pass categorization)
#[derive(Clone)]
pub struct UserEmailRules {
    data: Vec<EmailRule>,
    models: Vec<user_email_rule::Model>,
}

impl UserEmailRules {
    pub fn new(data: Vec<EmailRule>, models: Vec<user_email_rule::Model>) -> Self {
        Self { data, models }
    }

    pub async fn from_user(conn: &DatabaseConnection, user_id: i32) -> anyhow::Result<Self> {
        let models = UserEmailRuleCtrl::get_by_user_id(conn, user_id)
            .await
            .context("Failed to fetch user email rules")?;

        let data = models
            .iter()
            .map(|rule| EmailRule {
                prompt_content: rule.semantic_key.clone(),
                mail_label: rule.mail_label.clone(),
                extract_tasks: rule.extract_tasks,
                priority: rule.priority,
            })
            .collect();

        Ok(Self::new(data, models))
    }

    pub fn data(&self) -> &[EmailRule] {
        &self.data
    }

    pub fn get_custom_labels(&self) -> Vec<String> {
        self.data.iter().map(|r| r.mail_label.clone()).collect()
    }

    pub fn add_rule(&mut self, rule: EmailRule) {
        self.data.push(rule);
    }

    /// Collect all unique system labels that user rules are interested in
    pub fn get_matching_system_labels(&self) -> HashSet<String> {
        self.models
            .iter()
            .filter_map(|m| m.matching_labels.as_ref())
            .flat_map(|labels| labels.iter().cloned())
            .collect()
    }

    /// Check if a given system label is relevant to any user rule
    pub fn is_label_relevant(&self, system_label: &str) -> bool {
        self.models.iter().any(|m| {
            m.matching_labels
                .as_ref()
                .map(|labels| labels.iter().any(|l| l == system_label))
                .unwrap_or(false)
        })
    }
}

impl EmailRules for UserEmailRules {
    fn data(&self) -> &[EmailRule] {
        &self.data
    }

    fn get_prompt_input(&self) -> SystemPromptInput {
        let categories = self.data.iter().map(|r| r.prompt_content.clone()).collect();
        SystemPromptInput::UserDefined(categories)
    }
}
