use crate::{db_core::prelude::*, model::labels::UtilityLabels, model::user_email_rule::UserEmailRuleCtrl};
use anyhow::Context;
use lazy_static::lazy_static;

lazy_static! {
    /// The default rule used when no category matches or confidence is too low
    pub static ref UNKNOWN_RULE: EmailRule = EmailRule {
        prompt_content: "Unknown".to_string(),
        mail_label: UtilityLabels::Uncategorized.as_str().to_string(),
        extract_tasks: true,
        priority: 2,
    };
    /// Categories that should not fall back to unknown even with low confidence
    pub static ref EXCEPTION_RULES: &'static [&'static str] = &[
        "Terms of Service Update",
        "Verification Code",
        "Security Alert"
    ];
    static ref DEFAULT_EMAIL_RULES: Vec<EmailRule> = {
        use crate::server_config::cfg;
        let categories = &cfg.categories;
        categories
            .iter()
            .map(|c| EmailRule {
                prompt_content: c.content.clone(),
                mail_label: c.mail_label.clone(),
                extract_tasks: false,
                priority: c.priority,
            })
            .collect()
    };
    pub static ref HEURISTIC_EMAIL_RULES: Vec<EmailRule> = {
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
    };
}

#[derive(Debug, Clone)]
pub struct EmailRule {
    pub prompt_content: String,
    pub mail_label: String,
    pub extract_tasks: bool,
    pub priority: i32,
}

pub struct UserEmailRules {
    data: Vec<EmailRule>,
}

impl UserEmailRules {
    pub fn new(email_rules: Vec<EmailRule>) -> Self {
        Self { data: email_rules }
    }

    pub fn new_with_default_rules(email_rules: Vec<EmailRule>) -> Self {
        let rules = email_rules
            .iter()
            .chain(DEFAULT_EMAIL_RULES.iter())
            .cloned()
            .collect();

        Self::new(rules)
    }

    pub async fn from_user(conn: &DatabaseConnection, user_id: i32) -> anyhow::Result<Self> {
        let user_email_rules = UserEmailRuleCtrl::get_by_user_id(conn, user_id)
            .await
            .context("Failed to fetch user email rules")?;

        let data = user_email_rules
            .into_iter()
            .map(|rule| EmailRule {
                prompt_content: rule.description,
                mail_label: rule.mail_label,
                extract_tasks: rule.extract_tasks,
                priority: rule.priority,
            })
            .collect();

        Ok(Self::new(data))
    }

    pub fn data(&self) -> &[EmailRule] {
        &self.data
    }

    pub fn get_custom_labels(&self) -> Vec<String> {
        self.data.iter().map(|r| r.mail_label.clone()).collect()
    }

    pub fn get_prompt_categories(&self) -> Vec<String> {
        self.data.iter().map(|r| r.prompt_content.clone()).collect()
    }

    pub fn add_rule(&mut self, rule: EmailRule) {
        self.data.push(rule);
    }
}
