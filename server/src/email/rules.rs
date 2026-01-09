use std::collections::HashMap;

use crate::{
    db_core::prelude::*,
    model::{
        custom_email_rule::CustomEmailRuleCtrl,
        default_email_rule_override::DefaultEmailRuleOverrideCtrl,
    },
};
use anyhow::Context;
use futures::join;
use lazy_static::lazy_static;

lazy_static! {
    static ref DEFAULT_EMAIL_RULES: Vec<EmailRule> = {
        use crate::server_config::cfg;
        let categories = &cfg.categories;
        categories
            .iter()
            .map(|c| EmailRule {
                prompt_content: c.content.clone(),
                mail_label: c.mail_label.clone(),
                associated_email_client_category: c.gmail_categories.first().map(|s| {
                    AssociatedEmailClientCategory::try_from_value(s)
                        .unwrap_or_else(|_| panic!("Invalid email client category: {s}"))
                }),
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
                associated_email_client_category: h.gmail_categories.first().map(|s| {
                    AssociatedEmailClientCategory::try_from_value(s)
                        .unwrap_or_else(|_| panic!("Invalid email client category: {s}"))
                }),
            })
            .collect()
    };
}

#[derive(Debug, Clone)]
pub struct EmailRule {
    pub prompt_content: String,
    pub mail_label: String,
    pub associated_email_client_category: Option<AssociatedEmailClientCategory>,
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
        // let user_defined = None;
        let (default_rule_overrides, custom_email_rules) = join!(
            DefaultEmailRuleOverrideCtrl::get_by_user_id(conn, user_id),
            CustomEmailRuleCtrl::get_by_user_id(conn, user_id)
        );
        let default_rule_overrides =
            default_rule_overrides.context("Failed to fetch default overrides")?;
        let custom_email_rules = custom_email_rules.context("Failed to fetch custom rules")?;
        let data =
            Self::build_category_rules(default_rule_overrides.clone(), custom_email_rules.clone());

        Ok(Self::new(data))
    }

    fn build_category_rules(
        default_rule_overrides: Vec<default_email_rule_override::Model>,
        custom_rules: Vec<custom_email_rule::Model>,
    ) -> Vec<EmailRule> {
        let mut default_rules = DEFAULT_EMAIL_RULES
            .iter()
            .map(|rule| (rule.mail_label.clone(), rule.clone()))
            .collect::<HashMap<_, _>>();

        for ro in default_rule_overrides {
            if ro.is_disabled {
                default_rules.remove(&ro.category);
                continue;
            }
            default_rules.entry(ro.category).and_modify(|rule| {
                rule.associated_email_client_category = ro.associated_email_client_category
            });
        }

        let custom_rules = custom_rules.into_iter().map(|rule| EmailRule {
            prompt_content: rule.prompt_content,
            mail_label: rule.mail_label,
            associated_email_client_category: rule.associated_email_client_category,
        });

        custom_rules
            .chain(default_rules.values().cloned())
            .collect()
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

    pub fn add_custom_rule(&mut self, rule: EmailRule) {
        self.data.push(rule);
    }
}
