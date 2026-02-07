use std::collections::{HashMap, HashSet};

use sea_orm::DatabaseConnection;

use crate::db_core::prelude::*;
use crate::error::AppResult;
use crate::server_config::CategorizationRule;

pub struct SystemEmailRuleCtrl;

impl SystemEmailRuleCtrl {
    pub async fn get_all(conn: &DatabaseConnection) -> AppResult<Vec<system_email_rule::Model>> {
        let rules = SystemEmailRule::find().all(conn).await?;
        Ok(rules)
    }

    /// Syncs the system_email_rule table with the provided categorization rules.
    /// Only performs updates if the config differs from the database.
    /// Returns the number of rules synced, or None if no sync was needed.
    pub async fn sync_from_config(
        conn: &DatabaseConnection,
        rules: &[CategorizationRule],
    ) -> AppResult<Option<usize>> {
        let existing = Self::get_all(conn).await?;

        // Build a set of (semantic_key, mail_label, priority, extract_tasks) tuples for comparison
        let existing_set: HashSet<(String, String, i32, bool)> = existing
            .iter()
            .map(|r| {
                (
                    r.semantic_key.clone(),
                    r.mail_label.clone(),
                    r.priority,
                    r.extract_tasks,
                )
            })
            .collect();

        let config_set: HashSet<(String, String, i32, bool)> = rules
            .iter()
            .map(|r| {
                (
                    r.semantic_key.clone(),
                    r.mail_label.clone(),
                    r.priority,
                    r.extract_tasks,
                )
            })
            .collect();

        // Check if there are any differences
        if existing_set == config_set {
            return Ok(None);
        }

        // Delete all existing rules and insert new ones
        SystemEmailRule::delete_many().exec(conn).await?;

        let models: Vec<system_email_rule::ActiveModel> = rules
            .iter()
            .map(|rule| system_email_rule::ActiveModel {
                id: ActiveValue::NotSet,
                description: ActiveValue::Set(rule.semantic_key.clone()),
                semantic_key: ActiveValue::Set(rule.semantic_key.clone()),
                name: ActiveValue::Set(rule.semantic_key.clone()),
                mail_label: ActiveValue::Set(rule.mail_label.clone()),
                extract_tasks: ActiveValue::Set(rule.extract_tasks),
                priority: ActiveValue::Set(rule.priority),
            })
            .collect();

        let count = models.len();
        SystemEmailRule::insert_many(models).exec(conn).await?;

        Ok(Some(count))
    }

    /// Returns a map of mail_label -> priority for the given mail labels.
    pub async fn get_priorities_by_labels(
        conn: &DatabaseConnection,
        labels: &[String],
    ) -> AppResult<HashMap<String, i32>> {
        let results: Vec<(String, i32)> = SystemEmailRule::find()
            .select_only()
            .column(system_email_rule::Column::MailLabel)
            .column(system_email_rule::Column::Priority)
            .filter(system_email_rule::Column::MailLabel.is_in(labels))
            .into_tuple()
            .all(conn)
            .await?;

        Ok(results.into_iter().collect())
    }
}
