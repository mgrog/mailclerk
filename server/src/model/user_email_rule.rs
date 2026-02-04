use chrono::Utc;
use sea_orm::DatabaseConnection;

use crate::db_core::prelude::*;
use crate::error::AppResult;

pub struct UserEmailRuleCtrl;

pub struct CreateUserEmailRule {
    pub description: String,
    pub semantic_key: String,
    pub name: String,
    pub mail_label: String,
    pub extract_tasks: bool,
    pub priority: i32,
}

pub struct UpdateUserEmailRule {
    pub description: Option<String>,
    pub semantic_key: Option<String>,
    pub name: Option<String>,
    pub mail_label: Option<String>,
    pub extract_tasks: Option<bool>,
    pub priority: Option<i32>,
}

impl UserEmailRuleCtrl {
    pub async fn all_with_user_ids(
        conn: &DatabaseConnection,
        user_ids: Vec<i32>,
    ) -> AppResult<Vec<user_email_rule::Model>> {
        let user_email_rules = UserEmailRule::find()
            .filter(user_email_rule::Column::UserId.is_in(user_ids))
            .all(conn)
            .await?;

        Ok(user_email_rules)
    }

    /// Get the set of user IDs that have at least one custom email rule
    pub async fn get_users_with_rules(
        conn: &DatabaseConnection,
        user_ids: &[i32],
    ) -> AppResult<Vec<i32>> {
        use sea_orm::QuerySelect;

        let users_with_rules: Vec<i32> = UserEmailRule::find()
            .filter(user_email_rule::Column::UserId.is_in(user_ids.to_vec()))
            .select_only()
            .column(user_email_rule::Column::UserId)
            .group_by(user_email_rule::Column::UserId)
            .into_tuple()
            .all(conn)
            .await?;

        Ok(users_with_rules)
    }

    pub async fn get_by_user_id(
        conn: &DatabaseConnection,
        user_id: i32,
    ) -> AppResult<Vec<user_email_rule::Model>> {
        let user_email_rules = UserEmailRule::find()
            .filter(user_email_rule::Column::UserId.eq(user_id))
            .all(conn)
            .await?;

        Ok(user_email_rules)
    }

    pub async fn get_last_updated(
        conn: &DatabaseConnection,
        user_id: i32,
    ) -> AppResult<chrono::DateTime<Utc>> {
        let last_updated = UserEmailRule::find()
            .filter(user_email_rule::Column::UserId.eq(user_id))
            .order_by(user_email_rule::Column::UpdatedAt, Order::Desc)
            .select_only()
            .column(user_email_rule::Column::UpdatedAt)
            .one(conn)
            .await?;

        let latest = last_updated
            .map(|x| x.updated_at.to_utc())
            .unwrap_or(chrono::DateTime::<Utc>::MIN_UTC);

        Ok(latest)
    }

    // pub async fn update_label_colors(
    //     conn: &DatabaseConnection,
    //     user_id: i32,
    //     email_client_labels: Vec<Label>,
    // ) -> AppResult<()> {
    //     unimplemented!()
    // }

    pub async fn create_many(
        conn: &DatabaseConnection,
        user_id: i32,
        rules: Vec<CreateUserEmailRule>,
    ) -> AppResult<()> {
        let now = chrono::Utc::now().into();
        let models: Vec<user_email_rule::ActiveModel> = rules
            .into_iter()
            .map(|rule| user_email_rule::ActiveModel {
                id: ActiveValue::NotSet,
                user_id: ActiveValue::Set(user_id),
                description: ActiveValue::Set(rule.description),
                semantic_key: ActiveValue::Set(rule.semantic_key),
                name: ActiveValue::Set(rule.name),
                mail_label: ActiveValue::Set(rule.mail_label),
                created_at: ActiveValue::Set(now),
                updated_at: ActiveValue::Set(now),
                extract_tasks: ActiveValue::Set(rule.extract_tasks),
                priority: ActiveValue::Set(rule.priority),
                matching_labels: ActiveValue::NotSet,
            })
            .collect();

        let txn = conn.begin().await?;

        UserEmailRule::insert_many(models).exec(&txn).await?;

        User::update(user::ActiveModel {
            id: ActiveValue::Set(user_id),
            last_updated_email_rules: ActiveValue::Set(now),
            ..Default::default()
        })
        .exec(&txn)
        .await?;

        txn.commit().await?;

        Ok(())
    }

    pub async fn update_rule(
        conn: &DatabaseConnection,
        rule_id: i32,
        update: UpdateUserEmailRule,
    ) -> AppResult<user_email_rule::Model> {
        let rule = UserEmailRule::find_by_id(rule_id)
            .one(conn)
            .await?
            .ok_or_else(|| crate::error::AppError::NotFound("Rule not found".to_string()))?;

        let user_id = rule.user_id;
        let mut active_model: user_email_rule::ActiveModel = rule.into();

        if let Some(description) = update.description {
            active_model.description = ActiveValue::Set(description);
        }
        if let Some(semantic_key) = update.semantic_key {
            active_model.semantic_key = ActiveValue::Set(semantic_key);
        }
        if let Some(name) = update.name {
            active_model.name = ActiveValue::Set(name);
        }
        if let Some(mail_label) = update.mail_label {
            active_model.mail_label = ActiveValue::Set(mail_label);
        }
        if let Some(extract_tasks) = update.extract_tasks {
            active_model.extract_tasks = ActiveValue::Set(extract_tasks);
        }
        if let Some(priority) = update.priority {
            active_model.priority = ActiveValue::Set(priority);
        }

        let now = chrono::Utc::now().into();
        active_model.updated_at = ActiveValue::Set(now);

        let txn = conn.begin().await?;

        let updated_rule = active_model.update(&txn).await?;

        User::update(user::ActiveModel {
            id: ActiveValue::Set(user_id),
            last_updated_email_rules: ActiveValue::Set(now),
            ..Default::default()
        })
        .exec(&txn)
        .await?;

        txn.commit().await?;

        Ok(updated_rule)
    }
}
