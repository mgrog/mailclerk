use chrono::Utc;
use google_gmail1::api::Label;
use sea_orm::DatabaseConnection;

use crate::db_core::prelude::*;
use crate::error::AppResult;
use crate::server_config::cfg;

pub struct UserEmailRuleCtrl;

pub struct CreateUserEmailRule {
    pub description: String,
    pub semantic_key: String,
    pub name: String,
    pub mail_label: String,
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

    pub async fn update_label_colors(
        conn: &DatabaseConnection,
        user_id: i32,
        email_client_labels: Vec<Label>,
    ) -> AppResult<()> {
        unimplemented!()
    }

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
            })
            .collect();

        UserEmailRule::insert_many(models).exec(conn).await?;

        Ok(())
    }

    pub async fn create_default_rules(
        conn: &DatabaseConnection,
        user_id: i32,
    ) -> AppResult<()> {
        let default_rules: Vec<CreateUserEmailRule> = cfg
            .categories
            .iter()
            .map(|c| CreateUserEmailRule {
                description: c.content.clone(),
                semantic_key: c.mail_label.clone(),
                name: c.mail_label.clone(),
                mail_label: c.mail_label.clone(),
            })
            .collect();

        Self::create_many(conn, user_id, default_rules).await
    }
}
