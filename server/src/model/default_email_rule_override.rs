use chrono::Utc;
use sea_orm::DatabaseConnection;

use crate::{db_core::prelude::*, error::AppResult};

pub struct DefaultEmailRuleOverrideCtrl;

impl DefaultEmailRuleOverrideCtrl {
    pub async fn all_with_user_ids(
        conn: &DatabaseConnection,
        user_ids: Vec<i32>,
    ) -> AppResult<Vec<default_email_rule_override::Model>> {
        let default_email_rule_overrides = DefaultEmailRuleOverride::find()
            .filter(default_email_rule_override::Column::UserId.is_in(user_ids))
            .all(conn)
            .await?;

        Ok(default_email_rule_overrides)
    }

    pub async fn get_by_user_id(
        conn: &DatabaseConnection,
        user_id: i32,
    ) -> AppResult<Vec<default_email_rule_override::Model>> {
        let default_email_rule_overrides = DefaultEmailRuleOverride::find()
            .filter(default_email_rule_override::Column::UserId.eq(user_id))
            .all(conn)
            .await?;

        Ok(default_email_rule_overrides)
    }

    pub async fn get_last_updated(
        conn: &DatabaseConnection,
        user_id: i32,
    ) -> AppResult<chrono::DateTime<Utc>> {
        let last_updated = DefaultEmailRuleOverride::find()
            .filter(default_email_rule_override::Column::UserId.eq(user_id))
            .order_by(default_email_rule_override::Column::UpdatedAt, Order::Desc)
            .select_only()
            .column(default_email_rule_override::Column::UpdatedAt)
            .one(conn)
            .await?;

        let latest = last_updated
            .map(|x| x.updated_at.to_utc())
            .unwrap_or(chrono::DateTime::<Utc>::MIN_UTC);

        Ok(latest)
    }
}
