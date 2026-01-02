use chrono::Utc;
use google_gmail1::api::Label;
use sea_orm::DatabaseConnection;

use crate::db_core::prelude::*;
use crate::error::AppResult;

pub struct CustomEmailRuleCtrl;

impl CustomEmailRuleCtrl {
    pub async fn all_with_user_ids(
        conn: &DatabaseConnection,
        user_ids: Vec<i32>,
    ) -> AppResult<Vec<custom_email_rule::Model>> {
        let custom_email_rules = CustomEmailRule::find()
            .filter(custom_email_rule::Column::UserId.is_in(user_ids))
            .all(conn)
            .await?;

        Ok(custom_email_rules)
    }

    pub async fn get_by_user_id(
        conn: &DatabaseConnection,
        user_id: i32,
    ) -> AppResult<Vec<custom_email_rule::Model>> {
        let custom_email_rules = CustomEmailRule::find()
            .filter(custom_email_rule::Column::UserId.eq(user_id))
            .all(conn)
            .await?;

        Ok(custom_email_rules)
    }

    pub async fn get_last_updated(
        conn: &DatabaseConnection,
        user_id: i32,
    ) -> AppResult<chrono::DateTime<Utc>> {
        let last_updated = CustomEmailRule::find()
            .filter(custom_email_rule::Column::UserId.eq(user_id))
            .order_by(custom_email_rule::Column::UpdatedAt, Order::Desc)
            .select_only()
            .column(custom_email_rule::Column::UpdatedAt)
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
}
