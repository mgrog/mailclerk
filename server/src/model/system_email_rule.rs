use sea_orm::DatabaseConnection;

use crate::db_core::prelude::*;
use crate::error::AppResult;

pub struct SystemEmailRuleCtrl;

impl SystemEmailRuleCtrl {
    pub async fn get_all(conn: &DatabaseConnection) -> AppResult<Vec<system_email_rule::Model>> {
        let rules = SystemEmailRule::find().all(conn).await?;
        Ok(rules)
    }
}
