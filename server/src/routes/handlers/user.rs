use axum::{extract::State, Json};
use serde_json::json;

use crate::{
    auth::jwt::Claims,
    db_core::prelude::DatabaseConnection,
    error::AppJsonResult,
    model::user::UserCtrl,
};

pub async fn handler_unlock_daily_limit(
    claims: Claims,
    State(conn): State<DatabaseConnection>,
) -> AppJsonResult<serde_json::Value> {
    UserCtrl::unlock_daily_limit(&conn, claims.sub).await?;

    Ok(Json(json!({
        "message": "Daily limit unlocked",
        "user_id": claims.sub,
        "user_email": claims.email
    })))
}
