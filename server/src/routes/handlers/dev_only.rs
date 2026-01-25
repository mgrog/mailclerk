//! Development-only handlers. These are only compiled in debug builds.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    auth::jwt::generate_dev_token,
    error::AppJsonResult,
    model::user::{UserAccessCtrl, UserCtrl},
    HttpClient, ServerState,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DevTokenParams {
    pub user_id: Option<i32>,
    pub email: Option<String>,
}

#[derive(Serialize)]
struct DevTokenResponse {
    token: String,
}

pub async fn dev_token(
    State(state): State<ServerState>,
    Query(params): Query<DevTokenParams>,
) -> impl IntoResponse {
    // Look up user by user_id or email
    let user = match (params.user_id, params.email) {
        (Some(user_id), _) => UserCtrl::get_by_id(&state.conn, user_id).await,
        (None, Some(email)) => UserCtrl::get_by_email(&state.conn, &email).await,
        (None, None) => {
            return (
                StatusCode::BAD_REQUEST,
                "Must provide either user_id or email",
            )
                .into_response()
        }
    };

    match user {
        Ok(user) => match generate_dev_token(user.id, &user.email) {
            Ok(token) => (StatusCode::OK, Json(DevTokenResponse { token })).into_response(),
            Err(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "Failed to create token").into_response()
            }
        },
        Err(_) => (StatusCode::NOT_FOUND, "User not found").into_response(),
    }
}

pub async fn refresh_user_token(
    user_email: Path<String>,
    State(http_client): State<HttpClient>,
    State(conn): State<DatabaseConnection>,
) -> AppJsonResult<serde_json::Value> {
    let mut user = UserCtrl::get_with_account_access_by_email(&conn, user_email.as_str()).await?;
    UserAccessCtrl::get_refreshed_token(&http_client, &conn, &mut user).await?;

    Ok(Json(json!({
        "message": "Token refreshed"
    })))
}
