extern crate google_gmail1 as gmail;

use crate::{
    auth::{jwt::generate_redirect_auth_headers, session_store::AuthSessionStore},
    db_core::prelude::*,
    model::user::{AccountAccess, UserCtrl},
};
use axum::{
    extract::{Path, Query, State},
    response::{IntoResponse, Redirect},
    Json,
};
use chrono::{DateTime, Utc};
use entity::user_account_access::Column::*;
use sea_orm::TryInsertResult;
use serde::Deserialize;
use serde_json::json;
use url::Url;

use crate::{
    email::client::EmailClient,
    error::{AppError, AppJsonResult, AppResult},
    model::response::{GmailApiRefreshTokenResponse, GmailApiTokenResponse},
    server_config::{cfg, GmailConfig},
    HttpClient, ServerState,
};
use lib_utils::crypt;

const CONFIRM_CONNECTION_PATH: &str = "confirm-connection";

fn _get_auth_uri(session_store: &AuthSessionStore) -> String {
    let GmailConfig {
        auth_uri,
        client_id,
        redirect_uris,
        scopes,
        ..
    } = &cfg.gmail_config;

    let uuid = Uuid::new_v4();
    session_store.store_session(uuid.to_string());

    let mut url = Url::parse(auth_uri.as_str()).unwrap();
    url.query_pairs_mut().extend_pairs(&[
        ("client_id", client_id.as_str()),
        ("redirect_uri", redirect_uris[0].as_str()),
        ("response_type", "code"),
        ("scope", scopes.join(" ").as_str()),
        ("access_type", "offline"),
        ("prompt", "select_account"),
        ("state", &uuid.to_string()),
    ]);

    url.to_string()
}

pub async fn handler_auth_gmail(
    State(http_client): State<HttpClient>,
    State(session_store): State<AuthSessionStore>,
) -> AppResult<impl IntoResponse> {
    let req = http_client.get(_get_auth_uri(&session_store)).build()?;

    Ok(Redirect::to(req.url().as_str()))
}

#[derive(Deserialize, Debug)]
pub struct CallbackQuery {
    pub code: Option<String>,
    pub error: Option<String>,
    pub scope: Option<String>,
    pub state: Option<String>,
}

pub async fn handler_auth_gmail_callback(
    State(state): State<ServerState>,
    Query(query): Query<CallbackQuery>,
) -> Result<impl IntoResponse, AuthCallbackError> {
    if query.error.is_some() {
        tracing::error!("Error in oauth2 callback: {:?}", query.error);
        return Err(AuthCallbackError::Unexpected);
    }
    let GmailConfig {
        token_uri,
        client_id,
        client_secret,
        redirect_uris,
        ..
    } = &cfg.gmail_config;

    if query.code.is_none() || query.state.is_none() {
        return Ok(Redirect::to(&_get_auth_uri(&state.session_store)).into_response());
    }
    let auth_state = query.state.as_ref().unwrap();
    let code = query.code.as_ref().unwrap();

    // -- DEBUG
    // println!("Gmail config: {:?}", cfg.gmail_config);
    // -- DEBUG

    let resp = state
        .http_client
        .post(token_uri)
        .form(&[
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("code", code.as_str()),
            ("redirect_uri", redirect_uris[0].as_str()),
            ("grant_type", "authorization_code"),
        ])
        .send()
        .await
        .map_err(|_| AuthCallbackError::Unexpected)?;

    let resp: serde_json::Value = resp
        .json()
        .await
        .map_err(|_| AuthCallbackError::BadOauthResponse)?;
    let resp: GmailApiTokenResponse = serde_json::from_value(resp.clone()).map_err(|_| {
        tracing::error!("Failed to parse response: {:?}", resp);
        AuthCallbackError::BadOauthResponse
    })?;

    match state.session_store.load_session(auth_state) {
        Some(s) if s.expires_at > Utc::now().timestamp() => {
            state.session_store.destroy_session(auth_state);
        }
        _ => {
            return Err(AuthCallbackError::InvalidState);
        }
    }

    let email_client =
        EmailClient::from_access_code(state.http_client.clone(), resp.access_token.clone());
    let profile = email_client
        .get_profile()
        .await
        .map_err(|_| AuthCallbackError::Unexpected)?;
    // -- DEBUG
    // println!("Profile: {:?}", profile);
    // -- DEBUG
    let email = profile
        .email_address
        .ok_or(AuthCallbackError::NoEmailAddress)?;

    match UserCtrl::get_with_account_access_by_email(&state.conn, email.as_str()).await {
        Ok(user) if !user.needs_reauthentication && !user.access_is_expired() => {
            let headers =
                generate_redirect_auth_headers(email).map_err(|_| AuthCallbackError::Unexpected)?;
            let url = cfg.frontend_url.clone();
            return Ok((headers, Redirect::to(url.as_str())).into_response());
        }
        _ => {}
    }

    User::insert(user::ActiveModel {
        id: ActiveValue::NotSet,
        email: ActiveValue::Set(email.clone()),
        created_at: ActiveValue::NotSet,
        updated_at: ActiveValue::NotSet,
        subscription_status: ActiveValue::NotSet,
        last_payment_attempt_at: ActiveValue::NotSet,
        last_successful_payment_at: ActiveValue::NotSet,
    })
    .on_conflict(
        OnConflict::column(user::Column::Email)
            .do_nothing()
            .to_owned(),
    )
    .on_empty_do_nothing()
    .exec(&state.conn)
    .await
    .map_err(|e| {
        tracing::error!("Error inserting user: {:?}", e);
        AuthCallbackError::Unexpected
    })?;

    let enc_access_code =
        crypt::encrypt(resp.access_token.as_str()).map_err(|_| AuthCallbackError::Unexpected)?;

    let mut account_access = user_account_access::ActiveModel {
        id: ActiveValue::NotSet,
        user_email: ActiveValue::Set(email.clone()),
        access_token: ActiveValue::Set(enc_access_code),
        refresh_token: ActiveValue::Unchanged("".to_string()),
        expires_at: ActiveValue::Set(DateTime::from(
            chrono::Utc::now() + chrono::Duration::seconds(resp.expires_in as i64),
        )),
        needs_reauthentication: ActiveValue::Set(false),
        created_at: ActiveValue::NotSet,
        updated_at: ActiveValue::NotSet,
    };

    let has_refresh_token = resp.refresh_token.is_some();
    if let Some(refresh_token) = resp.refresh_token {
        let enc_refresh_token = crypt::encrypt(refresh_token.as_str()).unwrap();
        account_access.refresh_token = ActiveValue::Set(enc_refresh_token);
    };

    let user_account_access_insert_result = UserAccountAccess::insert(account_access)
        .on_conflict(
            OnConflict::column(UserEmail)
                .update_columns(if has_refresh_token {
                    vec![AccessToken, RefreshToken, ExpiresAt, UpdatedAt]
                } else {
                    vec![AccessToken, ExpiresAt, UpdatedAt]
                })
                .to_owned(),
        )
        .do_nothing()
        .exec(&state.conn)
        .await
        .map_err(|e| {
            tracing::error!("Error inserting user account access: {:?}", e);
            AuthCallbackError::Unexpected
        })?;

    let mut url = cfg.frontend_url.clone();
    match user_account_access_insert_result {
        TryInsertResult::Inserted(_) | TryInsertResult::Conflicted => {
            let headers =
                generate_redirect_auth_headers(email).map_err(|_| AuthCallbackError::Unexpected)?;
            url.path_segments_mut()
                .unwrap()
                .push(CONFIRM_CONNECTION_PATH);

            return Ok((headers, Redirect::to(url.as_str())).into_response());
        }
        // Need to handle this case by logging user in with google oauth2 jwt
        // TryInsertResult::Conflicted(_) => {
        // let headers = generate_headers_with_cookie(LONG_TTL, &email)
        //     .map_err(|_| AuthCallbackError::Unexpected)?;
        // let url = cfg.frontend_url.clone();
        // url.join("/dashboard").expect("Frontend url is invalid!");
        // return Ok((headers, Redirect::to(url.as_str())).into_response());
        // }
        _ => {
            tracing::error!("Unexpected result from user account access insert");
            Err(AuthCallbackError::Unexpected)
        }
    }
}

pub async fn handler_auth_token_callback() -> AppJsonResult<serde_json::Value> {
    Ok(Json(json!({
        "message": "Login success"
    })))
}

pub async fn handler_refresh_user_token(
    user_email: Path<String>,
    State(http_client): State<HttpClient>,
    State(conn): State<DatabaseConnection>,
) -> AppJsonResult<serde_json::Value> {
    let mut user = UserCtrl::get_with_account_access_by_email(&conn, user_email.as_str()).await?;
    crate::model::user::get_new_token(&http_client, &conn, &mut user).await?;

    Ok(Json(json!({
        "message": "Token refreshed"
    })))
}

pub async fn exchange_refresh_token(
    http_client: &HttpClient,
    refresh_token: &str,
) -> AppResult<GmailApiRefreshTokenResponse> {
    let GmailConfig {
        token_uri,
        client_id,
        client_secret,
        ..
    } = &cfg.gmail_config;

    let resp = http_client
        .post(token_uri)
        .form(&[
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("refresh_token", refresh_token),
            ("grant_type", "refresh_token"),
        ])
        .send()
        .await?;

    let resp = resp.json::<serde_json::Value>().await?;

    if resp.get("error").is_some() {
        tracing::error!("Error refreshing token: {:?}", resp);
        return Err(AppError::Oauth2);
    }

    let resp =
        serde_json::from_value::<GmailApiRefreshTokenResponse>(resp.clone()).map_err(|_| {
            tracing::error!("Unexpected gmail oauth2 response: {:?}", resp);
            AppError::Oauth2
        })?;

    Ok(resp)
}

pub(crate) enum AuthCallbackError {
    InvalidState,
    Unexpected,
    BadOauthResponse,
    NoEmailAddress,
    NotFound,
}

impl IntoResponse for AuthCallbackError {
    fn into_response(self) -> axum::response::Response {
        let mut url = cfg.frontend_url.clone();
        url.path_segments_mut()
            .unwrap()
            .push(CONFIRM_CONNECTION_PATH);

        match self {
            AuthCallbackError::InvalidState => {
                url.query_pairs_mut().append_pair("error", "state_mismatch");
                (Redirect::to(url.as_str())).into_response()
            }
            AuthCallbackError::NoEmailAddress => {
                url.query_pairs_mut().append_pair("error", "no_email");
                (Redirect::to(url.as_str())).into_response()
            }
            _ => {
                url.query_pairs_mut().append_pair("error", "unexpected");
                (Redirect::to(url.as_str())).into_response()
            }
        }
    }
}

// struct AuthRedirect;

// impl IntoResponse for AuthRedirect {
//     fn into_response(self) -> Response {
//         Redirect::temporary(&_get_auth_uri()).into_response()
//     }
// }

// impl<S> FromRequestParts<S> for User
// where
//     MemoryStore: FromRef<S>,
//     S: Send + Sync,
// {
//     // If anything goes wrong or no session is found, redirect to the auth page
//     type Rejection = AuthRedirect;

//     async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
//         let store = MemoryStore::from_ref(state);

//         let cookies = parts
//             .extract::<TypedHeader<headers::Cookie>>()
//             .await
//             .map_err(|e| match *e.name() {
//                 header::COOKIE => match e.reason() {
//                     TypedHeaderRejectionReason::Missing => AuthRedirect,
//                     _ => panic!("unexpected error getting Cookie header(s): {e}"),
//                 },
//                 _ => panic!("unexpected error getting cookies: {e}"),
//             })?;
//         let session_cookie = cookies.get(SESSION_COOKIE_NAME).ok_or(AuthRedirect)?;

//         let session = store
//             .load_session(session_cookie.to_string())
//             .await
//             .unwrap()
//             .ok_or(AuthRedirect)?;

//         let user = session.get::<User>("user").ok_or(AuthRedirect)?;

//         Ok(user)
//     }
// }
