extern crate google_gmail1 as gmail;

use crate::{
    auth::{
        jwt::{generate_redirect_jwt, get_jwt_headers, Claims},
        session_store::AuthSessionStore,
    },
    db_core::prelude::*,
    model::user::{AccountAccess, UserAccessCtrl, UserCtrl},
};
use axum::{
    extract::{Query, State},
    response::{IntoResponse, Redirect},
    Json,
};
use chrono::DateTime;
use derive_more::derive::Display;
use entity::user_account_access::Column::*;
use sea_orm::TryInsertResult;
use serde::{Deserialize, Serialize};
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
const SIGN_IN_PATH: &str = "sign-in";

fn _get_auth_uri(session_store: &AuthSessionStore, caller_type: CallerType) -> String {
    let GmailConfig {
        auth_uri,
        client_id,
        redirect_uris,
        scopes,
        ..
    } = &cfg.gmail_config;

    let session_id = Uuid::new_v4();
    session_store.store_session(session_id);

    let oauth_state = OAuthState::new(session_id, caller_type);

    let mut url = Url::parse(auth_uri.as_str()).unwrap();
    url.query_pairs_mut().extend_pairs(&[
        ("client_id", client_id.as_str()),
        ("redirect_uri", redirect_uris[0].as_str()),
        ("response_type", "code"),
        ("scope", scopes.join(" ").as_str()),
        ("access_type", "offline"),
        ("prompt", "select_account"),
        ("state", &oauth_state.encode()),
    ]);

    url.to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CallerType {
    Web,
    Native,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthState {
    pub session_id: Uuid,
    pub caller_type: CallerType,
}

impl OAuthState {
    pub fn new(session_id: Uuid, caller_type: CallerType) -> Self {
        Self {
            session_id,
            caller_type,
        }
    }

    pub fn encode(&self) -> String {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        let json = serde_json::to_string(self).expect("Failed to serialize OAuthState");
        URL_SAFE_NO_PAD.encode(json.as_bytes())
    }

    pub fn decode(encoded: &str) -> Result<Self, ()> {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| ())?;
        let json = String::from_utf8(bytes).map_err(|_| ())?;
        serde_json::from_str(&json).map_err(|_| ())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthOptions {
    caller_type: CallerType,
}

pub async fn handler_auth_gmail(
    State(http_client): State<HttpClient>,
    State(session_store): State<AuthSessionStore>,
    Query(options): Query<AuthOptions>,
) -> AppResult<impl IntoResponse> {
    let req = http_client
        .get(_get_auth_uri(&session_store, options.caller_type))
        .build()?;

    Ok(Redirect::to(req.url().as_str()))
}

fn _redirect_to_confirm_connection(session_id: Uuid, caller_type: &CallerType) -> Redirect {
    let mut url = get_redirect_url(RedirectType::ConfirmConnection, caller_type);
    url.query_pairs_mut()
        .append_pair("session", session_id.to_string().as_str());

    Redirect::to(url.as_str())
}

fn _redirect_to_sign_in(session_id: Uuid, caller_type: &CallerType) -> Redirect {
    let mut url = get_redirect_url(RedirectType::Signin, caller_type);
    url.query_pairs_mut()
        .append_pair("session", session_id.to_string().as_str());

    Redirect::to(url.as_str())
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
) -> OauthResult<impl IntoResponse> {
    if query.error.is_some() {
        let err = query.error.as_ref().unwrap();
        tracing::error!("Error in oauth2 callback: {}", err);
        return Err(AuthCallbackError::Unexpected(err.to_string()));
    }
    let GmailConfig {
        token_uri,
        client_id,
        client_secret,
        redirect_uris,
        ..
    } = &cfg.gmail_config;

    if query.code.is_none() || query.state.is_none() {
        return Ok(
            Redirect::to(&_get_auth_uri(&state.session_store, CallerType::Web)).into_response(),
        );
    }

    let oauth_state = OAuthState::decode(query.state.as_ref().unwrap())
        .map_err(|_| AuthCallbackError::InvalidState)?;
    let session_id = oauth_state.session_id;
    let caller_type = oauth_state.caller_type;

    if state.session_store.load_session(session_id).is_none() {
        return Err(AuthCallbackError::InvalidState);
    }

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
            ("access_type", "offline"),
            ("prompt", "consent"),
        ])
        .send()
        .await
        .map_err(|e| {
            tracing::error!("Error getting token: {:?}", e);
            AuthCallbackError::Unexpected(e.to_string())
        })?;

    let resp: serde_json::Value = resp
        .json()
        .await
        .map_err(|_| AuthCallbackError::BadOauthResponse)?;
    let resp: GmailApiTokenResponse = serde_json::from_value(resp.clone()).map_err(|_| {
        tracing::error!("Failed to parse response: {:?}", resp);
        AuthCallbackError::BadOauthResponse
    })?;

    let email_client =
        EmailClient::from_access_code(state.http_client.clone(), resp.access_token.clone());
    let profile = email_client.get_profile().await.map_err(|e| {
        tracing::error!("Error getting profile: {:?}", e);
        AuthCallbackError::Unexpected(e.to_string())
    })?;
    // -- DEBUG
    // println!("Profile: {:?}", profile);
    // -- DEBUG
    let email = profile
        .email_address
        .ok_or(AuthCallbackError::NoEmailAddress)?;

    if let Ok(mut user) =
        UserCtrl::get_with_account_access_by_email(&state.conn, email.as_str()).await
    {
        state
            .session_store
            .add_token_to_session(session_id, generate_redirect_jwt(&user).unwrap_or_default());

        match user {
            _ if user.needs_reauthentication && resp.refresh_token.is_none() => {
                return Err(AuthCallbackError::ResetGmailAccess);
            }
            _ if resp.refresh_token.is_some() => {
                UserAccessCtrl::update_account_access(&state.conn, &user, resp)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error updating account access: {:?}", e);
                        AuthCallbackError::Unexpected(e.to_string())
                    })?;
                return Ok(
                    _redirect_to_confirm_connection(session_id, &caller_type).into_response()
                );
            }
            _ if user.access_is_expired() => {
                UserAccessCtrl::get_refreshed_token(&state.http_client, &state.conn, &mut user)
                    .await
                    .map_err(|e| match e {
                        AppError::Oauth2(e) => e,
                        _ => AuthCallbackError::Unexpected(e.to_string()),
                    })?;

                return Ok(_redirect_to_sign_in(session_id, &caller_type).into_response());
            }
            _ => {
                return Ok(_redirect_to_sign_in(session_id, &caller_type).into_response());
            }
        }
    }

    let user = UserCtrl::create(&state.conn, &email)
        .await
        .inspect_err(|e| tracing::error!("Error inserting user: {} {:?}", email, e))
        .map_err(|_| AuthCallbackError::Unexpected("User creation failed".to_string()))?;

    state
        .session_store
        .add_token_to_session(session_id, generate_redirect_jwt(&user).unwrap_or_default());

    let enc_access_code = crypt::encrypt(resp.access_token.as_str()).map_err(|e| {
        AuthCallbackError::Unexpected(format!("Failed to encrypt access code: {}", e))
    })?;

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
        .inspect_err(|e| tracing::error!("Error inserting user account access: {:?}", e))
        .map_err(|e| AuthCallbackError::Unexpected(e.to_string()))?;

    match user_account_access_insert_result {
        TryInsertResult::Inserted(_) | TryInsertResult::Conflicted => {
            Ok(_redirect_to_confirm_connection(session_id, &caller_type).into_response())
        }
        _ => {
            tracing::error!("Unexpected result from user account access insert");
            Err(AuthCallbackError::Unexpected(
                "User account access insert failed".to_string(),
            ))
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LoginPayload {
    session_id: Uuid,
}

pub async fn handler_login(
    State(session_store): State<AuthSessionStore>,
    Json(payload): Json<LoginPayload>,
) -> AppResult<impl IntoResponse> {
    let session_id = payload.session_id;
    let token = session_store
        .load_session(session_id)
        .and_then(|s| s.token)
        .ok_or(AppError::Unauthorized("Unauthorized".to_string()))?;

    session_store.destroy_session(session_id);

    Ok((
        get_jwt_headers(&token),
        Json(json!({ "message": "Logged in" })),
    ))
}

pub async fn handler_me(
    claims: Claims,
    State(conn): State<DatabaseConnection>,
) -> AppJsonResult<user::Model> {
    let user = UserCtrl::get_by_id(&conn, claims.sub).await?;
    Ok(Json(user))
}

pub async fn exchange_refresh_token(
    http_client: &HttpClient,
    refresh_token: &str,
) -> OauthResult<GmailApiRefreshTokenResponse> {
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
        .await
        .map_err(|e| {
            tracing::error!("Error refreshing token: {:?}", e);
            AuthCallbackError::BadOauthResponse
        })?;

    let resp = resp.json::<serde_json::Value>().await.map_err(|e| {
        tracing::error!("Unexpected serde error: {:?}", e);
        AuthCallbackError::Unexpected(e.to_string())
    })?;

    if resp.get("error").is_some() {
        match resp.get("error_description").and_then(|d| d.as_str()) {
            Some("Token has been expired or revoked.") => {
                return Err(AuthCallbackError::ExpiredOrRevoked);
            }
            Some(desc) => {
                tracing::error!("Unexpected error refreshing token: {:?}", desc);
                return Err(AuthCallbackError::Unexpected(desc.to_string()));
            }
            None => {
                tracing::error!("Unknown error refreshing token: {:?}", resp);
                return Err(AuthCallbackError::Unexpected(resp.to_string()));
            }
        };
    }

    let resp =
        serde_json::from_value::<GmailApiRefreshTokenResponse>(resp.clone()).map_err(|_| {
            tracing::error!("Unexpected gmail oauth2 response: {:?}", resp);
            AuthCallbackError::BadOauthResponse
        })?;

    Ok(resp)
}

const NATIVE_DEEPLINK_SCHEME: &str = "mailclerk://";

enum RedirectType {
    ConfirmConnection,
    Signin,
}

fn get_redirect_url(redirect_type: RedirectType, caller_type: &CallerType) -> Url {
    match caller_type {
        CallerType::Web => match redirect_type {
            RedirectType::Signin => cfg.frontend.get_signin_url(),
            RedirectType::ConfirmConnection => cfg.frontend.get_confirm_url(),
        },
        CallerType::Native => {
            let path = match redirect_type {
                RedirectType::Signin => SIGN_IN_PATH,
                RedirectType::ConfirmConnection => CONFIRM_CONNECTION_PATH,
            };
            Url::parse(&format!("{}{}", NATIVE_DEEPLINK_SCHEME, path)).unwrap()
        }
    }
}

#[derive(Debug, Display)]
pub(crate) enum AuthCallbackError {
    InvalidState,
    Unexpected(String),
    BadOauthResponse,
    NoEmailAddress,
    NotFound,
    ExpiredOrRevoked,
    ResetGmailAccess,
}

pub type OauthResult<T> = Result<T, AuthCallbackError>;

impl IntoResponse for AuthCallbackError {
    fn into_response(self) -> axum::response::Response {
        // Default to Web for errors since we may not have caller_type available
        let mut url = get_redirect_url(RedirectType::ConfirmConnection, &CallerType::Web);

        match self {
            AuthCallbackError::InvalidState => {
                url.query_pairs_mut().append_pair("error", "state_mismatch");
                (Redirect::to(url.as_str())).into_response()
            }
            AuthCallbackError::NoEmailAddress => {
                url.query_pairs_mut().append_pair("error", "no_email");
                (Redirect::to(url.as_str())).into_response()
            }
            AuthCallbackError::ResetGmailAccess => {
                url.query_pairs_mut()
                    .append_pair("error", "reset_gmail_access");
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
