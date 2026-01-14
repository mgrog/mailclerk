use serde::{Deserialize, Serialize};

use crate::routes::account_connection::GmailAccountConnectionStatus;

#[derive(Debug, Serialize, Deserialize)]
pub struct GmailApiTokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u64,
    pub refresh_token: Option<String>,
    pub scope: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GmailApiRefreshTokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u64,
    pub scope: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LabelUpdate {
    pub added: Option<Vec<String>>,
    pub removed: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailWatchInboxPushNotification {
    pub email_address: String,
    pub history_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GmailError {
    pub code: u32,
    pub message: String,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GmailErrorResponse {
    pub error: GmailError,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CheckAccountConnectionResponse {
    pub email: String,
    pub result: GmailAccountConnectionStatus,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GoogleTokenInfo {
    pub issued_to: String,
    pub audience: String,
    pub scope: String,
    pub expires_in: i64,
    pub access_type: String,
}
