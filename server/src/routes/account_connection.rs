use std::{collections::HashSet, str::FromStr};

use axum::{extract::State, Json};
use lib_email_clients::gmail::AccessScopes;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};

use crate::{
    auth::jwt::Claims,
    email::client::{EmailClient, MessageListOptions},
    error::{AppError, AppJsonResult},
    model::{
        response::{CheckAccountConnectionResponse, GoogleTokenInfo},
        user::{AccountAccess, UserAccessCtrl, UserCtrl},
    },
    server_config::cfg,
    HttpClient,
};

async fn _missing_scopes(
    http_client: &HttpClient,
    access_token: &str,
) -> Result<Vec<AccessScopes>, ScopeError> {
    let required_scopes = &cfg.gmail_config.scopes;
    let mut missing_scopes = vec![];

    let resp = http_client
        .get("https://www.googleapis.com/oauth2/v1/tokeninfo")
        .query(&[("access_token", access_token)])
        .send()
        .await?;

    let resp = resp.json::<serde_json::Value>().await?;

    if resp.get("error").is_some() {
        let error = resp.get("error").unwrap().as_str().unwrap();
        match error {
            "invalid_token" => return Err(ScopeError::InvalidToken),
            _ => return Err(ScopeError::UnexpectedError),
        }
    }

    let data = serde_json::from_value::<GoogleTokenInfo>(resp.clone())
        .inspect_err(|_| {
            tracing::error!("Unexpected token info response: {:?}", resp);
        })
        .map_err(|_| ScopeError::UnexpectedError)?;

    let scopes = data.scope.split(' ').collect::<HashSet<&str>>();
    for scope in required_scopes {
        if !scopes.contains(scope.as_str()) {
            missing_scopes.push(AccessScopes::from_str(scope).unwrap());
        }
    }

    Ok(missing_scopes)
}

async fn _profile_ok(email_client: &EmailClient) -> bool {
    let profile_result = email_client.get_profile().await;
    matches!(profile_result, Ok(profile) if profile.email_address.is_some())
}

async fn _get_latest_message_id(email_client: &EmailClient) -> anyhow::Result<Option<String>> {
    let options = MessageListOptions {
        max_results: Some(1),
        ..Default::default()
    };
    let response = email_client.get_message_list(options).await?;

    Ok(response
        .messages
        .and_then(|msgs| msgs.first().and_then(|m| m.id.clone())))
}

async fn _read_messages_ok(email_client: &EmailClient) -> bool {
    let options = MessageListOptions {
        max_results: Some(10),
        ..Default::default()
    };

    email_client.get_message_list(options).await.is_ok()
}

//? Maybe get rid of message insert
// async fn _insert_messages_ok(email_client: &EmailClient) -> bool {
//     unimplemented!()
// }

async fn _labels_ok(email_client: &EmailClient) -> bool {
    email_client.get_labels().await.is_ok()
}

/// Checks the connection status of a Gmail account by performing various checks such as
/// verifying required scopes, profile information, reading messages, and labels.
///
/// # Arguments
///
/// * `State(http_client)`: The HTTP client used for making requests.
/// * `State(conn)`: The database connection.
/// * `Query(query)`: The query parameters containing the email address to check.
///
/// # Returns
///
/// Returns a JSON response containing the connection status of the Gmail account.
/// ```json {
///    "email": "example@gmail.com",
///    "result": {
///      "status": "passed | failed | not_found | access_denied",
///      "passed_checks": ["scopes_ok", "profile_ok", "read_messages_ok", "labels_ok"],
///      "failed_checks": ["missing_scopes", "profile_check_failed", "read_messages_check_failed", "labels_check_failed"]
///     }
/// }
/// ```
///
/// # Possible Results
///
/// * `GmailAccountConnectionStatus::NotFound`: The email account was not found in the database.
/// * `GmailAccountConnectionStatus::AccessDenied`: The access token is invalid or expired.
/// * `GmailAccountConnectionStatus::Failed`: One or more checks failed, with details of passed and failed checks.
/// * `GmailAccountConnectionStatus::Passed`: All checks passed successfully.
///
/// # Errors
///
/// Returns an `AppJsonResult` containing an `AppError` if there is an error during the process.
///
/// # Usage
///
/// ```javascript
///     fetch(`${API_BASE_URL}/check_account_connection`)
/// ```
pub async fn check_account_connection(
    claims: Claims,
    State(http_client): State<HttpClient>,
    State(conn): State<DatabaseConnection>,
) -> AppJsonResult<CheckAccountConnectionResponse> {
    let user_access = match UserCtrl::get_with_account_access_by_email(&conn, &claims.email).await {
        Ok(user) => user,
        Err(AppError::NotFound(_)) => {
            return Ok(Json(CheckAccountConnectionResponse {
                email: claims.email,
                result: GmailAccountConnectionStatus::NotFound,
            }))
        }
        Err(e) => {
            return Err(e);
        }
    };

    let email_client =
        EmailClient::new(http_client.clone(), conn.clone(), user_access.clone()).await?;

    let mut passed_checks = vec![];
    let mut failed_checks = vec![];

    let missing_scopes = match _missing_scopes(&http_client, &user_access.access_token()?).await {
        Ok(missing_scopes) => missing_scopes,
        Err(_) => {
            // If this fails, its likely the token is invalid
            return Ok(Json(CheckAccountConnectionResponse {
                email: claims.email,
                result: GmailAccountConnectionStatus::AccessDenied,
            }));
        }
    };

    if !missing_scopes.is_empty() {
        failed_checks.push(AccountCheckFailures::MissingScopes { missing_scopes });
    } else {
        passed_checks.push(AccountCheckSuccesses::ScopesOk);
    }

    if !_profile_ok(&email_client).await {
        failed_checks.push(AccountCheckFailures::ProfileCheckFailed);
    } else {
        passed_checks.push(AccountCheckSuccesses::ProfileOk);
    }

    if !_read_messages_ok(&email_client).await {
        failed_checks.push(AccountCheckFailures::ReadMessagesCheckFailed);
    } else {
        passed_checks.push(AccountCheckSuccesses::ReadMessagesOk);
    }

    if !_labels_ok(&email_client).await {
        failed_checks.push(AccountCheckFailures::LabelsCheckFailed);
    } else {
        passed_checks.push(AccountCheckSuccesses::LabelsOk);
    }

    if !failed_checks.is_empty() {
        return Ok(Json(CheckAccountConnectionResponse {
            email: claims.email,
            result: GmailAccountConnectionStatus::Failed {
                passed_checks,
                failed_checks,
            },
        }));
    }

    if user_access.needs_reauthentication {
        UserAccessCtrl::clear_needs_reauthentication(&conn, &user_access).await?;
    }

    Ok(Json(CheckAccountConnectionResponse {
        email: claims.email,
        result: GmailAccountConnectionStatus::Passed { passed_checks },
    }))
}

pub enum ScopeError {
    InvalidToken,
    NetworkError,
    UnexpectedError,
}

impl From<reqwest::Error> for ScopeError {
    fn from(_: reqwest::Error) -> Self {
        ScopeError::NetworkError
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged, rename_all = "snake_case")]
pub enum AccountCheckFailures {
    MissingScopes { missing_scopes: Vec<AccessScopes> },
    ProfileCheckFailed,
    ReadMessagesCheckFailed,
    LabelsCheckFailed,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
pub enum AccountCheckSuccesses {
    ScopesOk,
    ProfileOk,
    ReadMessagesOk,
    LabelsOk,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum GmailAccountConnectionStatus {
    NotFound,
    AccessDenied,
    Failed {
        passed_checks: Vec<AccountCheckSuccesses>,
        failed_checks: Vec<AccountCheckFailures>,
    },
    Passed {
        passed_checks: Vec<AccountCheckSuccesses>,
    },
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "integration")]
    use super::*;
    #[cfg(feature = "integration")]
    use crate::testing::common::{get_test_user_claims, setup};

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn test_check_account_connection_ok() {
        // This test requires a valid email account mpgrospamacc@gmail.com to be present in the database
        const EMAIL: &str = "mpgrospamacc@gmail.com";
        let (conn, http_client) = setup().await;

        let claims = get_test_user_claims(EMAIL);

        let check =
            check_account_connection(claims, State(http_client.clone()), State(conn.clone()))
                .await
                .unwrap();

        dbg!(&check.result);

        assert_eq!(check.email, EMAIL);
        assert!(matches!(
            check.result,
            GmailAccountConnectionStatus::Passed { .. }
        ));
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn test_check_account_connection_missing_scopes() {
        // This test requires a valid email account
        const EMAIL: &str = "mtest4966@gmail.com";
        let (conn, http_client) = setup().await;
        let claims = get_test_user_claims(EMAIL);

        let check =
            check_account_connection(claims, State(http_client.clone()), State(conn.clone()))
                .await
                .unwrap();

        dbg!(&check.result);

        assert_eq!(&check.email, &EMAIL);
        match &check.result {
            GmailAccountConnectionStatus::Failed { failed_checks, .. } => {
                assert!(failed_checks.len() == 1);
                assert!(matches!(
                    failed_checks[0],
                    AccountCheckFailures::MissingScopes { .. }
                ));
            }
            _ => panic!("Unexpected result"),
        }
    }
}
