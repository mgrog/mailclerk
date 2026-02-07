use super::common::fetch_email_client;
use crate::{
    email::{
        rules::{EmailRule, UserEmailRules},
        simplified_message::SimplifiedMessage,
    },
    ServerState,
};
use crate::{prompt, rate_limiters::RateLimiters, HttpClient};
use axum::{extract::State, Json};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};

use crate::{auth::jwt::Claims, error::AppJsonResult};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestRule {
    semantic_key: String,
    mail_label: String,
    extract_tasks: bool,
    priority: i32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestUserEmailRuleBody {
    email_id: String,
    rule: TestRule,
}

/// # POST /user_email_rule/check
pub async fn check(
    claims: Claims,
    State(http_client): State<HttpClient>,
    State(conn): State<DatabaseConnection>,
    State(rate_limiters): State<RateLimiters>,
    State(server_state): State<ServerState>,
    Json(TestUserEmailRuleBody { email_id, rule }): Json<TestUserEmailRuleBody>,
) -> AppJsonResult<UserEmailRuleResponse> {
    let user_id = claims.sub;
    let user_email = claims.email;

    let (user_email_rules, email_client) = tokio::join!(
        UserEmailRules::from_user(&conn, user_id),
        fetch_email_client(server_state, user_email)
    );

    let mut user_email_rules = user_email_rules?;
    let email_client = email_client?;

    let message = email_client.get_message_by_id(&email_id).await?;
    let simplified_msg = SimplifiedMessage::from_gmail_message(&message)?;

    user_email_rules.add_rule(EmailRule {
        prompt_content: rule.semantic_key.clone(),
        mail_label: rule.mail_label.clone(),
        extract_tasks: rule.extract_tasks,
        priority: rule.priority,
    });

    let response = prompt::mistral::on_demand::send_category_prompt(
        &http_client,
        &rate_limiters,
        &simplified_msg,
        &user_email_rules,
    )
    .await?;

    let result = if response.specific_type == rule.mail_label {
        UserEmailRuleResponse {
            kind: Kind::Success,
            message: "Category matches expected".to_string(),
            ai_response: response.specific_type,
        }
    } else {
        UserEmailRuleResponse {
            kind: Kind::Failure,
            message: "Category does not match expected".to_string(),
            ai_response: response.specific_type,
        }
    };

    Ok(Json(result))
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Success,
    Failure,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserEmailRuleResponse {
    kind: Kind,
    message: String,
    ai_response: String,
}
