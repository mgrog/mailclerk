use crate::email::{
    simplified_message::SimplifiedMessage,
    rules::{EmailRule, UserEmailRules},
};
use crate::{prompt, rate_limiters::RateLimiters, HttpClient};
use axum::{extract::State, Json};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};

use crate::{auth::jwt::Claims, error::AppJsonResult};

/// # POST /user_email_rule/test

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestUserEmailRuleBody {
    pub email_summary: String,
    pub email_content: String,
    pub mail_label: String,
}

pub async fn test(
    claims: Claims,
    State(http_client): State<HttpClient>,
    State(conn): State<DatabaseConnection>,
    State(rate_limiters): State<RateLimiters>,
    Json(TestUserEmailRuleBody {
        email_summary,
        email_content,
        mail_label,
    }): Json<TestUserEmailRuleBody>,
) -> AppJsonResult<UserEmailRuleResponse> {
    let user_id = claims.sub;
    let mut user_email_rules = UserEmailRules::from_user(&conn, user_id).await?;

    user_email_rules.add_rule(EmailRule {
        prompt_content: email_summary.clone(),
        mail_label: mail_label.clone(),
        extract_tasks: false,
    });

    let simplified_msg = SimplifiedMessage::from_string(email_content);

    let response = prompt::mistral::send_category_prompt(
        &http_client,
        &rate_limiters,
        &simplified_msg,
        &user_email_rules,
    )
    .await?;

    let result = if response.category == email_summary {
        UserEmailRuleResponse {
            kind: Kind::Success,
            message: "Category matches expected".to_string(),
            ai_response: response.category,
        }
    } else {
        UserEmailRuleResponse {
            kind: Kind::Failure,
            message: "Category does not match expected".to_string(),
            ai_response: response.category,
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
