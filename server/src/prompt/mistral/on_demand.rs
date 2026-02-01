use anyhow::{anyhow, Context};
use reqwest::StatusCode;
use serde_json::json;

use crate::email::rules::EmailRules;
use crate::email::simplified_message::SimplifiedMessage;
use crate::rate_limiters;
use crate::HttpClient;
use crate::{
    error::{AppError, AppResult},
    server_config::cfg,
};

use super::{
    categorization_user_prompt, parse_category_answer, system_prompt, CategoryChatResponse,
    ChatApiResponseOrError,
};

const AI_ENDPOINT: &str = "https://api.mistral.ai/v1/chat/completions";

pub async fn send_category_prompt<R: EmailRules>(
    http_client: &HttpClient,
    rate_limiters: &rate_limiters::RateLimiters,
    email_message: &SimplifiedMessage,
    email_rules: &R,
) -> AppResult<CategoryChatResponse> {
    let subject = email_message.subject.as_ref().map_or("", |s| s.as_str());
    let sender = email_message.from.as_ref().map_or("", |s| s.as_str());
    let body = email_message.body.as_ref().map_or("", |s| s.as_str());
    let user_content = categorization_user_prompt(subject, sender, body);

    let resp = http_client
        .post(AI_ENDPOINT)
        .bearer_auth(&cfg.api.key)
        .json(&json!(
          {
            "model": &cfg.model.id,
            "temperature": cfg.model.temperature,
            "messages": [
              {
                "role": "system",
                "content": system_prompt(email_rules.get_prompt_input())
              },
              {
                "role": "user",
                "content": user_content
              }
            ],
            "response_format": {
                "type": "json_object",
            }
          }
        ))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| {
            if let Some(status) = e.status() {
                match status {
                    StatusCode::BAD_REQUEST => AppError::BadRequest(e.to_string()),
                    StatusCode::REQUEST_TIMEOUT => AppError::RequestTimeout,
                    StatusCode::TOO_MANY_REQUESTS => AppError::TooManyRequests,
                    _ => AppError::Internal(e.into()),
                }
            } else {
                AppError::Internal(e.into())
            }
        })?;

    let parsed = serde_json::from_value::<ChatApiResponseOrError>(resp.clone())
        .context(format!("Could not parse chat response: {}", resp))?;

    let parsed = match parsed {
        ChatApiResponseOrError::Error(error) => {
            if error.message == "Requests rate limit exceeded" {
                rate_limiters.trigger_backoff();
            }
            return Err(anyhow!("Chat API error: {:?}", error).into());
        }
        ChatApiResponseOrError::Response(parsed) => parsed,
    };

    let choice = parsed.choices.first().context("No choices in response")?;
    let usage = parsed.usage;
    let answer = parse_category_answer(&choice.message.content).context(format!(
        "Could not parse JSON response: {}",
        choice.message.content
    ))?;

    Ok(CategoryChatResponse {
        general_category: answer.general_category,
        specific_category: answer.specific_category,
        confidence: answer.confidence,
        token_usage: usage.total_tokens,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "integration")]
    use crate::email::{
        rules::{EmailRule, SystemEmailRules, UserEmailRules},
        simplified_message::SimplifiedMessage,
    };
    #[cfg(feature = "integration")]
    use crate::testing::common::setup_email_client;

    #[test]
    fn test_system_prompt_user_defined() {
        use super::super::SystemPromptInput;

        let categories = vec!["category1".to_string(), "category2".to_string()];
        let result = system_prompt(SystemPromptInput::UserDefined(categories));

        assert!(result.contains("• \"category1\""));
        assert!(result.contains("• \"category2\""));
        assert!(result.contains("specific_category"));
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn test_send_category_prompt_custom_rule() {
        let http_client = HttpClient::new();
        let rate_limiters =
            rate_limiters::RateLimiters::new(10_000, 1_000, 1, 1_000_000, 1_000, 10_000);
        let email_client = setup_email_client("mpgrospamacc@gmail.com").await;
        let gmail_msg = email_client
            .get_message_by_id("192b150bc2c64ac5")
            .await
            .unwrap();

        let msg = SimplifiedMessage::from_gmail_message(&gmail_msg).unwrap();

        let test_content = "Seat Geek Upcoming Events".to_string();

        let email_rules = UserEmailRules::new_with_default_rules(vec![EmailRule {
            prompt_content: test_content.clone(),
            mail_label: "seatgeek".to_string(),
            extract_tasks: false,
            priority: 0,
        }]);

        let resp = send_category_prompt(&http_client, &rate_limiters, &msg, &email_rules)
            .await
            .unwrap();

        assert_eq!(resp.specific_category, test_content);
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn test_send_category_prompt_system_rules() {
        use crate::testing::common::setup;

        let (conn, http_client) = setup().await;
        let rate_limiters =
            rate_limiters::RateLimiters::new(10_000, 1_000, 1, 1_000_000, 1_000, 10_000);
        let email_client = setup_email_client("mpgrogan91@gmail.com").await;
        let gmail_msg = email_client
            .get_message_by_id("192b150bc2c64ac5")
            .await
            .unwrap();

        let msg = SimplifiedMessage::from_gmail_message(&gmail_msg).unwrap();

        let email_rules = SystemEmailRules::from_db(&conn).await.unwrap();

        let resp = send_category_prompt(&http_client, &rate_limiters, &msg, &email_rules)
            .await
            .unwrap();

        assert!(resp.general_category.is_some());
        assert!(!resp.specific_category.is_empty());
        assert!(resp.confidence >= 0.0 && resp.confidence <= 1.0);
        assert!(resp.token_usage > 0);
    }
}
