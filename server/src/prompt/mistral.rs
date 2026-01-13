use anyhow::anyhow;
use anyhow::Context;
use indoc::formatdoc;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::email::rules::UserEmailRules;
use crate::email::simplified_message::SimplifiedMessage;
use crate::rate_limiters;
use crate::HttpClient;
use crate::{
    error::{AppError, AppResult},
    server_config::cfg,
};

const AI_ENDPOINT: &str = "https://api.mistral.ai/v1/chat/completions";

fn system_prompt(prompt_categories: Vec<String>) -> String {
    formatdoc! {r#"
        You are a helpful assistant that can categorize emails such as the categories inside the square brackets below.
        [{categories}]
        You should try to choose a single category from the above, along with its confidence score.
        You will only respond with a JSON object with the keys category and confidence. Do not provide explanations or multiple categories."#, 
    categories = prompt_categories.join(", ")}
}

pub async fn send_category_prompt(
    http_client: &HttpClient,
    rate_limiters: &rate_limiters::RateLimiters,
    email_message: &SimplifiedMessage,
    email_rules: &UserEmailRules,
) -> AppResult<CategoryPromptResponse> {
    let subject = email_message.subject.as_ref().map_or("", |s| s.as_str());
    let body = email_message.body.as_ref().map_or("", |s| s.as_str());
    let email_content_str = format!("<subject>{}</subject>\n<body>{}</body>", subject, body);

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
                "content": system_prompt(email_rules.get_prompt_categories())
              },
              {
                "role": "user",
                "content": format!("r#
                  Categorize the following email based on the email subject between the <subject> tags and the email body between the <body> tags.
                  {}
                 #", email_content_str)
              }
            ],
            "response_format": { "type": "json_object" }
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

    let (category, confidence, usage) = {
        let choice = parsed.choices.first().context("No choices in response")?;
        let usage = parsed.usage;
        match serde_json::from_str(choice.message.content.as_str()) {
            Ok(AnswerJson {
                category,
                confidence,
            }) => Ok::<_, AppError>((category, confidence, usage)),
            Err(_) => {
                println!("Could not parse JSON response, parsing manually...");
                static RE_CAT: Lazy<Regex> =
                    Lazy::new(|| Regex::new(r#""category": "(.*)""#).unwrap());
                static RE_CONF: Lazy<Regex> =
                    Lazy::new(|| Regex::new(r#""confidence": (.*)"#).unwrap());
                let category = match RE_CAT.captures(&choice.message.content) {
                    Some(caps) => {
                        let category = caps
                            .get(1)
                            .context("No category in response")?
                            .as_str()
                            .to_string();

                        Ok(category)
                    }
                    None => Err(anyhow!(
                        "Could not parse category from response: {:?}",
                        choice
                    )),
                }?;

                let confidence = match RE_CONF.captures(&choice.message.content) {
                    Some(caps) => {
                        let confidence = caps
                            .get(1)
                            .context("No confidence in response")?
                            .as_str()
                            .parse::<f32>()
                            .context("Could not parse confidence")?;

                        Ok(confidence)
                    }
                    None => Err(anyhow!(
                        "Could not parse confidence from response: {:?}",
                        choice
                    )),
                }?;

                Ok((category, confidence, usage))
            }
        }
    }?;

    // -- DEBUG
    // println!("Email from: {:?}", email_message.from);
    // println!("Email subject: {}", subject);
    // println!("Email snippet: {}", email_message.snippet);
    // println!("Email body: {}", body.chars().take(400).collect::<String>());
    // println!("Answer: {}, Confidence: {}", category, confidence);
    // -- DEBUG

    Ok(CategoryPromptResponse {
        category,
        confidence,
        token_usage: usage.total_tokens,
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CategoryPromptResponse {
    pub category: String,
    pub confidence: f32,
    pub token_usage: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AnswerJson {
    pub category: String,
    pub confidence: f32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PromptUsage {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    Stop,
    Length,
    ModelLength,
    Error,
    ToolCalls,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatChoice {
    pub index: i32,
    pub message: ChatMessage,
    pub finish_reason: FinishReason,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatApiResponse {
    pub choices: Vec<ChatChoice>,
    pub usage: PromptUsage,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatApiError {
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChatApiResponseOrError {
    Response(ChatApiResponse),
    Error(ChatApiError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "integration")]
    use crate::email::{rules::EmailRule, simplified_message::SimplifiedMessage};
    #[cfg(feature = "integration")]
    use crate::testing::common::setup_email_client;

    #[test]
    fn test_system_prompt() {
        let prompt_categories = vec!["category1".to_string(), "category2".to_string()];
        let expected = concat!(
            "You are a helpful assistant that can categorize emails such as the categories inside the square brackets below.\n",
        "[category1, category2]\n",
        "You should try to choose a single category from the above, along with its confidence score.\n",
        "You will only respond with a JSON object with the keys category and confidence. Do not provide explanations or multiple categories.",
        );

        assert_eq!(system_prompt(prompt_categories), expected);
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn test_send_category_prompt_custom_rule() {
        let http_client = HttpClient::new();
        let rate_limiters = rate_limiters::RateLimiters::new(10_000, 1_000, 1);
        let email_client = setup_email_client("mpgrospamacc@gmail.com").await;
        let gmail_msg = email_client
            .get_message_by_id("192b150bc2c64ac5")
            .await
            .unwrap();

        let msg = SimplifiedMessage::from_gmail_message(gmail_msg).unwrap();

        let test_content = "Seat Geek Upcoming Events".to_string();

        let email_rules = UserEmailRules::new_with_default_rules(vec![EmailRule {
            prompt_content: test_content.clone(),
            mail_label: "seatgeek".to_string(),
        }]);

        let resp = send_category_prompt(&http_client, &rate_limiters, &msg, &email_rules)
            .await
            .unwrap();

        assert_eq!(resp.category, test_content);
    }
}
