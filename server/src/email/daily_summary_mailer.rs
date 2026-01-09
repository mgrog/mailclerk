use anyhow::Context;
use chrono::{Duration, Utc};
use entity::{prelude::*, processed_email};
use google_gmail1::api::Message;
use lettre::message::MultiPart;
use minijinja::render;
use sea_orm::{entity::*, query::*};
use std::collections::HashMap;

use sea_orm::DatabaseConnection;

use crate::{
    email::{client::EmailClient, email_template::DAILY_SUMMARY_EMAIL_TEMPLATE},
    error::AppResult,
    model::user::UserWithAccountAccess,
    HttpClient,
};

pub struct DailySummaryMailer {
    conn: DatabaseConnection,
    http_client: HttpClient,
    user: UserWithAccountAccess,
}

impl DailySummaryMailer {
    pub async fn new(
        conn: DatabaseConnection,
        http_client: HttpClient,
        user: UserWithAccountAccess,
    ) -> AppResult<Self> {
        Ok(Self {
            conn,
            http_client,
            user,
        })
    }

    async fn build_and_send_summary(
        &self,
        processed_emails: Vec<processed_email::Model>,
    ) -> anyhow::Result<()> {
        tracing::info!("Sending daily email for user {}", self.user.email);
        let raw_email = self.construct_daily_summary(&self.user.email, processed_emails)?;

        let email_client = EmailClient::new(
            self.http_client.clone(),
            self.conn.clone(),
            self.user.clone(),
        )
        .await?;

        let daily_summary_label_id = email_client.get_daily_summary_label_id().await?;

        let message = Message {
            id: None,
            thread_id: None,
            label_ids: Some(vec!["INBOX".to_string(), daily_summary_label_id]),
            snippet: None,
            history_id: None,
            internal_date: None,
            payload: None,
            size_estimate: None,
            raw: Some(raw_email),
        };

        email_client.insert_email(message).await?;

        Ok(())
    }

    pub async fn send(&self) {
        let twenty_four_hours_ago = Utc::now() - Duration::hours(24);

        match ProcessedEmail::find()
            .filter(processed_email::Column::UserId.eq(self.user.id))
            .filter(processed_email::Column::ProcessedAt.gt(twenty_four_hours_ago))
            .all(&self.conn)
            .await
        {
            Ok(processed_emails) if !processed_emails.is_empty() => {
                match self.build_and_send_summary(processed_emails).await {
                    Ok(_) => {
                        tracing::info!("Daily email sent for user {}", self.user.email);
                    }
                    Err(e) => {
                        tracing::error!(
                            "Could not send daily email for user {}: {:?}",
                            self.user.email,
                            e
                        );
                    }
                }
            }
            Ok(_) => {
                // No emails to send
                tracing::info!("No emails to send for user {}", self.user.email);
            }
            Err(e) => {
                tracing::error!(
                    "Could not fetch emails for user {}: {:?}",
                    self.user.email,
                    e
                );
            }
        }
    }

    fn construct_daily_summary(
        &self,
        user_email: &str,
        processed_emails: Vec<processed_email::Model>,
    ) -> anyhow::Result<Vec<u8>> {
        let mut category_counts = HashMap::new();
        for email in processed_emails {
            let labels = email.labels_applied.unwrap_or_default();

            let label = labels
                .iter()
                .find(|label| label.contains("Mailclerk/"))
                .cloned();

            if let Some(label) = label {
                let category = label.split(":").last().unwrap().to_string();
                let count = category_counts.entry(capitalize(&category)).or_insert(0);
                *count += 1;
            }
        }
        let category_counts = category_counts.into_iter().collect::<Vec<_>>();
        let categorys_str = category_counts
            .iter()
            .map(|(category, count)| format!("{} emails when to {}", count, category))
            .collect::<Vec<_>>()
            .join("\n");

        let plain = format!(
            "Hello {}! Here is your daily summary.\n\n{}",
            user_email, categorys_str
        );
        let html = render!(DAILY_SUMMARY_EMAIL_TEMPLATE, user_email, category_counts);

        let email = lettre::Message::builder()
            .to(format!("<{user_email}>")
                .parse()
                .context("Could not parse to in daily summary message builder")?)
            .from("Mailclerk <noreply@mailclerk.io>".parse()?)
            .subject("Breakdown of your emails from the last 24 hours")
            .multipart(MultiPart::alternative_plain_html(plain, html))?;

        Ok(email.formatted())
    }
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

// struct

// struct DailySummary {

// }

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::DateTime;
    use sea_orm::DbBackend;

    use super::*;

    // async fn setup_conn() -> DatabaseConnection {
    //     dotenvy::dotenv().ok();
    //     let db_url = env::var("DATABASE_URL").expect("DATABASE_URL is not set in .env file");
    //     let mut db_options = ConnectOptions::new(db_url);
    //     db_options.sqlx_logging(false);

    //     Database::connect(db_options)
    //         .await
    //         .expect("Database connection failed")
    // }

    #[tokio::test]
    async fn test_query() {
        // let conn = setup_conn().await;
        let dt: DateTime<Utc> = DateTime::from_str("2024-10-07 20:04:19 +00:00").unwrap();

        let query = ProcessedEmail::find()
            .filter(processed_email::Column::UserId.eq(1))
            .filter(processed_email::Column::ProcessedAt.gt(dt))
            .build(DbBackend::Postgres)
            .to_string();

        assert_eq!(query, "SELECT \"processed_email\".\"id\", \"processed_email\".\"user_account_access_id\", \"processed_email\".\"user_account_access_email\", \"processed_email\".\"processed_at\", \"processed_email\".\"labels_applied\", \"processed_email\".\"labels_removed\", \"processed_email\".\"ai_answer\" FROM \"processed_email\" WHERE \"processed_email\".\"user_account_access_id\" = 1 AND \"processed_email\".\"processed_at\" > '2024-10-07 20:04:19 +00:00'");
    }
}
