use chrono::{Duration, Utc};
use sea_orm::{ConnectionTrait, FromQueryResult, Statement};
use serde::{Deserialize, Serialize};

use crate::{db_core::prelude::*, error::AppResult, prompt::task_extraction::ExtractedTask};

pub struct ProcessedEmailCtrl;

/// Cursor for feed pagination, encoding the last item's score and history_id
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedCursor {
    pub score: sea_orm::prelude::Decimal,
    pub history_id: sea_orm::prelude::Decimal,
    /// Timestamp from the first request, used to keep priority scores stable across pages
    pub ts: chrono::DateTime<chrono::Utc>,
}
/// Feed item containing a processed email with its computed priority score
#[derive(Debug, Clone, FromQueryResult, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeedEmail {
    pub id: String,
    pub thread_id: Option<String>,
    pub user_id: i32,
    pub processed_at: chrono::DateTime<chrono::FixedOffset>,
    pub ai_answer: String,
    pub ai_confidence: String,
    pub category: String,
    pub history_id: sea_orm::prelude::Decimal,
    pub extracted_tasks: Option<Vec<ExtractedTask>>,
    pub is_read: bool,
    pub due_date: Option<chrono::NaiveDateTime>,
    pub tasks_done: bool,
    pub has_new_reply: bool,
    pub is_thread: bool,
    pub from: Option<String>,
    pub internal_date: i64,
    pub snippet: Option<String>,
    pub subject: Option<String>,
    pub priority: Option<i32>,
    pub priority_score: Decimal,
}

impl ProcessedEmailCtrl {
    pub async fn get_processed_emails_by_user(
        conn: &DatabaseConnection,
        user_id: i32,
    ) -> AppResult<Vec<processed_email::Model>> {
        let processed_emails = ProcessedEmail::find()
            .filter(processed_email::Column::UserId.eq(user_id))
            .all(conn)
            .await?;

        Ok(processed_emails)
    }

    /// Returns the SQL expression for calculating priority score.
    /// Takes a timestamp string to use instead of NOW() for stable pagination.
    /// Uses aliases: `pe` for processed_email, `uer` for user_email_rule subquery.
    fn priority_score_sql(reference_time: &str) -> String {
        format!(
            "(\
                CASE \
                    WHEN pe.due_date IS NULL THEN 0 \
                    WHEN pe.tasks_done = true THEN 0 \
                    WHEN pe.due_date >= {ts} + INTERVAL '7 days' THEN -200 \
                    WHEN pe.due_date >= {ts} THEN -200 + (EXTRACT(EPOCH FROM (pe.due_date - {ts})) / 86400) * 100 / 7 \
                    WHEN pe.due_date >= {ts} - INTERVAL '7 days' THEN -300 \
                    ELSE 0 \
                END \
            ) + \
            CASE \
                WHEN pe.is_read = false THEN -10 \
                ELSE 0 \
            END + \
            CASE \
                WHEN pe.has_new_reply = true THEN -200 \
                ELSE 0 \
            END + \
            COALESCE((4 - uer.priority) * 100, 200)",
            ts = reference_time,
        )
    }

    /// Get processed emails for a user sorted by weighted priority score with cursor pagination.
    ///
    /// Score calculation (lower score = higher priority):
    /// - Base score starts at 0, penalties are subtracted based on urgency factors
    /// - Due date penalty:
    ///   - No due date or tasks_done: 0
    ///   - 0-7 days before due date: linear from -200 to -300
    ///   - 0-7 days overdue: -200
    ///   - 7+ days overdue: 0
    /// - is_read = false: -10
    /// - has_new_reply = true: -200
    /// - user_email_rule.priority: +(4 - priority) * 100
    ///
    /// Returns (emails, reference_timestamp) where reference_timestamp should be used
    /// when creating the cursor for the next page.
    pub async fn get_feed_by_user(
        conn: &DatabaseConnection,
        user_id: i32,
        limit: u64,
        cursor: Option<FeedCursor>,
    ) -> AppResult<(Vec<FeedEmail>, chrono::DateTime<chrono::Utc>)> {
        // Use timestamp from cursor if available, otherwise use current time.
        // This keeps priority scores stable across paginated requests.
        let reference_time = cursor.as_ref().map(|c| c.ts).unwrap_or_else(Utc::now);
        let reference_time_sql = format!("'{}'::timestamptz", reference_time.to_rfc3339());
        let priority_score_sql = Self::priority_score_sql(&reference_time_sql);

        let cursor_filter = cursor
            .as_ref()
            .map(|c| {
                format!(
                    "AND (({ps}) > {score} OR (({ps}) = {score} AND pe.history_id < {hid}))",
                    ps = priority_score_sql,
                    score = c.score,
                    hid = c.history_id
                )
            })
            .unwrap_or_default();

        let sql = format!(
            r#"
            SELECT
                pe.id,
                pe.thread_id,
                pe.user_id,
                pe.processed_at,
                pe.ai_answer,
                pe.ai_confidence,
                pe.category,
                pe.history_id,
                array_to_json(pe.extracted_tasks) AS extracted_tasks,
                pe.is_read,
                pe.due_date,
                pe.tasks_done,
                pe.has_new_reply,
                pe.is_thread,
                pe."from",
                pe.internal_date,
                pe.snippet,
                pe.subject,
                uer.priority,
                ({priority_score}) AS priority_score
            FROM processed_email pe
            LEFT JOIN (
                SELECT DISTINCT ON (mail_label) mail_label, priority
                FROM user_email_rule
                WHERE user_id = $1
                ORDER BY mail_label
            ) uer ON uer.mail_label = pe.category
            WHERE pe.user_id = $1
            {cursor_filter}
            ORDER BY priority_score ASC, pe.history_id DESC
            LIMIT $2
            "#,
            priority_score = priority_score_sql,
            cursor_filter = cursor_filter,
        );

        let rows: Vec<FeedEmail> = FeedEmail::find_by_statement(Statement::from_sql_and_values(
            conn.get_database_backend(),
            sql,
            [user_id.into(), (limit as i64).into()],
        ))
        .all(conn)
        .await?;

        Ok((rows, reference_time))
    }

    /// Get total count of processed emails for a user (for pagination)
    pub async fn count_by_user(conn: &DatabaseConnection, user_id: i32) -> AppResult<u64> {
        let count = ProcessedEmail::find()
            .filter(processed_email::Column::UserId.eq(user_id))
            .count(conn)
            .await?;

        Ok(count)
    }

    pub async fn get_users_processed_emails_for_cleanup(
        conn: &DatabaseConnection,
        cleanup_setting: &auto_cleanup_setting::Model,
    ) -> AppResult<Vec<ProcessedEmailIdCategoryAndTimestamp>> {
        let user_id = cleanup_setting.user_id;
        let timestamp = Utc::now() - Duration::days(cleanup_setting.after_days_old as i64);
        let processed_emails = ProcessedEmail::find()
            .filter(processed_email::Column::UserId.eq(user_id))
            .filter(processed_email::Column::Category.eq(&cleanup_setting.mail_label))
            .filter(processed_email::Column::ProcessedAt.lt(timestamp))
            .select_only()
            .column(processed_email::Column::Id)
            .column(processed_email::Column::Category)
            .column(processed_email::Column::ProcessedAt)
            .into_model()
            .all(conn)
            .await?;

        Ok(processed_emails)
    }

    pub async fn insert(
        conn: &DatabaseConnection,
        active_model: processed_email::ActiveModel,
    ) -> Result<InsertResult<processed_email::ActiveModel>, DbErr> {
        ProcessedEmail::insert(active_model).exec(conn).await
    }

    pub async fn mark_as_read(
        conn: &DatabaseConnection,
        message_id: &str,
        user_id: i32,
    ) -> AppResult<()> {
        ProcessedEmail::update_many()
            .col_expr(
                processed_email::Column::IsRead,
                Value::Bool(Some(true)).into(),
            )
            .filter(processed_email::Column::Id.eq(message_id))
            .filter(processed_email::Column::UserId.eq(user_id))
            .exec(conn)
            .await?;

        Ok(())
    }

    pub async fn mark_as_unread(
        conn: &DatabaseConnection,
        message_id: &str,
        user_id: i32,
    ) -> AppResult<()> {
        ProcessedEmail::update_many()
            .col_expr(
                processed_email::Column::IsRead,
                Value::Bool(Some(false)).into(),
            )
            .filter(processed_email::Column::Id.eq(message_id))
            .filter(processed_email::Column::UserId.eq(user_id))
            .exec(conn)
            .await?;

        Ok(())
    }
}

#[derive(Debug, Clone, FromQueryResult)]
pub struct ProcessedEmailIdCategoryAndTimestamp {
    pub id: String,
    pub category: String,
    pub processed_at: chrono::DateTime<Utc>,
}
