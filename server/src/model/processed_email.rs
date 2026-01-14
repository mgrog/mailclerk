use chrono::{Duration, Utc};
use sea_orm::{FromQueryResult, JoinType, QuerySelect};
use serde::{Deserialize, Serialize};

use crate::{db_core::prelude::*, error::AppResult};

pub struct ProcessedEmailCtrl;

/// Cursor for feed pagination, encoding the last item's score and history_id
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedCursor {
    pub score: sea_orm::prelude::Decimal,
    pub history_id: sea_orm::prelude::Decimal,
}

/// Feed item containing a processed email with its computed priority score
#[derive(Debug, Clone, FromQueryResult, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeedEmail {
    pub id: String,
    pub thread_id: String,
    pub user_id: i32,
    pub processed_at: chrono::DateTime<chrono::FixedOffset>,
    pub ai_answer: String,
    pub ai_confidence: String,
    pub category: String,
    pub history_id: sea_orm::prelude::Decimal,
    pub extracted_tasks: Option<Vec<serde_json::Value>>,
    pub is_read: bool,
    pub due_date: Option<chrono::NaiveDateTime>,
    pub tasks_done: bool,
    pub has_new_reply: bool,
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

    // const PRIORITY_SCORE_SQL: &'static str = "processed.email.history_id";

    const PRIORITY_SCORE_SQL: &'static str = "(\
            CASE \
                WHEN processed_email.due_date IS NULL THEN 0 \
                WHEN processed_email.tasks_done = true THEN 0 \
                WHEN processed_email.due_date >= NOW() + INTERVAL '7 days' THEN -200 \
                WHEN processed_email.due_date >= NOW() THEN -200 + (EXTRACT(EPOCH FROM (processed_email.due_date - NOW())) / 86400) * 100 / 7 \
                WHEN processed_email.due_date >= NOW() - INTERVAL '7 days' THEN -300 \
                ELSE 0 \
            END \
        ) + \
        CASE \
            WHEN processed_email.is_read = false THEN -10 \
            ELSE 0 \
        END + \
        CASE \
            WHEN processed_email.has_new_reply = true THEN -200 \
            ELSE 0 \
        END + \
        COALESCE((4 - user_email_rule.priority) * 100, 200)";

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
    pub async fn get_feed_by_user(
        conn: &DatabaseConnection,
        user_id: i32,
        limit: u64,
        cursor: Option<FeedCursor>,
    ) -> AppResult<Vec<FeedEmail>> {
        let priority_score_expr = Expr::cust(Self::PRIORITY_SCORE_SQL);

        let mut query = ProcessedEmail::find()
            .filter(processed_email::Column::UserId.eq(user_id))
            .join_rev(
                JoinType::LeftJoin,
                user_email_rule::Entity::belongs_to(processed_email::Entity)
                    .from(user_email_rule::Column::MailLabel)
                    .to(processed_email::Column::Category)
                    .on_condition(move |_left, right| {
                        Expr::col((right, user_email_rule::Column::UserId))
                            .eq(user_id)
                            .into_condition()
                    })
                    .into(),
            )
            .select_only()
            .column(processed_email::Column::Id)
            .column(processed_email::Column::ThreadId)
            .column(processed_email::Column::UserId)
            .column(processed_email::Column::ProcessedAt)
            .column(processed_email::Column::AiAnswer)
            .column(processed_email::Column::AiConfidence)
            .column(processed_email::Column::Category)
            .column(processed_email::Column::HistoryId)
            .column(processed_email::Column::ExtractedTasks)
            .column(processed_email::Column::IsRead)
            .column(processed_email::Column::DueDate)
            .column(processed_email::Column::TasksDone)
            .column(processed_email::Column::HasNewReply)
            .column(user_email_rule::Column::Priority)
            .column_as(Expr::cust(Self::PRIORITY_SCORE_SQL), "priority_score");

        // Apply cursor filter: get rows after the cursor position
        if let Some(cursor) = cursor {
            let cursor_filter = Expr::cust(format!(
                "(({}) > {} OR (({}) = {} AND processed_email.history_id < {}))",
                Self::PRIORITY_SCORE_SQL,
                cursor.score,
                Self::PRIORITY_SCORE_SQL,
                cursor.score,
                cursor.history_id
            ));
            query = query.filter(cursor_filter);
        }

        let rows: Vec<FeedEmail> = query
            .order_by_asc(priority_score_expr)
            .order_by_desc(processed_email::Column::HistoryId)
            .limit(limit)
            .into_model()
            .all(conn)
            .await?;

        Ok(rows)
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
            .col_expr(processed_email::Column::IsRead, Value::Bool(Some(true)).into())
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
            .col_expr(processed_email::Column::IsRead, Value::Bool(Some(false)).into())
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
