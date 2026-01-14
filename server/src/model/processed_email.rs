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

    const PRIORITY_SCORE_SQL: &'static str = "CASE \
            WHEN processed_email.due_date IS NOT NULL \
                 AND processed_email.tasks_done = false \
                 AND processed_email.due_date > NOW() - INTERVAL '7 days' \
            THEN EXTRACT(EPOCH FROM (processed_email.due_date - NOW())) / 86400 \
            WHEN processed_email.has_new_reply = true AND processed_email.is_read = false \
            THEN 1000000 \
            WHEN processed_email.is_read = false \
            THEN 2000000 + (COALESCE(5 - user_email_rule.priority, 999) * 1000) \
            ELSE 3000000 + (COALESCE(5 - user_email_rule.priority, 999) * 1000) \
        END";

    /// Get processed emails for a user sorted by weighted priority score with cursor pagination.
    ///
    /// Priority tiers (lower score = higher priority):
    /// 1. Due date urgency - UNLESS tasks_done = true OR 7+ days overdue (score: days until due, -7 to +∞)
    /// 2. has_new_reply = true AND is_read = false (score: 1,000,000)
    /// 3. Email priority AND is_read = false (score: 2,000,000 + priority * 1000)
    /// 4. Everything else - read emails, done tasks, 7+ days overdue (score: 3,000,000 + priority * 1000)
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
            // .order_by_asc(priority_score_expr)
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
}

#[derive(Debug, Clone, FromQueryResult)]
pub struct ProcessedEmailIdCategoryAndTimestamp {
    pub id: String,
    pub category: String,
    pub processed_at: chrono::DateTime<Utc>,
}
