use std::fmt::Debug;

use crate::{
    db_core::prelude::*,
    error::{AppError, AppResult},
    routes::handlers::auth::{self, AuthCallbackError},
    server_config::cfg,
    HttpClient,
};
use anyhow::{anyhow, Context};
use chrono::DateTime;
use lib_utils::crypt;
use sea_orm::DbBackend;

use super::response::GmailApiTokenResponse;

trait UserAccountAccessCols {
    fn select_user_account_access_cols(self) -> Self;
}

impl UserAccountAccessCols for Select<User> {
    fn select_user_account_access_cols(self) -> Select<User> {
        self.column_as(user_account_access::Column::Id, "user_account_access_id")
            .column_as(user_account_access::Column::AccessToken, "access_token")
            .column_as(user_account_access::Column::RefreshToken, "refresh_token")
            .column_as(user_account_access::Column::ExpiresAt, "expires_at")
            .column_as(
                user_account_access::Column::NeedsReauthentication,
                "needs_reauthentication",
            )
    }
}

trait UserTokensConsumedCols {
    fn select_user_tokens_consumed(self) -> Self;
}

impl UserTokensConsumedCols for Select<User> {
    fn select_user_tokens_consumed(self) -> Select<User> {
        self.column_as(
            user_token_usage_stat::Column::TokensConsumed,
            "tokens_consumed",
        )
    }
}

pub struct UserCtrl;

impl UserCtrl {
    pub async fn create(conn: &DatabaseConnection, email: &str) -> AppResult<user::Model> {
        let now = chrono::Utc::now().into();
        let active_model = user::ActiveModel {
            id: ActiveValue::NotSet,
            email: ActiveValue::Set(email.to_string()),
            subscription_status: ActiveValue::Set(SubscriptionStatus::Unpaid),
            last_successful_payment_at: ActiveValue::Set(None),
            last_payment_attempt_at: ActiveValue::Set(None),
            created_at: ActiveValue::Set(now),
            updated_at: ActiveValue::Set(now),
            is_setup_complete: ActiveValue::NotSet,
            is_initial_scan_complete: ActiveValue::NotSet,
            last_updated_email_rules: ActiveValue::Set(now),
            daily_token_limit: ActiveValue::Set(
                cfg.api.token_limits.default_daily_user_limit as i64,
            ),
        };

        let insert_result = User::insert(active_model).exec(conn).await;

        match insert_result {
            Ok(_) => {
                let user = Self::get_by_email(conn, email).await?;
                Ok(user)
            }
            Err(DbErr::Query(RuntimeErr::SqlxError(sqlx::Error::Database(db_err))))
                if db_err.is_unique_violation() =>
            {
                Self::get_by_email(conn, email).await
            }
            Err(e) => Err(e).context("Error creating user")?,
        }
    }

    pub async fn get_by_email(conn: &DatabaseConnection, email: &str) -> AppResult<user::Model> {
        let user = User::find()
            .filter(user::Column::Email.eq(email))
            .one(conn)
            .await
            .context("Error fetching user by email")?
            .ok_or(AppError::NotFound("User not found".to_string()))?;

        Ok(user)
    }

    pub async fn get_by_id(conn: &DatabaseConnection, user_id: i32) -> AppResult<user::Model> {
        let user = User::find_by_id(user_id)
            .one(conn)
            .await
            .context("Error fetching user by id")?
            .ok_or(AppError::NotFound("User not found".to_string()))?;

        Ok(user)
    }

    pub async fn get_with_account_access_by_id(
        conn: &DatabaseConnection,
        user_id: i32,
    ) -> AppResult<UserWithAccountAccess> {
        let user = User::find()
            .filter(user::Column::Id.eq(user_id))
            .join(JoinType::InnerJoin, user::Relation::UserAccountAccess.def())
            .select_user_account_access_cols()
            .into_model::<UserWithAccountAccess>()
            .one(conn)
            .await
            .context("Error fetching user with account access")?
            .ok_or(AppError::NotFound("User not found".to_string()))?;

        Ok(user)
    }

    pub async fn get_with_account_access_by_email(
        conn: &DatabaseConnection,
        user_email: &str,
    ) -> AppResult<UserWithAccountAccess> {
        let user = User::find()
            .filter(user::Column::Email.eq(user_email))
            .join(JoinType::InnerJoin, user::Relation::UserAccountAccess.def())
            .select_user_account_access_cols()
            .into_model::<UserWithAccountAccess>()
            .one(conn)
            .await
            .context("Error fetching user with account access")?
            .ok_or(AppError::NotFound("User not found".to_string()))?;

        Ok(user)
    }

    pub async fn get_with_account_access_and_usage_by_email(
        conn: &DatabaseConnection,
        user_email: &str,
    ) -> AppResult<UserWithAccountAccessAndUsage> {
        let user = User::find()
            .filter(user::Column::Email.eq(user_email))
            .join(JoinType::InnerJoin, user::Relation::UserAccountAccess.def())
            .join(
                JoinType::InnerJoin,
                user::Relation::UserTokenUsageStat.def(),
            )
            .select_user_account_access_cols()
            .select_user_tokens_consumed()
            .column(user::Column::DailyTokenLimit)
            .into_model::<UserWithAccountAccessAndUsage>()
            .one(conn)
            .await
            .context("Error fetching user with account access")?
            .ok_or(AppError::NotFound("User not found".to_string()))?;

        Ok(user)
    }

    pub async fn get_with_account_access_and_usage_by_id(
        conn: &DatabaseConnection,
        user_id: i32,
    ) -> AppResult<UserWithAccountAccessAndUsage> {
        let user = User::find()
            .filter(user::Column::Id.eq(user_id))
            .join(JoinType::InnerJoin, user::Relation::UserAccountAccess.def())
            .join(
                JoinType::InnerJoin,
                user::Relation::UserTokenUsageStat.def(),
            )
            .select_user_account_access_cols()
            .select_user_tokens_consumed()
            .column(user::Column::DailyTokenLimit)
            .into_model::<UserWithAccountAccessAndUsage>()
            .one(conn)
            .await
            .context("Error fetching user with account access")?
            .ok_or(AppError::NotFound("User not found".to_string()))?;

        Ok(user)
    }

    pub async fn all(conn: &DatabaseConnection) -> AppResult<Vec<UserWithAccountAccessAndUsage>> {
        let users = User::find()
            .join(JoinType::InnerJoin, user::Relation::UserAccountAccess.def())
            .join(
                JoinType::InnerJoin,
                user::Relation::UserTokenUsageStat.def(),
            )
            .select_user_account_access_cols()
            .select_user_tokens_consumed()
            .into_model::<UserWithAccountAccessAndUsage>()
            .all(conn)
            .await
            .context("Error fetching users")?;

        Ok(users)
    }

    pub async fn all_with_active_subscriptions(
        conn: &DatabaseConnection,
    ) -> AppResult<Vec<UserWithAccountAccess>> {
        let users = User::find()
            .filter(user::Column::SubscriptionStatus.eq(SubscriptionStatus::Active))
            .filter(user_account_access::Column::NeedsReauthentication.eq(false))
            .join(JoinType::InnerJoin, user::Relation::UserAccountAccess.def())
            .select_user_account_access_cols()
            .into_model::<UserWithAccountAccess>()
            .all(conn)
            .await
            .context("Error fetching users with active subscriptions")?;

        Ok(users)
    }

    pub async fn all_available_for_processing(
        conn: &DatabaseConnection,
    ) -> AppResult<Vec<UserWithAccountAccessAndUsage>> {
        let today = chrono::Utc::now().date_naive();

        let raw_sql = r#"
            SELECT
                u.id,
                u.email,
                CAST(u.subscription_status AS text),
                u.last_successful_payment_at,
                u.last_payment_attempt_at,
                u.created_at,
                u.updated_at,
                uaa.id AS user_account_access_id,
                uaa.access_token,
                uaa.refresh_token,
                uaa.expires_at,
                uaa.needs_reauthentication,
                COALESCE("user_token_usage_stat".tokens_consumed, 0) AS tokens_consumed,
                u.last_updated_email_rules,
                u.daily_token_limit
            FROM
                "user" AS u
            JOIN
                "user_account_access" AS uaa ON u.email = uaa.user_email
            LEFT JOIN
                "user_token_usage_stat" ON u.email = "user_token_usage_stat".user_email AND "user_token_usage_stat".date = $1::date
            WHERE
                u.subscription_status = (CAST('ACTIVE' AS subscription_status))
                AND (COALESCE("user_token_usage_stat".tokens_consumed, 0) < u.daily_token_limit)
                AND uaa.needs_reauthentication = FALSE
                AND u.is_initial_scan_complete = TRUE
        "#;

        let users =
            UserWithAccountAccessAndUsage::find_by_statement(Statement::from_sql_and_values(
                DbBackend::Postgres,
                raw_sql,
                [today.format("%Y-%m-%d").to_string().into()],
            ))
            .all(conn)
            .await
            .context("Error fetching users with available quota")?;

        Ok(users)
    }

    /// Fetches users available for polling with only the fields needed for
    /// email client creation and quota checks. This is more efficient than
    /// `all_available_for_processing` when full user data is not required.
    pub async fn all_pollable(conn: &DatabaseConnection) -> AppResult<Vec<PollableUser>> {
        let today = chrono::Utc::now().date_naive();

        let raw_sql = r#"
            SELECT
                u.id,
                u.email,
                uaa.id AS user_account_access_id,
                uaa.access_token,
                uaa.refresh_token,
                uaa.expires_at,
                COALESCE("user_token_usage_stat".tokens_consumed, 0) AS tokens_consumed,
                u.daily_token_limit
            FROM
                "user" AS u
            JOIN
                "user_account_access" AS uaa ON u.email = uaa.user_email
            LEFT JOIN
                "user_token_usage_stat" ON u.email = "user_token_usage_stat".user_email AND "user_token_usage_stat".date = $1::date
            WHERE
                u.subscription_status = (CAST('ACTIVE' AS subscription_status))
                AND (COALESCE("user_token_usage_stat".tokens_consumed, 0) < u.daily_token_limit)
                AND uaa.needs_reauthentication = FALSE
                AND u.is_initial_scan_complete = TRUE
        "#;

        let users = PollableUser::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            raw_sql,
            [today.format("%Y-%m-%d").to_string().into()],
        ))
        .all(conn)
        .await
        .context("Error fetching pollable users")?;

        Ok(users)
    }

    /// Fetches a single pollable user by email. Used primarily for testing.
    pub async fn get_pollable_by_email(
        conn: &DatabaseConnection,
        email: &str,
    ) -> AppResult<PollableUser> {
        let today = chrono::Utc::now().date_naive();

        let raw_sql = r#"
            SELECT
                u.id,
                u.email,
                uaa.id AS user_account_access_id,
                uaa.access_token,
                uaa.refresh_token,
                uaa.expires_at,
                COALESCE("user_token_usage_stat".tokens_consumed, 0) AS tokens_consumed,
                u.daily_token_limit
            FROM
                "user" AS u
            JOIN
                "user_account_access" AS uaa ON u.email = uaa.user_email
            LEFT JOIN
                "user_token_usage_stat" ON u.email = "user_token_usage_stat".user_email AND "user_token_usage_stat".date = $1::date
            WHERE
                u.email = $2
        "#;

        let user = PollableUser::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            raw_sql,
            [
                today.format("%Y-%m-%d").to_string().into(),
                email.into(),
            ],
        ))
        .one(conn)
        .await
        .context("Error fetching pollable user")?
        .ok_or_else(|| AppError::NotFound(format!("User not found: {}", email)))?;

        Ok(user)
    }

    pub async fn all_with_cancelled_subscriptions(
        conn: &DatabaseConnection,
    ) -> AppResult<Vec<user::Model>> {
        let users = User::find()
            .filter(user::Column::SubscriptionStatus.eq(SubscriptionStatus::Cancelled))
            .all(conn)
            .await
            .context("Error fetching users with cancelled subscriptions")?;

        Ok(users)
    }

    pub async fn unlock_daily_limit(conn: &DatabaseConnection, user_id: i32) -> AppResult<()> {
        User::update(user::ActiveModel {
            id: ActiveValue::Set(user_id),
            daily_token_limit: ActiveValue::Set(i64::MAX),
            updated_at: ActiveValue::Set(chrono::Utc::now().into()),
            ..Default::default()
        })
        .exec(conn)
        .await
        .context("Error unlocking daily limit")?;

        Ok(())
    }

    pub async fn set_initial_scan_complete(
        conn: &DatabaseConnection,
        user_id: i32,
    ) -> AppResult<()> {
        User::update(user::ActiveModel {
            id: ActiveValue::Set(user_id),
            is_initial_scan_complete: ActiveValue::Set(true),
            ..Default::default()
        })
        .exec(conn)
        .await
        .context("Error setting initial scan flag")?;

        Ok(())
    }
}

pub trait Id {
    fn id(&self) -> i32;
}

pub trait EmailAddress {
    fn email(&self) -> &str;
}

pub trait AccountAccess {
    fn get_user_account_access_id(&self) -> i32;
    fn access_token(&self) -> anyhow::Result<String>;
    fn refresh_token(&self) -> anyhow::Result<String>;
    fn get_expires_at(&self) -> DateTimeWithTimeZone;
    fn set_new_access_token(&mut self, new_access_token: &str) -> anyhow::Result<()>;
    fn access_is_expired(&self) -> bool {
        self.get_expires_at() < chrono::Utc::now()
    }
}

impl Id for user::Model {
    fn id(&self) -> i32 {
        self.id
    }
}

impl EmailAddress for user::Model {
    fn email(&self) -> &str {
        &self.email
    }
}

#[derive(FromQueryResult, Clone, Debug)]
pub struct UserWithAccountAccess {
    pub id: i32,
    pub email: String,
    pub subscription_status: SubscriptionStatus,
    pub last_successful_payment_at: Option<DateTimeWithTimeZone>,
    pub last_payment_attempt_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    pub user_account_access_id: i32,
    access_token: String,
    refresh_token: String,
    pub needs_reauthentication: bool,
    pub expires_at: DateTimeWithTimeZone,
}

impl Id for UserWithAccountAccess {
    fn id(&self) -> i32 {
        self.id
    }
}

impl EmailAddress for UserWithAccountAccess {
    fn email(&self) -> &str {
        &self.email
    }
}

impl AccountAccess for UserWithAccountAccess {
    fn get_user_account_access_id(&self) -> i32 {
        self.user_account_access_id
    }

    fn access_token(&self) -> anyhow::Result<String> {
        decrypt_token("access", &self.access_token, &self.email)
    }

    fn refresh_token(&self) -> anyhow::Result<String> {
        decrypt_token("refresh", &self.refresh_token, &self.email)
    }

    fn get_expires_at(&self) -> DateTimeWithTimeZone {
        self.expires_at
    }

    fn set_new_access_token(&mut self, new_access_token: &str) -> anyhow::Result<()> {
        let enc_access_token = crypt::encrypt(new_access_token)
            .map_err(|e| anyhow!("Failed to encrypt access code: {e}"))?;

        self.access_token = enc_access_token;

        Ok(())
    }
}

#[derive(FromQueryResult, Clone, Debug)]
pub struct UserWithAccountAccessAndUsage {
    pub id: i32,
    pub email: String,
    pub subscription_status: SubscriptionStatus,
    pub last_successful_payment_at: Option<DateTimeWithTimeZone>,
    pub last_payment_attempt_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    pub user_account_access_id: i32,
    access_token: String,
    refresh_token: String,
    pub needs_reauthentication: bool,
    pub expires_at: DateTimeWithTimeZone,
    pub tokens_consumed: i64,
    pub last_updated_email_rules: DateTimeWithTimeZone,
    pub daily_token_limit: i64,
}

impl Id for UserWithAccountAccessAndUsage {
    fn id(&self) -> i32 {
        self.id
    }
}

impl EmailAddress for UserWithAccountAccessAndUsage {
    fn email(&self) -> &str {
        &self.email
    }
}

impl AccountAccess for UserWithAccountAccessAndUsage {
    fn get_user_account_access_id(&self) -> i32 {
        self.user_account_access_id
    }

    fn access_token(&self) -> anyhow::Result<String> {
        decrypt_token("access", &self.access_token, &self.email)
    }

    fn refresh_token(&self) -> anyhow::Result<String> {
        decrypt_token("refresh", &self.refresh_token, &self.email)
    }

    fn get_expires_at(&self) -> DateTimeWithTimeZone {
        self.expires_at
    }

    fn set_new_access_token(&mut self, new_access_token: &str) -> anyhow::Result<()> {
        let enc_access_token = crypt::encrypt(new_access_token)
            .map_err(|e| anyhow!("Failed to encrypt access code: {e}"))?;

        self.access_token = enc_access_token;

        Ok(())
    }
}

/// Lightweight struct for polling operations - contains only fields needed for
/// email client creation and quota checks, avoiding overfetching.
#[derive(FromQueryResult, Clone, Debug)]
pub struct PollableUser {
    pub id: i32,
    pub email: String,
    pub user_account_access_id: i32,
    access_token: String,
    refresh_token: String,
    pub expires_at: DateTimeWithTimeZone,
    pub tokens_consumed: i64,
    pub daily_token_limit: i64,
}

impl Id for PollableUser {
    fn id(&self) -> i32 {
        self.id
    }
}

impl EmailAddress for PollableUser {
    fn email(&self) -> &str {
        &self.email
    }
}

impl AccountAccess for PollableUser {
    fn get_user_account_access_id(&self) -> i32 {
        self.user_account_access_id
    }

    fn access_token(&self) -> anyhow::Result<String> {
        decrypt_token("access", &self.access_token, &self.email)
    }

    fn refresh_token(&self) -> anyhow::Result<String> {
        decrypt_token("refresh", &self.refresh_token, &self.email)
    }

    fn get_expires_at(&self) -> DateTimeWithTimeZone {
        self.expires_at
    }

    fn set_new_access_token(&mut self, new_access_token: &str) -> anyhow::Result<()> {
        let enc_access_token = crypt::encrypt(new_access_token)
            .map_err(|e| anyhow!("Failed to encrypt access code: {e}"))?;

        self.access_token = enc_access_token;

        Ok(())
    }
}

pub struct UserAccessCtrl;

impl UserAccessCtrl {
    async fn refresh_account_access(
        conn: &DatabaseConnection,
        user: &mut impl AccountAccess,
        refreshed_access_token: &str,
        expires_in: i64,
    ) -> anyhow::Result<()> {
        let enc_access_token = crypt::encrypt(refreshed_access_token)
            .map_err(|e| anyhow!("Failed to encrypt access code: {e}"))?;

        UserAccountAccess::update(user_account_access::ActiveModel {
            id: ActiveValue::Set(user.get_user_account_access_id()),
            access_token: ActiveValue::Set(enc_access_token),
            expires_at: ActiveValue::Set(DateTime::from(
                chrono::Utc::now() + chrono::Duration::seconds(expires_in),
            )),
            needs_reauthentication: ActiveValue::Set(false),
            updated_at: ActiveValue::Set(chrono::Utc::now().into()),
            ..Default::default()
        })
        .exec(conn)
        .await?;

        user.set_new_access_token(refreshed_access_token)?;

        Ok(())
    }

    pub async fn get_refreshed_token(
        http_client: &HttpClient,
        conn: &DatabaseConnection,
        user: &mut impl AccountAccess,
    ) -> AppResult<String> {
        let access_token = user.access_token()?;
        let refresh_token = user.refresh_token()?;
        let is_expired = user.access_is_expired();

        let new_access_token = if is_expired {
            let resp = match auth::exchange_refresh_token(http_client, &refresh_token).await {
                Ok(resp) => resp,
                Err(AuthCallbackError::ExpiredOrRevoked) => {
                    tracing::info!(
                        "User {} access token expired or revoked. Flagging for re-authentication",
                        user.get_user_account_access_id()
                    );
                    UserAccessCtrl::flag_needs_reauthentication(conn, user).await?;

                    return Err(AppError::Oauth2(AuthCallbackError::ExpiredOrRevoked));
                }
                Err(e) => return Err(AppError::Oauth2(e)),
            };

            UserAccessCtrl::refresh_account_access(
                conn,
                user,
                &resp.access_token,
                resp.expires_in as i64,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Error updating account access: {:?}", e))?;

            resp.access_token
        } else {
            access_token
        };

        Ok(new_access_token)
    }

    pub async fn update_account_access(
        conn: &DatabaseConnection,
        user: &impl AccountAccess,
        response: GmailApiTokenResponse,
    ) -> AppResult<()> {
        let enc_access_token = crypt::encrypt(response.access_token.as_str())
            .map_err(|e| anyhow!("Failed to encrypt access code: {e}"))?;

        let refresh_token = response
            .refresh_token
            .context("Could not find refresh token in response")?;

        let enc_refresh_token = crypt::encrypt(refresh_token)
            .map_err(|e| anyhow!("Failed to encrypt refresh code: {e}"))?;

        UserAccountAccess::update(user_account_access::ActiveModel {
            id: ActiveValue::Set(user.get_user_account_access_id()),
            access_token: ActiveValue::Set(enc_access_token),
            refresh_token: ActiveValue::Set(enc_refresh_token),
            expires_at: ActiveValue::Set(DateTime::from(
                chrono::Utc::now() + chrono::Duration::seconds(response.expires_in as i64),
            )),
            needs_reauthentication: ActiveValue::Set(false),
            updated_at: ActiveValue::Set(chrono::Utc::now().into()),
            ..Default::default()
        })
        .exec(conn)
        .await?;

        Ok(())
    }

    async fn _update_needs_reauthentication(
        conn: &DatabaseConnection,
        user: &impl AccountAccess,
        update: bool,
    ) -> anyhow::Result<()> {
        UserAccountAccess::update(user_account_access::ActiveModel {
            id: ActiveValue::Set(user.get_user_account_access_id()),
            needs_reauthentication: ActiveValue::Set(update),
            updated_at: ActiveValue::Set(chrono::Utc::now().into()),
            ..Default::default()
        })
        .exec(conn)
        .await
        .context(format!(
            "Error flagging needs_reauthentication for user: {}",
            user.get_user_account_access_id()
        ))?;

        Ok(())
    }

    pub async fn flag_needs_reauthentication(
        conn: &DatabaseConnection,
        user: &impl AccountAccess,
    ) -> anyhow::Result<()> {
        UserAccessCtrl::_update_needs_reauthentication(conn, user, true).await?;

        Ok(())
    }

    pub async fn clear_needs_reauthentication(
        conn: &DatabaseConnection,
        user: &impl AccountAccess,
    ) -> anyhow::Result<()> {
        UserAccessCtrl::_update_needs_reauthentication(conn, user, false).await?;

        Ok(())
    }
}

fn decrypt_token(kind: &str, token: &str, email: &str) -> anyhow::Result<String> {
    let decoded = crypt::decrypt(token).map_err(|e| {
        anyhow!(
            "Failed to decrypt {} code: <{}> for: {} {:?}",
            kind,
            token,
            email,
            e,
        )
    })?;

    Ok(decoded)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "integration")]
    use std::path::Path;

    #[cfg(feature = "integration")]
    use sea_orm::{Database, DbBackend};

    #[cfg(feature = "integration")]
    use crate::db_core::prelude::*;
    #[cfg(feature = "integration")]
    use crate::model::user::UserCtrl;
    #[cfg(feature = "integration")]
    use crate::model::user_email_rule::UserEmailRuleCtrl;
    #[cfg(feature = "integration")]
    use crate::server_config::cfg;

    #[cfg(feature = "integration")]
    async fn setup_test_env() -> DatabaseConnection {
        let cargo_root = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let proj_root = Path::new(&cargo_root).parent().unwrap();
        let config_path = proj_root.join("config");
        dotenvy::from_filename(config_path.join(".env.integration")).ok();
        std::env::set_var("APP_DIR", &config_path);
        let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL is not set in .env file");
        Database::connect(db_url).await.unwrap()
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn test_create_user() {
        let conn = setup_test_env().await;
        let test_email = format!("test_user_{}@example.com", chrono::Utc::now().timestamp());

        // Create a new user
        let user = UserCtrl::create(&conn, &test_email).await.unwrap();

        assert_eq!(user.email, test_email);
        assert_eq!(user.subscription_status, SubscriptionStatus::Unpaid);
        assert!(!user.is_setup_complete);

        // Try to create the same user again (should return existing user)
        let existing_user = UserCtrl::create(&conn, &test_email).await.unwrap();
        assert_eq!(existing_user.id, user.id);

        // Cleanup
        User::delete_by_id(user.id).exec(&conn).await.unwrap();
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn test_query_statement() {
        dotenvy::from_filename(".env.integration").unwrap();
        let daily_quota = cfg.api.token_limits.default_daily_user_limit;

        let query = User::find()
            .filter(user::Column::SubscriptionStatus.eq(SubscriptionStatus::Active))
            .filter(user_token_usage_stat::Column::TokensConsumed.lt(daily_quota as i64))
            .join(JoinType::InnerJoin, user::Relation::UserAccountAccess.def())
            .join(
                JoinType::InnerJoin,
                user::Relation::UserTokenUsageStat.def(),
            )
            .column_as(user_account_access::Column::Id, "user_account_access_id")
            .column_as(
                user_token_usage_stat::Column::TokensConsumed,
                "tokens_consumed",
            )
            .build(DbBackend::Postgres)
            .to_string();

        assert_eq!(query, "")
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn test_query() {
        let cargo_root = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let proj_root = Path::new(&cargo_root).parent().unwrap();
        // let server_root = proj_root.join("server");
        let config_path = proj_root.join("config");
        dbg!(&config_path);
        dotenvy::from_filename(config_path.join(".env.integration")).unwrap();
        std::env::set_var("APP_DIR", config_path);
        let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL is not set in .env file");
        let users =
            UserCtrl::all_available_for_processing(&Database::connect(db_url).await.unwrap())
                .await
                .unwrap();

        dbg!(&users);

        assert!(users
            .iter()
            .all(|u| u.tokens_consumed < cfg.api.token_limits.default_daily_user_limit as i64));
    }
}
