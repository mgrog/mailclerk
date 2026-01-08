use axum::{extract::State, Json};
use google_gmail1::api::Label;
use sea_orm::DatabaseConnection;
use serde::Serialize;
use strum::IntoEnumIterator;

use crate::{
    auth::jwt::Claims,
    email::client::EmailClient,
    error::AppJsonResult,
    model::{labels::UtilityLabels, user::UserCtrl},
    HttpClient,
};

pub async fn get_user_gmail_labels(
    claims: Claims,
    State(conn): State<DatabaseConnection>,
    State(http_client): State<HttpClient>,
) -> AppJsonResult<UserGmailLabelsResponse> {
    let user_id = claims.sub;
    let user = UserCtrl::get_with_account_access_by_id(&conn, user_id).await?;
    let email_client = EmailClient::new(http_client, conn, user).await?;
    let mut labels = email_client
        .get_labels()
        .await?
        .into_iter()
        // Remove non-mailclerk labels
        .filter(|label| {
            label
                .name
                .as_ref()
                .is_some_and(|name| name.starts_with("Mailclerk/"))
        })
        // Remove utility labels like keep and uncategorized
        .filter(|label| {
            UtilityLabels::iter().all(|utility_label| {
                label.name.as_ref().map_or(true, |name| {
                    name != format!("Mailclerk/{}", utility_label.as_str()).as_str()
                })
            })
        })
        .collect::<Vec<Label>>();

    labels.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(Json(UserGmailLabelsResponse { labels }))
}

#[derive(Debug, Serialize)]
pub struct UserGmailLabelsResponse {
    labels: Vec<Label>,
}
