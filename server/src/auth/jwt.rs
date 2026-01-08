use std::sync::LazyLock;

use axum::{async_trait, extract::FromRequestParts, http::HeaderMap, RequestPartsExt};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use chrono::Utc;
use http::{header::SET_COOKIE, request::Parts};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

use crate::{
    error::AppError,
    model::user::{EmailAddress, Id},
};

#[cfg(debug_assertions)]
pub fn generate_dev_token(user_id: i32, email: &str) -> Result<String, AuthError> {
    let claims = Claims {
        sub: user_id,
        email: email.to_string(),
        company: COMPANY.to_string(),
        exp: Utc::now().timestamp() as usize + LONG_TTL,
    };

    jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &KEYS.encoding)
        .map_err(|_| AuthError::TokenCreation)
}

static KEYS: LazyLock<Keys> = LazyLock::new(|| {
    let secret = std::env::var("JWT_SECRET").expect("JWT_SECRET must be set");
    Keys::new(&secret)
});

const COMPANY: &str = "mailclerk.io";

pub const SHORT_TTL: usize = 5 * 60; // 5 minutes
pub const LONG_TTL: usize = 24 * 60 * 60; // 24 hours
const COOKIE_NAME: &str = "session";
const DOMAIN: &str = "mailclerk.io";

pub fn generate_redirect_jwt(user: &(impl Id + EmailAddress)) -> Result<String, AuthError> {
    let claims = Claims {
        sub: user.id(),
        email: user.email().to_string(),
        company: COMPANY.to_string(),
        exp: Utc::now().timestamp() as usize + LONG_TTL,
    };

    jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &KEYS.encoding)
        .map_err(|_| AuthError::TokenCreation)
}

pub fn get_jwt_headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        SET_COOKIE,
        format!("{COOKIE_NAME}={token}; SameSite=None; HttpOnly; Secure",)
            .parse()
            .unwrap(),
    );

    headers
}

struct Keys {
    encoding: EncodingKey,
    decoding: DecodingKey,
}

impl Keys {
    fn new(secret: &str) -> Self {
        let decoded_secret = hex::decode(secret).expect("Secret was not valid hex");
        Self {
            encoding: EncodingKey::from_secret(&decoded_secret),
            decoding: DecodingKey::from_secret(&decoded_secret),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Claims {
    pub sub: i32,
    pub email: String,
    pub company: String,
    pub exp: usize,
}

#[derive(Debug)]
pub(crate) enum AuthError {
    WrongCredentials,
    MissingCredentials,
    TokenCreation,
    InvalidToken,
}

#[async_trait]
impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Extract the token from the authorization header
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| AuthError::MissingCredentials)?;

        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_aud = false;
        validation.leeway = 10 * 60; // 10 minute leeway for expired tokens

        // Decode the user data
        let token_data =
            jsonwebtoken::decode::<Claims>(bearer.token(), &KEYS.decoding, &validation).map_err(
                |e| {
                    tracing::error!("Error decoding token: {:?}", e);
                    AuthError::InvalidToken
                },
            )?;

        Ok(token_data.claims)
    }
}
