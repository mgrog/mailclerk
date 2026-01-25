use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use chrono::Utc;
use sea_orm::prelude::Uuid;

use crate::routes::handlers::auth::CallerType;

const SESSION_TTL: i64 = 30 * 60;

#[derive(Debug, Clone)]
pub struct AuthSession {
    pub expires_at: i64,
    pub token: Option<String>,
    pub caller_type: Option<CallerType>,
}

impl AuthSession {
    pub fn is_expired(&self) -> bool {
        self.expires_at < Utc::now().timestamp()
    }
}

#[derive(Debug, Clone)]
pub struct AuthSessionStore {
    inner: Arc<RwLock<HashMap<Uuid, AuthSession>>>,
}

impl AuthSessionStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn load_session(&self, session_id: Uuid) -> Option<AuthSession> {
        self.inner.read().unwrap().get(&session_id).and_then(|s| {
            if s.is_expired() {
                None
            } else {
                Some(s.clone())
            }
        })
    }

    pub fn store_session(&self, session_id: Uuid) {
        let session = AuthSession {
            expires_at: Utc::now().timestamp() + SESSION_TTL,
            token: None,
            caller_type: None,
        };
        self.inner.write().unwrap().insert(session_id, session);
    }

    pub fn destroy_session(&self, session_id: Uuid) {
        self.inner.write().unwrap().remove(&session_id);
    }

    pub fn add_token_to_session(&self, session_id: Uuid, token: String) {
        self.inner
            .write()
            .unwrap()
            .entry(session_id)
            .and_modify(|session| {
                if !session.is_expired() {
                    session.token = Some(token);
                }
            });
    }

    pub fn add_caller_type_to_session(&self, session_id: Uuid, caller_type: CallerType) {
        self.inner
            .write()
            .unwrap()
            .entry(session_id)
            .and_modify(|session| {
                if !session.is_expired() {
                    session.caller_type = Some(caller_type);
                }
            });
    }

    pub fn clean_store(&self) {
        let now = Utc::now().timestamp();
        self.inner
            .write()
            .unwrap()
            .retain(|_, session| session.expires_at > now);
    }
}
