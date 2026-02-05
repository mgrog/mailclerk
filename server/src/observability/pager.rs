//! Pager Module
//!
//! Provides critical alert notifications via Pushover with debouncing and rate limiting.

use chrono::{DateTime, Duration, Utc};
use config::Config;
use reqwest::Client;
use serde::Deserialize;
use std::collections::VecDeque;
use std::env;
use std::path::Path;
use std::sync::LazyLock;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

const PUSHOVER_API_URL: &str = "https://api.pushover.net/1/messages.json";
const DEBOUNCE_DURATION_MS: u64 = 1000;
const RATE_LIMIT_HOURS: i64 = 2;
const MAX_HELD_ALERTS: usize = 500;

static PUSHOVER_CONFIG: LazyLock<Option<PushoverConfig>> = LazyLock::new(|| {
    match PushoverConfig::load() {
        Ok(config) => Some(config),
        Err(e) => {
            warn!("Pushover pager not configured: {}", e);
            None
        }
    }
});

static HTTP_CLIENT: LazyLock<Client> = LazyLock::new(Client::new);

#[derive(Debug, Clone, Deserialize)]
struct PushoverConfig {
    user_key: String,
    api_token: String,
}

impl PushoverConfig {
    fn load() -> Result<Self, config::ConfigError> {
        let root = env::var("APP_DIR").unwrap_or_else(|_| {
            let dir =
                env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR or APP_DIR is required");
            let dir = Path::new(&dir).parent().unwrap().display().to_string();
            format!("{}/config", dir)
        });
        let path = format!("{root}/pager.toml");

        Config::builder()
            .add_source(config::File::with_name(&path))
            .build()?
            .try_deserialize()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PagerError {
    #[error("Pushover not configured (missing or invalid config/pager.toml)")]
    NotConfigured,
    #[error("Failed to send notification: {0}")]
    RequestFailed(#[from] reqwest::Error),
    #[error("Pushover API error: {0}")]
    ApiError(String),
}

/// A message to be sent as a critical alert.
#[derive(Debug, Clone)]
pub struct AlertMessage {
    pub message: String,
    pub timestamp: DateTime<Utc>,
}

enum PagerCommand {
    Alert(AlertMessage),
    FlushHeld(oneshot::Sender<Vec<AlertMessage>>),
}

/// Handle for sending alerts to the PagerService.
#[derive(Clone)]
pub struct PagerHandle {
    tx: mpsc::UnboundedSender<PagerCommand>,
}

impl PagerHandle {
    /// Queue a critical alert. Messages are debounced and rate-limited by the service.
    pub fn send_critical_alert(&self, message: impl Into<String>) {
        let alert = AlertMessage {
            message: message.into(),
            timestamp: Utc::now(),
        };
        if self.tx.send(PagerCommand::Alert(alert)).is_err() {
            error!("PagerService has shut down, could not send alert");
        }
    }

    /// Drain and return all rate-limited alerts held in memory.
    pub async fn flush_alerts(&self) -> Vec<AlertMessage> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PagerCommand::FlushHeld(tx)).is_err() {
            error!("PagerService has shut down, could not flush alerts");
            return vec![];
        }
        rx.await.unwrap_or_default()
    }
}

/// Service that collects alerts, debounces them, and rate-limits sending.
pub struct PagerService {
    rx: mpsc::UnboundedReceiver<PagerCommand>,
    last_alert_time: Option<DateTime<Utc>>,
    held_alerts: VecDeque<AlertMessage>,
}

impl PagerService {
    /// Create a new PagerService and its handle for sending alerts.
    pub fn new() -> (Self, PagerHandle) {
        let (tx, rx) = mpsc::unbounded_channel();
        let service = Self {
            rx,
            last_alert_time: None,
            held_alerts: VecDeque::new(),
        };
        let handle = PagerHandle { tx };
        (service, handle)
    }

    /// Run the pager service. This should be spawned as a background task.
    pub async fn run(mut self) {
        if !is_configured() {
            warn!("PagerService started but Pushover is not configured");
        }

        loop {
            let Some(cmd) = self.rx.recv().await else {
                info!("PagerService shutting down");
                return;
            };

            match cmd {
                PagerCommand::FlushHeld(reply) => {
                    let alerts = std::mem::take(&mut self.held_alerts);
                    let _ = reply.send(alerts.into());
                }
                PagerCommand::Alert(first_alert) => {
                    self.handle_alert(first_alert).await;
                }
            }
        }
    }

    async fn handle_alert(&mut self, first_alert: AlertMessage) {
        // Collect alerts for the debounce period
        let mut alerts = vec![first_alert];
        let debounce_deadline =
            tokio::time::Instant::now() + tokio::time::Duration::from_millis(DEBOUNCE_DURATION_MS);

        loop {
            let timeout = tokio::time::timeout_at(debounce_deadline, self.rx.recv());
            match timeout.await {
                Ok(Some(PagerCommand::Alert(alert))) => alerts.push(alert),
                Ok(Some(PagerCommand::FlushHeld(reply))) => {
                    let held = std::mem::take(&mut self.held_alerts);
                    let _ = reply.send(held.into());
                }
                Ok(None) => {
                    info!("PagerService shutting down");
                    return;
                }
                Err(_) => break, // Debounce period elapsed
            }
        }

        // Check rate limit
        let now = Utc::now();
        if let Some(last_time) = self.last_alert_time {
            if now - last_time < Duration::hours(RATE_LIMIT_HOURS) {
                warn!(
                    "Rate limited: {} alert(s) held (last alert was {})",
                    alerts.len(),
                    last_time
                );
                self.hold_alerts(alerts);
                return;
            }
        }

        // Combine and send alerts
        let combined_message = Self::combine_alerts(&alerts);
        let timestamp = alerts.first().map(|a| a.timestamp).unwrap_or_else(Utc::now);

        match send_critical_alert(&combined_message, timestamp).await {
            Ok(()) => {
                self.last_alert_time = Some(now);
                info!("Sent combined alert with {} message(s)", alerts.len());
            }
            Err(e) => {
                error!("Failed to send alert: {}", e);
                self.hold_alerts(alerts);
            }
        }
    }

    fn combine_alerts(alerts: &[AlertMessage]) -> String {
        if alerts.len() == 1 {
            return alerts[0].message.clone();
        }

        let mut combined = format!("{} critical errors:\n\n", alerts.len());
        for (i, alert) in alerts.iter().enumerate() {
            combined.push_str(&format!("{}. {}\n", i + 1, alert.message));
        }
        combined
    }

    /// Add alerts to held_alerts, evicting oldest if capacity exceeded.
    fn hold_alerts(&mut self, alerts: Vec<AlertMessage>) {
        for alert in alerts {
            if self.held_alerts.len() >= MAX_HELD_ALERTS {
                self.held_alerts.pop_front();
            }
            self.held_alerts.push_back(alert);
        }
    }
}

/// Send a critical alert via Pushover.
///
/// Requires `config/pager.toml` with `user_key` and `api_token` fields.
/// If not configured, returns `PagerError::NotConfigured`.
///
/// Prefer using `PagerHandle::send_critical_alert` for debouncing and rate limiting.
async fn send_critical_alert(message: &str, timestamp: DateTime<Utc>) -> Result<(), PagerError> {
    let config = PUSHOVER_CONFIG
        .as_ref()
        .ok_or(PagerError::NotConfigured)?;

    let params = [
        ("token", config.api_token.as_str()),
        ("user", config.user_key.as_str()),
        ("message", message),
        ("priority", "1"), // High priority
        ("title", "Critical Alert"),
        ("timestamp", &timestamp.timestamp().to_string()),
    ];

    let response = HTTP_CLIENT
        .post(PUSHOVER_API_URL)
        .form(&params)
        .send()
        .await?;

    if response.status().is_success() {
        info!("Critical alert sent successfully");
        Ok(())
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        error!("Pushover API error: {} - {}", status, body);
        Err(PagerError::ApiError(format!("{}: {}", status, body)))
    }
}

/// Check if Pushover paging is configured.
pub fn is_configured() -> bool {
    PUSHOVER_CONFIG.is_some()
}
