use config::{Config, ConfigError};
use lazy_static::lazy_static;
use serde::Deserialize;
use std::{env, fs::File, io::Read, path::Path, result::Result};
use url::Url;

#[derive(Debug, Deserialize)]
pub struct GmailConfig {
    pub client_id: String,
    pub project_id: String,
    pub auth_uri: String,
    pub token_uri: String,
    pub auth_provider_x509_cert_url: String,
    pub client_secret: String,
    pub redirect_uris: Vec<String>,
    pub scopes: Vec<String>,
}

impl GmailConfig {
    pub fn from_file(path: &str) -> Result<Self, ConfigError> {
        let builder = Config::builder()
            .add_source(config::File::with_name(path))
            .build()?;

        builder.try_deserialize()
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Category {
    pub content: String,
    pub mail_label: String,
    pub priority: i32,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Heuristic {
    pub from: String,
    pub mail_label: String,
    pub priority: i32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelConfig {
    pub id: String,
    pub temperature: f64,
    pub email_confidence_threshold: f32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PromptLimits {
    pub rate_limit_per_sec: usize,
    pub refill_interval_ms: usize,
    pub refill_amount: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenLimits {
    pub rate_limit_per_min: usize,
    pub refill_interval_ms: usize,
    pub refill_amount: usize,
    pub estimated_token_usage_per_email: usize,
    pub daily_user_quota: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub training_mode: bool,
    pub email_max_age_days: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmbeddingConfig {
    pub batch_size: usize,
    pub batch_wait_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InitialScanConfig {
    pub max_emails: usize,
    pub lookback_days: i64,
    pub batch_token_limit: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    pub key: String,
    pub prompt_limits: PromptLimits,
    pub token_limits: TokenLimits,
}

#[derive(Debug, Clone, Deserialize)]
struct FEConfig {
    pub confirm_path: String,
    pub login_path: String,
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    settings: Settings,
    api: ApiConfig,
    categories: Vec<Category>,
    heuristics: Vec<Heuristic>,
    model: ModelConfig,
    frontend: FEConfig,
    embedding: EmbeddingConfig,
    initial_scan: InitialScanConfig,
}

#[derive(Debug, Deserialize)]
pub struct Frontend {
    base_url: Url,
    config: FEConfig,
}

impl Frontend {
    pub fn get_confirm_url(&self) -> Url {
        let mut url = self.base_url.clone();
        url.set_path(&self.config.confirm_path);
        url
    }

    pub fn get_signin_url(&self) -> Url {
        let mut url = self.base_url.clone();
        url.set_path(&self.config.login_path);
        url
    }
}

#[derive(Debug)]
pub struct ServerConfig {
    pub settings: Settings,
    pub api: ApiConfig,
    pub categories: Vec<Category>,
    pub heuristics: Vec<Heuristic>,
    pub gmail_config: GmailConfig,
    pub model: ModelConfig,
    pub frontend: Frontend,
    pub embedding: EmbeddingConfig,
    pub initial_scan: InitialScanConfig,
}

impl std::fmt::Display for ServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Server Config:\n{:?}\n\nAPI: {:?}\n\nCategories:\n{}\n\nHeuristics:\n{}\n\nGmail Config: {:?}\n\nModel Config: {:?}\n\nFrontend Config: {:?}\n\nEmbedding Config: {:?}\n\nInitial Scan Config: {:?}",
            self.settings,
            self.api,
            self.categories
                .iter()
                .map(|c| format!("{} -> {}", c.content, c.mail_label))
                .collect::<Vec<_>>().join("\n"),
                self.heuristics
                .iter()
                .map(|c| format!("{} -> {}", c.from, c.mail_label))
                .collect::<Vec<_>>().join("\n"),
            self.gmail_config,
            self.model,
            self.frontend,
            self.embedding,
            self.initial_scan,
        )
    }
}

pub fn get_cert() -> Vec<u8> {
    let path = {
        if let Ok(dir) = env::var("APP_DIR") {
            format!("{}/cert.pem", dir)
        } else {
            let cargo_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
            let parent_dir = Path::new(&cargo_dir)
                .parent()
                .expect("Failed to get parent dir")
                .display()
                .to_string();
            format!("{}/config/cert.pem", parent_dir)
        }
    };

    let mut cert_buf = vec![];
    File::open(path)
        .expect("Failed to open cert.pem")
        .read_to_end(&mut cert_buf)
        .expect("Failed to read cert.pem");

    cert_buf
}

lazy_static! {
    pub static ref cfg: ServerConfig = {
        let root = env::var("APP_DIR").unwrap_or_else(|_| {
            let dir =
                env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR or APP_DIR is required");
            let dir = Path::new(&dir).parent().unwrap().display().to_string();
            format!("{}/config", dir)
        });
        let path = format!("{root}/client_secret.toml");
        let mut gmail_config =
            GmailConfig::from_file(&path).expect("client_secret.toml is required");
        let redirect_uris = [
            env::var("GMAIL_REDIRECT_URI_AUTH").unwrap_or(gmail_config.redirect_uris[0].clone()),
            env::var("GMAIL_REDIRECT_URI_TOKEN").unwrap_or(gmail_config.redirect_uris[1].clone()),
        ];
        gmail_config.redirect_uris = redirect_uris.to_vec();
        let path = format!("{root}/config.toml");
        let cfg_file: ConfigFile = Config::builder()
            .add_source(config::File::with_name(&path))
            .build()
            .expect("config.toml is required")
            .try_deserialize()
            .expect("config.toml is invalid");

        let ConfigFile {
            settings,
            api,
            categories,
            model,
            heuristics,
            frontend,
            embedding,
            initial_scan,
        } = cfg_file;

        let frontend = Frontend {
            base_url: Url::parse(&env::var("FRONTEND_URL").expect("FRONTEND_URL is required"))
                .expect("FRONTEND_URL is invalid"),
            config: frontend,
        };

        ServerConfig {
            settings,
            api,
            categories,
            heuristics,
            gmail_config,
            model,
            frontend,
            embedding,
            initial_scan,
        }
    };
    pub static ref UNKNOWN_CATEGORY: Category = Category {
        content: "Unknown".to_string(),
        mail_label: "uncategorized".to_string(),
        priority: 2
    };
}
