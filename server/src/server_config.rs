use crate::util::banner;
use config::{Config, ConfigError};
use itertools::Itertools;
use serde::Deserialize;
use std::sync::LazyLock;
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
pub struct Heuristic {
    pub from: String,
    pub mail_label: String,
    pub priority: i32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ContinuousScanConfig {
    pub max_lookback_days: i64,
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
    pub default_daily_user_limit: usize,
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
pub struct ScannerPipelineConfig {
    /// Polling interval for new email IDs in seconds (default: 5)
    #[serde(default = "default_polling_interval")]
    pub polling_interval_secs: u64,

    /// Batch job interval in seconds (default: 60)
    #[serde(default = "default_batch_interval")]
    pub batch_interval_secs: u64,

    /// Max users to poll in parallel (default: 10)
    #[serde(default = "default_max_parallel_users")]
    pub max_parallel_poll_users: usize,

    /// Max retries for failed items (default: 3)
    #[serde(default = "default_max_retries")]
    pub max_retry_count: u32,

    /// TTL for recently processed IDs in seconds (default: 3600 = 1 hour)
    #[serde(default = "default_recently_processed_ttl")]
    pub recently_processed_ttl_secs: u64,
}

fn default_polling_interval() -> u64 {
    5
}
fn default_batch_interval() -> u64 {
    60
}
fn default_max_parallel_users() -> usize {
    10
}
fn default_max_retries() -> u32 {
    3
}
fn default_recently_processed_ttl() -> u64 {
    3600
}

impl Default for ScannerPipelineConfig {
    fn default() -> Self {
        Self {
            polling_interval_secs: default_polling_interval(),
            batch_interval_secs: default_batch_interval(),
            max_parallel_poll_users: default_max_parallel_users(),
            max_retry_count: default_max_retries(),
            recently_processed_ttl_secs: default_recently_processed_ttl(),
        }
    }
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
    api: ApiConfig,
    heuristics: Vec<Heuristic>,
    model: ModelConfig,
    frontend: FEConfig,
    embedding: EmbeddingConfig,
    initial_scan: InitialScanConfig,
    continuous_scan: ContinuousScanConfig,
    #[serde(default)]
    scanner_pipeline: ScannerPipelineConfig,
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
    pub api: ApiConfig,
    pub heuristics: Vec<Heuristic>,
    pub gmail_config: GmailConfig,
    pub model: ModelConfig,
    pub frontend: Frontend,
    pub embedding: EmbeddingConfig,
    pub initial_scan: InitialScanConfig,
    pub continuous_scan: ContinuousScanConfig,
    pub scanner_pipeline: ScannerPipelineConfig,
}

impl std::fmt::Display for ServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sections = [
            ("API", format!("{:?}", self.api)),
            ("Gmail Config", format!("{:?}", self.gmail_config)),
            ("Model Config", format!("{:?}", self.model)),
            ("Frontend Config", format!("{:?}", self.frontend)),
            ("Embedding Config", format!("{:?}", self.embedding)),
            ("Initial Scan Config", format!("{:?}", self.initial_scan)),
            (
                "Continuous Scan Config",
                format!("{:?}", self.continuous_scan),
            ),
            (
                "Scanner Pipeline Config",
                format!("{:?}", self.scanner_pipeline),
            ),
        ];

        writeln!(f, "{}", banner("SERVER CONFIG"))?;
        for (name, value) in sections {
            writeln!(f)?;
            writeln!(f, "{}: {}", name, value)?;
        }
        Ok(())
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

#[allow(non_upper_case_globals)]
pub static cfg: LazyLock<ServerConfig> = LazyLock::new(|| {
    let root = env::var("APP_DIR").unwrap_or_else(|_| {
        let dir =
            env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR or APP_DIR is required");
        let dir = Path::new(&dir).parent().unwrap().display().to_string();
        format!("{}/config", dir)
    });
    let path = format!("{root}/client_secret.toml");
    let mut gmail_config = GmailConfig::from_file(&path).expect("client_secret.toml is required");
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
        api,
        model,
        heuristics,
        frontend,
        embedding,
        initial_scan,
        continuous_scan,
        scanner_pipeline,
    } = cfg_file;

    let frontend = Frontend {
        base_url: Url::parse(&env::var("FRONTEND_URL").expect("FRONTEND_URL is required"))
            .expect("FRONTEND_URL is invalid"),
        config: frontend,
    };

    ServerConfig {
        api,
        heuristics,
        gmail_config,
        model,
        frontend,
        embedding,
        initial_scan,
        continuous_scan,
        scanner_pipeline,
    }
});

pub static UNKNOWN_CATEGORY: LazyLock<CategorizationRule> = LazyLock::new(|| CategorizationRule {
    semantic_key: "Unknown".to_string(),
    mail_label: "uncategorized".to_string(),
    priority: 2,
    extract_tasks: true,
});

pub static CATEGORIZATION_CONFIG: LazyLock<CategorizationConfig> = LazyLock::new(|| {
    CategorizationConfig::load().expect("Failed to load categorization_config.toml")
});

#[derive(Debug, Clone, Deserialize)]
pub struct CategorizationRuleEntry {
    pub semantic_key: String,
    pub priority: i32,
    #[serde(default)]
    pub extract_tasks: bool,
}

#[derive(Debug, Clone)]
pub struct CategorizationRule {
    pub mail_label: String,
    pub semantic_key: String,
    pub priority: i32,
    pub extract_tasks: bool,
}

#[derive(Debug, Clone)]
pub struct CategorizationConfig {
    pub rules: Vec<CategorizationRule>,
}

impl CategorizationConfig {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let root = env::var("APP_DIR").unwrap_or_else(|_| {
            let dir =
                env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR or APP_DIR is required");
            let dir = Path::new(&dir).parent().unwrap().display().to_string();
            format!("{}/config", dir)
        });
        let path = format!("{root}/categorization_config.toml");

        let contents = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read {}: {}", path, e))?;

        let parsed: toml::Value =
            toml::from_str(&contents).map_err(|e| format!("Failed to parse {}: {}", path, e))?;

        let categories = parsed
            .get("categories")
            .and_then(|c| c.as_table())
            .ok_or("Missing 'categories' table in categorization_config.toml")?;

        let mut rules = Vec::new();

        for (label_name, entries) in categories {
            let entries_array = entries
                .as_array()
                .ok_or(format!("Expected array for categories.{}", label_name))?;

            for entry in entries_array {
                let entry: CategorizationRuleEntry = entry
                    .clone()
                    .try_into()
                    .map_err(|e| format!("Invalid entry in categories.{}: {}", label_name, e))?;

                rules.push(CategorizationRule {
                    mail_label: label_name.clone(),
                    semantic_key: entry.semantic_key,
                    priority: entry.priority,
                    extract_tasks: entry.extract_tasks,
                });
            }
        }

        Ok(Self { rules })
    }
}

impl std::fmt::Display for CategorizationConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let table = self
            .rules
            .iter()
            .map(|c| format!("{} -> {}", c.semantic_key, c.mail_label))
            .join("\n");

        write!(f, "{}", table)
    }
}
