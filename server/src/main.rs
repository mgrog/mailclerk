#![allow(dead_code)]
#[macro_use]
mod macros;

mod auth;
mod cron_time_utils;
mod db_core;
mod email;
mod embed;
mod error;
mod model;
mod notify;
mod prompt;
mod rate_limiters;
mod request_tracing;
mod routes;
mod server_config;
mod state;
mod testing;
mod util;

use std::{
    env,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use auth::session_store::AuthSessionStore;
use axum::{extract::FromRef, routing::get, Router};
use db_core::prelude::*;
use mimalloc::MiMalloc;
use prompt::TaskQueue;
use rate_limiters::RateLimiters;
use reqwest::Certificate;
use routes::AppRouter;
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use server_config::get_cert;
use state::email_scanner::ActiveEmailProcessorMap;
use tokio::{signal, task::JoinHandle};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::state::email_client_map::{self, EmailClientMap};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub type TokenCounter = Arc<AtomicU64>;
pub type HttpClient = reqwest::Client;
pub type PubsubClient = Arc<google_cloud_pubsub::client::Client>;
pub type EmailClientCache = Arc<tokio::sync::RwLock<EmailClientMap>>;

#[derive(Clone, FromRef)]
struct ServerState {
    http_client: HttpClient,
    conn: DatabaseConnection,
    rate_limiters: RateLimiters,
    session_store: AuthSessionStore,
    pub task_queue: TaskQueue,
    pub email_client_cache: EmailClientCache,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL is not set in .env file");
    let mut db_options = ConnectOptions::new(db_url);
    db_options.sqlx_logging(false);

    let conn = Database::connect(db_options)
        .await
        .expect("Database connection failed");

    let cert = get_cert();
    let http_client = reqwest::ClientBuilder::new()
        .use_rustls_tls()
        .add_root_certificate(Certificate::from_pem(&cert)?)
        .build()?;
    let session_store = AuthSessionStore::new();
    let email_client_cache = {
        let map = email_client_map::EmailClientMap::builder()
            .with_http_client(http_client.clone())
            .with_connection(conn.clone())
            .build()
            .await
            .expect("Email client map failed to build!");

        Arc::new(tokio::sync::RwLock::new(map))
    };

    let rate_limiters = RateLimiters::from_env();
    let task_queue = TaskQueue::new();

    let state = ServerState {
        http_client,
        conn,
        rate_limiters,
        session_store,
        task_queue,
        email_client_cache,
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with(tracing_subscriber::fmt::Layer::default().with_ansi(false))
        .init();

    let router = AppRouter::create(state.clone());
    let email_processing_map = ActiveEmailProcessorMap::new(state.clone());

    let mut scheduler = JobScheduler::new()
        .await
        .expect("Failed to create scheduler");

    {
        let state_clone = state.clone();
        let map = email_processing_map.clone();
        scheduler
            .add(Job::new_one_shot_async(
                Duration::from_secs(1),
                move |uuid, l| {
                    create_processors_for_users(uuid, l, state_clone.clone(), map.clone())
                },
            )?)
            .await?;

        let queue = state.task_queue.clone();
        let map = email_processing_map.clone();
        scheduler
            .add(Job::new_one_shot(
                Duration::from_secs(2),
                move |_uuid, _l| {
                    state::tasks::run_email_queueing_loop(queue.clone(), map.clone());
                },
            )?)
            .await?;

        let queue = state.task_queue.clone();
        let map = email_processing_map.clone();
        scheduler
            .add(Job::new_one_shot(
                Duration::from_secs(5),
                move |_uuid, _l| {
                    state::tasks::run_categorization_loop(queue.clone(), map.clone());
                },
            )?)
            .await?;

        // Embedding processing loop - pulls from Background priority
        let queue = state.task_queue.clone();
        let http_client = state.http_client.clone();
        let conn = state.conn.clone();
        let rate_limiters = state.rate_limiters.clone();
        scheduler
            .add(Job::new_one_shot(
                Duration::from_secs(10),
                move |_uuid, _l| {
                    state::tasks::run_embedding_loop(
                        queue.clone(),
                        http_client.clone(),
                        conn.clone(),
                        rate_limiters.clone(),
                    );
                },
            )?)
            .await?;

        let state_clone = state.clone();
        let map = email_processing_map.clone();
        // Every 60 seconds, create processors for active users
        scheduler
            .add(Job::new_repeated_async(
                Duration::from_secs(60),
                move |uuid, l| {
                    create_processors_for_users(uuid, l, state_clone.clone(), map.clone())
                },
            )?)
            .await?;

        let http_client = state.http_client.clone();
        let conn = state.conn.clone();
        // Every 30 minutes, run auto email cleanup
        scheduler
            .add(Job::new_async("0 */30 * * * *", move |uuid, mut l| {
                let http_client = http_client.clone();
                let conn = conn.clone();
                Box::pin(async move {
                    tracing::info!("Running auto cleanup job {}", uuid);
                    match state::tasks::run_auto_email_cleanup(http_client, conn).await {
                        Ok(_) => {
                            tracing::info!("Auto cleanup job {} succeeded", uuid);
                        }
                        Err(e) => {
                            tracing::error!("Failed to run auto cleanup: {:?}", e);
                        }
                    }

                    let next_tick = l.next_tick_for_job(uuid).await;
                    if let Ok(Some(ts)) = next_tick {
                        tracing::info!("Next time for auto cleanup job is {:?}", ts)
                    }
                })
            })?)
            .await?;

        // Cleanup session storage
        let state_clone = state.clone();
        scheduler
            .add(Job::new_repeated(
                Duration::from_secs(3 * 60),
                move |_uuid, _lock| {
                    state_clone.session_store.clean_store();
                },
            )?)
            .await?;
    }

    scheduler.set_shutdown_handler(Box::new(move || {
        Box::pin(async move {
            tracing::info!("Shutting down scheduler");
        })
    }));

    let scanner_only = env::var("SCANNER_ONLY").is_ok_and(|v| v == "true");
    let server_only = env::var("SERVER_ONLY").is_ok_and(|v| v == "true");

    println!(
        "SCANNER_ONLY={:?}, SERVER_ONLY={:?}",
        env::var("SCANNER_ONLY"),
        env::var("SERVER_ONLY")
    );

    if server_only {
        println!("-------- RUNNING SERVER ONLY --------");
        run_server(router, scheduler).await.unwrap();
        return Ok(());
    }

    println!("Starting scheduler...");
    match scheduler.start().await {
        Ok(_) => {
            println!("-------- SCHEDULER STARTED --------");
        }
        Err(e) => {
            println!("Failed to start scheduler: {:?}", e);
        }
    }

    println!("Creating processing watch handle...");
    let processing_watch_handle = state::tasks::watch(
        state.task_queue.clone(),
        email_processing_map.clone(),
        state.rate_limiters.clone(),
    );
    if scanner_only {
        println!("-------- RUNNING SCANNER ONLY --------");
        let health_router = Router::new().route("/", get(|| async { "OK" }));
        let server_handle = run_server(health_router, scheduler);
        tokio::select! {
            _ = server_handle => {
                tracing::info!("Server shut down, exiting");
            }
            _ = processing_watch_handle => {
                tracing::info!("Processing watch ended");
            }
        }
        return Ok(());
    }

    let server_handle = run_server(router, scheduler);
    tokio::select! {
        _ = server_handle => {
            tracing::info!("Server shut down, exiting");
        }
        _ = processing_watch_handle => {
            tracing::info!("Processing watch ended");
        }
    }

    Ok(())
}

async fn shutdown_signal(mut scheduler: JobScheduler) {
    if env::var("NO_SHUTDOWN").unwrap_or("false".to_string()) == "true" {
        return;
    }

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            scheduler.shutdown().await.unwrap();
            println!("Cleanups done, shutting down");
            std::process::exit(0);

        },
        _ = terminate => {
            scheduler.shutdown().await.unwrap();
            println!("Cleanups done, shutting down");
            std::process::exit(0);
        },
    }
}

fn run_server(router: Router, scheduler: JobScheduler) -> JoinHandle<()> {
    tokio::spawn(async {
        // Start the server
        let port = env::var("PORT").unwrap_or("5006".to_string());
        tracing::info!("Mailclerk server running on http://0.0.0.0:{}", port);
        // check config
        println!("{}", *server_config::cfg);

        // run it with hyper
        let addr = SocketAddr::from(([0, 0, 0, 0], port.parse::<u16>().unwrap()));
        tracing::debug!("listening on {addr}");
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_signal(scheduler))
        .await
        .unwrap();
    })
}

fn create_processors_for_users(
    uuid: Uuid,
    mut l: JobScheduler,
    state: ServerState,
    map: ActiveEmailProcessorMap,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
    let state = state.clone();
    let map = map.clone();
    tracing::info!("Job: {}\n Creating processors for active users...", uuid);
    Box::pin(async move {
        match state::tasks::add_users_to_processing(state, map.clone()).await {
            Ok(_) => {
                tracing::info!("Processor Creation Job {} succeeded", uuid);
            }
            Err(e) => {
                tracing::error!("Job failed: {:?}", e);
            }
        }

        let next_tick = l.next_tick_for_job(uuid).await;
        if let Ok(Some(ts)) = next_tick {
            tracing::info!("Next time for processor creation job is {:?}", ts)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    #[cfg(feature = "integration")]
    use tokio::net::TcpListener;

    pub struct TestServer {
        pub addr: SocketAddr,
        pub state: ServerState,
        shutdown_tx: tokio::sync::oneshot::Sender<()>,
    }

    impl TestServer {
        pub fn url(&self) -> String {
            format!("http://{}", self.addr)
        }

        pub async fn shutdown(self) {
            let _ = self.shutdown_tx.send(());
        }
    }

    #[cfg(feature = "integration")]
    pub async fn setup() -> anyhow::Result<TestServer> {
        dotenvy::dotenv().ok();

        let db_url = env::var("DATABASE_URL").expect("DATABASE_URL is not set in .env file");
        let mut db_options = ConnectOptions::new(db_url);
        db_options.sqlx_logging(false);

        let conn = Database::connect(db_options)
            .await
            .expect("Database connection failed");

        let cert = server_config::get_cert();
        let http_client = reqwest::ClientBuilder::new()
            .use_rustls_tls()
            .add_root_certificate(Certificate::from_pem(&cert)?)
            .build()?;

        let session_store = AuthSessionStore::new();

        let email_client_cache = {
            let map = email_client_map::EmailClientMap::builder()
                .with_http_client(http_client.clone())
                .with_connection(conn.clone())
                .build()
                .await
                .expect("Email client map failed to build!");

            Arc::new(tokio::sync::RwLock::new(map))
        };

        let state = ServerState {
            http_client,
            conn,
            rate_limiters: RateLimiters::from_env(),
            session_store,
            task_queue: TaskQueue::new(),
            email_client_cache,
        };

        let router = AppRouter::create(state.clone());

        // Bind to port 0 to get a random available port
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
        });

        Ok(TestServer {
            addr,
            state,
            shutdown_tx,
        })
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn test_server_starts() {
        let server = setup().await.expect("Failed to setup test server");
        assert!(!server.url().is_empty());
        server.shutdown().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn test_email_categorization() {
        use crate::email::client::{EmailClient, MessageListOptions};
        use crate::model::user::UserCtrl;

        let server = setup().await.expect("Failed to setup test server");

        let user = UserCtrl::get_with_account_access_by_email(
            &server.state.conn,
            "mpgrospamacc@gmail.com",
        )
        .await
        .expect("Failed to get user");

        let email_client = EmailClient::new(
            server.state.http_client.clone(),
            server.state.conn.clone(),
            user,
        )
        .await
        .expect("Failed to create email client");

        let email_ids = email_client
            .get_message_list(MessageListOptions {
                ..Default::default()
            })
            .await
            .expect("Failed to get message list");

        let first_id = email_ids
            .messages
            .as_ref()
            .expect("No messages found")
            .first()
            .expect("Message list is empty")
            .id
            .as_ref()
            .expect("Message has no ID");

        let email = email_client
            .get_simplified_message(first_id)
            .await
            .expect("Failed to get simplified message");

        println!("Email is: {:?}", email.to_string());

        let result =
            embed::categorize_email_with_confidence(&server.state.http_client, &email.to_string())
                .await;

        println!("Response is: {:?}", result);

        assert!(result.is_ok(), "Categorization failed: {:?}", result.err());

        server.shutdown().await;
    }
}
