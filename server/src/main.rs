#![allow(dead_code)]
#[macro_use]
mod macros;

mod auth;
mod cron_time_utils;
mod db_core;
mod email;
mod error;
mod model;
mod notify;
mod prompt;
mod rate_limiters;
mod request_tracing;
mod routes;
mod server_config;
mod testing;

use std::{
    env,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use auth::session_store::AuthSessionStore;
use axum::{extract::FromRef, Router};
use db_core::prelude::*;
use email::active_email_processors::ActiveEmailProcessorMap;
use futures::future::join_all;
use mimalloc::MiMalloc;
use prompt::priority_queue::PromptPriorityQueue;
use rate_limiters::RateLimiters;
use reqwest::Certificate;
use routes::AppRouter;
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use server_config::get_cert;
use tokio::{signal, task::JoinHandle};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    email::client::{EmailClient, MessageListOptions},
    model::user::UserCtrl,
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub type TokenCounter = Arc<AtomicU64>;
pub type HttpClient = reqwest::Client;
pub type PubsubClient = Arc<google_cloud_pubsub::client::Client>;

#[derive(Clone, FromRef)]
struct ServerState {
    http_client: HttpClient,
    conn: DatabaseConnection,
    rate_limiters: RateLimiters,
    session_store: AuthSessionStore,
    pub priority_queue: PromptPriorityQueue,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var("RUST_LOG", "info");
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

    let state = ServerState {
        http_client,
        conn,
        rate_limiters: RateLimiters::from_env(),
        session_store,
        priority_queue: PromptPriorityQueue::new(),
    };

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_env("RUST_LOG"))
        .with(tracing_subscriber::fmt::Layer::default().with_ansi(false))
        .init();

    let router = AppRouter::create(state.clone());
    let email_processing_map = ActiveEmailProcessorMap::new(state.clone());
    let processing_watch_handle = email::tasks::watch(
        state.priority_queue.clone(),
        email_processing_map.clone(),
        state.rate_limiters.clone(),
    );

    if env::var("TEST_ONLY").is_ok_and(|x| x == "true") {
        println!("-----TEST ONLY-----");
        // let user =
        //     UserCtrl::get_with_account_access_by_email(&state.conn, "mpgrospamacc@gmail.com")
        //         .await?;

        // let email_client =
        //     EmailClient::new(state.http_client.clone(), state.conn.clone(), user).await?;
        // let email_ids = email_client
        //     .get_message_list(MessageListOptions {
        //         ..Default::default()
        //     })
        //     .await?;

        // let first_id = &email_ids
        //     .messages
        //     .as_ref()
        //     .map(|x| x.first().as_ref().and_then(|x| x.id.as_ref()).unwrap())
        //     .unwrap();

        // let email = email_client.get_parsed_message(first_id).await?;

        // state.http_client.post("https://api.mistral.ai/v1/embeddings").bearer_auth(token)

        // println!("Email is: {:?}", email);

        return Ok(());
    }

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

        let queue = state.priority_queue.clone();
        let map = email_processing_map.clone();
        scheduler
            .add(Job::new_one_shot(
                Duration::from_secs(2),
                move |_uuid, _l| {
                    email::tasks::run_email_processing_loop(queue.clone(), map.clone());
                },
            )?)
            .await?;

        let queue = state.priority_queue.clone();
        let map = email_processing_map.clone();
        scheduler
            .add(Job::new_one_shot(
                chrono::Duration::minutes(30).to_std().unwrap(),
                move |_uuid, _l| {
                    email::tasks::run_processor_cleanup_loop(queue.clone(), map.clone());
                },
            )?)
            .await?;

        let state_clone = state.clone();
        let map = email_processing_map.clone();
        // Start of every minute, create processors for active users
        scheduler
            .add(Job::new_async("0 * * * * *", move |uuid, l| {
                create_processors_for_users(uuid, l, state_clone.clone(), map.clone())
            })?)
            .await?;

        let http_client = state.http_client.clone();
        let conn = state.conn.clone();
        // Start of every hour, run auto email cleanup
        scheduler
            .add(Job::new_async("0 0 * * * *", move |uuid, mut l| {
                let http_client = http_client.clone();
                let conn = conn.clone();
                Box::pin(async move {
                    tracing::info!("Running auto cleanup job {}", uuid);
                    match email::tasks::run_auto_email_cleanup(http_client, conn).await {
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

    scheduler.shutdown_on_ctrl_c();

    scheduler.set_shutdown_handler(Box::new(move || {
        Box::pin(async move {
            tracing::info!("Shutting down scheduler");
        })
    }));

    if env::var("SERVER_ONLY").is_ok_and(|v| v == "true") {
        tracing::info!("-------- RUNNING SERVER ONLY --------");
        // Handle Ctrl+C
        join_all([run_server(router, scheduler)]).await;
        return Ok(());
    }

    match scheduler.start().await {
        Ok(_) => {
            tracing::info!("Scheduler started");
        }
        Err(e) => {
            tracing::error!("Failed to start scheduler: {:?}", e);
        }
    }

    for join in join_all(vec![
        run_server(router, scheduler),
        // inbox_subscription_handle,
        processing_watch_handle,
    ])
    .await
    {
        join.unwrap();
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
        match email::tasks::add_users_to_processing(state, map.clone()).await {
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
