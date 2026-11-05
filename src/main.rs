#![recursion_limit = "256"]

mod plex_tls;

use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocketUpgrade},
        Form, FromRequestParts, Path, Query, RawForm, RawQuery, State,
    },
    http::{header, HeaderMap, HeaderValue, Method, Request, StatusCode, Uri},
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{any, delete, get, post, put},
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use clap::Parser;
use ed25519_dalek::{Signer, SigningKey};
use isolang::Language;
use plex_tls::PlexTls;
use rand_core::{OsRng, RngCore};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    io::{self, Write},
    net::SocketAddr,
    path::{Component, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock, RwLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
};
use tokio_util::io::ReaderStream;
use tower_http::trace::TraceLayer;
use tracing::{info, warn, Span};

const POSTER_CARD_MIN_WIDTH: usize = 140;
const PLEX_DEFAULT_AUDIO_LANGUAGE: &str = "en";
const USER_ID: &str = "mplex-user";

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(long, value_name = "DIRECTORY")]
    config: Option<PathBuf>,
    #[arg(long)]
    bind: Option<SocketAddr>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Config {
    #[serde(default = "default_bind")]
    bind: String,
    #[serde(default)]
    libraries: Vec<ConfigLibrary>,
    #[serde(default = "default_metadata_storage")]
    metadata_storage: String,
    #[serde(default)]
    tmdb_bearer_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ConfigLibrary {
    name: String,
    kind: String,
    #[serde(default)]
    folders: Vec<PathBuf>,
    #[serde(default = "default_true")]
    show_recently_added: bool,
    #[serde(default = "default_true")]
    show_continue_watching: bool,
    #[serde(default = "default_true")]
    scan_enabled: bool,
    #[serde(default = "default_scan_interval_value")]
    scan_interval_value: u32,
    #[serde(default = "default_scan_interval_unit")]
    scan_interval_unit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plex_agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plex_scanner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plex_language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata_agent_provider_group_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plex_preferences: Option<BTreeMap<String, String>>,
}

fn default_bind() -> String {
    "0.0.0.0:32400".to_owned()
}
fn default_metadata_storage() -> String {
    "database".to_owned()
}
fn default_true() -> bool {
    true
}

fn default_scan_interval_value() -> u32 {
    5
}

fn default_scan_interval_unit() -> String {
    "minutes".to_owned()
}

fn plex_machine_identifier(seed: &str) -> String {
    let digest = Sha256::digest(seed.as_bytes());
    digest[..20]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn plex_library_uuid(state: &AppState, library_name: &str, section: usize) -> String {
    let (stable_id, _, _) = library_publication_state(state, library_name, section);
    plex_library_uuid_for_id(&state.machine_identifier, stable_id)
}

fn plex_library_uuid_for_id(machine_identifier: &str, stable_id: i64) -> String {
    let digest = Sha256::digest(format!("{machine_identifier}:library:{stable_id}").as_bytes());
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        digest[0],
        digest[1],
        digest[2],
        digest[3],
        digest[4],
        digest[5],
        digest[6],
        digest[7],
        digest[8],
        digest[9],
        digest[10],
        digest[11],
        digest[12],
        digest[13],
        digest[14],
        digest[15]
    )
}

#[derive(Clone)]
struct AppState {
    config: Arc<Mutex<Config>>,
    config_path: Arc<PathBuf>,
    db: Arc<Mutex<Connection>>,
    server_name: Arc<RwLock<String>>,
    machine_identifier: Arc<str>,
    root: Arc<PathBuf>,
    media: Arc<RwLock<Arc<Vec<Media>>>>,
    inventory_scan_lock: Arc<tokio::sync::Mutex<()>>,
    plex_notifications: tokio::sync::broadcast::Sender<String>,
    media_info: Arc<Mutex<HashMap<String, serde_json::Value>>>,
    plex_stream_selections: Arc<Mutex<HashMap<u64, PlexStreamSelection>>>,
    plex_play_queues: Arc<Mutex<HashMap<u64, serde_json::Value>>>,
    next_plex_play_queue_id: Arc<AtomicU64>,
    plex_tls: PlexTls,
}

#[derive(Clone)]
struct Media {
    id: String,
    title: String,
    path: PathBuf,
    size: u64,
    container: String,
    mime: String,
    metadata_revision: u64,
}

fn media_snapshot(state: &AppState) -> Arc<Vec<Media>> {
    state
        .media
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

struct LocalSecondTimer;

impl tracing_subscriber::fmt::time::FormatTime for LocalSecondTimer {
    fn format_time(
        &self,
        writer: &mut tracing_subscriber::fmt::format::Writer<'_>,
    ) -> std::fmt::Result {
        write!(
            writer,
            "{}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S %:z")
        )
    }
}

const MAX_LOG_LINES: usize = 1000;
const MAX_LOG_LINE_BYTES: usize = 4096;
static LOG_BUFFER: OnceLock<Mutex<VecDeque<String>>> = OnceLock::new();

#[derive(Clone)]
struct LogBufferMakeWriter;

struct LogBufferWriter {
    pending: Vec<u8>,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBufferMakeWriter {
    type Writer = LogBufferWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogBufferWriter {
            pending: Vec::new(),
        }
    }
}

impl Write for LogBufferWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        io::stdout().lock().write_all(bytes)?;
        self.pending.extend_from_slice(bytes);
        while let Some(newline) = self.pending.iter().position(|byte| *byte == b'\n') {
            let line = self.pending.drain(..=newline).collect::<Vec<_>>();
            push_log_line(String::from_utf8_lossy(&line[..line.len() - 1]).into_owned());
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stdout().flush()?;
        if !self.pending.is_empty() {
            push_log_line(String::from_utf8_lossy(&std::mem::take(&mut self.pending)).into_owned());
        }
        Ok(())
    }
}

impl Drop for LogBufferWriter {
    fn drop(&mut self) {
        if !self.pending.is_empty() {
            push_log_line(String::from_utf8_lossy(&std::mem::take(&mut self.pending)).into_owned());
        }
    }
}

fn strip_ansi_sequences(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character != '\u{1b}' {
            output.push(character);
            continue;
        }
        match chars.next() {
            Some('[') => {
                for sequence_character in chars.by_ref() {
                    if ('@'..='~').contains(&sequence_character) {
                        break;
                    }
                }
            }
            Some(']') => {
                while let Some(sequence_character) = chars.next() {
                    if sequence_character == '\u{7}'
                        || (sequence_character == '\u{1b}' && chars.next() == Some('\\'))
                    {
                        break;
                    }
                }
            }
            Some(_) => {}
            None => break,
        }
    }
    output
}

fn push_log_line(line: String) {
    let line = strip_ansi_sequences(&line)
        .chars()
        .take(MAX_LOG_LINE_BYTES)
        .collect::<String>();
    let mut logs = LOG_BUFFER
        .get_or_init(|| Mutex::new(VecDeque::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if logs.len() == MAX_LOG_LINES {
        logs.pop_front();
    }
    logs.push_back(line);
}

fn recent_log_lines() -> Vec<String> {
    LOG_BUFFER
        .get_or_init(|| Mutex::new(VecDeque::new()))
        .lock()
        .map(|logs| logs.iter().cloned().collect())
        .unwrap_or_default()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_ansi(false)
        .with_timer(LocalSecondTimer)
        .with_writer(LogBufferMakeWriter)
        .init();
    let args = Args::parse();
    if run_server(args).await? == RunOutcome::Reload {
        restart_process()?;
    }
    Ok(())
}

#[derive(PartialEq)]
enum RunOutcome {
    Reload,
    Shutdown,
}

fn config_path_in_dir(config_dir: &std::path::Path) -> Result<PathBuf, std::io::Error> {
    let new_path = config_dir.join("config.toml");
    let old_path = config_dir.join("mplex.toml");
    if !new_path.exists() && old_path.exists() {
        std::fs::rename(old_path, &new_path)?;
    }
    Ok(new_path)
}

async fn run_server(args: Args) -> Result<RunOutcome, Box<dyn std::error::Error>> {
    let config_arg = args.config.unwrap_or_else(|| PathBuf::from("config"));
    let config_path = if config_arg
        .extension()
        .is_some_and(|extension| extension == "toml")
    {
        if config_arg.is_absolute() {
            config_arg
        } else {
            std::env::current_dir()?.join(config_arg)
        }
    } else {
        let config_dir = if config_arg.is_absolute() {
            config_arg
        } else {
            std::env::current_dir()?.join(config_arg)
        };
        config_path_in_dir(&config_dir)?
    };
    let mut config = load_config(&config_path)?;
    if let Some(bind) = args.bind {
        config.bind = bind.to_string();
    }
    save_config(&config_path, &config)?;
    let (db, machine_identifier) = open_database(&config_path)?;
    let server_name = db
        .query_row(
            "SELECT server_name FROM server_identity WHERE id = 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap_or_else(|_| "harbringer".to_owned());
    migrate_library_model(&db)?;
    sync_config_libraries(&db, &config)?;
    let root = config
        .libraries
        .iter()
        .flat_map(|library| library.folders.iter())
        .find_map(|path| std::fs::canonicalize(path).ok())
        .unwrap_or_else(|| {
            config_path
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .to_path_buf()
        });
    let mut media = Vec::new();
    for (library_index, library) in config.libraries.iter().enumerate() {
        for folder in &library.folders {
            let configured_folder = folder.clone();
            let Ok(canonical_folder) = tokio::fs::canonicalize(folder).await else {
                continue;
            };
            let mut library_media = scan_media(&canonical_folder).await?;
            assign_media_ids(
                &db,
                library_index + 1,
                &canonical_folder,
                &configured_folder,
                &mut library_media,
            )?;
            for item in &mut library_media {
                if let Ok(relative) = item.path.strip_prefix(&canonical_folder) {
                    item.path = configured_folder.join(relative);
                }
            }
            persist_media_records(&db, &configured_folder, &library_media)?;
            media.extend(library_media);
        }
    }
    for item in &media {
        info!(id = %item.id, file = %item.path.display(), size = item.size, "serving media");
    }

    let gdm_machine_identifier = machine_identifier.clone();
    let cert_dir = config_path
        .parent()
        .unwrap_or(std::path::Path::new("config"))
        .join("certs");
    let plex_tls = PlexTls::load(cert_dir).map_err(std::io::Error::other)?;
    let (plex_notifications, _) = tokio::sync::broadcast::channel(64);
    let state = AppState {
        config: Arc::new(Mutex::new(config.clone())),
        config_path: Arc::new(config_path),
        db: Arc::new(Mutex::new(db)),
        server_name: Arc::new(RwLock::new(server_name)),
        machine_identifier: Arc::from(machine_identifier),
        root: Arc::new(root),
        media: Arc::new(RwLock::new(Arc::new(media))),
        inventory_scan_lock: Arc::new(tokio::sync::Mutex::new(())),
        plex_notifications,
        media_info: Arc::new(Mutex::new(HashMap::new())),
        plex_stream_selections: Arc::new(Mutex::new(HashMap::new())),
        plex_play_queues: Arc::new(Mutex::new(HashMap::new())),
        next_plex_play_queue_id: Arc::new(AtomicU64::new(1)),
        plex_tls,
    };
    let scan_state = state.clone();
    let inventory_scan_task = tokio::spawn(library_scan_scheduler(scan_state));
    let server_tls = state.plex_tls.clone();
    let publication_state = state.clone();
    let publication_task = tokio::spawn(async move {
        let server_token = publication_state.db.lock().ok().and_then(|connection| {
            connection
                .query_row(
                    "SELECT plex_account_token FROM server_identity WHERE id = 1 AND claimed = 1",
                    [],
                    |row| row.get::<_, Option<String>>(0),
                )
                .ok()
                .flatten()
        });
        if let Some(server_token) = server_token {
            let client = reqwest::Client::new();
            if let Err(error) = publication_state
                .plex_tls
                .ensure_certificate(
                    &client,
                    &publication_state.machine_identifier,
                    &server_token,
                )
                .await
            {
                warn!(error = %error, "could not ensure Plex TLS certificate at startup");
            }
            if let Err(error) =
                publish_plex_connections(&client, &publication_state, &server_token).await
            {
                warn!(error = %error, "could not refresh Plex connection URLs at startup");
            } else {
                run_plex_presence(&client, &publication_state, &server_token).await;
            }
        }
    });
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind(config.bind.parse::<SocketAddr>()?).await?;
    let address = listener.local_addr()?;
    let gdm_task = tokio::spawn(run_gdm_responder(
        gdm_machine_identifier,
        address.port(),
        state.server_name.clone(),
    ));
    let host = if address.ip().is_unspecified() {
        "127.0.0.1".to_owned()
    } else {
        format!("{}", address.ip())
    };
    print!("\n");
    print_startup_banner();
    println!(
        "harbringer running on port {}\nWeb client: http://{}:{}/\n",
        address.port(),
        host,
        address.port()
    );
    #[cfg(unix)]
    let mut hangup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = server_tls.serve_until(listener, app, async move {
        let _ = shutdown_rx.await;
    });
    tokio::pin!(server);
    #[cfg(unix)]
    let outcome = tokio::select! {
        result = &mut server => {
            result?;
            RunOutcome::Shutdown
        }
        _ = hangup.recv() => {
            info!("SIGHUP received; gracefully reloading Harbringer");
            RunOutcome::Reload
        }
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown signal received");
            RunOutcome::Shutdown
        }
    };
    #[cfg(not(unix))]
    let outcome = tokio::select! {
        result = &mut server => {
            result?;
            RunOutcome::Shutdown
        }
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown signal received");
            RunOutcome::Shutdown
        }
    };
    let _ = shutdown_tx.send(());
    if tokio::time::timeout(Duration::from_secs(5), &mut server)
        .await
        .is_err()
    {
        warn!("timed out waiting for active connections to close during shutdown");
    }
    gdm_task.abort();
    publication_task.abort();
    inventory_scan_task.abort();
    Ok(outcome)
}

#[cfg(unix)]
fn restart_process() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::process::CommandExt;

    let executable = std::env::current_exe()?;
    let error = Command::new(executable)
        .args(std::env::args_os().skip(1))
        .exec();
    Err(error.into())
}

#[cfg(not(unix))]
fn restart_process() -> Result<(), Box<dyn std::error::Error>> {
    Err("SIGHUP process reload is only available on Unix".into())
}

fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/", get(root_handler))
        .route("/web/stream", any(web_stream))
        .route("/web/progress", get(web_progress).post(update_web_progress))
        .route("/web/watch-action", post(web_watch_action))
        .route(
            "/web/subtitle-selection",
            get(web_subtitle_selection).post(update_web_subtitle_selection),
        )
        .route("/web/subtitle", get(web_subtitle))
        .route("/web/artwork", get(web_artwork))
        .route("/web/info", get(web_info))
        .route("/web/tv-match/search", get(tv_match_search))
        .route("/web/tv-match/select", post(tv_match_select))
        .route("/settings", get(settings_page))
        .route("/settings/libraries", post(create_library))
        .route("/settings/libraries/:id/rename", post(rename_library))
        .route("/settings/libraries/:id/save", post(save_library_settings))

        .route("/settings/libraries/:id/delete", post(delete_library))
        .route("/settings/folders", post(add_library_folder))
        .route("/settings/folders/remove", post(remove_library_folder))
        .route("/settings/libraries/display", post(update_library_display))
        .route("/settings/libraries/scan-settings", post(update_library_scan_settings))
        .route("/settings/scan", post(scan_all_libraries))
        .route("/settings/preferences", post(update_preferences))
        .route("/settings/server-name", post(update_server_name))
        .route("/settings/plex/start", get(start_plex_auth))
        .route("/settings/plex/status", get(plex_auth_status))
        .route("/settings/plex/callback", get(plex_auth_callback))
        .route("/settings/plex/disconnect", post(disconnect_plex))
        .route("/settings/libraries/:id/scan", post(scan_library_metadata))
        .route("/settings/libraries/:id/scan-files", post(scan_library_inventory_handler))
        .route("/settings/media/scan", post(scan_media_metadata))
        .route("/settings/browse", get(settings_browse))
        .route("/identity", get(plex_identity))
        .route("/myplex/account", get(plex_myplex_account))
        .route("/accounts", get(plex_accounts))
        .route("/devices", get(plex_devices))
        .route("/accounts/:id", get(plex_account))
        .route("/activities", get(plex_activities))
        .route("/status/sessions", get(plex_sessions))
        .route("/status/sessions/history/all", get(plex_session_history))
        .route("/servers", get(plex_servers))
        .route("/media/providers", get(plex_media_providers))
        .route("/services/browse", get(plex_service_browse_root))
        .route("/services/browse/:path", get(plex_service_browse_path))
        .route("/security/resources", get(plex_security_resources))
        .route("/hubs", get(plex_global_hubs))
        .route("/hubs/promoted", get(plex_global_hubs))
        .route("/hubs/continueWatching", get(plex_continue_watching_hub))
        .route("/hubs/search", get(plex_hub_search))
        .route(
            "/actions/removeFromContinueWatching",
            put(plex_remove_from_continue_watching),
        )
        .route("/hubs/sections/:section", get(plex_section_hubs))
        .route("/hubs/metadata/:id/related", get(plex_related_hubs))
        .route(
            "/hubs/sections/:section/:hub",
            get(plex_section_hub),
        )
        .route("/library/sections", get(plex_sections).post(plex_create_section))
        .route("/library/sections/prefs", get(plex_library_section_prefs))
        .route("/:root/plugins/:plugin/prefs", get(plex_agent_prefs))
        .route(
            "/library/sections/all",
            get(plex_sections).post(plex_create_section_official),
        )


        .route("/system/scanners/:section", get(plex_system_scanners))
        .route("/system/agents", get(plex_system_agents))

        .route(
            "/library/sections/:section",
            get(plex_section)
                .put(plex_update_section)
                .delete(plex_delete_section),
        )
        .route(
            "/library/sections/:section/refresh",
            get(plex_section_refresh).post(plex_section_refresh),
        )
        .route(
            "/library/sections/:section/collections",
            get(plex_section_collections),
        )
        .route(
            "/library/collections",
            get(plex_collections).post(plex_create_collection),
        )
        .route(
            "/library/collections/:id/items",
            get(plex_collection_items).put(plex_add_collection_item),
        )
        .route(
            "/library/collections/:id/items/:item_id",
            put(plex_remove_collection_item),
        )
        .route(
            "/library/sections/:section/prefs",
            get(plex_section_prefs).put(plex_update_section_prefs),
        )
        .route("/library/sections/:section/sorts", get(plex_section_sorts))
        .route("/library/sections/:section/filters", get(plex_section_filters))
        .route("/library/sections/:section/firstCharacter", get(plex_section_first_characters))
        .route("/library/sections/:section/firstCharacter/:letter", get(plex_section_first_character_items))
        .route("/library/sections/:section/firstCharacters", get(plex_section_first_characters))
        .route("/library/search", get(plex_library_search))
        .route("/library/all", get(plex_library_all))
        .route("/library/sections/:section/all", get(plex_all))
        .route("/library/metadata/:id", get(plex_item))
        .route("/library/metadata/:id/children", get(plex_item))
        .route("/library/metadata/:id/related", get(plex_related_hubs))
        .route("/library/metadata/:id/extras", get(plex_extras))
        .route("/library/metadata/:id/media/:part", get(plex_part))
        .route("/library/parts/:id", put(plex_set_streams))
        .route("/library/parts/:id/*path", get(plex_part_file))
        .route("/library/streams/:stream", get(plex_stream))
        .route("/playQueues", post(plex_create_play_queue))
        .route("/playQueues/:id", get(plex_get_play_queue))
        .route("/playlists", get(plex_playlists).post(plex_create_playlist))
        .route(
            "/playlists/:id",
            get(plex_get_playlist)
                .put(plex_update_playlist)
                .delete(plex_delete_playlist),
        )
        .route(
            "/playlists/:id/items",
            get(plex_playlist_items)
                .put(plex_add_playlist_item)
                .delete(plex_clear_playlist_items),
        )
        .route(
            "/playlists/:id/items/:item_id",
            delete(plex_delete_playlist_item),
        )
        .route(
            "/playlists/:id/items/:item_id/move",
            put(plex_move_playlist_item),
        )
        .route("/library/metadata/:id/thumb", get(plex_thumb))
        .route("/library/metadata/:id/art", get(plex_art))
        .route("/media/:id", any(stream_media))
        .fallback(log_not_found)
        .layer(middleware::from_fn(plex_cors))
        .layer(
            TraceLayer::new_for_http()
                .on_request(|request: &Request<Body>, _span: &Span| {
                    let remote_ip = request
                        .extensions()
                        .get::<SocketAddr>()
                        .map(|addr| addr.ip().to_string())
                        .unwrap_or_else(|| "unknown".to_owned());
                    let plex_product = request
                        .headers()
                        .get("X-Plex-Product")
                        .and_then(|value| value.to_str().ok())
                        .or_else(|| plex_query_value(request.uri(), "X-Plex-Product"));
                    let client_profile = request
                        .headers()
                        .get("X-Plex-Client-Profile-Name")
                        .and_then(|value| value.to_str().ok())
                        .or_else(|| {
                            plex_query_value(request.uri(), "X-Plex-Client-Profile-Name")
                        });
                    let client_platform = request
                        .headers()
                        .get("X-Plex-Platform")
                        .and_then(|value| value.to_str().ok())
                        .or_else(|| plex_query_value(request.uri(), "X-Plex-Platform"));
                    info!(
                        "event=http_request remote_ip={} method={} path={} accept={:?} plex_product={:?} client_profile={:?} client_platform={:?} token_present={}",
                        remote_ip,
                        request.method(),
                        request.uri().path(),
                        request.headers().get(header::ACCEPT),
                        plex_product,
                        client_profile,
                        client_platform,
                        plex_request_token(request.headers(), request.uri()).is_some()
                    );
                })
                .on_response(|response: &Response, latency: Duration, _span: &Span| {
                    info!(
                        "event=http_response status={} latency_ms={}",
                        response.status(),
                        latency.as_millis()
                    );
                }),
        )
        .with_state(state)
}

async fn run_gdm_responder(
    machine_identifier: String,
    port: u16,
    configured_name: Arc<RwLock<String>>,
) {
    let multicast = std::net::Ipv4Addr::new(239, 0, 0, 250);
    for gdm_port in [32410, 32412, 32413, 32414] {
        let socket = match std::net::UdpSocket::bind((std::net::Ipv4Addr::UNSPECIFIED, gdm_port)) {
            Ok(socket) => socket,
            Err(error) => {
                warn!(%error, gdm_port, "Plex GDM discovery port unavailable");
                continue;
            }
        };
        if let Err(error) = socket.join_multicast_v4(&multicast, &std::net::Ipv4Addr::UNSPECIFIED) {
            warn!(%error, gdm_port, "could not join Plex GDM multicast group");
        }
        if let Err(error) = socket.set_nonblocking(true) {
            warn!(%error, gdm_port, "could not configure Plex GDM socket");
            continue;
        }
        let Ok(socket) = tokio::net::UdpSocket::from_std(socket) else {
            warn!(gdm_port, "could not hand Plex GDM socket to Tokio");
            continue;
        };
        let configured_name = configured_name.clone();
        let machine_identifier = machine_identifier.clone();
        tokio::spawn(async move {
            let mut buffer = [0u8; 2048];
            loop {
                let Ok((size, peer)) = socket.recv_from(&mut buffer).await else {
                    return;
                };
                if buffer[..size].starts_with(b"M-SEARCH * HTTP/1.") {
                    let name = configured_name
                        .read()
                        .map(|name| name.clone())
                        .unwrap_or_else(|_| "harbringer".to_owned());
                    let response = gdm_message("HTTP/1.0 200 OK", &machine_identifier, port, &name);
                    let _ = socket.send_to(response.as_bytes(), peer).await;
                }
            }
        });
    }

    let name = configured_name
        .read()
        .map(|name| name.clone())
        .unwrap_or_else(|_| "harbringer".to_owned());
    let announcement = gdm_message("HELLO", &machine_identifier, port, &name);
    let announcement_socket = std::net::UdpSocket::bind((std::net::Ipv4Addr::UNSPECIFIED, 0));
    if let Ok(announcement_socket) = announcement_socket {
        let _ = announcement_socket.send_to(announcement.as_bytes(), (multicast, 32413));
    }
}

fn gdm_message(prefix: &str, machine_identifier: &str, port: u16, name: &str) -> String {
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    format!(
        "{prefix}\r\nContent-Type: plex/media-server\r\nResource-Identifier: {machine_identifier}\r\nName: {name}\r\nPort: {port}\r\nUpdated-At: {updated_at}\r\nVersion: 1.43.4.10903-e5521bd8c\r\n\r\n"
    )
}

fn print_startup_banner() {
    println!(
        r#"
|              |         o
|---.,---.,---.|---.,---..,---.,---.,---.,---.
|   |,---||    |   ||    ||   ||   ||---'|
`   '`---^`    `---'`    ``   '`---|`---'`
                               `---'`
"#
    );
}

async fn scan_media(dir: &PathBuf) -> Result<Vec<Media>, Box<dyn std::error::Error>> {
    let mut directories = vec![dir.clone()];
    let mut paths = Vec::new();
    while let Some(directory) = directories.pop() {
        let mut entries = tokio::fs::read_dir(&directory).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if is_hidden_path(&path) {
                continue;
            }
            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                directories.push(path);
            } else if file_type.is_file() && media_type(&path).is_some() {
                paths.push(path);
            }
        }
    }
    paths.sort_by_key(|path| path.file_name().map(|name| name.to_ascii_lowercase()));

    let metadata_revision = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    let mut result = Vec::with_capacity(paths.len());
    for (index, path) in paths.into_iter().enumerate() {
        let metadata = tokio::fs::metadata(&path).await?;
        let (container, mime) = media_type(&path).expect("media type was checked during scan");
        let title = path
            .file_stem()
            .and_then(|name| name.to_str())
            .unwrap_or("Video")
            .to_owned();
        result.push(Media {
            id: (index + 1).to_string(),
            title,
            path,
            size: metadata.len(),
            container: container.to_owned(),
            mime: mime.to_owned(),
            metadata_revision,
        });
    }
    Ok(result)
}

fn media_type(path: &PathBuf) -> Option<(&'static str, &'static str)> {
    match path
        .extension()
        .and_then(|x| x.to_str())
        .map(|x| x.to_ascii_lowercase())
        .as_deref()
    {
        Some("mp4") | Some("m4v") => Some(("mp4", "video/mp4")),
        Some("mkv") => Some(("mkv", "video/x-matroska")),
        Some("webm") => Some(("webm", "video/webm")),
        _ => None,
    }
}

fn find_media(state: &AppState, id: &str) -> Option<Media> {
    let public_rating_key = id.parse::<u64>().ok();
    media_snapshot(state)
        .iter()
        .find(|item| item.id == id || public_rating_key == Some(plex_rating_key(item)))
        .cloned()
}

fn persist_media_records(
    connection: &Connection,
    root: &std::path::Path,
    media: &[Media],
) -> Result<(), rusqlite::Error> {
    let transaction = connection.unchecked_transaction()?;
    for item in media {
        let path = item
            .path
            .strip_prefix(root)
            .unwrap_or(&item.path)
            .to_string_lossy();
        transaction.execute(
            "INSERT INTO media (media_key, path, title) VALUES (?1, ?2, ?3)
             ON CONFLICT(media_key) DO UPDATE SET path = excluded.path, title = excluded.title",
            (&item.id, path.as_ref(), &item.title),
        )?;
    }
    transaction.commit()
}

fn assign_media_ids(
    connection: &Connection,
    library_section: usize,
    canonical_root: &std::path::Path,
    configured_root: &std::path::Path,
    media: &mut [Media],
) -> Result<(), rusqlite::Error> {
    const MAX_ITEM_ID: u64 = (1 << 20) - 1;

    let transaction = connection.unchecked_transaction()?;
    let section = library_section as i64;
    let id_prefix = format!("{library_section}-");
    let mut identity_by_path = HashMap::<String, String>::new();
    let mut legacy_by_path = HashMap::<String, Vec<String>>::new();
    let mut reserved_keys = std::collections::HashSet::new();
    let mut assigned_keys = std::collections::HashSet::new();
    let mut next_id = 1_u64;
    {
        let mut statement = transaction.prepare(
            "SELECT canonical_path, media_key FROM media_identity WHERE library_section = ?1",
        )?;
        let rows = statement.query_map([section], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (path, media_key) = row?;
            if let Some(item_id) = media_key
                .strip_prefix(&id_prefix)
                .and_then(|value| value.parse::<u64>().ok())
            {
                next_id = next_id.max(item_id.saturating_add(1));
            }
            reserved_keys.insert(media_key.clone());
            identity_by_path.insert(path, media_key);
        }
    }
    {
        let mut statement =
            transaction.prepare("SELECT media_key, path FROM media WHERE media_key LIKE ?1")?;
        let rows = statement.query_map([format!("{id_prefix}%")], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (media_key, relative_path) = row?;
            if let Some(item_id) = media_key
                .strip_prefix(&id_prefix)
                .and_then(|value| value.parse::<u64>().ok())
            {
                next_id = next_id.max(item_id.saturating_add(1));
            }
            reserved_keys.insert(media_key.clone());
            legacy_by_path
                .entry(relative_path)
                .or_default()
                .push(media_key);
        }
    }
    for item in media {
        let relative_path = item
            .path
            .strip_prefix(configured_root)
            .or_else(|_| item.path.strip_prefix(canonical_root))
            .unwrap_or(&item.path)
            .to_path_buf();
        let canonical_path = canonical_root
            .join(&relative_path)
            .to_string_lossy()
            .into_owned();
        let legacy_path = relative_path.to_string_lossy().into_owned();
        let existing_key = identity_by_path.get(&canonical_path).cloned().or_else(|| {
            legacy_by_path.get(&legacy_path).and_then(|keys| {
                keys.iter()
                    .find(|key| {
                        !assigned_keys.contains(*key)
                            && !identity_by_path.values().any(|existing| existing == *key)
                    })
                    .cloned()
            })
        });
        let id_is_non_numeric = item.id.split_once('-').map_or_else(
            || item.id.parse::<u64>().is_err(),
            |(section, item_id)| section.parse::<u64>().is_err() || item_id.parse::<u64>().is_err(),
        );
        let current_key = (item.id.starts_with(&id_prefix) || id_is_non_numeric)
            .then(|| item.id.clone())
            .filter(|key| !reserved_keys.contains(key) && !assigned_keys.contains(key));
        let media_key = if let Some(key) = existing_key.or(current_key) {
            key
        } else {
            loop {
                if next_id > MAX_ITEM_ID {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                let key = format!("{id_prefix}{next_id}");
                next_id = next_id.saturating_add(1);
                if !reserved_keys.contains(&key) && !assigned_keys.contains(&key) {
                    break key;
                }
            }
        };
        assigned_keys.insert(media_key.clone());
        reserved_keys.insert(media_key.clone());
        item.id.clone_from(&media_key);
        transaction.execute(
            "INSERT INTO media_identity (library_section, canonical_path, media_key) VALUES (?1, ?2, ?3)
             ON CONFLICT(library_section, canonical_path) DO UPDATE SET media_key = excluded.media_key",
            (section, canonical_path, media_key),
        )?;
    }
    transaction.commit()
}

fn library_scan_interval(library: &ConfigLibrary) -> Duration {
    let value = u64::from(library.scan_interval_value.max(1));
    let seconds = match library.scan_interval_unit.as_str() {
        "seconds" => value,
        "hours" => value.saturating_mul(60 * 60),
        _ => value.saturating_mul(60),
    };
    Duration::from_secs(seconds)
}

async fn scan_library_inventory(state: &AppState, library_id: usize) -> Result<usize, String> {
    let _scan_guard = state.inventory_scan_lock.lock().await;
    let library = state
        .config
        .lock()
        .map_err(|_| "Configuration is unavailable".to_owned())?
        .libraries
        .get(library_id)
        .cloned()
        .ok_or_else(|| "Library does not exist".to_owned())?;
    let mut roots = Vec::with_capacity(library.folders.len());
    for configured_root in &library.folders {
        let canonical_root = tokio::fs::canonicalize(configured_root)
            .await
            .map_err(|error| format!("Could not access configured library folder: {error}"))?;
        roots.push((configured_root.clone(), canonical_root));
    }
    let current = media_snapshot(state);
    let id_prefix = format!("{}-", library_id + 1);
    let mut existing_ids = HashMap::new();
    for media in current.iter() {
        for (root_index, (configured_root, canonical_root)) in roots.iter().enumerate() {
            let relative = media
                .path
                .strip_prefix(configured_root)
                .or_else(|_| media.path.strip_prefix(canonical_root));
            if let Ok(relative) = relative {
                existing_ids.insert((root_index, relative.to_path_buf()), media.id.clone());
                break;
            }
        }
    }
    let mut next_id = current
        .iter()
        .filter_map(|media| media.id.strip_prefix(&id_prefix)?.parse::<u64>().ok())
        .max()
        .unwrap_or_default()
        .saturating_add(1);
    let mut scanned_roots = Vec::new();
    let mut newly_found = Vec::new();
    for (root_index, (configured_root, canonical_root)) in roots.iter().enumerate() {
        let mut media = scan_media(canonical_root)
            .await
            .map_err(|error| format!("Could not scan library folder: {error}"))?;
        for item in &mut media {
            let relative = item
                .path
                .strip_prefix(canonical_root)
                .map_err(|error| format!("Scanned file escaped its library folder: {error}"))?
                .to_path_buf();
            if let Some(id) = existing_ids.get(&(root_index, relative.clone())) {
                item.id.clone_from(id);
            } else {
                let media_path = configured_root.join(&relative);
                info!(library = %library.name, path = %media_path.display(), media_kind = %library.kind, container = %item.container, "new media detected");
                newly_found.push(media_path);
                item.id = format!("{id_prefix}{next_id}");
                next_id = next_id.saturating_add(1);
            }
            item.path = configured_root.join(relative);
        }
        scanned_roots.push((configured_root.clone(), canonical_root.clone(), media));
    }
    {
        let connection = state
            .db
            .lock()
            .map_err(|_| "Media database is unavailable".to_owned())?;
        for (configured_root, canonical_root, media) in &mut scanned_roots {
            assign_media_ids(
                &connection,
                library_id + 1,
                canonical_root,
                configured_root,
                media,
            )
            .map_err(|error| format!("Could not assign stable media IDs: {error}"))?;
            persist_media_records(&connection, configured_root, media)
                .map_err(|error| format!("Could not save scanned media: {error}"))?;
        }
        mark_library_updated(&connection, &library.name, true)
            .map_err(|error| format!("Could not save library scan time: {error}"))?;
    }
    let scanned = scanned_roots
        .into_iter()
        .flat_map(|(_, _, media)| media)
        .collect::<Vec<_>>();
    let scanned_count = scanned.len();
    let belongs_to_library = |media: &Media| {
        roots.iter().any(|(configured_root, canonical_root)| {
            media.path.starts_with(configured_root) || media.path.starts_with(canonical_root)
        })
    };
    let scanned_ids = scanned
        .iter()
        .map(|media| media.id.as_str())
        .collect::<HashSet<_>>();
    let removed_media = current
        .iter()
        .filter(|media| belongs_to_library(media) && !scanned_ids.contains(media.id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let insertion_index = current
        .iter()
        .position(belongs_to_library)
        .unwrap_or(current.len());
    let mut updated = Vec::with_capacity(current.len() + scanned_count);
    for (index, media) in current.iter().enumerate() {
        if index == insertion_index {
            updated.extend(scanned.iter().cloned());
        }
        if !belongs_to_library(media) {
            updated.push(media.clone());
        }
    }
    if insertion_index == current.len() {
        updated.extend(scanned);
    }
    {
        let mut media = state
            .media
            .write()
            .map_err(|_| "Media inventory is unavailable".to_owned())?;
        *media = Arc::new(updated);
    }
    drop(_scan_guard);
    if !removed_media.is_empty() {
        let timeline_entries = removed_media
            .iter()
            .map(|media| {
                serde_json::json!({
                    "identifier": "com.plexapp.plugins.library",
                    "sectionID": (library_id + 1).to_string(),
                    "itemID": plex_rating_key(media).to_string(),
                    "type": 1,
                    "title": media.title,
                    "state": 9,
                    "updatedAt": SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                })
            })
            .collect::<Vec<_>>();
        let notification = serde_json::json!({
            "NotificationContainer": {
                "type": "timeline",
                "size": timeline_entries.len(),
                "TimelineEntry": timeline_entries
            }
        });
        let _ = state.plex_notifications.send(notification.to_string());
    }
    if !newly_found.is_empty() && library.kind != "other" {
        info!(library = %library.name, count = newly_found.len(), "automatic metadata detection started");
        let response = scan_library_metadata(
            State(state.clone()),
            Path(library_id),
            Query(ScanQuery {
                force: Some(false),
                path: None,
                refresh: None,
            }),
        )
        .await;
        info!(library = %library.name, status = %response.status(), "automatic metadata detection finished");
    }
    Ok(scanned_count)
}

async fn library_scan_scheduler(state: AppState) {
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    let mut last_scans = HashMap::<usize, tokio::time::Instant>::new();
    loop {
        ticker.tick().await;
        let libraries = match state.config.lock() {
            Ok(config) => config.libraries.clone(),
            Err(_) => continue,
        };
        let now = tokio::time::Instant::now();
        last_scans.retain(|index, _| *index < libraries.len());
        for (index, library) in libraries.iter().enumerate() {
            if !library.scan_enabled {
                last_scans.remove(&index);
                continue;
            }
            let last_scan = last_scans.entry(index).or_insert(now);
            if now.duration_since(*last_scan) < library_scan_interval(library) {
                continue;
            }
            *last_scan = now;
            if let Err(error) = scan_library_inventory(&state, index).await {
                warn!(library = %library.name, %error, "automatic library scan failed");
            }
        }
    }
}

fn load_config(path: &std::path::Path) -> Result<Config, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(Config {
            bind: default_bind(),
            libraries: Vec::new(),
            metadata_storage: default_metadata_storage(),
            tmdb_bearer_token: None,
        });
    }
    let contents = std::fs::read_to_string(path)?;
    let mut config: Config = toml::from_str(&contents)?;
    let legacy: toml::Value = toml::from_str(&contents)?;
    if legacy.get("metadata_storage").is_none()
        && legacy
            .get("metadata_in_media_folder")
            .and_then(toml::Value::as_bool)
            == Some(true)
    {
        config.metadata_storage = "sidecar".to_owned();
    }
    if !matches!(config.metadata_storage.as_str(), "database" | "sidecar") {
        config.metadata_storage = default_metadata_storage();
    }
    Ok(config)
}

fn save_config(path: &std::path::Path, config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let serialized = toml::to_string_pretty(config)?;
    if std::fs::read_to_string(path).ok().as_deref() == Some(serialized.as_str()) {
        return Ok(());
    }
    std::fs::write(path, serialized)?;
    Ok(())
}

fn sync_config_libraries(connection: &Connection, config: &Config) -> Result<(), rusqlite::Error> {
    for library in &config.libraries {
        let inserted = connection.execute(
            "INSERT OR IGNORE INTO library_groups (name, kind, added_at, updated_at) VALUES (?1, ?2, strftime('%s', 'now'), strftime('%s', 'now'))",
            (&library.name, &library.kind),
        )?;
        if inserted > 0 {
            update_library_catalog_timestamp(connection)?;
        }
        let id: i64 = connection.query_row(
            "SELECT id FROM library_groups WHERE name = ?1",
            [&library.name],
            |row| row.get(0),
        )?;
        let kind_changed = connection.execute(
            "UPDATE library_groups SET kind = ?1, updated_at = MAX(updated_at + 1, CAST(strftime('%s', 'now') AS INTEGER)) WHERE id = ?2 AND kind != ?1",
            (&library.kind, id),
        )?;
        if kind_changed > 0 {
            update_library_catalog_timestamp(connection)?;
        }
        for folder in &library.folders {
            let inserted = connection.execute(
                "INSERT OR IGNORE INTO library_folders (library_id, path, added_at) VALUES (?1, ?2, strftime('%s', 'now'))",
                (id, folder.to_string_lossy().as_ref()),
            )?;
            if inserted > 0 {
                connection.execute(
                    "UPDATE library_groups SET updated_at = MAX(updated_at + 1, CAST(strftime('%s', 'now') AS INTEGER)) WHERE id = ?1",
                    [id],
                )?;
                update_library_catalog_timestamp(connection)?;
            }
        }
    }
    Ok(())
}

fn update_library_catalog_timestamp(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute(
        "UPDATE library_catalog_state SET updated_at = MAX(updated_at + 1, CAST(strftime('%s', 'now') AS INTEGER)) WHERE id = 1",
        [],
    )?;
    Ok(())
}

fn mark_library_updated(
    connection: &Connection,
    library_name: &str,
    scanned: bool,
) -> Result<(), rusqlite::Error> {
    let query = if scanned {
        "UPDATE library_groups SET scanned_at = MAX(scanned_at + 1, CAST(strftime('%s', 'now') AS INTEGER)), updated_at = MAX(updated_at + 1, CAST(strftime('%s', 'now') AS INTEGER)) WHERE name = ?1"
    } else {
        "UPDATE library_groups SET updated_at = MAX(updated_at + 1, CAST(strftime('%s', 'now') AS INTEGER)) WHERE name = ?1"
    };
    connection.execute(query, [library_name])?;
    update_library_catalog_timestamp(connection)
}

fn library_publication_state(
    state: &AppState,
    library_name: &str,
    fallback_section: usize,
) -> (i64, u64, u64) {
    state
        .db
        .lock()
        .ok()
        .and_then(|connection| {
            connection
                .query_row(
                    "SELECT id, scanned_at, updated_at FROM library_groups WHERE name = ?1",
                    [library_name],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, u64>(1)?,
                            row.get::<_, u64>(2)?,
                        ))
                    },
                )
                .ok()
        })
        .unwrap_or((fallback_section as i64, 0, 0))
}

fn plex_library_section_details(
    state: &AppState,
    library_id: i64,
) -> (u64, Vec<serde_json::Value>) {
    let Ok(connection) = state.db.lock() else {
        return (0, Vec::new());
    };
    let created_at = connection
        .query_row(
            "SELECT added_at FROM library_groups WHERE id = ?1",
            [library_id],
            |row| row.get::<_, u64>(0),
        )
        .unwrap_or_default();
    let Ok(mut statement) = connection
        .prepare("SELECT id, path FROM library_folders WHERE library_id = ?1 ORDER BY id")
    else {
        return (created_at, Vec::new());
    };
    let Ok(locations) = statement.query_map([library_id], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    }) else {
        return (created_at, Vec::new());
    };
    let locations = locations
        .filter_map(Result::ok)
        .map(|(id, path)| serde_json::json!({"id": id, "path": path}))
        .collect();
    (created_at, locations)
}

fn library_catalog_updated_at(state: &AppState) -> u64 {
    state
        .db
        .lock()
        .ok()
        .and_then(|connection| {
            connection
                .query_row(
                    "SELECT updated_at FROM library_catalog_state WHERE id = 1",
                    [],
                    |row| row.get(0),
                )
                .ok()
        })
        .unwrap_or_default()
}

fn open_database(
    config_path: &std::path::Path,
) -> Result<(Connection, String), Box<dyn std::error::Error>> {
    let state_dir = config_path
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .to_path_buf();
    std::fs::create_dir_all(&state_dir)?;
    let connection = Connection::open(state_dir.join("state.sqlite"))?;
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS server_identity (
             id INTEGER PRIMARY KEY CHECK (id = 1),
             machine_identifier TEXT NOT NULL UNIQUE,
             server_name TEXT NOT NULL,
             plex_account_token TEXT,
             claimed INTEGER NOT NULL DEFAULT 0,
             plex_last_error TEXT
         );
         CREATE TABLE IF NOT EXISTS tv_show_identity (
             show_id TEXT PRIMARY KEY,
             library_section INTEGER NOT NULL,
             relative_path TEXT NOT NULL,
             title TEXT NOT NULL,
             UNIQUE (library_section, relative_path)
         );
         CREATE TABLE IF NOT EXISTS plex_auth (
             id INTEGER PRIMARY KEY,
             code TEXT NOT NULL,
             created_at INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS plex_device_keys (
             id INTEGER PRIMARY KEY CHECK (id = 1),
             private_key BLOB NOT NULL,
             public_key BLOB NOT NULL,
             kid TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS users (
             id TEXT PRIMARY KEY,
             plex_user_id TEXT UNIQUE,
             name TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS media (
             media_key TEXT PRIMARY KEY,
             path TEXT NOT NULL,
             title TEXT NOT NULL,
             metadata_json TEXT,
             matched_at INTEGER
         );
         CREATE TABLE IF NOT EXISTS media_identity (
             library_section INTEGER NOT NULL,
             canonical_path TEXT NOT NULL,
             media_key TEXT NOT NULL UNIQUE,
             PRIMARY KEY (library_section, canonical_path)
         );
         CREATE TABLE IF NOT EXISTS watch_progress (
             user_id TEXT NOT NULL,
             media_key TEXT NOT NULL,
             position_ms INTEGER NOT NULL DEFAULT 0,
             duration_ms INTEGER,
             watched INTEGER NOT NULL DEFAULT 0,
             hidden_from_continue INTEGER NOT NULL DEFAULT 0,
             updated_at INTEGER NOT NULL,
             PRIMARY KEY (user_id, media_key)
         );
         CREATE TABLE IF NOT EXISTS artwork (
             media_key TEXT NOT NULL,
             kind TEXT NOT NULL,
             source_url TEXT,
             local_path TEXT,
             downloaded_at INTEGER,
             PRIMARY KEY (media_key, kind)
         );
         CREATE TABLE IF NOT EXISTS libraries (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             path TEXT NOT NULL UNIQUE,
             kind TEXT NOT NULL DEFAULT 'other',
             added_at INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS library_groups (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             name TEXT NOT NULL UNIQUE,
             kind TEXT NOT NULL DEFAULT 'other',
             added_at INTEGER NOT NULL,
             scanned_at INTEGER NOT NULL DEFAULT 0,
             updated_at INTEGER NOT NULL DEFAULT 0
         );
         CREATE TABLE IF NOT EXISTS library_catalog_state (
             id INTEGER PRIMARY KEY CHECK (id = 1),
             updated_at INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS library_folders (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             library_id INTEGER NOT NULL REFERENCES library_groups(id) ON DELETE CASCADE,
             path TEXT NOT NULL UNIQUE,
             added_at INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS playlists (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             title TEXT NOT NULL,
             playlist_type TEXT NOT NULL,
             smart INTEGER NOT NULL DEFAULT 0,
             added_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS playlist_items (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             playlist_id INTEGER NOT NULL,
             media_key TEXT NOT NULL,
             position INTEGER NOT NULL,
             added_at INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS collections (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             title TEXT NOT NULL,
             section_id INTEGER NOT NULL,
             added_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS collection_items (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             collection_id INTEGER NOT NULL,
             media_key TEXT NOT NULL,
             position INTEGER NOT NULL,
             added_at INTEGER NOT NULL
         );
         CREATE UNIQUE INDEX IF NOT EXISTS collection_items_collection_media
             ON collection_items(collection_id, media_key);",
    )?;
    let _ = connection.execute(
        "ALTER TABLE server_identity ADD COLUMN plex_last_error TEXT",
        [],
    );
    let _ = connection.execute(
        "ALTER TABLE libraries ADD COLUMN kind TEXT NOT NULL DEFAULT 'other'",
        [],
    );
    let _ = connection.execute(
        "ALTER TABLE watch_progress ADD COLUMN hidden_from_continue INTEGER NOT NULL DEFAULT 0",
        [],
    );
    let _ = connection.execute(
        "ALTER TABLE library_groups ADD COLUMN scanned_at INTEGER NOT NULL DEFAULT 0",
        [],
    );
    let _ = connection.execute(
        "ALTER TABLE library_groups ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0",
        [],
    );
    connection.execute(
        "UPDATE library_groups SET updated_at = added_at WHERE updated_at = 0",
        [],
    )?;
    connection.execute(
        "INSERT OR IGNORE INTO library_catalog_state (id, updated_at) VALUES (1, strftime('%s', 'now'))",
        [],
    )?;
    let machine_identifier = connection
        .query_row(
            "SELECT machine_identifier FROM server_identity WHERE id = 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .or_else(|error| {
            if !matches!(error, rusqlite::Error::QueryReturnedNoRows) {
                return Err(error);
            }
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|error| {
                rusqlite::Error::ToSqlConversionFailure(Box::new(error))
            })?;
            let generated = format!("mplex-{:x}", timestamp.as_nanos());
            connection.execute(
                "INSERT INTO server_identity (id, machine_identifier, server_name) VALUES (1, ?1, 'harbringer')",
                [&generated],
            )?;
            Ok(generated)
        })?;
    let machine_identifier = if machine_identifier.len() == 40
        && machine_identifier
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        machine_identifier
    } else {
        let migrated = plex_machine_identifier(&machine_identifier);
        connection.execute(
            "UPDATE server_identity SET machine_identifier = ?1 WHERE id = 1",
            [&migrated],
        )?;
        migrated
    };
    Ok((connection, machine_identifier))
}

fn migrate_library_model(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT OR IGNORE INTO library_groups (name, kind, added_at) VALUES ('Default Library', 'other', strftime('%s', 'now'))",
        [],
    )?;
    let default_id: i64 = connection.query_row(
        "SELECT id FROM library_groups WHERE name = 'Default Library'",
        [],
        |row| row.get(0),
    )?;
    connection.execute(
        "INSERT OR IGNORE INTO library_folders (library_id, path, added_at) SELECT ?1, path, added_at FROM libraries",
        [default_id],
    )?;
    Ok(())
}

fn library_kind_label(kind: &str) -> &str {
    match kind {
        "movie" => "Movies",
        "tv" => "TV Shows",
        _ => "Other",
    }
}

#[derive(Deserialize)]
struct LibraryFolderForm {
    library_id: i64,
    path: String,
}

#[derive(Deserialize)]
struct RemoveLibraryFolderForm {
    library_id: usize,
    folder_index: usize,
    path: String,
}

#[derive(Deserialize)]
struct LibraryDisplayForm {
    library_id: usize,
    show_recently_added: Option<String>,
    show_continue_watching: Option<String>,
}

#[derive(Deserialize)]
struct RenameLibraryForm {
    name: String,
}

#[derive(Deserialize)]
struct LibrarySettingsForm {
    name: String,
    scan_enabled: Option<String>,
    scan_interval_value: u32,
    scan_interval_unit: String,
    show_recently_added: Option<String>,
    show_continue_watching: Option<String>,
}

#[derive(Deserialize)]
struct LibraryScanSettingsForm {
    library_id: usize,
    scan_enabled: Option<String>,
    scan_interval_value: u32,
    scan_interval_unit: String,
}

#[derive(Deserialize)]
struct PreferencesForm {
    metadata_storage: Option<String>,
    #[serde(default)]
    metadata_in_media_folder: Option<String>,
    tmdb_bearer_token: Option<String>,
}

#[derive(Deserialize)]
struct ServerNameForm {
    server_name: String,
}

#[derive(Deserialize, Default)]
struct SettingsQuery {
    folder: Option<String>,
    error: Option<String>,
    metadata_matched: Option<usize>,
    metadata_status: Option<String>,
    server_name_saved: Option<bool>,
    inventory_scan: Option<usize>,
    inventory_error: Option<bool>,
    tab: Option<String>,
    library_renamed: Option<bool>,
    library_saved: Option<bool>,

    library_created: Option<bool>,
}

#[derive(Deserialize)]
struct PlexPinResponse {
    id: i64,
    code: String,
    #[serde(rename = "authToken")]
    auth_token: Option<String>,
}

#[derive(Deserialize)]
struct PlexClaimTokenResponse {
    token: String,
}

fn plex_auth_client_identifier(state: &AppState) -> String {
    format!("{}-auth", state.machine_identifier)
}

fn plex_device_key(state: &AppState) -> Result<(SigningKey, String), String> {
    let connection = state
        .db
        .lock()
        .map_err(|_| "could not access Plex device key storage".to_owned())?;
    let existing = connection.query_row(
        "SELECT private_key, kid FROM plex_device_keys WHERE id = 1",
        [],
        |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?)),
    );
    if let Ok((private_key, kid)) = existing {
        let bytes: [u8; 32] = private_key
            .try_into()
            .map_err(|_| "stored Plex device key has an invalid length".to_owned())?;
        return Ok((SigningKey::from_bytes(&bytes), kid));
    }
    let signing_key = SigningKey::generate(&mut OsRng);
    let kid = format!("{}-jwk", state.machine_identifier);
    connection
        .execute(
            "INSERT INTO plex_device_keys (id, private_key, public_key, kid) VALUES (1, ?1, ?2, ?3)",
            (
                signing_key.to_bytes().as_slice(),
                signing_key.verifying_key().to_bytes().as_slice(),
                &kid,
            ),
        )
        .map_err(|_| "could not persist Plex device key".to_owned())?;
    Ok((signing_key, kid))
}

fn plex_device_jwk(signing_key: &SigningKey, kid: &str) -> serde_json::Value {
    serde_json::json!({
        "kty": "OKP",
        "crv": "Ed25519",
        "x": URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes()),
        "kid": kid,
        "alg": "EdDSA"
    })
}

fn plex_device_jwt(signing_key: &SigningKey, kid: &str, client_id: &str) -> String {
    let header = serde_json::json!({"alg": "EdDSA", "kid": kid, "typ": "JWT"});
    let issued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    let payload = serde_json::json!({
        "aud": "plex.tv",
        "iss": client_id,
        "iat": issued_at,
        "exp": issued_at + 3600,
        "scope": "username,email,friendly_name"
    });
    let encoded_header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap_or_default());
    let encoded_payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap_or_default());
    let signing_input = format!("{encoded_header}.{encoded_payload}");
    let signature = signing_key.sign(signing_input.as_bytes());
    format!(
        "{signing_input}.{}",
        URL_SAFE_NO_PAD.encode(signature.to_bytes())
    )
}

#[derive(Deserialize, Default)]
struct ScanQuery {
    force: Option<bool>,
    path: Option<String>,
    refresh: Option<bool>,
}

#[derive(Deserialize)]
struct TvMatchSearchQuery {
    library: usize,
    path: String,
    query: String,
}

#[derive(Deserialize)]
struct TvMatchSelection {
    library: usize,
    path: String,
    tmdb_id: i64,
}

#[derive(Deserialize)]
struct MediaScanForm {
    path: String,
    library: Option<usize>,
    title: String,
}

#[derive(Deserialize, Default)]
struct BrowseQuery {
    path: Option<String>,
}

#[derive(Deserialize)]
struct ProgressQuery {
    path: String,
}

#[derive(Deserialize)]
struct ProgressUpdate {
    path: String,
    position_ms: i64,
    duration_ms: Option<i64>,
    watched: bool,
}

#[derive(Deserialize)]
struct WebSubtitleSelectionUpdate {
    path: String,
    library: Option<usize>,
    stream_id: u64,
}

#[derive(Deserialize)]
struct WebSubtitleSelectionQuery {
    path: String,
    library: Option<usize>,
}

#[derive(Deserialize)]
struct WebWatchActionUpdate {
    path: String,
    library: Option<usize>,
    action: String,
    scope: Option<String>,
}

async fn web_progress(
    State(state): State<AppState>,
    Query(query): Query<ProgressQuery>,
) -> Response {
    let Some(path) = clean_relative_path(&query.path) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let progress = connection.query_row(
        "SELECT position_ms, duration_ms, watched FROM watch_progress WHERE user_id = ?1 AND media_key = ?2",
        (USER_ID, path.to_string_lossy().as_ref()),
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?, row.get::<_, bool>(2)?)),
    );
    match progress {
        Ok((position_ms, duration_ms, watched)) => Json(serde_json::json!({
            "positionMs": position_ms,
            "durationMs": duration_ms,
            "watched": watched
        }))
        .into_response(),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            Json(serde_json::json!({"positionMs": 0, "durationMs": null, "watched": false}))
                .into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn update_web_progress(
    State(state): State<AppState>,
    Json(update): Json<ProgressUpdate>,
) -> Response {
    let Some(path) = clean_relative_path(&update.path) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();
    if connection
        .execute(
            "INSERT INTO watch_progress (user_id, media_key, position_ms, duration_ms, watched, hidden_from_continue, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6) ON CONFLICT(user_id, media_key) DO UPDATE SET position_ms = excluded.position_ms, duration_ms = excluded.duration_ms, watched = excluded.watched, hidden_from_continue = 0, updated_at = excluded.updated_at",
            (USER_ID, path.to_string_lossy().as_ref(), update.position_ms.max(0), update.duration_ms, update.watched, updated_at),
        )
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn web_watch_action(
    State(state): State<AppState>,
    Json(update): Json<WebWatchActionUpdate>,
) -> Response {
    let Some(relative) = clean_relative_path(&update.path) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if contains_hidden_component(&relative) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let root = selected_library_root(&state, update.library);
    let Some(media) = web_media_for_relative(&state, &root, &relative).await else {
        warn!(path = %relative.display(), library = ?update.library, action = %update.action, "Web watch action could not find media");
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(media_key) = media.path.strip_prefix(&root) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let media_key = media_key.to_string_lossy().into_owned();
    let media_keys = if update.scope.as_deref() == Some("show") {
        let library_config = update.library.and_then(|index| {
            state
                .config
                .lock()
                .ok()
                .and_then(|config| config.libraries.get(index).cloned())
        });
        if let Some(library_config) = library_config.filter(|library| library.kind == "tv") {
            let show_folder = relative
                .components()
                .next()
                .map(|component| PathBuf::from(component.as_os_str()));
            show_folder
                .map(|show_folder| {
                    media_snapshot(&state)
                        .iter()
                        .filter(|item| media_in_library(item, &library_config))
                        .filter_map(|item| {
                            let relative = item.path.strip_prefix(&root).ok()?;
                            relative
                                .starts_with(&show_folder)
                                .then(|| relative.to_string_lossy().into_owned())
                        })
                        .collect::<Vec<_>>()
                })
                .filter(|keys| !keys.is_empty())
                .unwrap_or_else(|| vec![media_key.clone()])
        } else {
            vec![media_key.clone()]
        }
    } else {
        vec![media_key.clone()]
    };
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let sql = match update.action.as_str() {
        "watched" => "INSERT INTO watch_progress (user_id, media_key, position_ms, duration_ms, watched, hidden_from_continue, updated_at) VALUES (?1, ?2, 0, NULL, 1, 0, ?3) ON CONFLICT(user_id, media_key) DO UPDATE SET position_ms = 0, watched = 1, hidden_from_continue = 0, updated_at = excluded.updated_at",
        "unwatched" => "INSERT INTO watch_progress (user_id, media_key, position_ms, duration_ms, watched, hidden_from_continue, updated_at) VALUES (?1, ?2, 0, NULL, 0, 0, ?3) ON CONFLICT(user_id, media_key) DO UPDATE SET position_ms = 0, watched = 0, hidden_from_continue = 0, updated_at = excluded.updated_at",
        "remove_from_continue" => "INSERT INTO watch_progress (user_id, media_key, position_ms, duration_ms, watched, hidden_from_continue, updated_at) VALUES (?1, ?2, 0, NULL, 0, 1, ?3) ON CONFLICT(user_id, media_key) DO UPDATE SET hidden_from_continue = 1, updated_at = excluded.updated_at",
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    for media_key in media_keys {
        if let Err(error) = connection.execute(sql, (USER_ID, media_key, updated_at)) {
            warn!(action = %update.action, error = %error, "Web watch action database update failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }
    info!(
        action = %update.action,
        scope = ?update.scope,
        library = ?update.library,
        path = %media_key,
        "Web watch action saved"
    );
    StatusCode::NO_CONTENT.into_response()
}

async fn web_subtitle_selection(
    State(state): State<AppState>,
    Query(query): Query<WebSubtitleSelectionQuery>,
) -> Response {
    let Some(relative) = clean_relative_path(&query.path) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if contains_hidden_component(&relative) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let root = selected_library_root(&state, query.library);
    let Some(media) = web_media_for_relative(&state, &root, &relative).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let stream_id = state
        .plex_stream_selections
        .lock()
        .ok()
        .and_then(|selections| selections.get(&plex_part_id(&media)).copied())
        .and_then(|selection| selection.subtitle_stream_id)
        .unwrap_or_default();
    Json(serde_json::json!({ "streamId": stream_id })).into_response()
}

async fn update_web_subtitle_selection(
    State(state): State<AppState>,
    Json(update): Json<WebSubtitleSelectionUpdate>,
) -> Response {
    let Some(relative) = clean_relative_path(&update.path) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if contains_hidden_component(&relative) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let root = selected_library_root(&state, update.library);
    let Some(media) = web_media_for_relative(&state, &root, &relative).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if update.stream_id != 0 {
        let external = external_subtitle_files(&media.path).await;
        let is_external = external
            .iter()
            .any(|path| plex_external_subtitle_id(&media, path) == update.stream_id);
        let is_embedded = state
            .media_info
            .lock()
            .ok()
            .and_then(|cache| cache.get(&media.id).cloned());
        let info = match is_embedded {
            Some(info) => Some(info),
            None => match inspect_media(&media.path).await {
                Some(info) => {
                    if let Ok(mut cache) = state.media_info.lock() {
                        cache.insert(media.id.clone(), info.clone());
                    }
                    Some(info)
                }
                None => None,
            },
        };
        let is_embedded = info.is_some_and(|info| {
            technical_tracks(&info).iter().any(|track| {
                technical_text(track, "@type") == Some("Text")
                    && technical_track_id(track).saturating_add(1) == update.stream_id
            })
        });
        if !is_external && !is_embedded {
            return StatusCode::BAD_REQUEST.into_response();
        }
    }
    if let Ok(mut selections) = state.plex_stream_selections.lock() {
        selections
            .entry(plex_part_id(&media))
            .or_default()
            .subtitle_stream_id = Some(update.stream_id);
    } else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn web_media_for_relative(
    state: &AppState,
    root: &std::path::Path,
    relative: &std::path::Path,
) -> Option<Media> {
    let path = web_path(state, root, relative).await.ok()?;
    if !path.is_file() {
        return None;
    }
    let media_index = media_snapshot(state);
    for media in media_index.iter() {
        if tokio::fs::canonicalize(&media.path)
            .await
            .is_ok_and(|candidate| candidate == path)
        {
            return Some(media.clone());
        }
    }
    None
}

async fn start_plex_auth(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let client = reqwest::Client::new();
    let auth_client_identifier = plex_auth_client_identifier(&state);
    let Ok((signing_key, kid)) = plex_device_key(&state) else {
        return settings_error("Could not initialize Plex device authentication");
    };
    let response = match client
        .post("https://clients.plex.tv/api/v2/pins")
        .json(&serde_json::json!({
            "jwk": plex_device_jwk(&signing_key, &kid),
            "strong": true
        }))
        .header("Accept", "application/json")
        .header("X-Plex-Product", "harbringer")
        .header("X-Plex-Client-Identifier", &auth_client_identifier)
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => return settings_error("Plex.tv could not be reached"),
    };
    let Ok(pin) = response.json::<PlexPinResponse>().await else {
        return settings_error("Plex.tv returned an invalid authentication response");
    };
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if connection
        .execute(
            "INSERT OR REPLACE INTO plex_auth (id, code, created_at) VALUES (?1, ?2, strftime('%s', 'now'))",
            (pin.id, &pin.code),
        )
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("127.0.0.1:32400");
    let proto = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("http");
    let callback = format!("{proto}://{host}/settings/plex/callback");
    let link = format!(
        "https://app.plex.tv/auth#?clientID={}&code={}&context%5Bdevice%5D%5Bproduct%5D={}&context%5Bdevice%5D%5Bversion%5D=0.1.0&context%5Bdevice%5D%5Bplatform%5D=macOS&context%5Bdevice%5D%5BdeviceName%5D={}&forwardUrl={}",
        url_encode(&auth_client_identifier),
        url_encode(&pin.code),
        url_encode("harbringer"),
        url_encode("harbringer"),
        url_encode(&callback)
    );
    Redirect::to(&link).into_response()
}

async fn plex_auth_callback(State(state): State<AppState>) -> Response {
    let _ = plex_auth_status(State(state)).await;
    Redirect::to("/settings").into_response()
}

async fn plex_auth_status(State(state): State<AppState>) -> Response {
    let Some((pin_id, _pin_code)) = state.db.lock().ok().and_then(|connection| {
        connection
            .query_row("SELECT id, code FROM plex_auth WHERE id != 0", [], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .ok()
    }) else {
        return Json(serde_json::json!({"authorized": false})).into_response();
    };
    let Ok((signing_key, kid)) = plex_device_key(&state) else {
        return Json(serde_json::json!({"authorized": false})).into_response();
    };
    let device_jwt = plex_device_jwt(&signing_key, &kid, &plex_auth_client_identifier(&state));
    let response = reqwest::Client::new()
        .get(format!("https://clients.plex.tv/api/v2/pins/{pin_id}"))
        .query(&[("deviceJWT", device_jwt.as_str())])
        .header("Accept", "application/json")
        .header("X-Plex-Product", "harbringer")
        .header(
            "X-Plex-Client-Identifier",
            plex_auth_client_identifier(&state),
        )
        .send()
        .await;
    let Ok(response) = response else {
        return Json(serde_json::json!({"authorized": false})).into_response();
    };
    let Ok(pin) = response.json::<PlexPinResponse>().await else {
        return Json(serde_json::json!({"authorized": false})).into_response();
    };
    let Some(token) = pin.auth_token.filter(|token| !token.is_empty()) else {
        return Json(serde_json::json!({"authorized": false})).into_response();
    };
    let claim_result = claim_and_publish_plex_server(&state, &token).await;
    let (server_token, claimed, claim_error) = match claim_result {
        Ok((server_token, claimed, error)) => (server_token, claimed, error),
        Err(error) => {
            warn!(error = %error, "Plex server claim or resource publication failed");
            (token.clone(), false, Some(error))
        }
    };
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if connection
        .execute(
            "UPDATE server_identity SET plex_account_token = ?1, claimed = ?2, plex_last_error = ?3 WHERE id = 1",
            (&server_token, claimed, claim_error.as_deref()),
        )
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let _ = connection.execute("DELETE FROM plex_auth", []);
    drop(connection);
    if claimed {
        let presence_state = state.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            run_plex_presence(&client, &presence_state, &server_token).await;
        });
    }
    Json(serde_json::json!({
        "authorized": true,
        "claimed": claimed,
        "error": claim_error
    }))
    .into_response()
}

async fn disconnect_plex(State(state): State<AppState>) -> Response {
    let credentials = state.db.lock().ok().and_then(|connection| {
        connection
            .query_row(
                "SELECT plex_account_token FROM server_identity WHERE id = 1",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .ok()
            .flatten()
            .map(|token| (token, state.machine_identifier.to_string()))
    });
    let Some((token, machine_identifier)) = credentials else {
        return settings_error("harbringer is not connected to Plex.tv");
    };
    let client = reqwest::Client::new();
    let devices = match client
        .get("https://plex.tv/devices.xml")
        .header("Accept", "application/xml")
        .header("X-Plex-Product", "harbringer")
        .header("X-Plex-Client-Identifier", &machine_identifier)
        .header("X-Plex-Token", &token)
        .send()
        .await
    {
        Ok(response) => match response.error_for_status() {
            Ok(response) => response.text().await.ok(),
            Err(_) => None,
        },
        Err(_) => None,
    };
    if let Some(body) = devices {
        if let Some(device_id) = find_plex_device_id(&body, &machine_identifier) {
            let result = client
                .delete(format!("https://plex.tv/devices/{device_id}.xml"))
                .header("Accept", "application/xml")
                .header("X-Plex-Product", "harbringer")
                .header("X-Plex-Client-Identifier", &machine_identifier)
                .header("X-Plex-Token", &token)
                .send()
                .await;
            if !matches!(
                result.and_then(|response| response.error_for_status()),
                Ok(_)
            ) {
                return settings_error("Plex.tv refused to revoke harbringer");
            }
        }
    }
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if connection
        .execute(
            "UPDATE server_identity SET plex_account_token = NULL, claimed = 0, plex_last_error = NULL WHERE id = 1",
            [],
        )
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    Redirect::to("/settings?metadata_status=Disconnected%20and%20revoked%20from%20Plex.tv")
        .into_response()
}

fn find_plex_device_id(body: &str, machine_identifier: &str) -> Option<String> {
    body.split("<Device")
        .skip(1)
        .find(|device| {
            xml_attribute(device, "clientIdentifier").as_deref() == Some(machine_identifier)
        })
        .and_then(|device| xml_attribute(device, "id"))
}

fn xml_attribute(value: &str, name: &str) -> Option<String> {
    let marker = format!("{name}=\"");
    let start = value.find(&marker)? + marker.len();
    let end = value[start..].find('\"')? + start;
    Some(value[start..end].to_owned())
}

async fn claim_and_publish_plex_server(
    state: &AppState,
    account_token: &str,
) -> Result<(String, bool, Option<String>), String> {
    let client = reqwest::Client::new();
    let claim_response = client
        .get("https://plex.tv/api/claim/token.json")
        .header("Accept", "application/json")
        .header("X-Plex-Product", "Plex Media Server")
        .header("X-Plex-Version", "1.43.4.10903-e5521bd8c")
        .header("X-Plex-Platform", "MacOSX")
        .header("X-Plex-Platform-Version", std::env::consts::ARCH)
        .header("X-Plex-Device-Name", "harbringer")
        .header("X-Plex-Device", "MacOSX")
        .header("X-Plex-Provides", "server")
        .header(
            "X-Plex-Client-Identifier",
            state.machine_identifier.as_ref(),
        )
        .header("X-Plex-Token", account_token)
        .send()
        .await
        .map_err(|error| format!("could not request a Plex claim token: {error}"))?
        .error_for_status()
        .map_err(|error| format!("Plex rejected the claim-token request: {error}"))?;
    let claim = claim_response
        .json::<PlexClaimTokenResponse>()
        .await
        .map_err(|error| format!("Plex returned an invalid claim token: {error}"))?;
    if !claim.token.starts_with("claim-") {
        return Err("Plex returned an invalid claim token".to_owned());
    }

    let exchange_response = client
        .post("https://plex.tv/api/claim/exchange")
        .query(&[("token", claim.token.as_str())])
        .header("Accept", "application/xml")
        .header("X-Plex-Product", "Plex Media Server")
        .header("X-Plex-Version", "1.43.4.10903-e5521bd8c")
        .header("X-Plex-Platform", "MacOSX")
        .header("X-Plex-Platform-Version", std::env::consts::ARCH)
        .header("X-Plex-Device-Name", "harbringer")
        .header("X-Plex-Device", "MacOSX")
        .header("X-Plex-Provides", "server")
        .header(
            "X-Plex-Client-Identifier",
            state.machine_identifier.as_ref(),
        )
        .send()
        .await
        .map_err(|error| format!("could not exchange the Plex claim token: {error}"))?
        .error_for_status()
        .map_err(|error| format!("Plex rejected the server claim: {error}"))?;
    let exchange_body = exchange_response
        .text()
        .await
        .map_err(|error| format!("could not read the Plex claim response: {error}"))?;
    let server_token = extract_xml_token(&exchange_body)
        .ok_or_else(|| "Plex claim response did not contain a server token".to_owned())?;

    let tls_error = state
        .plex_tls
        .ensure_certificate(&client, &state.machine_identifier, &server_token)
        .await
        .err();
    if let Some(error) = &tls_error {
        warn!(error = %error, "Plex TLS certificate unavailable; publishing the HTTP connection fallback");
    }
    if let Err(error) = publish_plex_connections(&client, state, &server_token).await {
        return Ok((server_token, false, Some(error)));
    }

    if let Err(error) = verify_plex_resource(&client, state, &server_token).await {
        return Ok((server_token, false, Some(error)));
    }
    Ok((server_token, true, tls_error))
}

async fn publish_plex_connections(
    client: &reqwest::Client,
    state: &AppState,
    server_token: &str,
) -> Result<(), String> {
    let registration = plex_server_registration_xml(state)?;
    let registration_response = client
        .post("https://servers.plex.tv/servers.xml")
        .query(&[("auth_token", server_token)])
        .header("User-Agent", "PlexMediaServer/1.43.4.10903-e5521bd8c")
        .header("Accept", "*/*")
        .header("Accept-Encoding", "gzip")
        .header("Content-Type", "application/xml")
        .header("X-Plex-Product", "Plex Media Server")
        .header("X-Plex-Version", "1.43.4.10903-e5521bd8c")
        .header("X-Plex-Platform", "MacOSX")
        .header("X-Plex-Platform-Version", std::env::consts::ARCH)
        .header("X-Plex-Device-Name", "harbringer")
        .header("X-Plex-Device", "MacOSX")
        .header("X-Plex-Model", std::env::consts::ARCH)
        .header(
            "X-Plex-Client-Identifier",
            state.machine_identifier.as_ref(),
        )
        .header("X-Plex-Provides", "server")
        .header("X-Plex-Device-Vendor", "Apple")
        .body(registration)
        .send()
        .await
        .map_err(|error| format!("could not register harbringer libraries with Plex: {error}"))?;
    // Plex Media Server's measured servers.xml registration can answer 422
    // after creating/updating the server record. Continue with the device
    // announce, which is the request that publishes client connection URIs.
    if !registration_response.status().is_success()
        && registration_response.status() != reqwest::StatusCode::UNPROCESSABLE_ENTITY
    {
        return Err(format!(
            "Plex rejected harbringer library registration: HTTP {}",
            registration_response.status()
        ));
    }
    info!(
        status = %registration_response.status(),
        "Plex server registration response"
    );
    publish_plex_device(client, state, server_token).await?;
    verify_plex_resource(client, state, server_token).await?;
    Ok(())
}

async fn verify_plex_resource(
    client: &reqwest::Client,
    state: &AppState,
    server_token: &str,
) -> Result<(), String> {
    let resources = client
        .get("https://clients.plex.tv/api/v2/resources")
        .query(&[
            ("includeHttps", "1"),
            ("includeRelay", "1"),
            ("includeIPv6", "1"),
        ])
        .header("Accept", "application/json")
        .header("X-Plex-Product", "harbringer")
        .header(
            "X-Plex-Client-Identifier",
            state.machine_identifier.as_ref(),
        )
        .header("X-Plex-Token", server_token)
        .send()
        .await
        .map_err(|error| format!("could not verify harbringer in Plex resources: {error}"))?
        .error_for_status()
        .map_err(|error| format!("Plex resource verification failed: {error}"))?
        .json::<serde_json::Value>()
        .await
        .map_err(|error| format!("Plex returned invalid resource data: {error}"))?;
    if !plex_resources_contain(&resources, state.machine_identifier.as_ref()) {
        return Err("Plex.tv did not publish a usable Harbringer server resource".to_owned());
    }
    if state.plex_tls.hash().is_some()
        && !plex_resources_contain_local_https(&resources, state.machine_identifier.as_ref())
    {
        return Err(
            "Plex.tv did not publish Harbringer's local HTTPS plex.direct connection".to_owned(),
        );
    }
    Ok(())
}

async fn plex_account_id(
    client: &reqwest::Client,
    state: &AppState,
    server_token: &str,
) -> Result<String, String> {
    let response = client
        .get("https://servers.plex.tv/api/v2/user")
        .query(&[("includeSubscriptions", "1"), ("includeProviders", "1")])
        .header("User-Agent", "PlexMediaServer/1.43.4.10903-e5521bd8c")
        .header("Accept", "*/*")
        .header("Accept-Encoding", "gzip")
        .header(
            "X-Plex-Client-Identifier",
            state.machine_identifier.as_ref(),
        )
        .header("X-Plex-Version", "1.43.4.10903-e5521bd8c")
        .header("X-Plex-Provides", "server")
        .header("X-Plex-Platform", "MacOSX")
        .header("X-Plex-Product", "Plex Media Server")
        .header("X-Plex-Platform-Version", std::env::consts::ARCH)
        .header("X-Plex-Device-Name", "harbringer")
        .header("X-Plex-Device", "MacOSX")
        .header("X-Plex-Model", std::env::consts::ARCH)
        .header("X-Plex-Device-Vendor", "Apple")
        .header("X-Plex-Token", server_token)
        .send()
        .await
        .map_err(|error| format!("could not read the Plex account for presence: {error}"))?
        .error_for_status()
        .map_err(|error| format!("Plex account lookup for presence failed: {error}"))?;
    let body = response
        .text()
        .await
        .map_err(|error| format!("could not read the Plex account response: {error}"))?;
    let id = xml_attribute(&body, "id")
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| "Plex account response did not include a numeric id".to_owned())?;
    Ok(id)
}

async fn run_plex_presence(client: &reqwest::Client, state: &AppState, server_token: &str) {
    let account_id = loop {
        match plex_account_id(client, state, server_token).await {
            Ok(id) => break id,
            Err(error) => {
                warn!(error = %error, "Plex presence account lookup failed; retrying in 15 seconds");
                tokio::time::sleep(std::time::Duration::from_secs(15)).await;
            }
        }
    };

    loop {
        let stream = client
            .get(plex_presence_url(&account_id, &state.machine_identifier))
            .query(&[("X-Plex-Token", server_token)])
            .header(
                "User-Agent",
                "PlexMediaServer-EventSourceClient/1.43.4.10903-e5521bd8c",
            )
            .header("Accept", "*/*")
            .header(
                "X-Plex-Client-Identifier",
                state.machine_identifier.as_ref(),
            )
            .header("X-Plex-Version", "1.43.4.10903-e5521bd8c")
            .header("X-Plex-Provides", "server")
            .header("X-Plex-Platform", "MacOSX")
            .header("X-Plex-Product", "Plex Media Server")
            .header("X-Plex-Platform-Version", std::env::consts::ARCH)
            .header("X-Plex-Device-Name", "harbringer")
            .header("X-Plex-Device", "MacOSX")
            .header("X-Plex-Model", std::env::consts::ARCH)
            .header("X-Plex-Device-Vendor", "Apple")
            .send()
            .await;

        match stream {
            Ok(mut response) if response.status().is_success() => {
                info!("Plex presence stream connected");
                if let Err(error) = plex_connectivity_check(client, state, server_token).await {
                    warn!(error = %error, "Plex connectivity check failed");
                }
                loop {
                    match response.chunk().await {
                        Ok(Some(_event)) => {}
                        Ok(None) => {
                            warn!("Plex presence stream closed; reconnecting in 15 seconds");
                            break;
                        }
                        Err(error) => {
                            warn!(error = %error, "Plex presence stream failed; reconnecting in 15 seconds");
                            break;
                        }
                    }
                }
            }
            Ok(response) => {
                warn!(status = %response.status(), "Plex presence stream rejected; reconnecting in 15 seconds");
            }
            Err(error) => {
                warn!(error = %error, "Plex presence stream could not connect; reconnecting in 15 seconds");
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;
    }
}

async fn plex_connectivity_check(
    client: &reqwest::Client,
    state: &AppState,
    server_token: &str,
) -> Result<(), String> {
    let async_identifier = plex_async_identifier();
    let response = client
        .put(plex_connectivity_url(&state.machine_identifier))
        .query(&[
            ("X-Plex-Token", server_token),
            ("asyncIdentifier", async_identifier.as_str()),
        ])
        .header("User-Agent", "PlexMediaServer/1.43.4.10903-e5521bd8c")
        .header("Accept", "*/*")
        .header("Accept-Encoding", "gzip")
        .header(
            "X-Plex-Client-Identifier",
            state.machine_identifier.as_ref(),
        )
        .header("X-Plex-Version", "1.43.4.10903-e5521bd8c")
        .header("X-Plex-Provides", "server")
        .header("X-Plex-Platform", "MacOSX")
        .header("X-Plex-Product", "Plex Media Server")
        .header("X-Plex-Platform-Version", std::env::consts::ARCH)
        .header("X-Plex-Device-Name", "harbringer")
        .header("X-Plex-Device", "MacOSX")
        .header("X-Plex-Model", std::env::consts::ARCH)
        .header("X-Plex-Device-Vendor", "Apple")
        .body(Vec::new())
        .send()
        .await
        .map_err(|error| format!("could not request Plex connectivity check: {error}"))?;
    info!(status = %response.status(), "Plex connectivity check response");
    Ok(())
}

fn plex_presence_url(account_id: &str, machine_identifier: &str) -> String {
    format!("https://pubsub.plex.tv/sub/eventsource/{account_id}/{machine_identifier}")
}

fn plex_connectivity_url(machine_identifier: &str) -> String {
    format!("https://plex.tv/api/servers/{machine_identifier}/connectivity")
}

fn plex_async_identifier() -> String {
    let mut bytes = [0_u8; 16];
    OsRng.fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

async fn publish_plex_device(
    client: &reqwest::Client,
    state: &AppState,
    server_token: &str,
) -> Result<(), String> {
    let urls = plex_connection_urls(state);
    if urls.is_empty() {
        return Err("could not determine a local connection address".to_owned());
    }
    info!(urls = ?urls, "publishing Plex connection URLs");
    let https_enabled = state.plex_tls.hash().is_some();
    let query = plex_device_announce_query(&urls, https_enabled, server_token);
    let version = "1.43.4.10903-e5521bd8c";
    client
        .put(plex_device_announce_url(&state.machine_identifier))
        .query(&query)
        .header("User-Agent", format!("PlexMediaServer/{version}"))
        .header("Accept", "*/*")
        .header("Accept-Encoding", "gzip")
        .header("Content-Length", "0")
        .header("X-Plex-Product", "Plex Media Server")
        .header("X-Plex-Version", version)
        .header("X-Plex-Platform", "MacOSX")
        .header("X-Plex-Platform-Version", std::env::consts::ARCH)
        .header("X-Plex-Device-Name", "harbringer")
        .header("X-Plex-Device", "MacOSX")
        .header("X-Plex-Model", std::env::consts::ARCH)
        .header("X-Plex-Device-Vendor", "Apple")
        .header("X-Plex-Provides", "server")
        .header(
            "X-Plex-Client-Identifier",
            state.machine_identifier.as_ref(),
        )
        .body(Vec::new())
        .send()
        .await
        .map_err(|error| format!("could not publish harbringer to Plex: {error}"))?
        .error_for_status()
        .map_err(|error| format!("Plex rejected harbringer publication: {error}"))?;
    info!("Plex device announce accepted");
    Ok(())
}

fn plex_device_announce_url(machine_identifier: &str) -> String {
    format!(
        "https://servers.plex.tv/devices/{}",
        url_encode(machine_identifier)
    )
}

fn plex_device_announce_query<'a>(
    urls: &'a [String],
    https_enabled: bool,
    server_token: &'a str,
) -> Vec<(&'static str, &'a str)> {
    urls.iter()
        .map(|url| ("Connection[][uri]", url.as_str()))
        .chain([
            ("httpsEnabled", if https_enabled { "1" } else { "0" }),
            ("httpsRequired", "0"),
            ("dnsRebindingProtection", "0"),
            ("X-Plex-Token", server_token),
        ])
        .collect()
}

fn plex_server_registration_xml(state: &AppState) -> Result<String, String> {
    let bind = state
        .config
        .lock()
        .map_err(|_| "could not read harbringer configuration".to_owned())?
        .bind
        .parse::<SocketAddr>()
        .map_err(|_| "harbringer has an invalid bind address".to_owned())?;
    let local_addresses = plex_local_addresses(state);
    let local_addresses = local_addresses
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let libraries = state
        .config
        .lock()
        .map_err(|_| "could not read harbringer libraries".to_owned())?
        .libraries
        .clone();
    let mut directories = String::new();
    for (index, library) in libraries.iter().enumerate() {
        let section = index + 1;
        directories.push_str(&format!(
            "<Directory key=\"{section}\" type=\"{}\" title=\"{}\" uuid=\"mplex-{section}\" />",
            xml_escape(plex_kind(&library.kind)),
            xml_escape(&library.name),
        ));
    }
    let mut registration_date = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch".to_owned())?
        .as_micros() as i64
        & !0xf;
    registration_date ^= (registration_date >> 3) & 0x38;
    registration_date ^= (registration_date >> 3) & 0x7;
    Ok(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><MediaContainer size=\"{}\"><Server name=\"{}\" localAddresses=\"{}\" port=\"{}\" machineIdentifier=\"{}\" version=\"1.43.4.10903-e5521bd8c\" date=\"{}\" relay=\"0\" />{directories}</MediaContainer>",
        libraries.len() + 1,
        xml_escape(&server_name(state)),
        xml_escape(&local_addresses),
        bind.port(),
        xml_escape(&state.machine_identifier),
        registration_date,
    ))
}

fn plex_resources_contain(resources: &serde_json::Value, machine_identifier: &str) -> bool {
    resources.as_array().into_iter().flatten().any(|resource| {
        let identifier_matches = resource
            .get("clientIdentifier")
            .or_else(|| resource.get("machineIdentifier"))
            .and_then(serde_json::Value::as_str)
            == Some(machine_identifier);
        let provides_server = resource
            .get("provides")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|provides| {
                provides
                    .split(',')
                    .any(|capability| capability.trim() == "server")
            });
        let has_access_token = resource
            .get("accessToken")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|token| !token.is_empty());
        let has_connection = resource
            .get("connections")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|connections| {
                connections.iter().any(|connection| {
                    connection
                        .get("uri")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|uri| !uri.is_empty())
                        && connection
                            .get("address")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|address| !address.is_empty())
                        && connection
                            .get("protocol")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|protocol| !protocol.is_empty())
                        && connection
                            .get("port")
                            .and_then(serde_json::Value::as_u64)
                            .is_some_and(|port| port > 0)
                })
            });
        identifier_matches && provides_server && has_access_token && has_connection
    })
}

fn plex_resources_contain_local_https(
    resources: &serde_json::Value,
    machine_identifier: &str,
) -> bool {
    resources.as_array().into_iter().flatten().any(|resource| {
        resource
            .get("clientIdentifier")
            .and_then(serde_json::Value::as_str)
            == Some(machine_identifier)
            && resource
                .get("connections")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|connections| {
                    connections.iter().any(|connection| {
                        connection.get("local").and_then(serde_json::Value::as_bool) == Some(true)
                            && connection
                                .get("protocol")
                                .and_then(serde_json::Value::as_str)
                                == Some("https")
                            && connection
                                .get("uri")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|uri| {
                                    uri.starts_with("https://") && uri.contains(".plex.direct:")
                                })
                    })
                })
    })
}

fn extract_xml_token(body: &str) -> Option<String> {
    ["authentication-token", "authenticationToken", "authToken"]
        .iter()
        .find_map(|tag| {
            let start = body.find(&format!("<{tag}>"))? + tag.len() + 2;
            let end = body[start..].find(&format!("</{tag}>"))? + start;
            let value = body[start..end].trim();
            (!value.is_empty()).then(|| value.to_owned())
        })
}

fn plex_connection_urls(state: &AppState) -> Vec<String> {
    let bind = state
        .config
        .lock()
        .ok()
        .and_then(|config| config.bind.parse::<SocketAddr>().ok());
    let Some(bind) = bind else {
        return Vec::new();
    };
    plex_publish_urls(&plex_local_addresses(state), bind.port())
}

fn plex_publish_urls(addresses: &[std::net::IpAddr], port: u16) -> Vec<String> {
    // Match Plex Media Server and Ether: publish the local HTTP endpoint and
    // set httpsEnabled on the device record. Plex.tv then derives the trusted
    // plex.direct HTTPS URI from the LAN address and the issued certificate.
    addresses
        .iter()
        .map(|host| format!("http://{host}:{port}"))
        .collect()
}

fn plex_local_addresses(state: &AppState) -> Vec<std::net::IpAddr> {
    let bind = state
        .config
        .lock()
        .ok()
        .and_then(|config| config.bind.parse::<SocketAddr>().ok());
    let Some(bind) = bind else {
        return Vec::new();
    };
    if !bind.ip().is_unspecified() {
        return vec![bind.ip()];
    }
    let mut addresses = get_if_addrs::get_if_addrs()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|interface| match interface.addr {
            get_if_addrs::IfAddr::V4(address)
                if !address.ip.is_loopback() && !address.ip.is_unspecified() =>
            {
                Some(std::net::IpAddr::V4(address.ip))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        let fallback = std::net::UdpSocket::bind("0.0.0.0:0")
            .ok()
            .and_then(|socket| {
                socket.connect("8.8.8.8:80").ok()?;
                socket.local_addr().ok().map(|address| address.ip())
            })
            .unwrap_or_else(|| std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        addresses.push(fallback);
    }
    addresses
}

async fn settings_page(
    State(state): State<AppState>,
    Query(query): Query<SettingsQuery>,
) -> Response {
    let (libraries, tmdb_token_saved) = {
        let Ok(config) = state.config.lock() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        let libraries = config
            .libraries
            .iter()
            .enumerate()
            .map(|(id, library)| {
                (
                    id as i64,
                    library.name.clone(),
                    library.kind.clone(),
                    library
                        .folders
                        .iter()
                        .map(|path| path.to_string_lossy().into_owned())
                        .collect::<Vec<_>>(),
                    library.show_recently_added,
                    library.show_continue_watching,
                    library.scan_enabled,
                    library.scan_interval_value,
                    library.scan_interval_unit.clone(),
                )
            })
            .collect::<Vec<_>>();
        let token_saved = config
            .tmdb_bearer_token
            .as_deref()
            .is_some_and(|token| !token.trim().is_empty());
        (libraries, token_saved)
    };
    let list = libraries
        .iter()
        .map(
            |(
                id,
                name,
                kind,
                folders,
                show_recently_added,
                show_continue_watching,
                scan_enabled,
                interval_value,
                interval_unit,
            )| {
                let unit_options = ["seconds", "minutes", "hours"]
                    .into_iter()
                    .map(|unit| {
                        format!(
                            "<option value=\"{unit}\" {}>{unit}</option>",
                            if unit == interval_unit {
                                "selected"
                            } else {
                                ""
                            }
                        )
                    })
                    .collect::<String>();
                let library_settings = format!(
                    concat!(
                        "<form class=\"library-settings\" id=\"library-settings-{0}\" method=\"post\" action=\"/settings/libraries/{0}/save\" aria-label=\"Library settings\"></form><div class=\"library-rename\" style=\"display:flex;flex-wrap:nowrap;align-items:center;gap:.55rem\"><label for=\"library-name-{0}\" style=\"flex:0 0 auto\">Library name</label><input id=\"library-name-{0}\" name=\"name\" type=\"text\" form=\"library-settings-{0}\" value=\"{1}\" maxlength=\"64\" required style=\"flex:1 1 auto;width:auto;min-width:0\"></div>",
                        "<div class=\"library-scan-settings\"><strong>Automatic scan</strong><label><input type=\"checkbox\" name=\"scan_enabled\" value=\"true\" form=\"library-settings-{0}\" {2}> Enabled</label><label>Every <input type=\"number\" name=\"scan_interval_value\" min=\"1\" value=\"{3}\" required form=\"library-settings-{0}\"><select name=\"scan_interval_unit\" form=\"library-settings-{0}\">{4}</select></label>",
                        "<div class=\"library-scan-actions\" style=\"display:flex;flex:0 0 auto;align-items:center;gap:.4rem;white-space:nowrap\"><button type=\"submit\" form=\"library-settings-{0}\" formmethod=\"post\" formaction=\"/settings/libraries/{0}/scan\" formnovalidate>Scan metadata</button><button class=\"button-secondary\" type=\"submit\" form=\"library-settings-{0}\" formmethod=\"post\" formaction=\"/settings/libraries/{0}/scan?force=true\" formnovalidate>Force rescan</button></div></div>",
                        "<div class=\"library-display\" style=\"margin-top:.8rem\"><fieldset><legend>Show on Home</legend><label><input type=\"checkbox\" name=\"show_recently_added\" value=\"true\" form=\"library-settings-{0}\" {5}> Recently Added</label><label><input type=\"checkbox\" name=\"show_continue_watching\" value=\"true\" form=\"library-settings-{0}\" {6}> Continue Watching</label></fieldset></div>"
                    ),

                    id,
                    html(name),
                    if *scan_enabled { "checked" } else { "" },
                    interval_value,
                    unit_options,
                    if *show_recently_added { "checked" } else { "" },
                    if *show_continue_watching { "checked" } else { "" },
                );
                let add_folder_control = format!(
                    "<details class=\"library-add-folder\" style=\"margin:0\"><summary style=\"min-height:2.65rem;padding:.65rem .95rem;font-size:.87rem\">Add folder</summary><form method=\"post\" action=\"/settings/folders\" class=\"inline-form\"><input type=\"hidden\" name=\"library_id\" value=\"{id}\"><label class=\"sr-only\" for=\"folder-{id}\">Folder path</label><input id=\"folder-{id}\" name=\"path\" type=\"text\" placeholder=\"/another/folder\" required><button type=\"submit\">Add folder</button><button class=\"button-secondary\" type=\"button\" onclick=\"this.form.reset();this.closest('details').open=false\">Cancel</button></form></details>"
                );
                let folder_rows = folders
                    .iter()
                    .enumerate()
                    .map(|(folder_index, path)| {
                        let folder_path = html(path);
                        format!(
                            "<li class=\"library-folder-row\" style=\"display:flex;align-items:center;justify-content:space-between;gap:.55rem\"><code style=\"flex:1 1 auto;min-width:0\">{folder_path}</code><form method=\"post\" action=\"/settings/folders/remove\" class=\"inline-form\" data-settings-delete=\"folder\" style=\"margin:0\" onsubmit=\"return confirm('Remove this folder from the library? Media files will not be deleted.')\"><input type=\"hidden\" name=\"library_id\" value=\"{id}\"><input type=\"hidden\" name=\"folder_index\" value=\"{folder_index}\"><input type=\"hidden\" name=\"path\" value=\"{folder_path}\"><button class=\"button-secondary\" type=\"submit\" style=\"min-height:2.2rem;padding:.45rem .65rem;font-size:.8rem\">Remove folder</button></form></li>"
                        )
                    })
                    .collect::<String>();
                format!(
                    "<details class=\"library-card\" data-library-id=\"{id}\"><summary class=\"library-summary\"><span class=\"library-summary-title\"><strong>{}</strong><span class=\"type-pill\">{}</span></span><span class=\"folder-count\">{} folders</span></summary><div class=\"library-content\">{}<section class=\"library-folders\" aria-label=\"Folders\"><div style=\"display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:.55rem;margin-bottom:.6rem\"><h4 style=\"margin:0\">Folders</h4>{}</div><ul>{}</ul></section><div class=\"library-footer\" style=\"display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:1rem;margin-top:.9rem\"><form method=\"post\" action=\"/settings/libraries/{}/delete\" class=\"library-delete\" data-settings-delete=\"library\" style=\"display:flex;justify-content:flex-start;margin:0\" onsubmit=\"return confirm('Delete this library from Harbringer? Media files will not be deleted.')\"><button class=\"button-danger\" type=\"submit\">Delete library</button></form><button class=\"button-secondary\" type=\"submit\" form=\"library-settings-{id}\" style=\"margin-left:auto\">Save changes</button></div></div></details>",
                    html(name),
                    html(library_kind_label(kind)),
                    folders.len(),
                    library_settings,
                    add_folder_control,
                    folder_rows,
                    id,
                )
            },
        )
        .collect::<String>();
    let mut list = list;
    list.insert_str(
        0,
        r#"<form class="library-card" method="post" action="/settings/scan"><button type="submit">Scan all libraries</button></form>"#,
    );
    let error = query
        .error
        .as_deref()
        .map(|message| {
            format!(
                "<div class=\"flash-error\" role=\"alert\">{}</div>",
                html(message)
            )
        })
        .unwrap_or_default();
    let error = if error.is_empty() && query.inventory_error.unwrap_or(false) {
        "<div class=\"flash-error\" role=\"alert\">One or more libraries could not be scanned. Check the server log for details.</div>".to_owned()
    } else {
        error
    };
    let notice = query
        .metadata_matched
        .map(|count| {
            format!(
                "<div class=\"flash-notice\" role=\"status\">Matched {} metadata records.</div>",
                count
            )
        })
        .or_else(|| {
            query.metadata_status.as_deref().map(|status| {
                format!(
                    "<div class=\"flash-notice\" role=\"status\">{}</div>",
                    html(status)
                )
            })
        })
        .or_else(|| {
            query.server_name_saved.map(|_| {
                "<div class=\"flash-notice\" role=\"status\">Server name saved.</div>".to_owned()
            })
        })
        .or_else(|| {
            query.inventory_scan.map(|count| {
                format!(
                    "<div class=\"flash-notice\" role=\"status\">Scanned all libraries; found {count} media files.</div>"
                )
            })
        })
        .or_else(|| {
            query.library_created.filter(|created| *created).map(|_| {
                "<div class=\"flash-notice\" role=\"status\">Library created and added to Plex discovery.</div>".to_owned()
            })
        })
        .or_else(|| {
            query.library_renamed.filter(|saved| *saved).map(|_| {
                "<div class=\"flash-notice\" role=\"status\">Library name saved.</div>".to_owned()
            })
        })
        .or_else(|| {
            query.library_saved.filter(|saved| *saved).map(|_| {
                "<div class=\"flash-notice\" role=\"status\">Library settings saved.</div>".to_owned()
            })
        })
        .unwrap_or_default();
    let logs_active = query.tab.as_deref() == Some("logs");
    let settings_stack_style = if logs_active {
        " style=\"display:none\""
    } else {
        ""
    };
    let log_lines = recent_log_lines();
    let logs_content = if logs_active {
        let output = if log_lines.is_empty() {
            "<p class=\"note\">No log entries yet.</p>".to_owned()
        } else {
            format!(
                "<pre class=\"settings-logs\">{}</pre>",
                html(&log_lines.join("\n"))
            )
        };
        format!(
            "<section class=\"settings-card\" aria-labelledby=\"logs-heading\"><div class=\"section-heading\"><div><h2 id=\"logs-heading\">Logs</h2><p class=\"section-description\">Recent server and request events. The server keeps up to 1000 entries in memory.</p></div><a class=\"button-link button-secondary\" href=\"/settings?tab=logs\">Refresh</a></div>{output}</section>"
        )
    } else {
        String::new()
    };
    let notice = format!(
        "<nav class=\"settings-tabs\" aria-label=\"Settings sections\"><a class=\"{}\" href=\"/settings\">Libraries</a><a class=\"{}\" href=\"/settings?tab=logs\">Logs</a></nav>{}<dialog id=\"folder-picker\" aria-labelledby=\"folder-picker-title\" style=\"position:relative;width:min(48rem,calc(100vw - 2rem));height:min(38rem,calc(100vh - 2rem));padding:3.5rem 0 0;border:1px solid #394a63;border-radius:1rem;color:#e5eaf4;background:#101a2b\"><header style=\"position:absolute;top:.65rem;left:1rem;right:1rem;display:flex;align-items:center;justify-content:space-between\"><h2 id=\"folder-picker-title\">Choose a media folder</h2><button id=\"folder-picker-close\" type=\"button\" aria-label=\"Close folder picker\" title=\"Close folder picker\">×</button></header><iframe id=\"folder-picker-frame\" title=\"Media folder browser\" src=\"/settings/browse\" style=\"width:100%;height:calc(100% - 1rem);border:0;border-radius:0 0 1rem 1rem\"></iframe></dialog><script>document.addEventListener('DOMContentLoaded',()=>{{const params=new URLSearchParams(location.search);if(params.get('add_library')==='true')document.querySelector('.add-library')?.setAttribute('open','');const openLibrary=Number(params.get('library_open'));if(Number.isInteger(openLibrary)&&openLibrary>=0)document.querySelectorAll('.library-list details.library-card')[openLibrary]?.setAttribute('open','');}});</script>",
        if logs_active { "" } else { "active" },
        if logs_active { "active" } else { "" },
        notice
    );
    let current_server_name = html(&server_name(&state));
    let folder_picker_script = r#"<style>#folder-picker[open]{position:fixed!important;inset:0!important;margin:auto!important}#folder-picker::backdrop{background:rgba(4,8,16,.78)}#folder-picker-close{position:absolute;top:.3rem;right:0}</style><script>
const folderPicker = document.querySelector('#folder-picker');
const folderPickerFrame = document.querySelector('#folder-picker-frame');
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.library-add-folder .inline-form').forEach(form => {
    const input = form.querySelector('input[name="path"]');
    if (!input) return;
    const browse = document.createElement('a');
    browse.href = '/settings/browse';
    browse.className = 'button-link button-secondary';
    browse.textContent = 'Browse';
    input.insertAdjacentElement('afterend', browse);
  });
  if (!new URLSearchParams(location.search).has('library_open')) {
    document.querySelectorAll('.library-list details.library-card').forEach(card => card.removeAttribute('open'));
  }
});
document.addEventListener('click', event => {
  const browse = event.target.closest?.('a[href="/settings/browse"]');
  if (browse) {
    event.preventDefault();
    const input = browse.previousElementSibling;
    if (!input || !folderPicker) return;
    folderPicker.dataset.target = input.id;
    const url = new URL(browse.href, location.origin);
    if (input.value.trim()) url.searchParams.set('path', input.value.trim());
    folderPickerFrame.src = url.pathname + url.search;
    folderPicker.showModal();
    return;
  }
  if (event.target.closest?.('#folder-picker-close')) folderPicker?.close();
});
window.addEventListener('message', event => {
  if (event.origin !== location.origin || event.data?.type !== 'mplex-folder-selected') return;
  const input = document.getElementById(folderPicker?.dataset.target || '');
  if (input) input.value = event.data.path;
  folderPicker?.close();
});
</script>"#;

    let settings_delete_script = r#"<style>.settings-delete-status{flex-basis:100%;margin:0;color:#ffd6dc;font-size:.8rem}</style><script>
if (!window.__mplexSettingsDeleteBound) {
  window.__mplexSettingsDeleteBound = true;
  document.addEventListener("submit", async event => {
    const form = event.target.closest("form[data-settings-delete]");
    if (!form || event.defaultPrevented || form.dataset.pending === "true") return;
    event.preventDefault();
    form.dataset.pending = "true";
    form.setAttribute("aria-busy", "true");
    const button = form.querySelector('button[type="submit"]');
    const originalLabel = button?.textContent || "Delete";
    if (button) {
      button.disabled = true;
      button.textContent = form.dataset.settingsDelete === "folder" ? "Removing…" : "Deleting…";
    }
    form.querySelector(".settings-delete-status")?.remove();
    const showError = message => {
      const status = document.createElement("p");
      status.className = "settings-delete-status";
      status.setAttribute("role", "alert");
      status.textContent = message || "Could not complete this action. Try again.";
      form.append(status);
    };
    try {
      const response = await fetch(form.action, {
        method: "POST",
        headers: {
          "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
          "X-Mplex-Async": "1"
        },
        body: new URLSearchParams(new FormData(form))
      });
      if (!response.ok) throw new Error((await response.text()).trim());
      if (form.dataset.settingsDelete === "folder") {
        const card = form.closest("details.library-card");
        form.closest("li.library-folder-row")?.remove();
        const folderCount = card?.querySelectorAll(".library-folders ul > li").length || 0;
        const countLabel = card?.querySelector(".folder-count");
        if (countLabel) countLabel.textContent = `${folderCount} folder${folderCount === 1 ? "" : "s"}`;
        return;
      }
      const deletedId = Number(form.closest("details.library-card")?.dataset.libraryId);
      const scrollTop = window.scrollY;
      const openLibraryIds = new Set(
        [...document.querySelectorAll(".library-list details.library-card[open]")]
          .map(card => Number(card.dataset.libraryId))
          .filter(id => Number.isInteger(id) && id !== deletedId)
          .map(id => id > deletedId ? id - 1 : id)
      );
      const page = await fetch("/settings", { cache: "no-store" });
      if (!page.ok) throw new Error("Library deleted. Reload Settings to refresh the library list.");
      const parsed = new DOMParser().parseFromString(await page.text(), "text/html");
      const currentList = document.querySelector(".library-list");
      const nextList = parsed.querySelector(".library-list");
      const currentNav = document.querySelector(".sidebar-nav");
      const nextNav = parsed.querySelector(".sidebar-nav");
      if (!currentList || !nextList || !currentNav || !nextNav) {
        throw new Error("Library deleted. Reload Settings to refresh the library list.");
      }
      currentList.replaceWith(nextList);
      currentNav.replaceWith(nextNav);
      nextList.querySelectorAll("details.library-card[data-library-id]").forEach(card => {
        if (openLibraryIds.has(Number(card.dataset.libraryId))) card.open = true;
      });
      window.scrollTo(0, scrollTop);
    } catch (error) {
      form.dataset.pending = "false";
      form.removeAttribute("aria-busy");
      if (button) {
        button.disabled = false;
        button.textContent = originalLabel;
      }
      showError(error instanceof Error ? error.message : "Could not complete this action. Try again.");
    }
  });
}
</script>"#;
    let notice = format!("{notice}{folder_picker_script}{settings_delete_script}");
    let tmdb_token_status = if tmdb_token_saved {
        "<p class=\"credential-status is-saved\"><span class=\"status-dot\"></span>TMDB token is saved. Leave the field blank to keep it, or enter a new token to replace it.</p>"
    } else {
        "<p class=\"credential-status\">No TMDB token is saved. Add one to scan libraries for metadata.</p>"
    };
    let metadata_enabled = state
        .config
        .lock()
        .is_ok_and(|config| config.metadata_storage == "sidecar");
    let plex_status = state
        .db
        .lock()
        .ok()
        .and_then(|connection| {
            connection
                .query_row(
                    "SELECT plex_account_token IS NOT NULL, claimed, plex_last_error FROM server_identity WHERE id = 1",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, bool>(0)?,
                            row.get::<_, bool>(1)?,
                            row.get::<_, Option<String>>(2)?,
                        ))
                    },
                )
                .ok()
        })
        .unwrap_or((false, false, None));
    let plex_auth = if plex_status.1 {
        if let Some(error) = plex_status.2.as_deref() {
            format!(
                "<p class=\"flash-error\" role=\"status\">Signed in, but secure Plex client publication failed: {}. HTTP access remains published.</p><div class=\"action-row\"><form method=\"get\" action=\"/settings/plex/start\"><button type=\"submit\">Reconnect to Plex.tv</button></form><form method=\"post\" action=\"/settings/plex/disconnect\"><button class=\"button-secondary\" type=\"submit\">Disconnect and revoke</button></form></div>",
                html(error)
            )
        } else {
            r#"<p class="status-line"><span class="status-dot"></span>Connected to Plex.tv and published for Plex clients.</p><div class="action-row"><form method="get" action="/settings/plex/start"><button type="submit">Reconnect to Plex.tv</button></form><form method="post" action="/settings/plex/disconnect" onsubmit="return confirm('Disconnect and revoke harbringer from Plex.tv?')"><button class="button-secondary" type="submit">Disconnect and revoke</button></form></div>"#.to_owned()
        }
    } else if plex_status.0 {
        match plex_status.2.as_deref() {
            Some(error) => format!(
                "<p class=\"flash-error\" role=\"status\">harbringer is signed in to Plex.tv, but server claiming or resource publication failed: {}.</p><div class=\"action-row\"><form method=\"get\" action=\"/settings/plex/start\"><button type=\"submit\">Connect to Plex.tv again</button></form><form method=\"post\" action=\"/settings/plex/disconnect\"><button class=\"button-secondary\" type=\"submit\">Disconnect and revoke</button></form></div>",
                html(error)
            ),
            None => r#"<p class="flash-error" role="status">harbringer has an older Plex.tv account token but has not completed server claiming or resource publication.</p><div class="action-row"><form method="get" action="/settings/plex/start"><button type="submit">Connect to Plex.tv again</button></form><form method="post" action="/settings/plex/disconnect"><button class="button-secondary" type="submit">Disconnect and revoke</button></form></div>"#.to_owned(),
        }
    } else {
        r#"<p class="note">Connect harbringer to your Plex account so Plex clients can discover and authenticate with this server.</p><div class="action-row"><form method="get" action="/settings/plex/start"><button type="submit">Connect to Plex.tv</button></form></div>"#.to_owned()
    };
    Html(format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>harbringer settings</title><style>
:root {{ color-scheme: dark; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #0b1020; color: #e5eaf4; font-synthesis: none; }}
* {{ box-sizing: border-box; }} body {{ margin: 0; min-height: 100vh; background: radial-gradient(ellipse at 75% -20%, #1b3151 0, transparent 44rem), #0b1020; }}
.app-shell {{ display: grid; grid-template-columns: 240px minmax(0, 1fr); min-height: 100vh; transition: grid-template-columns .2s ease; }} .app-shell.sidebar-collapsed {{ grid-template-columns: 68px minmax(0, 1fr); }}
.sidebar {{ position: sticky; top: 0; height: 100vh; box-sizing: border-box; padding: 1.25rem .9rem; border-right: 1px solid #202a3c; background: rgba(10, 16, 29, .9); }} .sidebar-brand {{ display: block; padding: .5rem .7rem 1.5rem; color: #f8fafc; font-size: 1.25rem; font-weight: 750; text-decoration: none; }} .sidebar-top {{ display: flex; align-items: center; justify-content: space-between; padding-bottom: 1rem; }} .sidebar-top .sidebar-brand {{ padding-bottom: 0; }}
.sidebar-toggle {{ width: 2rem; height: 2rem; border: 0; border-radius: .5rem; color: #cbd5e1; background: transparent; cursor: pointer; font-size: 1.15rem; }} .sidebar-collapsed .sidebar-top {{ justify-content: center; }} .sidebar-collapsed .sidebar-brand, .sidebar-collapsed .sidebar-label, .sidebar-collapsed .sidebar-link {{ font-size: 0; }} .sidebar-collapsed .sidebar-link {{ justify-content: center; }} .sidebar-collapsed .sidebar-icon {{ font-size: 1.1rem; }} .sidebar-label {{ padding: 0 .7rem .45rem; color: #77839a; font-size: .68rem; font-weight: 700; letter-spacing: .12em; text-transform: uppercase; }}
.sidebar-nav {{ display: grid; gap: .3rem; }} .sidebar-link {{ display: flex; align-items: center; gap: .7rem; padding: .68rem .7rem; border-radius: .55rem; color: #aeb9cc; text-decoration: none; }} .sidebar-link:hover {{ background: #17243a; color: #fff; }} .sidebar-link.active {{ background: #1d4ed8; color: #fff; }} .sidebar-icon {{ width: 1.3rem; text-align: center; font-size: 1.1rem; }}
main {{ min-width: 0; width: min(100%, 1020px); margin: 0 auto; padding: clamp(1.5rem, 4vw, 3.25rem); }} a {{ color: #9bc5ff; }} .page-heading {{ margin: 0 0 1.8rem; }} .eyebrow {{ margin: 0 0 .45rem; color: #82a9e5; font-size: .74rem; font-weight: 750; letter-spacing: .14em; text-transform: uppercase; }} h1 {{ margin: 0; color: #f8fafc; font-size: clamp(2rem, 4vw, 2.65rem); letter-spacing: -.04em; }} .page-subtitle {{ max-width: 48rem; margin: .6rem 0 0; color: #9aa8bf; line-height: 1.65; }}
.settings-stack {{ display: grid; gap: 1.15rem; }} .settings-card {{ min-width: 0; padding: clamp(1.1rem, 2.8vw, 1.65rem); border: 1px solid #29364b; border-radius: 1rem; background: linear-gradient(145deg, rgba(22, 32, 50, .94), rgba(15, 23, 39, .97)); box-shadow: 0 18px 48px rgba(0, 0, 0, .14); }} .section-heading {{ display: flex; align-items: flex-start; justify-content: space-between; gap: 1rem; margin-bottom: 1.15rem; }} h2 {{ margin: 0; color: #f1f5fb; font-size: 1.18rem; letter-spacing: -.015em; }} h3 {{ margin: 0; color: #f1f5fb; font-size: 1.05rem; }} h4 {{ margin: 0 0 .6rem; color: #aab8cc; font-size: .77rem; letter-spacing: .07em; text-transform: uppercase; }} .note, .section-description {{ color: #98a6bb; line-height: 1.6; }} .section-description {{ max-width: 48rem; margin: .45rem 0 0; font-size: .91rem; }}
.flash-error, .flash-notice {{ margin: 0 0 1rem; padding: .85rem 1rem; border-radius: .7rem; line-height: 1.5; }} .flash-error {{ border: 1px solid #793f50; color: #ffd6dc; background: #381d2a; }} .flash-notice {{ border: 1px solid #286a52; color: #c2f5d8; background: #123426; }} .status-line {{ display: flex; align-items: center; gap: .65rem; margin: 0; color: #c4d3e8; }} .status-dot {{ width: .58rem; height: .58rem; border-radius: 50%; background: #4ade80; box-shadow: 0 0 0 .24rem rgba(74, 222, 128, .13); }} .credential-status {{ display: flex; align-items: center; gap: .65rem; margin: -.35rem 0 0; color: #9aa8bf; font-size: .82rem; line-height: 1.55; }} .credential-status.is-saved {{ color: #bdebd1; }} .action-row {{ display: flex; flex-wrap: wrap; gap: .65rem; margin-top: 1rem; }}
.settings-form, .inline-form, .library-display {{ margin: 0; }} .settings-form {{ display: grid; gap: 1rem; }} .server-name-form {{ margin-top: 1.15rem; padding-top: 1.05rem; border-top: 1px solid #26354a; }} .server-name-form .field {{ max-width: 34rem; }} .field small {{ font-size: .78rem; font-weight: 400; }} .form-grid {{ display: grid; grid-template-columns: minmax(0, 1fr) minmax(180px, .6fr); gap: 1rem; }} .field {{ display: grid; min-width: 0; gap: .45rem; }} .field > span, .field > label {{ color: #c9d4e4; font-size: .84rem; font-weight: 650; }} input:not([type="checkbox"]):not([type="radio"]), select {{ width: 100%; min-width: 0; min-height: 2.75rem; padding: .68rem .78rem; border: 1px solid #3a4961; border-radius: .6rem; outline: none; color: #f3f6fb; background: #0e1727; font: inherit; }} input::placeholder {{ color: #738198; }} input:focus, select:focus {{ border-color: #60a5fa; box-shadow: 0 0 0 3px rgba(96, 165, 250, .17); }}
button, .button-link {{ display: inline-flex; min-height: 2.65rem; align-items: center; justify-content: center; gap: .4rem; padding: .65rem .95rem; border: 1px solid transparent; border-radius: .6rem; color: #fff; background: #2563eb; font: inherit; font-size: .87rem; font-weight: 700; text-decoration: none; cursor: pointer; transition: background .15s, border-color .15s, transform .15s; }} button:hover, .button-link:hover {{ background: #3476f6; }} button:active, .button-link:active {{ transform: translateY(1px); }} button:focus-visible, a:focus-visible, summary:focus-visible {{ outline: 2px solid #93c5fd; outline-offset: 2px; }} .button-secondary {{ border-color: #3a4961; color: #d4deec; background: #182337; }} .button-secondary:hover {{ border-color: #53647e; background: #22314a; }} .button-quiet {{ min-width: 2.65rem; padding-inline: .72rem; border-color: #3a4961; color: #c7d1df; background: #182337; }}
.metadata-options {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: .8rem; }} .metadata-option {{ display: flex; min-height: 9rem; align-items: flex-start; gap: .8rem; padding: 1rem; border: 1px solid #35445b; border-radius: .75rem; background: rgba(12, 21, 36, .68); cursor: pointer; transition: border-color .15s, background .15s; }} .metadata-option:has(input:checked) {{ border-color: #4e8cec; background: rgba(28, 65, 120, .22); }} .metadata-option input {{ flex: 0 0 auto; width: 1.05rem; height: 1.05rem; margin: .14rem 0 0; accent-color: #4389f5; }} .option-copy {{ display: grid; gap: .55rem; }} .option-copy strong {{ color: #e7edf7; }} .option-copy .note {{ margin: 0; font-size: .84rem; }} code {{ color: #c5dcff; font: .88em ui-monospace, SFMono-Regular, Menlo, monospace; }}
.library-list {{ display: grid; gap: .85rem; margin: 1.2rem 0 0; padding: 0; list-style: none; }} .library-card {{ min-width: 0; padding: 1.05rem; border: 1px solid #2c3a50; border-radius: .8rem; background: rgba(10, 17, 30, .46); }} .library-heading {{ display: flex; align-items: center; justify-content: space-between; gap: 1rem; }} .library-heading > div {{ display: flex; flex-wrap: wrap; align-items: center; gap: .65rem; }} .type-pill {{ padding: .28rem .55rem; border: 1px solid #354a68; border-radius: 999px; color: #b9cee9; background: #17243a; font-size: .72rem; font-weight: 700; }} .library-folders {{ margin-top: 1.15rem; }} .library-folders ul {{ display: grid; gap: .45rem; margin: 0; padding: 0; list-style: none; }} .library-folders li {{ min-width: 0; margin: 0; padding: .6rem .7rem; overflow-wrap: anywhere; border-radius: .48rem; background: #0d1727; }} .library-folders code {{ overflow-wrap: anywhere; }} .library-add-folder, .add-library {{ margin-top: .8rem; }} .add-library {{ margin: 0; }} .add-library .settings-form {{ margin-top: 1rem; }} summary {{ display: inline-flex; align-items: center; gap: .5rem; padding: .48rem .68rem; border: 1px solid #394a63; border-radius: .55rem; color: #bfcee2; background: #172337; font-size: .82rem; font-weight: 650; cursor: pointer; list-style: none; }} summary::before {{ content: "+"; color: #83b4ff; font-size: 1rem; }} details[open] summary::before {{ content: "−"; }} summary::-webkit-details-marker {{ display: none; }} .inline-form {{ display: flex; flex-wrap: wrap; align-items: end; gap: .55rem; margin-top: .75rem; }} .inline-form input:not([type="hidden"]) {{ flex: 1 1 18rem; }} .path-list {{ display: grid; gap: .55rem; }} .path-row {{ display: flex; flex-wrap: wrap; align-items: center; gap: .5rem; }} .path-row input {{ flex: 1 1 18rem; }} .path-row .button-link {{ flex: 0 0 auto; }} .library-tools {{ display: grid; grid-template-columns: minmax(0, auto) minmax(0, 1fr); align-items: start; gap: 1rem; margin-top: 1.1rem; padding-top: 1rem; border-top: 1px solid #26354a; }} .library-tools > .inline-form {{ margin: 0; }} .library-display fieldset {{ display: flex; flex-wrap: wrap; align-items: center; gap: .6rem 1rem; min-width: 0; margin: 0; padding: 0; border: 0; }} .library-display legend {{ width: 100%; margin-bottom: .55rem; color: #91a0b6; font-size: .72rem; font-weight: 750; letter-spacing: .09em; text-transform: uppercase; }} .library-display label {{ display: inline-flex; align-items: center; gap: .4rem; color: #c6d0df; font-size: .82rem; }} input[type="checkbox"] {{ width: 1rem; height: 1rem; margin: 0; accent-color: #4389f5; }} .library-display button {{ min-height: 2.3rem; padding: .48rem .7rem; font-size: .78rem; }} .empty-state {{ margin: 0; padding: 1rem; border: 1px dashed #35445b; border-radius: .7rem; color: #9aa8bf; text-align: center; }} .sr-only {{ position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0,0,0,0); white-space: nowrap; border: 0; }}
.library-card {{ padding: 0; overflow: hidden; }} .library-summary {{ display: flex; width: 100%; align-items: center; justify-content: space-between; gap: .8rem; padding: 1rem 1.05rem; border: 0; border-radius: 0; color: #e8eef8; background: transparent; font-size: .95rem; text-align: left; }} .library-summary::before {{ content: none !important; }} .library-summary::after {{ content: "⌄"; margin-left: auto; color: #91a8c7; transition: transform .15s; }} .library-card[open] > .library-summary::after {{ transform: rotate(180deg); }} .library-summary-title {{ display: flex; flex-wrap: wrap; align-items: center; gap: .65rem; }} .folder-count {{ flex: 0 0 auto; color: #91a0b6; font-size: .78rem; font-weight: 500; }} .library-content {{ padding: 0 1.05rem 1.05rem; }} .library-tools {{ grid-template-columns: minmax(0, auto) minmax(0, 1fr) auto; }} .library-delete {{ display: flex; justify-content: flex-end; }} .button-danger {{ border-color: #713841; color: #ffd3d7; background: #3a202a; }} .button-danger:hover {{ border-color: #9d4b55; background: #512630; }}
@media (max-width: 820px) {{ .library-tools {{ grid-template-columns: 1fr; }} .library-delete {{ justify-content: flex-start; }} }} @media (max-width: 700px) {{ .app-shell {{ display: block; }} .sidebar {{ position: static; height: auto; padding: .7rem; border-right: 0; border-bottom: 1px solid #263244; }} .sidebar-top {{ padding-bottom: .35rem; }} .sidebar-brand {{ padding: .35rem .55rem; }} .sidebar-toggle {{ display: none; }} .sidebar-label {{ display: none; }} .sidebar-nav {{ display: flex; overflow-x: auto; }} .sidebar-link {{ white-space: nowrap; }} main {{ padding: 1.35rem 1rem 2.5rem; }} }} @media (max-width: 560px) {{ .metadata-options, .form-grid {{ grid-template-columns: 1fr; }} .metadata-option {{ min-height: auto; }} .section-heading {{ display: block; }} .library-tools .inline-form > * {{ flex: 1 1 auto; }} .library-display fieldset {{ align-items: flex-start; }} .action-row > form, .action-row button {{ width: 100%; }} .action-row > form {{ flex: 1 1 100%; }} }}
.library-scan-settings {{ display: flex; grid-column: 1 / -1; flex-wrap: wrap; align-items: center; gap: .4rem; padding-top: .9rem; border-top: 1px solid #26354a; }} .library-scan-settings > strong {{ margin-right: .25rem; }} .library-scan-settings label {{ display: flex; align-items: center; gap: .4rem; color: #c9d4e4; font-size: .84rem; }} .library-scan-settings input[type="number"] {{ width: 5.5rem; }} .library-scan-settings select {{ width: auto; }}

.library-add-folder[open] > summary::before {{ content: "+"; }}
.settings-tabs {{ display: flex; gap: .35rem; margin: 0 0 1.15rem; padding-bottom: .7rem; border-bottom: 1px solid #29364b; }} .settings-tabs a {{ padding: .55rem .8rem; border-radius: .5rem; color: #aeb9cc; text-decoration: none; }} .settings-tabs a.active, .settings-tabs a:hover {{ color: #fff; background: #1d4ed8; }} .settings-logs {{ max-height: 70vh; margin: 0; padding: 1rem; overflow: auto; border: 1px solid #29364b; border-radius: .65rem; color: #d4deec; background: #0a111e; font: .8rem/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; white-space: pre-wrap; overflow-wrap: anywhere; }}
</style></head><body><div class="app-shell">{}
<main><header class="page-heading"><p class="eyebrow">harbringer</p><h1>Settings</h1><p class="page-subtitle">Manage Plex access, metadata storage, and the libraries available on this server.</p></header>{}{}
<div class="settings-stack"{}>
<section class="settings-card" aria-labelledby="plex-heading"><div class="section-heading"><div><h2 id="plex-heading">Plex account</h2><p class="section-description">Connect harbringer to Plex so compatible clients can discover and authenticate with this server.</p></div></div>{}<form method="post" action="/settings/server-name" class="settings-form server-name-form"><label class="field"><span>Server name</span><input name="server_name" type="text" value="{}" maxlength="64" required><small class="note">This is the name Plex clients see. It defaults to harbringer.</small></label><div><button class="button-secondary" type="submit">Save server name</button></div></form></section>
<section class="settings-card" aria-labelledby="metadata-heading"><div class="section-heading"><div><h2 id="metadata-heading">Metadata storage</h2><p class="section-description">Choose one place for matched movie and TV metadata. Sidecars let another mplex server reuse matches and artwork without its database or a new TMDB lookup. SQLite still stores server identity, library configuration, and playback progress.</p></div></div><form method="post" action="/settings/preferences" class="settings-form"><div class="metadata-options"><label class="metadata-option"><input type="radio" name="metadata_storage" value="database" {}><span class="option-copy"><strong>SQLite database {}</strong><span class="note">Matched metadata stays in this server’s database.</span></span></label><label class="metadata-option"><input type="radio" name="metadata_storage" value="sidecar" {}><span class="option-copy"><strong>.mplex files in media folders {}</strong><span class="note">Artwork and metadata are written to <code>.mplex/cover.jpg</code>, <code>banner.jpg</code>, and <code>data.json</code>.</span></span></label></div><label class="field"><span>TMDB bearer token</span><input name="tmdb_bearer_token" type="password" autocomplete="new-password" placeholder="Enter a token to save or replace it"></label>{}<div><button type="submit">Save metadata settings</button></div></form></section>
<section class="settings-card" aria-labelledby="libraries-heading"><div class="section-heading"><div><h2 id="libraries-heading">Libraries</h2><p class="section-description">A library groups media of one type from one or more monitored folders. Add a folder to an existing library when the same collection spans multiple locations.</p></div></div><details class="add-library"><summary>Add a library</summary><form method="post" action="/settings/libraries" class="settings-form"><div class="form-grid"><label class="field"><span>Library name</span><input name="name" type="text" placeholder="e.g. Films" required></label><label class="field"><span>Content type</span><select name="kind"><option value="movie">Movies</option><option value="tv">TV Shows</option><option value="other">Other</option></select></label></div><div class="field"><span>Media folders</span><div class="path-list" id="new-library-paths"><div class="path-row"><label class="sr-only" for="new-path-0">Folder path</label><input id="new-path-0" name="path[]" type="text" placeholder="/path/to/folder" value="{}" required><a class="button-link button-secondary" href="/settings/browse">Browse</a><button type="button" class="button-quiet remove-path" aria-label="Remove folder path" title="Remove folder path" disabled>−</button></div></div></div><div class="action-row path-actions"><button type="button" class="button-secondary" id="add-path" aria-label="Add another folder path" title="Add another folder path">＋ Add folder</button><button type="submit">Create library</button></div></form></details>
<div class="library-list">{}</div></section>
</div>{}
 </main></div><script>
const shell = document.querySelector('.app-shell'); const toggle = document.querySelector('.sidebar-toggle');
if (localStorage.getItem('mplex-sidebar-collapsed') === 'true') shell.classList.add('sidebar-collapsed');
toggle?.addEventListener('click', () => {{ const collapsed = shell.classList.toggle('sidebar-collapsed'); localStorage.setItem('mplex-sidebar-collapsed', collapsed ? 'true' : 'false'); }});
const pathList = document.querySelector('#new-library-paths'); const updatePathButtons = () => {{ const rows = [...pathList.querySelectorAll('.path-row')]; rows.forEach((row, index) => {{ const button = row.querySelector('.remove-path'); button.disabled = rows.length === 1; button.setAttribute('aria-label', `Remove folder path ${{index + 1}}`); button.title = `Remove folder path ${{index + 1}}`; }}); }};
document.querySelector('#add-path')?.addEventListener('click', () => {{ const index = pathList.querySelectorAll('.path-row').length; const row = document.createElement('div'); row.className = 'path-row'; row.innerHTML = `<label class="sr-only" for="new-path-${{index}}">Folder path</label><input id="new-path-${{index}}" name="path[]" type="text" placeholder="/path/to/folder" required><a class="button-link button-secondary" href="/settings/browse">Browse</a><button type="button" class="button-quiet remove-path">−</button>`; row.querySelector('.remove-path').addEventListener('click', () => {{ row.remove(); updatePathButtons(); }}); pathList.appendChild(row); updatePathButtons(); }});
</script></body></html>"#,
        sidebar_nav_for_settings(&state),
        error,
        notice,
        settings_stack_style,
        format!("<div class=\"section-content\">{}</div>", plex_auth),
        current_server_name,
        if metadata_enabled { "" } else { "checked" },
        if metadata_enabled { "" } else { "(current)" },
        if metadata_enabled { "checked" } else { "" },
        if metadata_enabled { "(current)" } else { "" },
        tmdb_token_status,
        html(query.folder.as_deref().unwrap_or("")),
        if list.is_empty() { "<p class=\"empty-state\">No libraries yet. Add a library to start organizing your media.</p>".to_owned() } else { list },
        logs_content
    ))
    .into_response()
}

async fn update_preferences(
    State(state): State<AppState>,
    Form(form): Form<PreferencesForm>,
) -> Response {
    let requested_token = form
        .tmdb_bearer_token
        .as_deref()
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_owned);
    if let Some(token) = requested_token.as_deref() {
        let Ok(response) = reqwest::Client::new()
            .get("https://api.themoviedb.org/3/configuration")
            .bearer_auth(token)
            .send()
            .await
        else {
            return settings_error("TMDB could not be reached; token was not saved");
        };
        if !response.status().is_success() {
            return settings_error("TMDB rejected this bearer token; token was not saved");
        }
    }
    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let token = requested_token.or_else(|| config.tmdb_bearer_token.clone());
    if form.metadata_storage.as_deref() == Some("sidecar")
        || form.metadata_in_media_folder.is_some()
    {
        config.metadata_storage = "sidecar".to_owned();
    } else if form.metadata_storage.as_deref() == Some("database") {
        config.metadata_storage = "database".to_owned();
    }
    config.tmdb_bearer_token = token;
    if save_config(&state.config_path, &config).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let location = if config.tmdb_bearer_token.is_some() {
        "/settings?metadata_status=TMDB%20token%20accepted"
    } else {
        "/settings?metadata_status=TMDB%20token%20cleared"
    };
    Redirect::to(location).into_response()
}

async fn update_server_name(
    State(state): State<AppState>,
    Form(form): Form<ServerNameForm>,
) -> Response {
    let name = form.server_name.trim();
    if name.is_empty() || name.chars().count() > 64 || name.chars().any(char::is_control) {
        return settings_error(
            "Server name must be 1–64 characters and contain no control characters",
        );
    }
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if connection
        .execute(
            "UPDATE server_identity SET server_name = ?1 WHERE id = 1",
            [name],
        )
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if let Ok(mut current_name) = state.server_name.write() {
        *current_name = name.to_owned();
    }
    Redirect::to("/settings?server_name_saved=true").into_response()
}

fn server_name(state: &AppState) -> String {
    state
        .server_name
        .read()
        .map(|name| name.clone())
        .unwrap_or_else(|_| "harbringer".to_owned())
}

#[axum::debug_handler]
async fn scan_library_metadata(
    State(state): State<AppState>,
    Path(library_id): Path<usize>,
    Query(scan_query): Query<ScanQuery>,
) -> Response {
    let Ok(config) = state.config.lock().map(|config| config.clone()) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Some(library) = config.libraries.get(library_id) else {
        return settings_error("Library does not exist");
    };
    if library.kind == "other" {
        info!(library = %library.name, "TMDB metadata matching skipped for other library kind");
        return Redirect::to("/settings?metadata_matched=0").into_response();
    }
    let client = reqwest::Client::new();
    let token = config
        .tmdb_bearer_token
        .as_deref()
        .filter(|token| !token.is_empty());
    let sidecar_mode = config.metadata_storage == "sidecar";
    let kind = if library.kind == "tv" { "tv" } else { "movie" };
    let mut matched = 0;
    let indexed_ids = media_snapshot(&state)
        .iter()
        .map(|media| (media.path.clone(), media.id.clone()))
        .collect::<HashMap<_, _>>();
    let forced = scan_query.force.unwrap_or(false);
    let refreshing = scan_query.refresh.unwrap_or(false);
    let target_path = match scan_query.path.as_deref() {
        Some(path) => match clean_relative_path(path) {
            Some(path) => Some(path),
            None => return settings_error("Media path is invalid"),
        },
        None => None,
    };
    if refreshing && target_path.is_none() {
        return settings_error("Refresh requires a media path");
    }
    info!(library = %library.name, forced, refreshing, "metadata scan started");
    for folder in &library.folders {
        let mut files = match scan_media(folder).await {
            Ok(files) => files,
            Err(error) => {
                warn!(library = %library.name, folder = %folder.display(), error = %error, "metadata scan could not read a library folder");
                continue;
            }
        };
        for media in &mut files {
            if let Some(id) = indexed_ids.get(&media.path) {
                media.id.clone_from(id);
            }
        }
        let mut groups = BTreeMap::<PathBuf, Vec<Media>>::new();
        for media in files {
            let group_path = metadata_group_path(folder, &media.path, kind);
            groups.entry(group_path).or_default().push(media);
        }
        for (group_path, group) in groups {
            if let Some(target_path) = &target_path {
                let group_relative = group_path.strip_prefix(folder).unwrap_or(&group_path);
                if group_relative != target_path {
                    continue;
                }
            }

            let group_title = metadata_group_title(&group_path, folder);
            if refreshing {
                if let Err(error) = refresh_matched_metadata(
                    &state,
                    &client,
                    token,
                    folder,
                    &group_path,
                    &group_title,
                    &group,
                    library_id,
                    kind,
                    sidecar_mode,
                )
                .await
                {
                    return settings_error(&error);
                }
                matched += 1;
                continue;
            }
            if !forced {
                let media_key = database_media_key(library_id + 1, &group[0].id);
                let selected = if sidecar_mode {
                    read_sidecar_metadata(&group_path)
                } else {
                    read_database_metadata(&state, &media_key)
                };
                if let Some(mut metadata) = selected.filter(|metadata| metadata.get("id").is_some())
                {
                    if kind == "tv"
                        && (metadata.get("genres").is_none()
                            || metadata.get("content_ratings").is_none())
                    {
                        if let Some(token) = token {
                            if let Some(details) =
                                tmdb_tv_show_details(&client, token, &metadata).await
                            {
                                metadata = merge_tv_show_details(&metadata, &details);
                                if sidecar_mode {
                                    if store_metadata_sidecar(
                                        folder,
                                        &group_path,
                                        &group_title,
                                        &group[0],
                                        &metadata,
                                    )
                                    .await
                                    .is_err()
                                    {
                                        return settings_error(
                                            "Could not update TV show details in the media-folder sidecar",
                                        );
                                    }
                                } else {
                                    for media in &group {
                                        let metadata = tv_metadata_for_media(
                                            &metadata,
                                            folder,
                                            &media.path,
                                            kind,
                                        );
                                        let key = database_media_key(library_id + 1, &media.id);
                                        if !write_database_metadata(&state, &key, &metadata) {
                                            return StatusCode::INTERNAL_SERVER_ERROR
                                                .into_response();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if sidecar_mode {
                        if (kind == "tv" || !metadata_sidecar_info_path(&group_path).is_file())
                            && store_metadata_sidecar(
                                folder,
                                &group_path,
                                &group_title,
                                &group[0],
                                &metadata,
                            )
                            .await
                            .is_err()
                        {
                            return settings_error(
                                "Could not migrate matched metadata to the JSON sidecar",
                            );
                        }

                        clear_group_database_metadata(&state, library_id, &group);
                    } else {
                        for media in &group {
                            let key = database_media_key(library_id + 1, &media.id);
                            let existing = read_database_metadata(&state, &key);
                            if existing
                                .as_ref()
                                .is_some_and(|metadata| metadata.get("id").is_some())
                            {
                                continue;
                            }
                            let metadata =
                                tv_metadata_for_media(&metadata, folder, &media.path, kind);
                            if !write_database_metadata(&state, &key, &metadata) {
                                warn!(library = %library.name, path = %media.path.display(), "existing match could not be applied to newly found media");
                                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                            }
                        }
                    }
                    if let Some(token) = token {
                        let media_key = database_media_key(library_id + 1, &group[0].id);
                        if let Err(error) = download_tmdb_artwork(
                            &client,
                            token,
                            folder,
                            &group_path,
                            &media_key,
                            &metadata,
                            &state,
                            false,
                        )
                        .await
                        {
                            warn!(library = %library.name, title = %group_title, error = %error, "TMDB artwork refresh failed");
                        }
                    }

                    matched += 1;
                    if kind == "tv" {
                        if let Some(token) = token {
                            if let Err(error) = ensure_tv_season_metadata(
                                &client,
                                token,
                                folder,
                                &group,
                                library_id + 1,
                                &metadata,
                                sidecar_mode,
                                &state,
                                false,
                            )
                            .await
                            {
                                warn!(library = %library.name, title = %group_title, error = %error, "TV season metadata scan failed");
                            }
                        }
                    }
                    continue;
                }
                let previous = if sidecar_mode {
                    read_database_metadata(&state, &media_key)
                } else {
                    read_sidecar_metadata(&group_path)
                };
                if let Some(metadata) = previous.filter(|metadata| metadata.get("id").is_some()) {
                    if sidecar_mode {
                        let representative = &group[0];
                        if store_metadata_sidecar(
                            folder,
                            &group_path,
                            &group_title,
                            representative,
                            &metadata,
                        )
                        .await
                        .is_err()
                        {
                            return settings_error(
                                "Could not move matched metadata to the media-folder sidecar",
                            );
                        }
                        clear_group_database_metadata(&state, library_id, &group);
                        if let Some(token) = token {
                            let _ = download_tmdb_artwork(
                                &client,
                                token,
                                folder,
                                &group_path,
                                &media_key,
                                &metadata,
                                &state,
                                true,
                            )
                            .await;
                        }
                    } else {
                        for media in &group {
                            let metadata =
                                tv_metadata_for_media(&metadata, folder, &media.path, kind);
                            let key = database_media_key(library_id + 1, &media.id);
                            if !write_database_metadata(&state, &key, &metadata) {
                                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                            }
                        }
                    }
                    matched += 1;
                    if kind == "tv" {
                        if let Some(token) = token {
                            if let Err(error) = ensure_tv_season_metadata(
                                &client,
                                token,
                                folder,
                                &group,
                                library_id + 1,
                                &metadata,
                                sidecar_mode,
                                &state,
                                false,
                            )
                            .await
                            {
                                warn!(library = %library.name, title = %group_title, error = %error, "TV season metadata scan failed");
                            }
                        }
                    }
                    continue;
                }
            }
            let Some(token) = token else {
                warn!(library = %library.name, title = %group_title, "metadata matching skipped because no TMDB bearer token is configured");
                return settings_error(
                    "Add a TMDB bearer token before matching media without stored metadata",
                );
            };
            let title = metadata_search_title(&group_title);
            info!(library = %library.name, title = %group_title, media_kind = kind, "TMDB metadata search started");
            let response = client
                .get(format!("https://api.themoviedb.org/3/search/{}", kind))
                .bearer_auth(token)
                .query(&[
                    ("query", title.as_str()),
                    ("include_adult", "false"),
                    ("language", "en-US"),
                ])
                .send()
                .await;
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    warn!(library = %library.name, title = %group_title, error = %error, "TMDB metadata search request failed");
                    continue;
                }
            };
            let status = response.status();
            if !status.is_success() {
                warn!(library = %library.name, title = %group_title, status = %status, "TMDB metadata search returned an error status");
                continue;
            }
            let payload = match response.json::<serde_json::Value>().await {
                Ok(payload) => payload,
                Err(error) => {
                    warn!(library = %library.name, title = %group_title, error = %error, "TMDB metadata response could not be parsed");
                    continue;
                }
            };
            let Some(results) = payload.get("results").and_then(serde_json::Value::as_array) else {
                info!(library = %library.name, title = %group_title, search = %title, "TMDB metadata search returned no results");
                continue;
            };
            let Some((result, match_score)) = best_metadata_result(results, &group_title, kind)
            else {
                info!(library = %library.name, title = %group_title, search = %title, result_count = results.len(), "TMDB results had no confident title match");
                continue;
            };
            let full_result = if kind == "tv" {
                if let Some(details) = tmdb_tv_show_details(&client, token, result).await {
                    merge_tv_show_details(result, &details)
                } else {
                    result.clone()
                }
            } else {
                result.clone()
            };
            let result = &full_result;
            info!(
                library = %library.name,
                title = %group_title,
                match_score,
                matched_title = ?result.get(if kind == "tv" { "name" } else { "title" }),
                tmdb_id = ?result.get("id"),
                poster_available = result.get("poster_path").and_then(serde_json::Value::as_str).is_some(),
                "TMDB metadata matched"
            );
            for media in &group {
                let media_metadata = tv_metadata_for_media(result, folder, &media.path, kind);
                if !sidecar_mode {
                    let metadata_json =
                        serde_json::to_string(&media_metadata).unwrap_or_else(|_| "{}".to_owned());
                    let media_key = database_media_key(library_id + 1, &media.id);
                    let database_ok = state
                        .db
                        .lock()
                        .ok()
                        .and_then(|connection| {
                            connection
                                .execute(
                                    "UPDATE media SET metadata_json = ?1, matched_at = strftime('%s', 'now') WHERE media_key = ?2",
                                    (&metadata_json, &media_key),
                                )
                                .ok()
                        })
                        .is_some();
                    if !database_ok {
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    }
                }
            }
            if sidecar_mode {
                let representative = &group[0];
                if store_metadata_sidecar(
                    folder,
                    &group_path,
                    &group_title,
                    representative,
                    &tv_metadata_for_media(result, folder, &representative.path, kind),
                )
                .await
                .is_err()
                {
                    return settings_error(
                        "Could not write matched metadata to the media-folder sidecar",
                    );
                }
                let media_key = database_media_key(library_id + 1, &representative.id);
                if let Err(error) = download_tmdb_artwork(
                    &client,
                    token,
                    folder,
                    &group_path,
                    &media_key,
                    result,
                    &state,
                    true,
                )
                .await
                {
                    warn!(library = %library.name, title = %group_title, error = %error, "TMDB artwork download failed");
                }
            }
            if !sidecar_mode {
                let representative = &group[0];
                let media_key = database_media_key(library_id + 1, &representative.id);
                if let Err(error) = download_tmdb_artwork(
                    &client,
                    token,
                    folder,
                    &group_path,
                    &media_key,
                    result,
                    &state,
                    true,
                )
                .await
                {
                    warn!(library = %library.name, title = %group_title, error = %error, "TMDB artwork download failed");
                }
            }
            matched += 1;
            if kind == "tv" {
                if let Err(error) = ensure_tv_season_metadata(
                    &client,
                    token,
                    folder,
                    &group,
                    library_id + 1,
                    result,
                    sidecar_mode,
                    &state,
                    forced,
                )
                .await
                {
                    warn!(library = %library.name, title = %group_title, error = %error, "TV season metadata scan failed");
                }
            }
        }
    }
    info!(library = %library.name, matched, "metadata scan finished");
    let suffix = if forced { "&metadata_forced=true" } else { "" };
    Redirect::to(&format!("/settings?metadata_matched={matched}{suffix}")).into_response()
}

async fn refresh_matched_metadata(
    state: &AppState,
    client: &reqwest::Client,
    token: Option<&str>,
    folder: &PathBuf,
    group_path: &std::path::Path,
    group_title: &str,
    group: &[Media],
    library_id: usize,
    kind: &str,
    sidecar_mode: bool,
) -> Result<(), String> {
    let media_key = database_media_key(library_id + 1, &group[0].id);
    let sidecar_metadata = read_sidecar_metadata(group_path);
    let database_metadata = read_database_metadata(state, &media_key);
    let metadata = if sidecar_mode {
        sidecar_metadata.or(database_metadata)
    } else {
        database_metadata.or(sidecar_metadata)
    }
    .filter(|metadata| {
        metadata
            .get("id")
            .and_then(serde_json::Value::as_i64)
            .is_some()
    })
    .ok_or_else(|| "Refresh requires an existing metadata match".to_owned())?;
    let token =
        token.ok_or_else(|| "Add a TMDB bearer token before refreshing metadata".to_owned())?;
    let details = if kind == "tv" {
        tmdb_tv_show_details(client, token, &metadata)
            .await
            .ok_or_else(|| "Could not refresh the matched TV show from TMDB".to_owned())?
    } else {
        let id = metadata["id"].as_i64().unwrap_or_default();
        let response = client
            .get(format!("https://api.themoviedb.org/3/movie/{id}"))
            .bearer_auth(token)
            .query(&[("language", "en-US")])
            .send()
            .await
            .map_err(|_| "Could not reach TMDB to refresh the matched movie".to_owned())?;
        if !response.status().is_success() {
            return Err("TMDB could not refresh the matched movie".to_owned());
        }
        response
            .json::<serde_json::Value>()
            .await
            .map_err(|_| "TMDB returned invalid movie details".to_owned())?
    };
    if details.get("id").and_then(serde_json::Value::as_i64)
        != metadata.get("id").and_then(serde_json::Value::as_i64)
    {
        return Err("TMDB returned details for a different match".to_owned());
    }
    let refreshed = merge_tv_show_details(&metadata, &details);
    if sidecar_mode {
        store_metadata_sidecar(folder, group_path, group_title, &group[0], &refreshed)
            .await
            .map_err(|_| "Could not save refreshed metadata sidecar".to_owned())?;
        clear_group_database_metadata(state, library_id, group);
    } else {
        for media in group {
            let item_metadata = tv_metadata_for_media(&refreshed, folder, &media.path, kind);
            let key = database_media_key(library_id + 1, &media.id);
            if !write_database_metadata(state, &key, &item_metadata) {
                return Err("Could not save refreshed metadata".to_owned());
            }
        }
    }
    if let Err(error) = download_tmdb_artwork(
        client, token, folder, group_path, &media_key, &refreshed, state, true,
    )
    .await
    {
        warn!(title = %group_title, error = %error, "TMDB artwork refresh failed");
    }
    if kind == "tv" {
        if let Err(error) = ensure_tv_season_metadata(
            client,
            token,
            folder,
            group,
            library_id + 1,
            &refreshed,
            sidecar_mode,
            state,
            true,
        )
        .await
        {
            warn!(title = %group_title, error = %error, "TV season metadata refresh failed");
        }
    }
    Ok(())
}

fn tv_match_target(
    state: &AppState,
    library_index: usize,
    relative_path: &str,
) -> Result<(ConfigLibrary, PathBuf, PathBuf, String, Vec<Media>), String> {
    let library = state
        .config
        .lock()
        .map_err(|_| "Configuration is unavailable".to_owned())?
        .libraries
        .get(library_index)
        .cloned()
        .ok_or_else(|| "Library does not exist".to_owned())?;
    if !matches!(library.kind.as_str(), "tv" | "movie") {
        return Err("Fix Match is only available for movie and TV libraries".to_owned());
    }
    let relative = clean_relative_path(relative_path).ok_or_else(|| {
        if library.kind == "tv" {
            "Invalid TV show path".to_owned()
        } else {
            "Invalid media path".to_owned()
        }
    })?;
    if library.kind == "tv" {
        if relative.components().count() != 1 {
            return Err("Invalid TV show path".to_owned());
        }
        let (root, show_directory) = library
            .folders
            .iter()
            .find_map(|root| {
                let show_directory = root.join(&relative);
                show_directory
                    .is_dir()
                    .then(|| (root.clone(), show_directory))
            })
            .ok_or_else(|| "TV show folder does not exist".to_owned())?;
        let episodes = media_snapshot(state)
            .iter()
            .filter(|media| media.path.starts_with(&show_directory))
            .cloned()
            .collect::<Vec<_>>();
        if episodes.is_empty() {
            return Err("No indexed media found for this TV show".to_owned());
        }
        let title = show_directory
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("TV Show")
            .to_owned();
        return Ok((library, root, show_directory, title, episodes));
    }
    for root in library.folders.clone() {
        let media = media_snapshot(state)
            .iter()
            .filter(|media| {
                metadata_group_path(&root, &media.path, "movie")
                    .strip_prefix(&root)
                    .is_ok_and(|path| path == relative)
            })
            .cloned()
            .collect::<Vec<_>>();
        if let Some(first) = media.first() {
            let group_path = metadata_group_path(&root, &first.path, "movie");
            let title = metadata_group_title(&group_path, &root);
            return Ok((library, root, group_path, title, media));
        }
    }
    Err("No indexed media found for this movie".to_owned())
}

async fn tv_match_search(
    State(state): State<AppState>,
    Query(query): Query<TvMatchSearchQuery>,
) -> Response {
    let (library, _, _, title, _) = match tv_match_target(&state, query.library, &query.path) {
        Ok(target) => target,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": error})),
            )
                .into_response()
        }
    };
    let search = query.query.trim();
    if search.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Enter a search title"})),
        )
            .into_response();
    }
    let token = state
        .config
        .lock()
        .ok()
        .and_then(|config| config.tmdb_bearer_token.clone())
        .filter(|token| !token.trim().is_empty());
    let Some(token) = token else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Add a TMDB bearer token in Settings first"})),
        )
            .into_response();
    };
    let media_kind = if library.kind == "tv" { "tv" } else { "movie" };
    info!(library = %library.name, title = %title, query = search, "metadata fix-match search started");
    let response = match reqwest::Client::new()
        .get(format!("https://api.themoviedb.org/3/search/{media_kind}"))
        .bearer_auth(token)
        .query(&[("query", search), ("language", "en-US")])
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            warn!(library = %library.name, show = %title, error = %error, "TV fix-match search request failed");
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"error": "TMDB search request failed"})),
            )
                .into_response();
        }
    };
    let status = response.status();
    if !status.is_success() {
        warn!(library = %library.name, show = %title, %status, "TV fix-match search returned an error status");
        return (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("TMDB returned {status}")})),
        )
            .into_response();
    }
    let payload = match response.json::<serde_json::Value>().await {
        Ok(payload) => payload,
        Err(error) => {
            warn!(library = %library.name, show = %title, error = %error, "TV fix-match search response was invalid");
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"error": "TMDB search response was invalid"})),
            )
                .into_response();
        }
    };
    let results = payload
        .get("results")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|result| {
            let id = result.get("id")?.as_i64()?;
            Some(serde_json::json!({
                "id": id,
                "title": result.get(if media_kind == "tv" { "name" } else { "title" }).and_then(serde_json::Value::as_str).unwrap_or("Untitled"),
                "first_air_date": result.get(if media_kind == "tv" { "first_air_date" } else { "release_date" }).and_then(serde_json::Value::as_str).unwrap_or(""),
                "overview": result.get("overview").and_then(serde_json::Value::as_str).unwrap_or(""),
                "poster_path": result.get("poster_path").and_then(serde_json::Value::as_str),
            }))
        })
        .take(20)
        .collect::<Vec<_>>();
    info!(library = %library.name, show = %title, result_count = results.len(), "TV fix-match search finished");
    Json(serde_json::json!({"results": results})).into_response()
}

async fn tv_match_select(
    State(state): State<AppState>,
    Json(selection): Json<TvMatchSelection>,
) -> Response {
    let (library, root, media_directory, title, media_items) =
        match tv_match_target(&state, selection.library, &selection.path) {
            Ok(target) => target,
            Err(error) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": error})),
                )
                    .into_response();
            }
        };
    if selection.tmdb_id <= 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Invalid TMDB result"})),
        )
            .into_response();
    }
    let config = match state.config.lock() {
        Ok(config) => config.clone(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let Some(token) = config
        .tmdb_bearer_token
        .filter(|token| !token.trim().is_empty())
    else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Add a TMDB bearer token in Settings first"})),
        )
            .into_response();
    };
    let media_kind = if library.kind == "tv" { "tv" } else { "movie" };
    info!(library = %library.name, title = %title, tmdb_id = selection.tmdb_id, "metadata fix-match selection started");
    let client = reqwest::Client::new();
    let response = match client
        .get(format!(
            "https://api.themoviedb.org/3/{media_kind}/{}",
            selection.tmdb_id
        ))
        .bearer_auth(&token)
        .query(&[("language", "en-US")])
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            warn!(library = %library.name, show = %title, error = %error, "TV fix-match details request failed");
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"error": "TMDB details request failed"})),
            )
                .into_response();
        }
    };
    let status = response.status();
    if !status.is_success() {
        warn!(library = %library.name, show = %title, %status, "TV fix-match details returned an error status");
        return (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("TMDB returned {status}")})),
        )
            .into_response();
    }
    let metadata = match response.json::<serde_json::Value>().await {
        Ok(metadata)
            if metadata.get("id").and_then(serde_json::Value::as_i64)
                == Some(selection.tmdb_id) =>
        {
            metadata
        }
        Ok(_) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"error": "TMDB returned a different show"})),
            )
                .into_response()
        }
        Err(error) => {
            warn!(library = %library.name, show = %title, error = %error, "TV fix-match details response was invalid");
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"error": "TMDB details response was invalid"})),
            )
                .into_response();
        }
    };
    if config.metadata_storage == "sidecar" {
        if let Err(error) =
            store_metadata_sidecar(&root, &media_directory, &title, &media_items[0], &metadata)
                .await
        {
            warn!(library = %library.name, title = %title, error = %error, "Fix-match metadata could not be saved");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        clear_group_database_metadata(&state, selection.library, &media_items);
    } else {
        for media in &media_items {
            let media_key = database_media_key(selection.library + 1, &media.id);
            let item_metadata = if media_kind == "tv" {
                tv_metadata_for_media(&metadata, &root, &media.path, "tv")
            } else {
                metadata.clone()
            };
            if !write_database_metadata(&state, &media_key, &item_metadata) {
                warn!(library = %library.name, title = %title, media = %media.path.display(), "Fix-match metadata could not be saved");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }
    }
    if let Err(error) = download_tmdb_artwork(
        &client,
        &token,
        &root,
        &media_directory,
        &database_media_key(selection.library + 1, &media_items[0].id),
        &metadata,
        &state,
        true,
    )
    .await
    {
        warn!(library = %library.name, title = %title, error = %error, "Fix-match artwork download failed");
    }
    if media_kind == "tv" {
        if let Err(error) = ensure_tv_season_metadata(
            &client,
            &token,
            &root,
            &media_items,
            selection.library + 1,
            &metadata,
            config.metadata_storage == "sidecar",
            &state,
            true,
        )
        .await
        {
            warn!(library = %library.name, title = %title, error = %error, "TV season metadata update failed");
        }
    }
    let artwork_directory = media_directory.join(".mplex");
    for media in &media_items {
        let media_key = database_media_key(selection.library + 1, &media.id);
        for (kind, filename) in [("cover", "cover.jpg"), ("banner", "banner.jpg")] {
            let artwork = artwork_directory.join(filename);
            if artwork.is_file() {
                record_artwork(&state.db, &media_key, kind, &artwork);
            }
        }
    }
    info!(library = %library.name, title = %title, tmdb_id = selection.tmdb_id, "metadata fix-match selection saved");
    Json(serde_json::json!({"ok": true})).into_response()
}

async fn scan_media_metadata(
    State(state): State<AppState>,
    Form(form): Form<MediaScanForm>,
) -> Response {
    let library_id = form.library.unwrap_or(0);
    let Ok(config) = state.config.lock().map(|config| config.clone()) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Some(library) = config.libraries.get(library_id) else {
        return settings_error("Library does not exist");
    };
    if library.kind == "other" {
        return settings_error("TMDB matching is not available for Other libraries");
    }
    let sidecar_mode = config.metadata_storage == "sidecar";
    let Some(token) = config
        .tmdb_bearer_token
        .as_deref()
        .filter(|token| !token.is_empty())
    else {
        return settings_error("Add a TMDB bearer token in Settings before matching media");
    };
    let Some(relative) = clean_relative_path(form.path.as_str()) else {
        return settings_error("Media path is invalid");
    };
    let Some(root) = library.folders.first() else {
        return settings_error("Library has no folders");
    };
    let path = match web_path(&state, root, &relative).await {
        Ok(path) if path.is_file() && media_type(&path).is_some() => path,
        _ => return settings_error("Media file does not exist"),
    };
    let Ok(files) = scan_media(root).await else {
        return settings_error("Library could not be scanned");
    };
    let Some(media) = files.into_iter().find(|media| media.path == path) else {
        return settings_error("Media file is not in the selected library");
    };
    let kind = if library.kind == "tv" { "tv" } else { "movie" };
    let title = metadata_search_title(form.title.trim());
    if title.is_empty() {
        return settings_error("A match title is required");
    }
    let client = reqwest::Client::new();
    let Ok(response) = client
        .get(format!("https://api.themoviedb.org/3/search/{kind}"))
        .bearer_auth(token)
        .query(&[
            ("query", title.as_str()),
            ("include_adult", "false"),
            ("language", "en-US"),
        ])
        .send()
        .await
    else {
        return settings_error("TMDB could not be reached");
    };
    let Ok(payload) = response.json::<serde_json::Value>().await else {
        return settings_error("TMDB returned an invalid response");
    };
    let Some(result) = payload
        .get("results")
        .and_then(|results| results.as_array())
        .and_then(|results| results.first())
    else {
        return settings_error("No metadata match was found");
    };
    let media_key = database_media_key(library_id + 1, &media.id);
    let metadata_json = serde_json::to_string(result).unwrap_or_else(|_| "{}".to_owned());
    if !sidecar_mode {
        let database_ok = state
            .db
            .lock()
            .ok()
            .and_then(|connection| {
                connection
                    .execute(
                        "UPDATE media SET metadata_json = ?1, matched_at = strftime('%s', 'now') WHERE media_key = ?2",
                        (&metadata_json, &media_key),
                    )
                    .ok()
            })
            .is_some();
        if !database_ok {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }
    if sidecar_mode {
        let group_path = metadata_group_path(root, &media.path, kind);
        if store_metadata_sidecar(root, &group_path, &title, &media, result)
            .await
            .is_err()
        {
            return settings_error("Could not write matched metadata to the media-folder sidecar");
        }
        let _ = download_tmdb_artwork(
            &client,
            token,
            root,
            &group_path,
            &media_key,
            result,
            &state,
            true,
        )
        .await;
    }
    let parent = relative.parent().unwrap_or(std::path::Path::new(""));
    Redirect::to(&format!(
        "/?library={library_id}&path={}",
        url_encode(&parent.to_string_lossy())
    ))
    .into_response()
}

fn metadata_group_path(root: &PathBuf, media: &std::path::Path, kind: &str) -> PathBuf {
    if kind == "tv" {
        if let Ok(relative) = media.strip_prefix(root) {
            if let Some(Component::Normal(show)) = relative.components().next() {
                return root.join(show);
            }
        }
    }
    let parent = media.parent().unwrap_or(root);
    if parent == root {
        media.to_path_buf()
    } else {
        parent.to_path_buf()
    }
}

fn metadata_sidecar_info_path(group_path: &std::path::Path) -> PathBuf {
    let directory = if group_path.is_file() {
        group_path.parent().unwrap_or(group_path)
    } else {
        group_path
    };
    directory.join(".mplex/data.json")
}

fn parse_sidecar_info(contents: &str) -> Option<serde_json::Value> {
    serde_json::from_str(contents).ok()
}

async fn read_sidecar_info(directory: &std::path::Path) -> Option<serde_json::Value> {
    tokio::fs::read_to_string(directory.join(".mplex/data.json"))
        .await
        .ok()
        .and_then(|contents| parse_sidecar_info(&contents))
}

fn parse_sidecar_metadata(contents: &str) -> Option<serde_json::Value> {
    parse_sidecar_info(contents)?
        .get("metadata_json")?
        .as_object()
        .map(|metadata| serde_json::Value::Object(metadata.clone()))
}

fn read_sidecar_metadata(group_path: &std::path::Path) -> Option<serde_json::Value> {
    std::fs::read_to_string(metadata_sidecar_info_path(group_path))
        .ok()
        .and_then(|contents| parse_sidecar_metadata(&contents))
}

fn database_media_key(section_id: usize, media_id: &str) -> String {
    let prefix = format!("{section_id}-");
    if media_id.starts_with(&prefix) {
        media_id.to_owned()
    } else {
        format!("{prefix}{media_id}")
    }
}

fn read_database_metadata(state: &AppState, media_key: &str) -> Option<serde_json::Value> {
    let connection = state.db.lock().ok()?;
    let contents = connection
        .query_row(
            "SELECT metadata_json FROM media WHERE media_key = ?1",
            [media_key],
            |row| row.get::<_, Option<String>>(0),
        )
        .ok()??;
    serde_json::from_str(&contents).ok()
}

fn write_database_metadata(
    state: &AppState,
    media_key: &str,
    metadata: &serde_json::Value,
) -> bool {
    let Ok(connection) = state.db.lock() else {
        return false;
    };
    let metadata_json = serde_json::to_string(metadata).unwrap_or_else(|_| "{}".to_owned());
    connection
        .execute(
            "UPDATE media SET metadata_json = ?1, matched_at = strftime('%s', 'now') WHERE media_key = ?2",
            (&metadata_json, media_key),
        )
        .is_ok()
}

fn clear_group_database_metadata(state: &AppState, library_id: usize, group: &[Media]) {
    let Ok(connection) = state.db.lock() else {
        return;
    };
    for media in group {
        let media_key = database_media_key(library_id + 1, &media.id);
        let _ = connection.execute(
            "UPDATE media SET metadata_json = NULL, matched_at = NULL WHERE media_key = ?1",
            [media_key],
        );
    }
}

fn metadata_group_title(group: &std::path::Path, root: &PathBuf) -> String {
    if group.is_file() {
        return group
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("Video")
            .to_owned();
    }
    group
        .file_name()
        .and_then(|value| value.to_str())
        .or_else(|| root.file_name().and_then(|value| value.to_str()))
        .unwrap_or("Media")
        .to_owned()
}

fn metadata_search_title(title: &str) -> String {
    let mut words = Vec::new();
    let title = title
        .replace('.', " ")
        .replace('_', " ")
        .replace('-', " ")
        .replace('(', " ")
        .replace(')', " ")
        .replace('[', " ")
        .replace(']', " ");
    for word in title.split_whitespace() {
        let lower = word.to_ascii_lowercase();
        let is_year = word.len() == 4
            && word
                .parse::<u16>()
                .is_ok_and(|year| (1900..=2099).contains(&year));
        let is_release_tag = matches!(
            lower.as_str(),
            "480p"
                | "720p"
                | "1080p"
                | "2160p"
                | "4k"
                | "8k"
                | "bluray"
                | "brrip"
                | "dvdrip"
                | "hdrip"
                | "hdtv"
                | "webrip"
                | "webdl"
                | "web-dl"
                | "x264"
                | "x265"
                | "h264"
                | "h265"
                | "hevc"
                | "remux"
        );
        if is_release_tag || (is_year && !words.is_empty()) {
            break;
        }
        words.push(word);
    }
    words.join(" ")
}

fn normalized_metadata_title(title: &str) -> String {
    metadata_search_title(title)
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|character| character.is_alphanumeric())
        .collect()
}

fn metadata_title_year(title: &str) -> Option<&str> {
    title
        .split(|character: char| !character.is_ascii_digit())
        .find(|part| {
            part.len() == 4
                && part
                    .parse::<u16>()
                    .is_ok_and(|year| (1900..=2099).contains(&year))
        })
}

fn metadata_result_score(title: &str, result: &serde_json::Value, kind: &str) -> i32 {
    let candidate = if kind == "tv" {
        result.get("name").or_else(|| result.get("original_name"))
    } else {
        result.get("title").or_else(|| result.get("original_title"))
    }
    .and_then(serde_json::Value::as_str)
    .unwrap_or_default();
    let expected = normalized_metadata_title(title);
    let actual = normalized_metadata_title(candidate);
    if expected.is_empty() || actual.is_empty() {
        return 0;
    }
    let mut score = if expected == actual {
        100
    } else if expected.contains(&actual) || actual.contains(&expected) {
        70
    } else {
        let expected_words = metadata_search_title(title)
            .split_whitespace()
            .map(str::to_lowercase)
            .collect::<Vec<_>>();
        let actual_words = metadata_search_title(candidate)
            .split_whitespace()
            .map(str::to_lowercase)
            .collect::<Vec<_>>();
        let overlap = expected_words
            .iter()
            .filter(|word| actual_words.contains(word))
            .count();
        (60 * overlap / expected_words.len().max(1)) as i32
    };
    let expected_year = metadata_title_year(title);
    let candidate_year = result
        .get(if kind == "tv" {
            "first_air_date"
        } else {
            "release_date"
        })
        .and_then(serde_json::Value::as_str)
        .and_then(metadata_title_year);
    if let Some(year) = expected_year {
        score += if candidate_year == Some(year) {
            15
        } else {
            -10
        };
    }
    score
}

fn best_metadata_result<'a>(
    results: &'a [serde_json::Value],
    title: &str,
    kind: &str,
) -> Option<(&'a serde_json::Value, i32)> {
    results
        .iter()
        .map(|result| (result, metadata_result_score(title, result, kind)))
        .max_by_key(|(_, score)| *score)
        .filter(|(_, score)| *score >= 40)
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct TvEpisodeInfo {
    season: u32,
    episode: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    episode_end: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    episode_title: Option<String>,
}

async fn tmdb_tv_show_details(
    client: &reqwest::Client,
    token: &str,
    metadata: &serde_json::Value,
) -> Option<serde_json::Value> {
    let id = metadata.get("id")?.as_u64()?;
    let response = client
        .get(format!("https://api.themoviedb.org/3/tv/{id}"))
        .bearer_auth(token)
        .query(&[
            ("language", "en-US"),
            ("append_to_response", "content_ratings"),
        ])
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    response.json().await.ok()
}

fn merge_tv_show_details(
    metadata: &serde_json::Value,
    details: &serde_json::Value,
) -> serde_json::Value {
    let mut metadata = metadata.clone();
    if let (Some(target), Some(details)) = (metadata.as_object_mut(), details.as_object()) {
        target.extend(details.clone());
    }
    metadata
}

fn tv_metadata_for_media(
    metadata: &serde_json::Value,
    root: &PathBuf,
    media: &std::path::Path,
    kind: &str,
) -> serde_json::Value {
    if kind != "tv" {
        return metadata.clone();
    }
    let mut enriched = metadata.clone();
    let Some(object) = enriched.as_object_mut() else {
        return enriched;
    };
    for key in [
        "mplex_season",
        "mplex_episode",
        "mplex_episode_end",
        "mplex_episode_title",
    ] {
        object.remove(key);
    }
    let Some(episode) = tv_episode_info(root, media) else {
        return enriched;
    };
    object.insert(
        "mplex_season".to_owned(),
        serde_json::Value::from(episode.season),
    );
    object.insert(
        "mplex_episode".to_owned(),
        serde_json::Value::from(episode.episode),
    );
    if let Some(end) = episode.episode_end {
        object.insert("mplex_episode_end".to_owned(), serde_json::Value::from(end));
    }
    if let Some(title) = episode.episode_title {
        object.insert(
            "mplex_episode_title".to_owned(),
            serde_json::Value::from(title),
        );
    }
    enriched
}

fn tv_episode_info(root: &PathBuf, media: &std::path::Path) -> Option<TvEpisodeInfo> {
    let stem = media.file_stem()?.to_str()?;
    let (season_from_name, episode, marker_end) = parse_tv_episode_marker(stem)?;
    let season = media
        .strip_prefix(root)
        .ok()?
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .find_map(parse_tv_season_directory)
        .or(season_from_name)
        .unwrap_or(1);
    Some(TvEpisodeInfo {
        season,
        episode: episode.0,
        episode_end: episode.1,
        episode_title: clean_episode_title(&stem[marker_end..]),
    })
}

fn tv_season_number(root: &PathBuf, media_path: &std::path::Path) -> Option<u32> {
    tv_episode_info(root, media_path)
        .map(|episode| episode.season)
        .or_else(|| {
            media_path
                .strip_prefix(root)
                .ok()?
                .components()
                .filter_map(|component| component.as_os_str().to_str())
                .find_map(parse_tv_season_directory)
        })
        .or(Some(1))
}

fn tv_season_directory(
    show_directory: &std::path::Path,
    season: u32,
    episode_path: &std::path::Path,
) -> PathBuf {
    let physical_directory = episode_path.parent();
    if physical_directory
        .and_then(std::path::Path::file_name)
        .and_then(|name| name.to_str())
        .and_then(parse_tv_season_directory)
        == Some(season)
    {
        physical_directory.unwrap_or(show_directory).to_path_buf()
    } else {
        show_directory.join(format!("Season {season:02}"))
    }
}

struct PlexTvShowGroup<'a> {
    title: String,
    rating_key: String,
    episodes: Vec<&'a Media>,
}

fn tv_show_title(root: &PathBuf, media: &std::path::Path) -> String {
    let relative = media.strip_prefix(root).unwrap_or(media);
    let components = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>();
    if components.len() > 1 {
        if parse_tv_season_directory(components[0]).is_some() {
            return root
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("TV Shows")
                .to_owned();
        }
        return components[0].to_owned();
    }
    let stem = media
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("Episode");
    if let Some(marker_start) = (0..stem.len()).find(|index| {
        stem.is_char_boundary(*index) && parse_tv_episode_marker(&stem[*index..]).is_some()
    }) {
        let prefix = stem[..marker_start]
            .trim_end_matches([' ', '.', '_', '-'])
            .replace(['.', '_'], " ");
        if !prefix.trim().is_empty() {
            return prefix.trim().to_owned();
        }
    }
    root.file_name()
        .and_then(|value| value.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(stem)
        .to_owned()
}

fn plex_tv_show_key(section: usize, title: &str) -> String {
    format!(
        "{section}-show-{}",
        plex_safe_numeric_id(&format!("tv-show:{section}:{}", title.to_lowercase()))
    )
}

fn plex_tv_season_key(show_key: &str, season: u32) -> String {
    format!("{show_key}-season-{season}")
}

fn plex_tv_show_groups<'a>(
    section: usize,
    roots: &[PathBuf],
    media: impl IntoIterator<Item = &'a Media>,
) -> Vec<PlexTvShowGroup<'a>> {
    let mut groups = BTreeMap::<String, PlexTvShowGroup<'a>>::new();
    for item in media {
        let Some(root) = roots.iter().find(|root| item.path.starts_with(root)) else {
            continue;
        };
        let title = tv_show_title(root, &item.path);
        let grouping_key = title.to_lowercase();
        let group = groups
            .entry(grouping_key)
            .or_insert_with(|| PlexTvShowGroup {
                rating_key: plex_tv_show_key(section, &title),
                title,
                episodes: Vec::new(),
            });
        group.episodes.push(item);
    }
    groups.into_values().collect()
}

fn plex_ranked_tv_shows<'a>(
    ranked_media: &[&'a Media],
    shows: &'a [PlexTvShowGroup<'a>],
) -> Vec<&'a PlexTvShowGroup<'a>> {
    let mut seen = BTreeSet::new();
    ranked_media
        .iter()
        .filter_map(|media| {
            shows
                .iter()
                .find(|show| show.episodes.iter().any(|episode| episode.id == media.id))
        })
        .filter(|show| seen.insert(show.rating_key.clone()))
        .collect()
}

fn parse_tv_season_directory(name: &str) -> Option<u32> {
    let normalized_name = name.trim().replace(['.', '_', '-'], " ");
    let normalized = normalized_name.split_whitespace().collect::<Vec<_>>();
    if normalized.len() == 1 && normalized[0].eq_ignore_ascii_case("specials") {
        return Some(0);
    }
    if normalized.len() == 2 && normalized[0].eq_ignore_ascii_case("season") {
        return normalized[1].parse().ok();
    }
    None
}

fn parse_tv_episode_marker(stem: &str) -> Option<(Option<u32>, (u32, Option<u32>), usize)> {
    let bytes = stem.as_bytes();
    for start in 0..bytes.len() {
        if !bytes[start].eq_ignore_ascii_case(&b's')
            || (start > 0 && bytes[start - 1].is_ascii_alphanumeric())
        {
            continue;
        }
        let Some((season, after_season)) = parse_digits(bytes, start + 1) else {
            continue;
        };
        if bytes.get(after_season)?.to_ascii_lowercase() != b'e' {
            continue;
        }
        let Some((episode, after_episode)) = parse_digits(bytes, after_season + 1) else {
            continue;
        };
        let (episode_end, marker_end) = if bytes.get(after_episode) == Some(&b'-')
            && bytes
                .get(after_episode + 1)
                .is_some_and(|byte| byte.eq_ignore_ascii_case(&b'e'))
        {
            if let Some((end, marker_end)) = parse_digits(bytes, after_episode + 2) {
                (Some(end), marker_end)
            } else {
                (None, after_episode)
            }
        } else {
            (None, after_episode)
        };
        return Some((Some(season), (episode, episode_end), marker_end));
    }
    for start in 0..bytes.len() {
        if start + 7 > bytes.len() || !bytes[start..start + 7].eq_ignore_ascii_case(b"episode") {
            continue;
        }
        let mut number_start = start + 7;
        while bytes
            .get(number_start)
            .is_some_and(|byte| byte.is_ascii_whitespace() || matches!(byte, b'.' | b'-' | b'#'))
        {
            number_start += 1;
        }
        if let Some((episode, marker_end)) = parse_digits(bytes, number_start) {
            return Some((None, (episode, None), marker_end));
        }
    }
    None
}

fn parse_digits(bytes: &[u8], start: usize) -> Option<(u32, usize)> {
    let mut end = start;
    while end < bytes.len() && bytes[end].is_ascii_digit() && end - start < 3 {
        end += 1;
    }
    if end == start {
        return None;
    }
    Some((
        std::str::from_utf8(&bytes[start..end]).ok()?.parse().ok()?,
        end,
    ))
}

fn clean_episode_title(value: &str) -> Option<String> {
    let title = value
        .trim_start_matches(|character: char| {
            character == '-' || character == '_' || character == '.' || character.is_whitespace()
        })
        .trim();
    (!title.is_empty()).then(|| title.to_owned())
}

async fn store_metadata_sidecar(
    folder: &PathBuf,
    group_path: &std::path::Path,
    title: &str,
    media: &Media,
    metadata: &serde_json::Value,
) -> Result<(), std::io::Error> {
    let media_directory = if group_path.is_file() {
        group_path.parent().unwrap_or(folder)
    } else {
        group_path
    };
    let directory = media_directory.join(".mplex");
    tokio::fs::create_dir_all(&directory).await?;
    let tv_show =
        metadata.get("first_air_date").is_some() || metadata.get("number_of_seasons").is_some();
    let path = media
        .path
        .strip_prefix(folder)
        .unwrap_or(&media.path)
        .to_string_lossy();
    let episode = (!tv_show)
        .then(|| tv_episode_info(folder, &media.path))
        .flatten();
    let show_id = read_sidecar_info(media_directory)
        .await
        .and_then(|value| value.get("show_id")?.as_str().map(str::to_owned));
    let info = MediaInfo {
        title: title.to_owned(),
        media_path: (!tv_show).then(|| path.into_owned()),
        description: json_string(metadata, "overview"),
        release_date: json_string(metadata, "release_date"),
        first_air_date: json_string(metadata, "first_air_date"),
        number_of_seasons: json_i64(metadata, "number_of_seasons"),
        number_of_episodes: json_i64(metadata, "number_of_episodes"),
        season: episode.as_ref().map(|value| value.season),
        episode: episode.as_ref().map(|value| value.episode),
        episode_end: episode.as_ref().and_then(|value| value.episode_end),
        episode_title: episode.and_then(|value| value.episode_title),
        metadata_json: metadata.clone(),
        technical: if tv_show {
            None
        } else {
            inspect_media(&media.path).await
        },
        show_id,
    };
    let contents = serde_json::to_string_pretty(&info).map_err(std::io::Error::other)?;
    tokio::fs::write(directory.join("data.json"), contents).await
}

#[derive(Serialize)]
struct MediaInfo {
    title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    media_path: Option<String>,
    description: String,
    release_date: String,
    first_air_date: String,
    number_of_seasons: Option<i64>,
    number_of_episodes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    season: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    episode: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    episode_end: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    episode_title: Option<String>,
    metadata_json: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    technical: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    show_id: Option<String>,
}

#[derive(Serialize)]
struct SeasonInfo {
    title: String,
    description: String,
    season: u32,
    metadata_json: serde_json::Value,
}

async fn inspect_media(path: &std::path::Path) -> Option<serde_json::Value> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        vuio_media_info::inspect(path)
            .ok()?
            .to_json()
            .ok()
            .and_then(|json| serde_json::from_str(&json).ok())
    })
    .await
    .ok()
    .flatten()
}

fn technical_tracks(info: &serde_json::Value) -> &[serde_json::Value] {
    info.get("media")
        .and_then(|media| media.get("track"))
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn technical_text<'a>(track: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    track.get(key).and_then(serde_json::Value::as_str)
}

fn technical_bitrate_kbps(track: &serde_json::Value) -> Option<u64> {
    technical_text(track, "BitRate")
        .or_else(|| technical_text(track, "BitRate_Nominal"))
        .and_then(|value| value.parse::<u64>().ok())
        .map(|bits_per_second| bits_per_second / 1000)
}

fn average_bitrate_kbps(size_bytes: u64, duration_ms: i64) -> Option<u64> {
    let duration_ms = u128::try_from(duration_ms).ok()?;
    if duration_ms == 0 {
        return None;
    }
    let bits = u128::from(size_bytes).checked_mul(8)?;
    u64::try_from((bits + duration_ms / 2) / duration_ms).ok()
}

fn plex_video_profile(value: &str) -> String {
    let profile = value
        .split('@')
        .next()
        .unwrap_or(value)
        .trim()
        .to_ascii_lowercase();
    match profile.as_str() {
        "constrained baseline" => "baseline".to_owned(),
        "high 10" | "high 10 intra" => "high10".to_owned(),
        "high 4:2:2" => "high422".to_owned(),
        "high 4:4:4" => "high444".to_owned(),
        "main 10" => "main10".to_owned(),
        _ => profile.replace([' ', '-'], ""),
    }
}

fn plex_video_level(value: &str) -> Option<u64> {
    let level = value.trim().parse::<f64>().ok()?;
    (level.is_finite() && level > 0.0).then(|| (level * 10.0).round() as u64)
}

fn technical_track_id(track: &serde_json::Value) -> u64 {
    technical_text(track, "StreamOrder")
        .or_else(|| technical_text(track, "ID"))
        .and_then(|id| id.parse::<u64>().ok())
        .unwrap_or_default()
}

fn technical_duration_ms(info: &serde_json::Value) -> Option<i64> {
    technical_tracks(info)
        .iter()
        .find(|track| technical_text(track, "@type") == Some("General"))
        .and_then(|track| technical_text(track, "Duration"))
        .and_then(|seconds| seconds.parse::<f64>().ok())
        .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
        .map(|seconds| (seconds * 1000.0).round() as i64)
}

const PLEX_MAX_SAFE_INTEGER: u64 = (1_u64 << 53) - 1;

fn plex_safe_numeric_id(value: &str) -> u64 {
    let digest = Sha256::digest(value.as_bytes());
    let id = u64::from_be_bytes(digest[..8].try_into().unwrap_or_default()) & PLEX_MAX_SAFE_INTEGER;
    id.max(1)
}

fn plex_stream_id(media: &Media, path: &std::path::Path) -> u64 {
    plex_safe_numeric_id(&format!("stream:{}:{}", media.id, path.to_string_lossy()))
}

fn plex_part_id(media: &Media) -> u64 {
    plex_safe_numeric_id(&format!("part:{}", media.id))
}

fn plex_media_id(media: &Media) -> u64 {
    plex_safe_numeric_id(&format!("media:{}", media.id))
}

fn plex_rating_key(media: &Media) -> u64 {
    const ITEM_ID_BITS: u32 = 20;
    const ITEM_ID_MASK: u64 = (1 << ITEM_ID_BITS) - 1;
    const MAX_SECTION_ID: u64 = (i32::MAX as u64) >> ITEM_ID_BITS;

    media
        .id
        .split_once('-')
        .and_then(|(section, item)| {
            let section = section.parse::<u64>().ok()?;
            let item = item.parse::<u64>().ok()?;
            (section > 0 && section <= MAX_SECTION_ID && item > 0 && item <= ITEM_ID_MASK)
                .then_some((section << ITEM_ID_BITS) | item)
        })
        .unwrap_or_else(|| plex_safe_numeric_id(&format!("rating-key:{}", media.id)))
}

fn find_media_part(state: &AppState, id: &str) -> Option<Media> {
    let id = id.parse::<u64>().ok()?;
    media_snapshot(state)
        .iter()
        .find(|media| plex_part_id(media) == id)
        .cloned()
}

fn plex_external_subtitle_id(media: &Media, path: &std::path::Path) -> u64 {
    plex_stream_id(media, path)
}

fn plex_language(language: &str) -> (&'static str, &'static str) {
    let input = language.trim();
    if input.is_empty()
        || input.eq_ignore_ascii_case("und")
        || input.eq_ignore_ascii_case("unknown")
    {
        return ("und", "Unknown");
    }
    let input_code = input.split(['-', '_']).next().unwrap_or(input);
    let normalized = input_code.to_ascii_lowercase();
    let code = iso_639_2_bibliographic_alias(&normalized).unwrap_or(&normalized);
    let language = Language::from_639_1(code)
        .or_else(|| Language::from_639_3(code))
        .or_else(|| Language::from_name(input))
        .or_else(|| Language::from_name_lowercase(&input.to_ascii_lowercase()));
    language
        .map(|language| {
            (
                language.to_639_1().unwrap_or_else(|| language.to_639_3()),
                language.to_name(),
            )
        })
        .unwrap_or(("und", "Unknown"))
}

fn plex_language_code_3(code: &str) -> &'static str {
    if code == "und" {
        return "und";
    }
    Language::from_639_1(code)
        .or_else(|| Language::from_639_3(code))
        .map(|language| language.to_639_3())
        .unwrap_or("und")
}

fn iso_639_2_bibliographic_alias(code: &str) -> Option<&'static str> {
    match code {
        "alb" => Some("sqi"),
        "arm" => Some("hye"),
        "baq" => Some("eus"),
        "bur" => Some("mya"),
        "chi" => Some("zho"),
        "cze" => Some("ces"),
        "dut" => Some("nld"),
        "fre" => Some("fra"),
        "geo" => Some("kat"),
        "ger" => Some("deu"),
        "gre" => Some("ell"),
        "ice" => Some("isl"),
        "mac" => Some("mkd"),
        "mao" => Some("mri"),
        "may" => Some("msa"),
        "per" => Some("fas"),
        "rum" => Some("ron"),
        "slo" => Some("slk"),
        "tib" => Some("bod"),
        "wel" => Some("cym"),
        _ => None,
    }
}

fn plex_track_language(track: &serde_json::Value) -> (&'static str, &'static str) {
    let language = technical_text(track, "Language").unwrap_or("und");
    let normalized = plex_language(language);
    if normalized.0 != "und" {
        return normalized;
    }

    // Matroska files often store the human-readable language as the track name
    // while leaving the language tag unset.
    let title_language = technical_text(track, "Title")
        .unwrap_or_default()
        .split(|character: char| !character.is_ascii_alphabetic())
        .next()
        .unwrap_or_default();
    plex_language(title_language)
}

fn plex_video_resolution(height: u64) -> &'static str {
    match height {
        0..=480 => "480",
        481..=576 => "576",
        577..=720 => "720",
        721..=1080 => "1080",
        _ => "4k",
    }
}

fn plex_codec(value: &str) -> String {
    match value.to_ascii_lowercase().as_str() {
        "avc" | "avc/h.264" | "h.264" => "h264".to_owned(),
        "hevc" | "h.265" | "hev1" => "hevc".to_owned(),
        "ac-3" | "ac3" => "ac3".to_owned(),
        "e-ac-3" | "eac3" => "eac3".to_owned(),
        "mpeg audio" | "mp3" => "mp3".to_owned(),
        _ => value.to_ascii_lowercase().replace(' ', ""),
    }
}

fn plex_track_label(track: &serde_json::Value, stream_type: u8) -> String {
    plex_track_label_with_fallback(track, stream_type, None)
}

fn plex_track_label_with_fallback(
    track: &serde_json::Value,
    stream_type: u8,
    fallback_language: Option<&str>,
) -> String {
    let (_, detected_language) = plex_track_language(track);
    let language = if detected_language == "Unknown" {
        fallback_language.unwrap_or(detected_language)
    } else {
        detected_language
    };
    let codec = match technical_text(track, "Format").unwrap_or("Unknown") {
        "AC-3" | "AC3" => "AC3",
        "E-AC-3" | "EAC3" => "EAC3",
        "AVC" => "H.264",
        "HEVC" => "H.265",
        codec => codec,
    };
    if stream_type == 1 {
        let height = technical_text(track, "Height")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_default();
        let resolution = match height {
            0..=576 => "SD",
            577..=720 => "720p",
            721..=1080 => "1080p",
            _ => "4K",
        };
        return format!("{resolution} ({codec})");
    }
    let detail = if stream_type == 2 {
        let count = technical_text(track, "Channels").and_then(|value| value.parse::<u8>().ok());
        let channels = match count {
            Some(6) => "5.1",
            Some(8) => "7.1",
            Some(2) => "Stereo",
            _ => technical_text(track, "ChannelLayout").unwrap_or_default(),
        };
        let channels = match channels {
            "6" => "5.1",
            "8" => "7.1",
            "2" => "Stereo",
            other => other,
        };
        if channels.is_empty() {
            codec.to_owned()
        } else {
            format!("{codec} {channels}")
        }
    } else {
        codec.to_owned()
    };
    if language == "Unknown" {
        detail
    } else {
        format!("{language} ({detail})")
    }
}

async fn add_plex_technical_metadata(
    state: &AppState,
    item: &mut serde_json::Value,
    media: &Media,
) {
    let cached = state
        .media_info
        .lock()
        .ok()
        .and_then(|cache| cache.get(&media.id).cloned());
    let info = if let Some(info) = cached {
        info
    } else {
        match inspect_media(&media.path).await {
            Some(info) => {
                if let Ok(mut cache) = state.media_info.lock() {
                    cache.insert(media.id.clone(), info.clone());
                }
                info
            }
            None => serde_json::json!({"media": {"track": []}}),
        }
    };
    let external_subtitles = external_subtitle_files(&media.path).await;
    let selection = state
        .plex_stream_selections
        .lock()
        .ok()
        .and_then(|selections| selections.get(&plex_part_id(media)).copied());
    apply_plex_technical_metadata(item, media, &info, &external_subtitles, selection);
}

fn apply_plex_technical_metadata(
    item: &mut serde_json::Value,
    media: &Media,
    info: &serde_json::Value,
    external_subtitles: &[PathBuf],
    selection: Option<PlexStreamSelection>,
) {
    let tracks = technical_tracks(info);
    let duration = technical_duration_ms(&info)
        .or_else(|| item.get("duration").and_then(serde_json::Value::as_i64))
        .unwrap_or_default();
    let selected_stream_id = |stream_type: &str| {
        tracks
            .iter()
            .filter(|track| technical_text(track, "@type") == Some(stream_type))
            .find(|track| technical_text(track, "Default") == Some("Yes"))
            .or_else(|| {
                tracks
                    .iter()
                    .find(|track| technical_text(track, "@type") == Some(stream_type))
            })
            .map(|track| technical_track_id(track))
    };
    let selected_video = selected_stream_id("Video");
    let selected_audio = selected_stream_id("Audio");
    let audio_track_count = tracks
        .iter()
        .filter(|track| technical_text(track, "@type") == Some("Audio"))
        .count();
    if let Some(metadata) = item.as_object_mut() {
        metadata.insert("duration".to_owned(), serde_json::json!(duration));
    }
    let Some(media_json) = item
        .get_mut("Media")
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|media| media.first_mut())
    else {
        return;
    };
    media_json["duration"] = serde_json::json!(duration);
    if let Some(bitrate) = tracks
        .iter()
        .find(|track| technical_text(track, "@type") == Some("General"))
        .and_then(|track| technical_bitrate_kbps(track))
        .or_else(|| average_bitrate_kbps(media.size, duration))
    {
        media_json["bitrate"] = serde_json::json!(bitrate);
    }
    let mut plex_streams = Vec::new();
    for track in tracks {
        let stream_type = match technical_text(track, "@type") {
            Some("Video") => 1,
            Some("Audio") => 2,
            Some("Text") => 3,
            _ => continue,
        };
        let track_id = technical_track_id(track);
        let codec_value = technical_text(track, "Format")
            .or_else(|| technical_text(track, "CodecID"))
            .unwrap_or("unknown");
        let codec = plex_codec(codec_value);
        let (mut language_code, mut language_name) = plex_track_language(track);
        let fallback_audio_language =
            (stream_type == 2 && audio_track_count == 1 && language_code == "und")
                .then_some(plex_language(PLEX_DEFAULT_AUDIO_LANGUAGE).1);
        if fallback_audio_language.is_some() {
            (language_code, language_name) = plex_language(PLEX_DEFAULT_AUDIO_LANGUAGE);
        }
        let display_title = fallback_audio_language
            .map(|language| plex_track_label_with_fallback(track, stream_type, Some(language)))
            .unwrap_or_else(|| plex_track_label(track, stream_type));
        let mut plex_stream = serde_json::json!({
            "id": track_id + 1,
            "streamType": stream_type,
            "type": stream_type,
            "index": track_id,
            "codec": codec,
            "language": if language_code == "und" { "None" } else { language_name },
            "languageCode": plex_language_code_3(language_code),
            "languageTag": language_code,
            "displayTitle": display_title,
            "extendedDisplayTitle": display_title,
            "title": technical_text(track, "Title")
                .map(str::to_owned)
                .unwrap_or_else(|| display_title.clone()),
            "default": technical_text(track, "Default") == Some("Yes"),
            "selected": match stream_type {
                1 => selected_video == Some(track_id),
                2 => selection
                    .and_then(|selection| selection.audio_stream_id)
                    .map(|selected| selected == track_id + 1)
                    .unwrap_or(selected_audio == Some(track_id)),
                3 => selection
                    .and_then(|selection| selection.subtitle_stream_id)
                    .is_some_and(|selected| selected == track_id + 1),
                _ => false,
            },
            "forced": technical_text(track, "Forced") == Some("Yes")
        });
        if stream_type == 1 {
            let width = technical_text(track, "Width")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_default();
            let height = technical_text(track, "Height")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_default();
            if height > 0 {
                media_json["videoResolution"] = serde_json::json!(plex_video_resolution(height));
            }
            media_json["width"] = serde_json::json!(width);
            media_json["height"] = serde_json::json!(height);
            media_json["videoCodec"] = serde_json::json!(codec);
            plex_stream["width"] = serde_json::json!(width);
            plex_stream["height"] = serde_json::json!(height);
            if let Some(profile) = technical_text(track, "Format_Profile") {
                let profile = plex_video_profile(profile);
                media_json["videoProfile"] = serde_json::json!(profile);
                plex_stream["profile"] = serde_json::json!(profile);
            }
            if let Some(level) = technical_text(track, "Format_Level").and_then(plex_video_level) {
                media_json["videoLevel"] = serde_json::json!(level);
                plex_stream["level"] = serde_json::json!(level);
            }
            if let Some(bitrate) = technical_bitrate_kbps(track) {
                plex_stream["bitrate"] = serde_json::json!(bitrate);
            }
            if let Some(aspect_ratio) = technical_text(track, "DisplayAspectRatio")
                .and_then(|value| value.parse::<f64>().ok())
            {
                media_json["aspectRatio"] = serde_json::json!(aspect_ratio);
            } else if height > 0 {
                media_json["aspectRatio"] = serde_json::json!(width as f64 / height as f64);
            }
            if let Some(frame_rate) = technical_text(track, "FrameRate") {
                media_json["videoFrameRate"] = serde_json::json!(frame_rate);
                plex_stream["frameRate"] = serde_json::json!(frame_rate);
            }
        } else if stream_type == 2 {
            media_json["audioCodec"] = serde_json::json!(codec);
            if let Some(layout) = technical_text(track, "ChannelLayout") {
                media_json["audioChannelLayout"] = serde_json::json!(layout);
            }
            if let Some(channels) =
                technical_text(track, "Channels").and_then(|value| value.parse::<u64>().ok())
            {
                media_json["audioChannels"] = serde_json::json!(channels);
                plex_stream["channels"] = serde_json::json!(channels);
            }
            if let Some(rate) =
                technical_text(track, "SamplingRate").and_then(|value| value.parse::<u64>().ok())
            {
                plex_stream["samplingRate"] = serde_json::json!(rate);
            }
        }
        plex_streams.push(plex_stream);
    }
    for subtitle_path in external_subtitles {
        let id = plex_external_subtitle_id(media, &subtitle_path);
        let label = external_subtitle_label(&media.path, &subtitle_path);
        plex_streams.push(serde_json::json!({
            "id": id,
            "streamType": 3,
            "type": 3,
            "index": -1,
            "codec": subtitle_path.extension().and_then(|value| value.to_str()).unwrap_or("srt").to_ascii_lowercase(),
            "language": external_subtitle_language(&media.path, &subtitle_path).1,
            "languageCode": plex_language_code_3(external_subtitle_language(&media.path, &subtitle_path).0),
            "languageTag": external_subtitle_language(&media.path, &subtitle_path).0,
            "displayTitle": label,
            "extendedDisplayTitle": label,
            "title": label,
            "external": true,
            "key": format!(
                "/library/streams/{id}.{}",
                subtitle_path.extension().and_then(|value| value.to_str()).unwrap_or("srt").to_ascii_lowercase()
            ),
            "default": false,
            "selected": selection
                .and_then(|selection| selection.subtitle_stream_id)
                .is_some_and(|selected| selected == id),
            "forced": false
        }));
    }
    media_json["id"] = serde_json::json!(plex_media_id(media));
    let part_technical_fields = ["bitrate", "videoProfile", "videoLevel"]
        .iter()
        .filter_map(|field| media_json.get(*field).cloned().map(|value| (*field, value)))
        .collect::<Vec<_>>();
    let part = media_json
        .get_mut("Part")
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|parts| parts.first_mut());
    if let Some(part) = part {
        let part_id = plex_part_id(media);
        part["id"] = serde_json::json!(part_id);
        part["key"] = serde_json::json!(format!(
            "/library/parts/{part_id}/{}/file.{}",
            media_modified_at(media),
            media.container
        ));
        part["duration"] = serde_json::json!(duration);
        part["Stream"] = serde_json::json!(plex_streams);
        for (field, value) in part_technical_fields {
            part[field] = value;
        }
    }
}

fn json_string(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_owned()
}

fn json_i64(value: &serde_json::Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|value| value.as_i64())
}

async fn download_tmdb_artwork(
    client: &reqwest::Client,
    token: &str,
    folder: &PathBuf,
    group_path: &std::path::Path,
    media_key: &str,
    metadata: &serde_json::Value,
    state: &AppState,
    replace_existing: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let media_directory = if group_path.is_file() {
        group_path.parent().unwrap_or(folder)
    } else {
        group_path
    };
    let artwork = media_directory.join(".mplex");
    tokio::fs::create_dir_all(&artwork).await?;
    for (kind, field) in [("cover", "poster_path"), ("banner", "backdrop_path")] {
        let Some(path) = metadata.get(field).and_then(|value| value.as_str()) else {
            info!(artwork = kind, "TMDB match has no artwork path");
            continue;
        };
        let local = artwork.join(format!("{kind}.jpg"));
        if !replace_existing && tokio::fs::metadata(&local).await.is_ok() {
            continue;
        }

        info!(
            artwork = kind,
            image_path = path,
            "TMDB artwork request started"
        );
        let response = match client
            .get(format!("https://image.tmdb.org/t/p/w780{}", path))
            .bearer_auth(token)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                warn!(artwork = kind, error = %error, "TMDB artwork request failed");
                return Err(Box::new(error));
            }
        };
        let status = response.status();
        if !status.is_success() {
            warn!(artwork = kind, %status, "TMDB artwork request returned an error status");
            continue;
        }
        let bytes = response.bytes().await?;
        tokio::fs::write(&local, &bytes).await?;
        record_artwork(&state.db, media_key, kind, &local);
        info!(
            artwork = kind,
            bytes = bytes.len(),
            "TMDB artwork downloaded"
        );
    }
    Ok(())
}

async fn download_tmdb_season_artwork(
    client: &reqwest::Client,
    token: &str,
    season_directory: &std::path::Path,
    show_metadata: &serde_json::Value,
    season: u32,
    media_key: &str,
    state: &AppState,
    replace_existing: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !replace_existing
        && read_sidecar_info(season_directory)
            .await
            .and_then(|info| info.get("metadata_json").cloned())
            .is_some_and(|metadata| metadata.is_object())
    {
        return Ok(());
    }

    let Some(show_id) = show_metadata.get("id").and_then(|value| value.as_i64()) else {
        return Ok(());
    };
    let response = client
        .get(format!(
            "https://api.themoviedb.org/3/tv/{show_id}/season/{season}"
        ))
        .bearer_auth(token)
        .query(&[("language", "en-US")])
        .send()
        .await?;
    if !response.status().is_success() {
        return Ok(());
    }
    let metadata = response.json::<serde_json::Value>().await?;
    let artwork = season_directory.join(".mplex");
    tokio::fs::create_dir_all(&artwork).await?;
    let info = SeasonInfo {
        title: format!("Season {season:02}"),
        description: json_string(&metadata, "overview"),
        season,
        metadata_json: metadata.clone(),
    };
    tokio::fs::write(
        artwork.join("data.json"),
        serde_json::to_string_pretty(&info)?,
    )
    .await?;
    let Some(path) = metadata.get("poster_path").and_then(|value| value.as_str()) else {
        return Ok(());
    };
    let image = client
        .get(format!("https://image.tmdb.org/t/p/w780{path}"))
        .bearer_auth(token)
        .send()
        .await?;
    if !image.status().is_success() {
        return Ok(());
    }
    let local = artwork.join("cover.jpg");
    tokio::fs::write(&local, image.bytes().await?).await?;
    record_artwork(&state.db, media_key, "season-cover", &local);
    Ok(())
}

async fn download_tmdb_season_database(
    client: &reqwest::Client,
    token: &str,
    folder: &PathBuf,
    show_metadata: &serde_json::Value,
    season: u32,
    episodes: &[&Media],
    library_key: usize,
    state: &AppState,
    replace_existing: bool,
) -> Result<(), String> {
    let Some(show_id) = show_metadata.get("id").and_then(serde_json::Value::as_i64) else {
        return Ok(());
    };
    if !replace_existing
        && episodes.iter().all(|media| {
            read_database_metadata(state, &database_media_key(library_key, &media.id))
                .and_then(|metadata| metadata.get("mplex_season_metadata_fetched").cloned())
                .and_then(|value| value.as_bool())
                == Some(true)
        })
    {
        return Ok(());
    }
    let response = client
        .get(format!(
            "https://api.themoviedb.org/3/tv/{show_id}/season/{season}"
        ))
        .bearer_auth(token)
        .query(&[("language", "en-US")])
        .send()
        .await
        .map_err(|error| format!("Could not fetch season {season}: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "TMDB returned {} for season {season}",
            response.status()
        ));
    }
    let season_metadata = response
        .json::<serde_json::Value>()
        .await
        .map_err(|error| format!("TMDB returned invalid details for season {season}: {error}"))?;
    for media in episodes {
        let key = database_media_key(library_key, &media.id);
        let metadata = read_database_metadata(state, &key)
            .unwrap_or_else(|| tv_metadata_for_media(show_metadata, folder, &media.path, "tv"));
        let metadata = tv_metadata_with_season_details(
            &metadata,
            &season_metadata,
            tv_episode_info(folder, &media.path).map(|episode| episode.episode),
        );
        if !write_database_metadata(state, &key, &metadata) {
            return Err(format!(
                "Could not save season {season} details for {}",
                media.path.display()
            ));
        }
    }
    Ok(())
}

fn tv_metadata_with_season_details(
    metadata: &serde_json::Value,
    season_metadata: &serde_json::Value,
    episode_number: Option<u32>,
) -> serde_json::Value {
    let mut metadata = metadata.clone();
    let Some(object) = metadata.as_object_mut() else {
        return metadata;
    };
    object.insert(
        "mplex_season_description".to_owned(),
        serde_json::Value::String(json_string(season_metadata, "overview")),
    );
    object.insert(
        "mplex_season_metadata_fetched".to_owned(),
        serde_json::Value::Bool(true),
    );
    object.remove("mplex_episode_metadata");
    if let Some(episode_number) = episode_number {
        if let Some(details) = season_metadata
            .get("episodes")
            .and_then(serde_json::Value::as_array)
            .and_then(|items| {
                items.iter().find(|item| {
                    item.get("episode_number")
                        .and_then(serde_json::Value::as_u64)
                        == Some(episode_number as u64)
                })
            })
        {
            object.insert("mplex_episode_metadata".to_owned(), details.clone());
        }
    }
    metadata
}

async fn ensure_tv_season_metadata(
    client: &reqwest::Client,
    token: &str,
    folder: &PathBuf,
    group: &[Media],
    library_key: usize,
    show_metadata: &serde_json::Value,
    sidecar_mode: bool,
    state: &AppState,
    replace_existing: bool,
) -> Result<(), String> {
    let mut seasons = BTreeMap::<u32, Vec<&Media>>::new();
    for media in group {
        if let Some(season) = tv_season_number(folder, &media.path) {
            seasons.entry(season).or_default().push(media);
        }
    }
    for (season, episodes) in seasons {
        let Some(episode_path) = episodes.first().map(|media| media.path.as_path()) else {
            continue;
        };
        let Some(show_name) = episode_path
            .strip_prefix(folder)
            .ok()
            .and_then(|path| path.components().next())
        else {
            continue;
        };
        let show_directory = folder.join(show_name.as_os_str());
        let season_directory = tv_season_directory(&show_directory, season, episode_path);
        let media_key = database_media_key(library_key, &episodes[0].id);
        let result = if sidecar_mode {
            download_tmdb_season_artwork(
                client,
                token,
                &season_directory,
                show_metadata,
                season,
                &media_key,
                state,
                replace_existing,
            )
            .await
            .map_err(|error| error.to_string())
        } else {
            download_tmdb_season_database(
                client,
                token,
                folder,
                show_metadata,
                season,
                &episodes,
                library_key,
                state,
                replace_existing,
            )
            .await
        };
        result?;
    }
    Ok(())
}

fn record_artwork(
    db: &Arc<Mutex<Connection>>,
    media_key: &str,
    kind: &str,
    path: &std::path::Path,
) {
    if let Ok(connection) = db.lock() {
        let _ = connection.execute(
            "INSERT OR REPLACE INTO artwork (media_key, kind, local_path, downloaded_at) VALUES (?1, ?2, ?3, strftime('%s', 'now'))",
            (media_key, kind, path.to_string_lossy().as_ref()),
        );
    }
}

fn settings_error(message: &str) -> Response {
    let location = format!("/settings?error={}", url_encode(message));
    Redirect::to(&location).into_response()
}

fn is_async_settings_action(headers: &HeaderMap) -> bool {
    headers
        .get("X-Mplex-Async")
        .and_then(|value| value.to_str().ok())
        == Some("1")
}

fn settings_action_error(headers: &HeaderMap, status: StatusCode, message: &str) -> Response {
    if is_async_settings_action(headers) {
        (status, message.to_owned()).into_response()
    } else {
        settings_error(message)
    }
}

fn settings_action_response(headers: &HeaderMap, redirect: &str) -> Response {
    if is_async_settings_action(headers) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        Redirect::to(redirect).into_response()
    }
}

fn library_form_error(message: &str) -> Response {
    Redirect::to(&format!(
        "/settings?add_library=true&error={}",
        url_encode(message)
    ))
    .into_response()
}

async fn settings_browse(Query(query): Query<BrowseQuery>) -> Response {
    let at_root = query.path.is_none();
    let directory = query
        .path
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/")));
    let Ok(metadata) = tokio::fs::metadata(&directory).await else {
        return (StatusCode::NOT_FOUND, "Directory does not exist").into_response();
    };
    if !metadata.is_dir() {
        return (StatusCode::BAD_REQUEST, "Path is not a directory").into_response();
    }
    let mut entries = match tokio::fs::read_dir(&directory).await {
        Ok(entries) => entries,
        Err(_) => return StatusCode::FORBIDDEN.into_response(),
    };
    let mut folders = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        let path = entry.path();
        if entry.file_type().await.is_ok_and(|kind| kind.is_dir()) {
            folders.push((name, path));
        }
    }
    folders.sort_by_key(|(name, _)| name.to_ascii_lowercase());
    let parent = directory.parent().unwrap_or(&directory);
    let items = folders
        .iter()
        .map(|(name, path)| {
            let encoded = url_encode(&path.to_string_lossy());
            format!(
                "<li class=\"folder\"><span class=\"folder-name\">📁 {}</span><span><a class=\"open\" href=\"/settings/browse?path={}\">Open</a><a class=\"choose\" href=\"/settings?folder={}\">Choose</a></span></li>",
                html(name), encoded, encoded
            )
        })
        .collect::<String>();
    let shortcuts = if at_root {
        let candidates = [
            std::env::current_dir().ok(),
            Some(PathBuf::from("/Users")),
            Some(PathBuf::from("/Volumes")),
            Some(PathBuf::from("/home")),
            Some(PathBuf::from("/mnt")),
            Some(PathBuf::from("/media")),
        ];
        candidates
            .into_iter()
            .flatten()
            .filter(|path| path.is_dir())
            .map(|path| {
                format!(
                    "<a class=\"shortcut\" href=\"/settings/browse?path={}\">📂 {}</a>",
                    url_encode(&path.to_string_lossy()),
                    html(&path.to_string_lossy())
                )
            })
            .collect::<String>()
    } else {
        String::new()
    };
    let page = format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Choose folder · harbringer</title><style>body {{ margin: 0; padding: 2rem; color: #f9fafb; background: #111827; font-family: system-ui, sans-serif; }} main {{ max-width: 820px; margin: auto; }} a {{ color: #93c5fd; }} .toolbar {{ display: flex; flex-wrap: wrap; gap: .6rem; margin: 1.2rem 0; }} .shortcut, .action {{ display: inline-block; padding: .6rem .8rem; border: 1px solid #374151; border-radius: .5rem; text-decoration: none; background: #1f2937; }} .folder-list {{ padding: 0; list-style: none; border: 1px solid #374151; border-radius: .6rem; overflow: hidden; }} .folder {{ display: flex; justify-content: space-between; gap: 1rem; padding: .8rem 1rem; border-bottom: 1px solid #374151; }} .folder:last-child {{ border-bottom: 0; }} .open {{ margin-right: 1rem; }} code, .note {{ color: #9ca3af; overflow-wrap: anywhere; }} </style></head><body><main><p><a href="/settings">← Back to Settings</a></p><h1>Choose a media folder</h1><p class="note">Pick a folder on the harbringer server. You can add more folders to the same library after this.</p><p><strong>Current folder:</strong> <code>{}</code></p><div class="toolbar"><a class="action" href="/settings/browse?path={}">⬆ Parent</a><a class="action" href="/settings?folder={}">Choose this folder</a></div>{}<ul class="folder-list">{}</ul></main></body></html>"#,
        html(&directory.to_string_lossy()),
        url_encode(&parent.to_string_lossy()),
        url_encode(&directory.to_string_lossy()),
        shortcuts,
        items
    );
    let page = page.replace(
        "</body>",
        "<script>document.addEventListener('click',event=>{const link=event.target.closest?.('a.choose, .toolbar a.action[href*=\\\"/settings?folder=\\\"]');if(!link||window.parent===window)return;event.preventDefault();const path=new URL(link.href,location.origin).searchParams.get('folder');window.parent.postMessage({type:'mplex-folder-selected',path},location.origin);});</script></body>",
    );
    Html(page).into_response()
}

async fn create_library(State(state): State<AppState>, RawForm(raw): RawForm) -> Response {
    let fields = match serde_urlencoded::from_bytes::<Vec<(String, String)>>(&raw) {
        Ok(fields) => fields,
        Err(_) => return library_form_error("Invalid library form"),
    };
    let mut name = String::new();
    let mut kind = String::new();
    let mut paths = Vec::new();
    for (key, value) in fields {
        match key.as_str() {
            "name" => name = value,
            "kind" => kind = value,
            "path" | "path[]" => paths.push(value),
            _ => {}
        }
    }
    let kind = match kind.as_str() {
        "movie" | "tv" | "other" => kind,
        _ => return library_form_error("Invalid library type"),
    };
    let name = name.trim();
    if name.is_empty() {
        return library_form_error("Library name is required");
    }
    if paths.is_empty() {
        return library_form_error("At least one folder is required");
    }
    let mut canonical_paths = Vec::with_capacity(paths.len());
    for value in paths
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        let Ok(path_metadata) = tokio::fs::metadata(value).await else {
            return library_form_error(&format!("Directory does not exist: {}", value));
        };
        if !path_metadata.is_dir() {
            return library_form_error(&format!("Not a directory: {}", value));
        }
        let Ok(path) = tokio::fs::canonicalize(value).await else {
            return library_form_error(&format!("Directory could not be resolved: {}", value));
        };
        if !canonical_paths.contains(&path) {
            canonical_paths.push(path);
        }
    }
    if canonical_paths.is_empty() {
        return library_form_error("At least one folder is required");
    }
    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if config.libraries.iter().any(|library| library.name == name) {
        return library_form_error("A library with that name already exists");
    }
    config.libraries.push(ConfigLibrary {
        name: name.to_owned(),
        kind,
        folders: canonical_paths,
        show_recently_added: true,
        show_continue_watching: true,
        scan_enabled: true,
        scan_interval_value: default_scan_interval_value(),
        scan_interval_unit: default_scan_interval_unit(),
        plex_agent: None,
        plex_scanner: None,
        plex_language: None,
        metadata_agent_provider_group_id: None,
        plex_preferences: None,
    });
    if save_config(&state.config_path, &config).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if sync_config_libraries(&connection, &config).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    Redirect::to(&format!(
        "/settings?library_created=true&add_library=true&library_open={}",
        config.libraries.len() - 1
    ))
    .into_response()
}

async fn add_library_folder(
    State(state): State<AppState>,
    Form(form): Form<LibraryFolderForm>,
) -> Response {
    let requested = PathBuf::from(form.path.trim());
    let Ok(metadata) = tokio::fs::metadata(&requested).await else {
        return settings_error("Directory does not exist");
    };
    if !metadata.is_dir() {
        return settings_error("Path is not a directory");
    }
    let Ok(path) = tokio::fs::canonicalize(requested).await else {
        return settings_error("Directory could not be resolved");
    };
    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Some(library) = config.libraries.get_mut(form.library_id as usize) else {
        return settings_error("Library does not exist");
    };
    if library.folders.iter().any(|folder| folder == &path) {
        return Redirect::to(&format!("/settings?library_open={}", form.library_id))
            .into_response();
    }
    library.folders.push(path);
    if save_config(&state.config_path, &config).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if sync_config_libraries(&connection, &config).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    Redirect::to(&format!("/settings?library_open={}", form.library_id)).into_response()
}

async fn remove_library_folder(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<RemoveLibraryFolderForm>,
) -> Response {
    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Some(library) = config.libraries.get(form.library_id) else {
        return settings_action_error(&headers, StatusCode::NOT_FOUND, "Library does not exist");
    };
    let Some(folder) = library.folders.get(form.folder_index) else {
        return settings_action_error(&headers, StatusCode::NOT_FOUND, "Folder does not exist");
    };
    let path = folder.to_string_lossy().into_owned();
    if path != form.path {
        return settings_action_error(
            &headers,
            StatusCode::CONFLICT,
            "Folder list changed. Reload and try again",
        );
    }
    let library_name = library.name.clone();
    let mut updated_config = config.clone();
    updated_config.libraries[form.library_id]
        .folders
        .remove(form.folder_index);

    let Ok(mut connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Ok(transaction) = connection.transaction() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if transaction
        .execute(
            "DELETE FROM library_folders WHERE library_id = (SELECT id FROM library_groups WHERE name = ?1) AND path = ?2",
            (&library_name, &path),
        )
        .is_err()
        || transaction
            .execute(
                "UPDATE library_groups SET updated_at = MAX(updated_at + 1, CAST(strftime('%s', 'now') AS INTEGER)) WHERE name = ?1",
                [&library_name],
            )
            .is_err()
        || update_library_catalog_timestamp(&transaction).is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if save_config(&state.config_path, &updated_config).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if transaction.commit().is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    *config = updated_config;
    settings_action_response(
        &headers,
        &format!("/settings?library_open={}", form.library_id),
    )
}

async fn rename_library(
    State(state): State<AppState>,
    Path(library_id): Path<usize>,
    RawForm(raw): RawForm,
) -> Response {
    let form = match serde_urlencoded::from_bytes::<RenameLibraryForm>(&raw) {
        Ok(form) => form,
        Err(_) => return settings_error("Invalid library name form"),
    };
    let name = form.name.trim();
    if name.is_empty() {
        return settings_error("Library name is required");
    }
    if name.chars().count() > 64 {
        return settings_error("Library names must be 64 characters or fewer");
    }
    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if library_id >= config.libraries.len() {
        return settings_error("Library does not exist");
    }
    let old_name = config.libraries[library_id].name.clone();
    if config
        .libraries
        .iter()
        .enumerate()
        .any(|(index, library)| index != library_id && library.name.eq_ignore_ascii_case(name))
    {
        return settings_error("A library with that name already exists");
    }
    config.libraries[library_id].name = name.to_owned();
    if save_config(&state.config_path, &config).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let updated = connection.execute(
        "UPDATE library_groups SET name = ?1, updated_at = MAX(updated_at + 1, CAST(strftime('%s', 'now') AS INTEGER)) WHERE name = ?2",
        (&name, &old_name),
    );
    if updated.is_err() || update_library_catalog_timestamp(&connection).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    Redirect::to("/settings?library_renamed=true").into_response()
}

async fn save_library_settings(
    State(state): State<AppState>,
    Path(library_id): Path<usize>,
    Form(form): Form<LibrarySettingsForm>,
) -> Response {
    let name = form.name.trim();
    if name.is_empty() {
        return settings_error("Library name is required");
    }
    if name.chars().count() > 64 {
        return settings_error("Library names must be 64 characters or fewer");
    }
    if form.scan_interval_value == 0
        || !matches!(
            form.scan_interval_unit.as_str(),
            "seconds" | "minutes" | "hours"
        )
    {
        return settings_error("Scan intervals need a positive integer and a valid time unit");
    }
    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if library_id >= config.libraries.len() {
        return settings_error("Library does not exist");
    }
    let old_name = config.libraries[library_id].name.clone();
    if config
        .libraries
        .iter()
        .enumerate()
        .any(|(index, library)| index != library_id && library.name.eq_ignore_ascii_case(name))
    {
        return settings_error("A library with that name already exists");
    }
    let library = &mut config.libraries[library_id];
    library.name = name.to_owned();
    library.scan_enabled = form.scan_enabled.is_some();
    library.scan_interval_value = form.scan_interval_value;
    library.scan_interval_unit = form.scan_interval_unit;
    library.show_recently_added = form.show_recently_added.is_some();
    library.show_continue_watching = form.show_continue_watching.is_some();
    if save_config(&state.config_path, &config).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if old_name != name {
        let Ok(connection) = state.db.lock() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        let updated = connection.execute(
            "UPDATE library_groups SET name = ?1, updated_at = MAX(updated_at + 1, CAST(strftime('%s', 'now') AS INTEGER)) WHERE name = ?2",
            (name, &old_name),
        );
        if updated.is_err() || update_library_catalog_timestamp(&connection).is_err() {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }
    Redirect::to(&format!(
        "/settings?library_saved=true&library_open={library_id}"
    ))
    .into_response()
}

async fn update_library_display(
    State(state): State<AppState>,
    Form(form): Form<LibraryDisplayForm>,
) -> Response {
    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Some(library) = config.libraries.get_mut(form.library_id) else {
        return settings_error("Library does not exist");
    };
    library.show_recently_added = form.show_recently_added.is_some();
    library.show_continue_watching = form.show_continue_watching.is_some();
    if save_config(&state.config_path, &config).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    Redirect::to("/settings").into_response()
}

async fn update_library_scan_settings(
    State(state): State<AppState>,
    Form(form): Form<LibraryScanSettingsForm>,
) -> Response {
    if form.scan_interval_value == 0
        || !matches!(
            form.scan_interval_unit.as_str(),
            "seconds" | "minutes" | "hours"
        )
    {
        return settings_error("Scan intervals need a positive integer and a valid time unit");
    }
    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Some(library) = config.libraries.get_mut(form.library_id) else {
        return settings_error("Library does not exist");
    };
    library.scan_enabled = form.scan_enabled.is_some();
    library.scan_interval_value = form.scan_interval_value;
    library.scan_interval_unit = form.scan_interval_unit;
    if save_config(&state.config_path, &config).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    Redirect::to("/settings").into_response()
}

async fn scan_all_libraries(State(state): State<AppState>) -> Response {
    let library_count = match state.config.lock() {
        Ok(config) => config.libraries.len(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let mut media_count = 0;
    let mut failed = false;
    for library_id in 0..library_count {
        match scan_library_inventory(&state, library_id).await {
            Ok(count) => media_count += count,
            Err(error) => {
                failed = true;
                warn!(library_id, %error, "manual library scan failed");
            }
        }
    }
    Redirect::to(&format!(
        "/settings?inventory_scan={media_count}&inventory_error={failed}"
    ))
    .into_response()
}

async fn scan_library_inventory_handler(
    State(state): State<AppState>,
    Path(library_id): Path<usize>,
) -> Response {
    match scan_library_inventory(&state, library_id).await {
        Ok(count) => Redirect::to(&format!("/settings?inventory_scan={count}")).into_response(),
        Err(error) => {
            warn!(library_id, %error, "manual library scan failed");
            Redirect::to("/settings?inventory_error=true").into_response()
        }
    }
}

async fn plex_section_refresh(
    State(state): State<AppState>,
    Path(section): Path<usize>,
) -> Response {
    let Some(library_id) = section.checked_sub(1) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let library_exists = match state.config.lock() {
        Ok(config) => config.libraries.get(library_id).is_some(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if !library_exists {
        return StatusCode::NOT_FOUND.into_response();
    }
    if let Err(error) = scan_library_inventory(&state, library_id).await {
        warn!(section, %error, "Plex library scan failed");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let metadata_response = scan_library_metadata(
        State(state.clone()),
        Path(library_id),
        Query(ScanQuery {
            force: Some(false),
            path: None,
            refresh: None,
        }),
    )
    .await;
    if !metadata_response.status().is_success() && !metadata_response.status().is_redirection() {
        warn!(section, status = %metadata_response.status(), "Plex metadata refresh failed");
        return metadata_response;
    }
    StatusCode::OK.into_response()
}

async fn delete_library(
    State(state): State<AppState>,
    Path(library_id): Path<usize>,
    headers: HeaderMap,
) -> Response {
    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Some(library) = config.libraries.get(library_id) else {
        return settings_action_error(&headers, StatusCode::NOT_FOUND, "Library does not exist");
    };
    let library_name = library.name.clone();
    let mut updated_config = config.clone();
    updated_config.libraries.remove(library_id);

    let Ok(mut connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Ok(transaction) = connection.transaction() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if transaction
        .execute(
            "DELETE FROM library_folders WHERE library_id = (SELECT id FROM library_groups WHERE name = ?1)",
            [&library_name],
        )
        .and_then(|_| {
            transaction.execute("DELETE FROM library_groups WHERE name = ?1", [&library_name])
        })
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if update_library_catalog_timestamp(&transaction).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if save_config(&state.config_path, &updated_config).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if transaction.commit().is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    *config = updated_config;
    settings_action_response(&headers, "/settings")
}

#[derive(Deserialize, Default)]
struct WebPath {
    path: Option<String>,
    library: Option<usize>,
    kind: Option<String>,
    stream: Option<usize>,
    play: Option<String>,
    episode: Option<String>,
    view: Option<String>,
    season: Option<u32>,
}

fn selected_library_root(state: &AppState, library: Option<usize>) -> PathBuf {
    let Ok(config) = state.config.lock() else {
        return state.root.as_ref().clone();
    };
    library
        .and_then(|index| config.libraries.get(index))
        .and_then(|library| library.folders.first())
        .cloned()
        .unwrap_or_else(|| state.root.as_ref().clone())
}

async fn home_client(State(state): State<AppState>) -> Response {
    let Ok(config) = state.config.lock().map(|config| config.clone()) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let media_index = media_snapshot(&state);
    let metadata_by_library = config
        .libraries
        .iter()
        .enumerate()
        .map(|(index, _)| plex_section_metadata(&state, index + 1))
        .collect::<Vec<_>>();
    let mut libraries = String::new();
    for (index, library) in config.libraries.iter().enumerate() {
        let artwork = if config.metadata_storage == "sidecar" {
            library_artwork(library, index).await
        } else {
            None
        };
        let artwork_markup = artwork
            .map(|(cover, banner)| {
                format!(
                    r#"<span class="library-art" style="display:grid;place-items:center;min-height:120px;background-size:cover;background-position:center;background-image:linear-gradient(90deg,rgba(17,24,39,.2),rgba(17,24,39,.8)),url('{}')">{}</span>"#,
                    banner,
                    cover_artwork_markup(&cover, &library.name)
                )
            })
            .unwrap_or_else(|| {
                format!(
                    r#"<span class="library-art library-art-fallback" style="display:grid;place-items:center;min-height:120px;font-size:2.5rem;background:linear-gradient(135deg,#1e3a8a,#312e81)">{}</span>"#,
                    if library.kind == "tv" { "📺" } else { "🎬" }
                )
            });
        libraries.push_str(&format!(
            "<a class=\"library-card\" data-spa-link style=\"overflow:hidden;padding:0\" href=\"/?library={}\">{}<span class=\"library-card-info\" style=\"display:flex;flex-direction:column;gap:.25rem;padding:.85rem 1rem 1rem\"><strong>{}</strong><span class=\"note\">{}</span></span></a>",
            index,
            artwork_markup,
            html(&library.name),
            format!(
                "{} · {} folders",
                library_kind_label(&library.kind),
                library.folders.len()
            )
        ));
    }
    let (continue_watching, watched_paths, hidden_paths, last_activity) = state
        .db
        .lock()
        .ok()
        .and_then(|connection| {
            let mut statement = connection
                .prepare("SELECT media_key, watched, hidden_from_continue, updated_at FROM watch_progress WHERE user_id = ?1")
                .ok()?;
            let rows = statement
                .query_map([USER_ID], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, bool>(1)?,
                        row.get::<_, bool>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                })
                .ok()?;
            let mut continue_watching = Vec::new();
            let mut watched_paths = std::collections::HashSet::new();
            let mut hidden_paths = std::collections::HashSet::new();
            let mut last_activity = std::collections::HashMap::new();
            for row in rows.filter_map(Result::ok) {
                let (path, watched, hidden, updated_at) = row;
                if watched {
                    watched_paths.insert(path.clone());
                }
                if hidden {
                    hidden_paths.insert(path);
                    continue;
                }
                last_activity.insert(path.clone(), updated_at);
                if !watched {
                    continue_watching.push((path, updated_at));
                }
            }
            Some((
                continue_watching,
                watched_paths,
                hidden_paths,
                last_activity,
            ))
        })
        .unwrap_or_default();
    let mut tv_episode_paths = std::collections::HashSet::new();
    for library in config
        .libraries
        .iter()
        .filter(|library| library.kind == "tv")
    {
        for root in &library.folders {
            for media in media_index.iter() {
                let Ok(relative) = media.path.strip_prefix(root) else {
                    continue;
                };
                if tv_episode_info(root, &media.path).is_some() {
                    tv_episode_paths.insert(relative.to_string_lossy().into_owned());
                }
            }
        }
    }
    let mut continue_candidates = next_tv_episode_paths(
        &config.libraries,
        media_index.as_ref(),
        &watched_paths,
        &hidden_paths,
        &last_activity,
    );
    for (path, updated_at) in continue_watching {
        if !tv_episode_paths.contains(&path) {
            continue_candidates.push((path, updated_at));
        }
    }
    let continue_paths = ordered_continue_paths(continue_candidates);
    let mut continue_cards = String::new();
    for path in continue_paths {
        let Some((library_index, root, media)) =
            config
                .libraries
                .iter()
                .enumerate()
                .find_map(|(index, library)| {
                    if !library.show_continue_watching {
                        return None;
                    }
                    library.folders.iter().find_map(|root| {
                        media_index
                            .iter()
                            .find(|media| {
                                media
                                    .path
                                    .strip_prefix(root)
                                    .ok()
                                    .is_some_and(|relative| relative.to_string_lossy() == path)
                            })
                            .map(|media| (index, root, media))
                    })
                })
        else {
            continue;
        };
        let Some(folder) = media.path.parent() else {
            continue;
        };
        let Ok(relative_folder) = folder.strip_prefix(root) else {
            continue;
        };
        let Ok(relative_media) = media.path.strip_prefix(root) else {
            continue;
        };
        let metadata = metadata_by_library
            .get(library_index)
            .and_then(|items| items.get(&media.id));
        continue_cards.push_str(&media_card(
            media,
            &config.libraries[library_index],
            metadata,
            root,
            relative_folder,
            relative_media,
            library_index,
            true,
        ));
    }
    let continue_watching = if continue_cards.is_empty() {
        "<p class=\"note empty-state\">Nothing to resume yet. Start watching something and it will appear here.</p>".to_owned()
    } else {
        continue_cards
    };
    let mut recent = String::new();
    let mut recent_count = 0;
    for media in media_index.iter() {
        if recent_count >= 12 {
            break;
        }
        let Some((library_index, root)) =
            config
                .libraries
                .iter()
                .enumerate()
                .find_map(|(index, library)| {
                    if !library.show_recently_added {
                        return None;
                    }
                    library
                        .folders
                        .iter()
                        .find(|folder| media.path.starts_with(folder))
                        .map(|folder| (index, folder))
                })
        else {
            continue;
        };
        let Some(folder) = media.path.parent() else {
            continue;
        };
        let Ok(relative_folder) = folder.strip_prefix(root) else {
            continue;
        };
        let Ok(relative_media) = media.path.strip_prefix(root) else {
            continue;
        };
        recent.push_str(&media_card(
            media,
            &config.libraries[library_index],
            metadata_by_library
                .get(library_index)
                .and_then(|items| items.get(&media.id)),
            root,
            relative_folder,
            relative_media,
            library_index,
            false,
        ));
        recent_count += 1;
    }
    if recent.is_empty() {
        recent = "<p class=\"note empty-state\">No recently added media yet.</p>".to_owned();
    }
    Html(format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>harbringer</title><style>body {{ margin: 0; color: #f9fafb; background: #111827; font-family: system-ui, sans-serif; }} .app-shell {{ display: grid; grid-template-columns: 240px minmax(0, 1fr); min-height: 100vh; }} .sidebar {{ position: sticky; top: 0; height: 100vh; box-sizing: border-box; padding: 1.25rem .9rem; border-right: 1px solid #263244; background: #0d1421; }} .sidebar-brand {{ display: block; padding: .5rem .7rem 1.5rem; color: #f9fafb; font-size: 1.25rem; font-weight: 700; text-decoration: none; }} .sidebar-top {{ display: flex; justify-content: space-between; align-items: center; }} .sidebar-toggle {{ background: none; border: 0; color: #cbd5e1; font-size: 1.3rem; cursor: pointer; }} .sidebar-label {{ padding: 1rem .7rem .4rem; color: #6b7280; font-size: .7rem; text-transform: uppercase; }} .sidebar-nav {{ display: grid; gap: .25rem; }} .sidebar-link {{ display: flex; gap: .7rem; padding: .65rem .7rem; border-radius: .5rem; color: #cbd5e1; text-decoration: none; }} .sidebar-link.active, .sidebar-link:hover {{ background: #1d4ed8; color: #fff; }} .sidebar-icon {{ width: 1.3rem; text-align: center; }} main {{ max-width: 1100px; width: 100%; box-sizing: border-box; margin: auto; padding: 2rem 1rem; }} a {{ color: #93c5fd; text-decoration: none; }} h1 {{ margin-top: 0; }} .section {{ margin: 2rem 0; }} .library-grid, .recent-grid, .continue-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax({}px, 1fr)); gap: 1rem; }} .library-card {{ display: flex; flex-direction: column; gap: .4rem; padding: 1rem; border: 1px solid #374151; border-radius: .7rem; background: #1f2937; }} .media-card {{ position: relative; min-width: 0; padding: .7rem; border: 1px solid transparent; border-radius: .6rem; background: #1f2937; transition: border-color .15s, transform .15s; }} .media-card:hover {{ border-color: #374151; transform: translateY(-2px); }} .media-card-artwork {{ position: relative; }} .media-card-poster, .media-card-poster img {{ display: block; width: 100%; aspect-ratio: 2 / 3; }} .media-card-poster img {{ object-fit: cover; border-radius: .35rem; background: #111827; }} .media-card-info {{ display: grid; gap: .12rem; padding: .55rem 0 1.6rem; color: inherit; text-align: center; text-decoration: none; }} .media-card-title {{ display: block; height: 2.4em; overflow: hidden; margin: .25rem 0 .15rem; font-size: .8rem; font-weight: 500; line-height: 1.2; }} .media-card-tv .media-card-title {{ height: auto; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }} .media-card-subtitle, .media-card-details {{ overflow: hidden; color: #9ca3af; font-size: .75rem; line-height: 1.35; text-overflow: ellipsis; white-space: nowrap; }} .media-card-progress-row {{ display: flex; align-items: center; gap: .45rem; margin: .3rem 0 .4rem; }} .media-card-progress {{ flex: 1 1 auto; min-width: 0; height: .3rem; border-radius: 999px; background: #374151; overflow: hidden; }} .media-card-progress span {{ display: block; height: 100%; border-radius: inherit; background: linear-gradient(90deg, #2563eb, #22c55e); }} .media-card-percent {{ flex: 0 0 auto; color: #93c5fd; font-size: .68rem; line-height: 1; }} .media-card-menu {{ position: absolute; right: .45rem; bottom: .45rem; z-index: 4; }} .media-card-menu summary {{ display: grid; width: 1.65rem; height: 1.65rem; place-items: center; border: 1px solid rgba(255,255,255,.25); border-radius: 50%; color: #f9fafb; background: rgba(17,24,39,.58); backdrop-filter: blur(3px); font-size: 1rem; font-weight: 800; line-height: 1; list-style: none; cursor: pointer; }} .media-card-menu summary::-webkit-details-marker {{ display: none; }} .media-card-menu-items {{ position: absolute; right: 0; bottom: calc(100% + .35rem); display: grid; min-width: 12.5rem; gap: .2rem; padding: .35rem; border: 1px solid #374151; border-radius: .5rem; background: #111827; box-shadow: 0 8px 24px #0008; }} .media-card-menu-items button {{ padding: .55rem .65rem; border: 0; border-radius: .3rem; color: #e5e7eb; background: transparent; font: inherit; font-size: .78rem; text-align: left; cursor: pointer; }} .media-card-menu-items button:hover {{ background: #263244; }} .library-icon {{ font-size: 2rem; }} .note {{ color: #9ca3af; }} .empty-state {{ padding: 1.5rem; border: 1px dashed #4b5563; border-radius: .7rem; }} @media (max-width: 700px) {{ .app-shell {{ display: block; }} .sidebar {{ position: static; height: auto; width: 100%; }} .sidebar-nav {{ display: flex; overflow-x: auto; }} .sidebar-link {{ white-space: nowrap; }} .sidebar-label {{ display: none; }} .sidebar-toggle {{ display: none; }} }}</style></head><body><div class="app-shell">{}<main><h1>Home</h1><section class="section"><h2>Continue watching</h2><div class="continue-grid">{}</div></section><section class="section"><h2>Recently added</h2><div class="recent-grid">{}</div></section><section class="section"><h2>Your libraries</h2><div class="library-grid">{}</div></section></main></div>{}{}<script>const shell=document.querySelector('.app-shell'), toggle=document.querySelector('.sidebar-toggle'); if(localStorage.getItem('mplex-sidebar-collapsed')==='true') shell.classList.add('sidebar-collapsed'); toggle?.addEventListener('click',()=>{{shell.classList.toggle('sidebar-collapsed');localStorage.setItem('mplex-sidebar-collapsed',shell.classList.contains('sidebar-collapsed')?'true':'false')}});</script></body></html>"#,
        POSTER_CARD_MIN_WIDTH, sidebar_nav_for(&state, std::path::Path::new(""), None), continue_watching, recent, libraries, media_card_action_script(), spa_navigation_script()
    )).into_response()
}

fn next_tv_episode_paths(
    libraries: &[ConfigLibrary],
    media_items: &[Media],
    watched_paths: &std::collections::HashSet<String>,
    hidden_paths: &std::collections::HashSet<String>,
    last_activity: &std::collections::HashMap<String, i64>,
) -> Vec<(String, i64)> {
    let mut next_paths = Vec::new();
    for library in libraries
        .iter()
        .filter(|library| library.kind == "tv" && library.show_continue_watching)
    {
        for root in &library.folders {
            let mut shows =
                std::collections::BTreeMap::<String, Vec<(String, (u32, u32, u32))>>::new();
            for media in media_items {
                let Ok(relative) = media.path.strip_prefix(root) else {
                    continue;
                };
                let Some(show_folder) = relative.components().next() else {
                    continue;
                };
                let Some(episode) = tv_episode_info(root, &media.path) else {
                    continue;
                };
                let path = relative.to_string_lossy().into_owned();
                let show = show_folder.as_os_str().to_string_lossy().into_owned();
                let last_episode = episode.episode_end.unwrap_or(episode.episode);
                shows
                    .entry(show)
                    .or_default()
                    .push((path, (episode.season, episode.episode, last_episode)));
            }
            for mut episodes in shows.into_values() {
                episodes.sort_by_key(|(_, (season, episode, _))| (*season, *episode));
                let Some(show_activity) = episodes
                    .iter()
                    .filter_map(|(path, _)| last_activity.get(path))
                    .copied()
                    .max()
                else {
                    continue;
                };
                if let Some((path, _)) = episodes
                    .iter()
                    .filter(|(path, _)| {
                        !watched_paths.contains(path) && !hidden_paths.contains(path)
                    })
                    .min_by_key(|(_, (season, episode, _))| (*season, *episode))
                {
                    next_paths.push((path.clone(), show_activity));
                }
            }
        }
    }
    next_paths
}

fn ordered_continue_paths(mut candidates: Vec<(String, i64)>) -> Vec<String> {
    candidates.sort_by(|(left_path, left_time), (right_path, right_time)| {
        right_time
            .cmp(left_time)
            .then_with(|| left_path.cmp(right_path))
    });
    let mut seen = std::collections::HashSet::new();
    candidates
        .into_iter()
        .filter_map(|(path, _)| seen.insert(path.clone()).then_some(path))
        .take(12)
        .collect()
}

async fn library_artwork(library: &ConfigLibrary, index: usize) -> Option<(String, String)> {
    let root = library.folders.first()?;
    let media = scan_media(root).await.ok()?;
    for item in media {
        let parent = item.path.parent()?;
        if tokio::fs::metadata(parent.join(".mplex/cover.jpg"))
            .await
            .is_err()
        {
            continue;
        }
        let relative = parent.strip_prefix(root).ok()?;
        let cover = artwork_url(relative, Some(index), "cover");
        let banner = if tokio::fs::metadata(parent.join(".mplex/banner.jpg"))
            .await
            .is_ok()
        {
            artwork_url(relative, Some(index), "banner")
        } else {
            cover.clone()
        };
        return Some((cover, banner));
    }
    None
}

fn cover_artwork_markup(url: &str, label: &str) -> String {
    format!(
        r#"<img class="cover-art" style="width:30%;max-height:110px;object-fit:cover;border-radius:.25rem;box-shadow:0 3px 12px #0008" src="{}" alt="Cover for {}">"#,
        html(url),
        html(label)
    )
}

async fn web_client(State(s): State<AppState>, Query(mut query): Query<WebPath>) -> Response {
    if query.library.is_none() {
        query.library = s
            .config
            .lock()
            .ok()
            .and_then(|config| (!config.libraries.is_empty()).then_some(0));
    }
    let files_view = query.view.as_deref() == Some("files");
    let media_index = media_snapshot(&s);
    let relative = match clean_relative_path(query.path.as_deref().unwrap_or("")) {
        Some(path) => path,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };
    if contains_hidden_component(&relative) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let library_root = selected_library_root(&s, query.library);
    let library_kind = query.library.and_then(|index| {
        s.config.lock().ok().and_then(|config| {
            config
                .libraries
                .get(index)
                .map(|library| library.kind.clone())
        })
    });
    let sidecar_mode = s
        .config
        .lock()
        .is_ok_and(|config| config.metadata_storage == "sidecar");
    let library_config = query.library.and_then(|index| {
        s.config
            .lock()
            .ok()
            .and_then(|config| config.libraries.get(index).cloned())
    });
    let library_top_level =
        relative.as_os_str().is_empty() && query.play.is_none() && query.episode.is_none();
    let directory = match web_path(&s, &library_root, &relative).await {
        Ok(path) if path.is_dir() => path,
        Ok(_) => return StatusCode::NOT_FOUND.into_response(),
        Err(status) => return status.into_response(),
    };
    let selected_relative = query
        .episode
        .as_deref()
        .or(query.play.as_deref())
        .and_then(clean_relative_path);
    if let Some(selected_relative) = selected_relative.as_deref() {
        match web_path(&s, &library_root, selected_relative).await {
            Ok(path) if path.is_file() && media_type(&path).is_some() => {}
            _ => return StatusCode::NOT_FOUND.into_response(),
        }
    }
    let pane_banner = if sidecar_mode
        && tokio::fs::metadata(directory.join(".mplex/banner.jpg"))
            .await
            .is_ok()
    {
        format!(
            r#" style="background-size:cover;background-position:center;background-attachment:fixed;background-image:linear-gradient(180deg,rgba(17,24,39,.72),rgba(17,24,39,.96)),url('{}')""#,
            artwork_url(&relative, query.library, "banner")
        )
    } else {
        String::new()
    };
    let tv_season_view = library_kind.as_deref() == Some("tv")
        && (query.season.is_some()
            || relative
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(parse_tv_season_directory)
                .is_some());
    let tv_show_view = library_kind.as_deref() == Some("tv")
        && relative.components().count() == 1
        && !tv_season_view;

    let info_panel = if let Some(selected_relative) = selected_relative.as_deref() {
        if tv_episode_info(&library_root, &library_root.join(selected_relative)).is_some() {
            episode_info_panel(&s, &library_root, selected_relative).await
        } else {
            let selected_path = library_root.join(selected_relative);
            let database_metadata = if sidecar_mode {
                None
            } else {
                media_index
                    .iter()
                    .find(|media| media.path == selected_path)
                    .and_then(|media| {
                        media_library(&s, media)
                            .map(|(library_id, _)| format!("{library_id}-{}", media.id))
                    })
                    .and_then(|media_key| read_database_metadata(&s, &media_key))
            };
            media_info_panel(
                selected_path.parent().unwrap_or(&library_root),
                database_metadata.as_ref(),
                false,
                None,
            )
            .await
        }
    } else if sidecar_mode || tv_season_view || tv_show_view {
        let cover_url = if tv_show_view
            && sidecar_mode
            && tokio::fs::metadata(directory.join(".mplex/cover.jpg"))
                .await
                .is_ok()
        {
            Some(artwork_url(&relative, query.library, "cover"))
        } else {
            None
        };
        let show_metadata_directory = library_root.join(&relative);
        let show_metadata = (tv_show_view || query.season.is_some())
            .then(|| matched_tv_show_metadata(&s, &media_index, &show_metadata_directory))
            .flatten();
        let database_metadata = (tv_show_view && !sidecar_mode)
            .then_some(show_metadata.as_ref())
            .flatten();
        let panel_directory = query
            .season
            .map(|season| tv_season_directory(&directory, season, &directory))
            .unwrap_or_else(|| directory.clone());
        let mut panel = media_info_panel(
            &panel_directory,
            database_metadata,
            tv_show_view,
            cover_url.as_deref(),
        )
        .await;
        if tv_season_view && !panel.contains("class=\"metadata-description\"") {
            let season = query.season.or_else(|| {
                relative
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(parse_tv_season_directory)
            });
            let show_directory = if query.season.is_some() {
                Some(directory.as_path())
            } else {
                directory.parent()
            };
            if let (Some(season), Some(show_directory)) = (season, show_directory) {
                let show_metadata_directory = library_root.join(if query.season.is_some() {
                    relative.as_path()
                } else {
                    relative.parent().unwrap_or(std::path::Path::new(""))
                });
                let show_metadata =
                    matched_tv_show_metadata(&s, &media_index, &show_metadata_directory);
                let representative_path = media_index
                    .iter()
                    .find(|media| {
                        media
                            .path
                            .strip_prefix(&library_root)
                            .is_ok_and(|path| path.starts_with(&relative))
                            && tv_season_number(&library_root, &media.path) == Some(season)
                    })
                    .map(|media| media.path.clone())
                    .unwrap_or_else(|| show_directory.to_path_buf());
                let season_directory =
                    tv_season_directory(show_directory, season, &representative_path);
                let season_metadata = if sidecar_mode {
                    read_sidecar_info(&season_directory)
                        .await
                        .and_then(|info| info.get("metadata_json").cloned())
                } else {
                    media_index
                        .iter()
                        .filter(|media| {
                            media
                                .path
                                .strip_prefix(&library_root)
                                .is_ok_and(|path| path.starts_with(&relative))
                                && tv_season_number(&library_root, &media.path) == Some(season)
                        })
                        .find_map(|media| {
                            let (library_id, _) = media_library(&s, media)?;
                            let key = format!("{library_id}-{}", media.id);
                            read_database_metadata(&s, &key)
                        })
                        .and_then(|metadata| {
                            metadata
                                .get("mplex_season_description")
                                .cloned()
                                .map(|overview| {
                                    serde_json::json!({
                                        "name": format!("Season {season}"),
                                        "overview": overview
                                    })
                                })
                        })
                }
                .unwrap_or_else(|| serde_json::json!({"name": format!("Season {season}")}));
                panel = render_season_description_panel(
                    &season_metadata,
                    season,
                    show_metadata.as_ref(),
                );
            }
        }
        panel
    } else {
        String::new()
    };
    let mut directories = Vec::new();
    let mut files = Vec::new();
    let virtual_tv_view = !files_view
        && library_kind.as_deref() == Some("tv")
        && (tv_show_view || query.season.is_some());
    if (!library_top_level || files_view) && !virtual_tv_view {
        let mut entries = match tokio::fs::read_dir(&directory).await {
            Ok(entries) => entries,
            Err(_) => return StatusCode::NOT_FOUND.into_response(),
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let Ok(file_type) = entry.file_type().await else {
                continue;
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            let child_relative = relative.join(&name);
            if file_type.is_dir() {
                directories.push((name, child_relative));
            } else if file_type.is_file()
                && (files_view || media_type(&path).is_some())
                && selected_relative
                    .as_deref()
                    .is_none_or(|selected| selected == child_relative)
            {
                files.push((name, child_relative, path));
            }
        }
    }
    directories.sort_by_key(|(name, _)| name.to_ascii_lowercase());
    files.sort_by_key(|(name, _, _)| name.to_ascii_lowercase());

    let mut items = String::new();
    items.push_str(&breadcrumb_nav(&relative, query.library, files_view));
    let view_library = query.library.unwrap_or_default();
    let view_path = url_encode(&relative.to_string_lossy());
    let media_view_href = format!("/?library={view_library}&path={view_path}");
    let files_view_href = format!("{media_view_href}&view=files");
    let media_view_style = if files_view {
        "padding:.45rem .75rem;border-radius:.45rem;color:#cbd5e1;background:#1f2937;text-decoration:none"
    } else {
        "padding:.45rem .75rem;border-radius:.45rem;color:#fff;background:#1d4ed8;text-decoration:none"
    };
    let files_view_style = if files_view {
        "padding:.45rem .75rem;border-radius:.45rem;color:#fff;background:#1d4ed8;text-decoration:none"
    } else {
        "padding:.45rem .75rem;border-radius:.45rem;color:#cbd5e1;background:#1f2937;text-decoration:none"
    };
    items.push_str(&format!(
        "<nav aria-label=\"Library view\" style=\"display:flex;grid-column:1 / -1;gap:.4rem;margin-bottom:.5rem\"><a data-spa-link href=\"{media_view_href}\" style=\"{media_view_style}\">Media</a><a data-spa-link href=\"{files_view_href}\" style=\"{files_view_style}\">Files</a></nav>"
    ));
    let sidebar_library = query.library.or_else(|| {
        s.config
            .lock()
            .ok()
            .and_then(|config| (!config.libraries.is_empty()).then_some(0))
    });
    let sidebar = sidebar_nav_for(&s, &relative, sidebar_library);
    let page_heading = library_config
        .as_ref()
        .map(|library| html(&library.name))
        .unwrap_or_else(|| "Home".to_owned());
    let mut folder_cards = String::new();
    let mut season_sections = String::new();
    let mut has_content = false;
    let section_metadata = query
        .library
        .map(|index| plex_section_metadata(&s, index + 1))
        .unwrap_or_default();
    let mut virtual_seasons = BTreeMap::<u32, Vec<&Media>>::new();
    if virtual_tv_view {
        for media in media_index.iter() {
            if !media
                .path
                .strip_prefix(&library_root)
                .is_ok_and(|path| path.starts_with(&relative))
            {
                continue;
            }
            if let Some(season) = tv_season_number(&library_root, &media.path) {
                virtual_seasons.entry(season).or_default().push(media);
            }
        }
        for episodes in virtual_seasons.values_mut() {
            episodes.sort_by_key(|media| {
                tv_episode_info(&library_root, &media.path)
                    .map(|episode| episode.episode)
                    .unwrap_or(u32::MAX)
            });
        }
    }
    if tv_show_view {
        let show_path = relative.to_string_lossy();
        for (season, episodes) in &virtual_seasons {
            let Some(representative) = episodes.first() else {
                continue;
            };
            let href = format!(
                "/?library={}&path={}&season={season}",
                query.library.unwrap_or_default(),
                url_encode(&show_path)
            );
            let representative_path = representative
                .path
                .strip_prefix(&library_root)
                .unwrap_or(std::path::Path::new(""));
            folder_cards.push_str(&media_poster_card(
                Some(&representative.id),
                &href,
                &format!("Season {season:02}"),
                Some(&format!("{} episodes", episodes.len())),
                None,
                average_media_progress(episodes, &section_metadata),
                representative_path,
                query.library.unwrap_or_default(),
                false,
                false,
                "item",
                false,
            ));
        }
        has_content = !folder_cards.is_empty();
    }
    if query.season.is_some() && selected_relative.is_none() {
        for episode in virtual_seasons
            .get(&query.season.unwrap_or_default())
            .into_iter()
            .flatten()
        {
            let Some(episode_path) = episode.path.strip_prefix(&library_root).ok() else {
                continue;
            };
            let item_metadata = section_metadata.get(&episode.id);
            let (episode_title, episode_number) =
                tv_episode_card_labels(&library_root, &episode.path, item_metadata);
            let title = episode_title.unwrap_or_else(|| episode.title.clone());
            let href = format!(
                "/?library={}&path={}&season={}&episode={}",
                query.library.unwrap_or_default(),
                url_encode(&relative.to_string_lossy()),
                query.season.unwrap_or_default(),
                url_encode(&episode_path.to_string_lossy())
            );
            items.push_str(&media_poster_card(
                Some(&episode.id),
                &href,
                &title,
                episode_number.as_deref(),
                relative.file_name().and_then(|name| name.to_str()),
                item_metadata.map(media_card_progress).unwrap_or_default(),
                episode_path,
                query.library.unwrap_or_default(),
                true,
                true,
                "item",
                false,
            ));
        }
        has_content = virtual_seasons.contains_key(&query.season.unwrap_or_default());
    } else if query.season.is_some() {
        if let Some(selected_relative) = selected_relative.as_ref() {
            if let Some(media) = media_index
                .iter()
                .find(|media| media.path == library_root.join(selected_relative))
            {
                if let (Some(name), Some(path)) = (media.path.file_name(), Some(media.path.clone()))
                {
                    files.push((
                        name.to_string_lossy().into_owned(),
                        selected_relative.clone(),
                        path,
                    ));
                }
            }
        }
    }
    if library_top_level && !files_view {
        if let (Some(library_index), Some(library)) = (query.library, library_config.as_ref()) {
            let media = media_index
                .iter()
                .filter(|item| media_in_library(item, library))
                .collect::<Vec<_>>();
            if library.kind == "tv" {
                for show in
                    plex_tv_show_groups(library_index + 1, &library.folders, media.iter().copied())
                {
                    let Some(representative) = show.episodes.iter().max_by_key(|episode| {
                        section_metadata
                            .get(&episode.id)
                            .and_then(|metadata| metadata.get("mplex_last_viewed_at"))
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or_default()
                    }) else {
                        continue;
                    };
                    let show_path = representative
                        .path
                        .strip_prefix(&library_root)
                        .ok()
                        .and_then(|path| path.components().next())
                        .map(|component| PathBuf::from(component.as_os_str()));
                    let Some(show_path) = show_path else {
                        continue;
                    };

                    let progress = average_media_progress(&show.episodes, &section_metadata);
                    let season_count = show
                        .episodes
                        .iter()
                        .filter_map(|episode| tv_season_number(&library_root, &episode.path))
                        .collect::<BTreeSet<_>>()
                        .len();
                    let season_label = format!(
                        "{season_count} season{}",
                        if season_count == 1 { "" } else { "s" }
                    );

                    let Some(representative_path) =
                        representative.path.strip_prefix(&library_root).ok()
                    else {
                        continue;
                    };
                    let href = format!(
                        "/?library={library_index}&path={}",
                        url_encode(&show_path.to_string_lossy())
                    );
                    folder_cards.push_str(&media_poster_card(
                        Some(&representative.id),
                        &href,
                        &show.title,
                        Some(&season_label),
                        None,
                        progress,
                        representative_path,
                        library_index,
                        true,
                        true,
                        "show",
                        false,
                    ));
                }
            } else {
                for media_item in media {
                    let Some(media_path) = media_item.path.strip_prefix(&library_root).ok() else {
                        continue;
                    };
                    let folder = media_path.parent().unwrap_or(std::path::Path::new(""));
                    let item_metadata = section_metadata.get(&media_item.id);
                    folder_cards.push_str(&media_card(
                        media_item,
                        library,
                        item_metadata,
                        &library_root,
                        folder,
                        media_path,
                        library_index,
                        false,
                    ));
                }
            }
            has_content = !folder_cards.is_empty();
        }
    }
    for (name, child_relative) in directories {
        has_content = true;
        if files_view {
            let href = format!(
                "/?library={}&path={}&view=files",
                query.library.unwrap_or_default(),
                url_encode(&child_relative.to_string_lossy())
            );
            folder_cards.push_str(&format!(
                "<a class=\"file-item\" data-spa-link href=\"{}\" style=\"display:flex;align-items:center;gap:.6rem;padding:.7rem .85rem;border:1px solid #374151;border-radius:.5rem;background:#1f2937;color:inherit;text-decoration:none\">📁 <span>{}</span></a>",
                html(&href),
                html(&name)
            ));
            continue;
        }
        let directory = directory.join(&name);
        if library_kind.as_deref() == Some("tv") && parse_tv_season_directory(&name).is_some() {
            let season_number = parse_tv_season_directory(&name).unwrap_or_default();
            let mut season_entries = match tokio::fs::read_dir(&directory).await {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            let mut episode_count = 0;
            while let Ok(Some(entry)) = season_entries.next_entry().await {
                let path = entry.path();
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with('.')
                    || !entry.file_type().await.is_ok_and(|kind| kind.is_file())
                    || media_type(&path).is_none()
                {
                    continue;
                }
                episode_count += 1;
            }
            if episode_count > 0 {
                let season_path = directory.clone();
                let season_media = media_index
                    .iter()
                    .filter(|media| media.path.starts_with(&season_path))
                    .collect::<Vec<_>>();
                let progress = average_media_progress(&season_media, &section_metadata);
                let season_card = season_media
                    .first()
                    .map(|representative| {
                        media_poster_card(
                            Some(&representative.id),
                            &format!(
                                "/?library={}&path={}",
                                query.library.unwrap_or_default(),
                                url_encode(&child_relative.to_string_lossy())
                            ),
                            &format!("Season {season_number:02}"),
                            Some(&format!("{episode_count} episodes")),
                            None,
                            progress,
                            representative
                                .path
                                .strip_prefix(&library_root)
                                .unwrap_or(std::path::Path::new("")),
                            query.library.unwrap_or_default(),
                            false,
                            false,
                            "item",
                            false,
                        )
                    })
                    .unwrap_or_default();
                season_sections.push_str(&format!(
                    r#"<section class="tv-season"><div class="tv-season-card">{season_card}</div></section>"#
                ));
                continue;
            }
        }
        let media_count = media_file_count(&directory).await;
        if media_count > 0 {
            let preview = first_media_file(&directory)
                .await
                .and_then(|path| path.strip_prefix(&library_root).ok().map(PathBuf::from));
            let cover = sidecar_mode
                && tokio::fs::metadata(directory.join(".mplex/cover.jpg"))
                    .await
                    .is_ok();
            let banner = sidecar_mode
                && tokio::fs::metadata(directory.join(".mplex/banner.jpg"))
                    .await
                    .is_ok();
            folder_cards.push_str(&folder_card(
                &name,
                &child_relative,
                query.library,
                preview.as_deref(),
                cover,
                banner,
                None,
            ));
        }
    }
    for (name, child_relative, path) in files {
        has_content = true;
        if files_view && selected_relative.is_none() {
            let row = if media_type(&path).is_some() {
                let folder = child_relative.parent().unwrap_or(&relative);
                let href = format!(
                    "/?library={}&path={}&play={}&view=files",
                    query.library.unwrap_or_default(),
                    url_encode(&folder.to_string_lossy()),
                    url_encode(&child_relative.to_string_lossy())
                );
                format!(
                    "<a class=\"file-item\" data-spa-link href=\"{}\" style=\"display:flex;align-items:center;gap:.6rem;padding:.7rem .85rem;border:1px solid #374151;border-radius:.5rem;background:#1f2937;color:inherit;text-decoration:none\">📄 <span>{}</span></a>",
                    html(&href),
                    html(&name)
                )
            } else {
                file_item(&name)
            };
            items.push_str(&row);
            continue;
        }
        if let Some((_, mime)) = media_type(&path) {
            let matched_media = web_media_for_relative(&s, &library_root, &child_relative).await;
            if tv_season_view && selected_relative.is_none() {
                let item_metadata = matched_media
                    .as_ref()
                    .and_then(|media| section_metadata.get(&media.id));
                let media_path = library_root.join(&child_relative);
                let (episode_title, episode_number) =
                    tv_episode_card_labels(&library_root, &media_path, item_metadata);
                let title = episode_title.unwrap_or_else(|| tv_episode_display_name(&name));
                let show_title = child_relative
                    .components()
                    .next()
                    .map(|component| component.as_os_str().to_string_lossy().into_owned());
                let folder = child_relative.parent().unwrap_or(std::path::Path::new(""));
                let library = query.library.unwrap_or_default();
                let href = format!(
                    "/?library={library}&path={}&episode={}",
                    url_encode(&folder.to_string_lossy()),
                    url_encode(&child_relative.to_string_lossy())
                );
                let progress = item_metadata.map(media_card_progress).unwrap_or_default();
                items.push_str(&media_poster_card(
                    matched_media.as_ref().map(|media| media.id.as_str()),
                    &href,
                    &title,
                    episode_number.as_deref(),
                    show_title.as_deref(),
                    progress,
                    &child_relative,
                    library,
                    true,
                    true,
                    "item",
                    false,
                ));
                continue;
            }
            let artwork = matched_media
                .as_ref()
                .map(|media| media_thumbnail_url(&media.id));
            let artwork = if artwork.is_some() {
                artwork
            } else if sidecar_mode
                && tokio::fs::metadata(
                    path.parent()
                        .unwrap_or(std::path::Path::new("."))
                        .join(".mplex/cover.jpg"),
                )
                .await
                .is_ok()
            {
                path.parent()
                    .and_then(|parent| parent.strip_prefix(&library_root).ok())
                    .map(|parent| artwork_url(parent, query.library, "cover"))
            } else {
                None
            };
            let subtitles = if let Some(media) = matched_media.as_ref() {
                subtitle_tracks(media, &child_relative, query.library).await
            } else {
                Vec::new()
            };
            let selected_subtitle = matched_media
                .as_ref()
                .and_then(|media| {
                    s.plex_stream_selections
                        .lock()
                        .ok()
                        .and_then(|selections| selections.get(&plex_part_id(media)).copied())
                })
                .and_then(|selection| selection.subtitle_stream_id);
            items.push_str(&web_client_item(
                &name,
                &child_relative.to_string_lossy(),
                mime,
                query.library,
                &subtitles,
                artwork,
                selected_subtitle,
            ));
        } else {
            items.push_str(&file_item(&name));
        }
    }
    items.push_str(&folder_cards);
    items.push_str(&season_sections);
    if !has_content {
        items.push_str(if files_view {
            r#"<p class="note">No files or folders here.</p>"#
        } else {
            r#"<p class="note">No supported media or subfolders here.</p>"#
        });
    }
    (
        [(header::CACHE_CONTROL, "no-store")],
        Html(format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>mplex</title>
  <style>
    :root {{ color-scheme: dark; font-family: system-ui, sans-serif; }}
    body {{ margin: 0; background: #111827; color: #f9fafb; }}
    .app-shell {{ display: grid; grid-template-columns: 240px minmax(0, 1fr); min-height: 100vh; transition: grid-template-columns .2s ease; }}
    .app-shell.sidebar-collapsed {{ grid-template-columns: 68px minmax(0, 1fr); }}
    .sidebar {{ position: sticky; top: 0; height: 100vh; box-sizing: border-box; padding: 1.25rem .9rem; border-right: 1px solid #263244; background: #0d1421; }}
    .sidebar-brand {{ display: block; padding: .5rem .7rem 1.5rem; color: #f9fafb; font-size: 1.25rem; font-weight: 700; text-decoration: none; letter-spacing: .02em; }}
    .sidebar-top {{ display: flex; align-items: center; justify-content: space-between; gap: .4rem; padding-bottom: 1rem; }}
    .sidebar-top .sidebar-brand {{ padding-bottom: 0; }}
    .sidebar-toggle {{ display: grid; place-items: center; width: 2rem; height: 2rem; border: 0; border-radius: .4rem; color: #cbd5e1; background: transparent; cursor: pointer; font-size: 1.15rem; }}
    .sidebar-toggle:hover {{ background: #17243a; color: #fff; }}
    .sidebar-label {{ padding: 0 .7rem .45rem; color: #6b7280; font-size: .7rem; font-weight: 700; letter-spacing: .1em; text-transform: uppercase; }}
    .sidebar-nav {{ display: grid; gap: .25rem; }}
    .sidebar-link {{ display: flex; align-items: center; gap: .7rem; padding: .65rem .7rem; border-radius: .5rem; color: #cbd5e1; text-decoration: none; }}
    .sidebar-link:hover {{ background: #17243a; color: #fff; }}
    .sidebar-link.active {{ background: #1d4ed8; color: #fff; }}
    .sidebar-icon {{ width: 1.3rem; text-align: center; font-size: 1.1rem; }}
    .sidebar-library {{ overflow-wrap: anywhere; }}
    .sidebar-collapsed .sidebar-top {{ justify-content: center; }}
    .sidebar-collapsed .sidebar-brand, .sidebar-collapsed .sidebar-label, .sidebar-collapsed .sidebar-link {{ font-size: 0; }}
    .sidebar-collapsed .sidebar-toggle {{ transform: rotate(180deg); }}
    .sidebar-collapsed .sidebar-link {{ justify-content: center; padding-left: 0; padding-right: 0; }}
    .sidebar-collapsed .sidebar-icon {{ font-size: 1.1rem; }}
    main {{ min-width: 0; max-width: 1100px; width: 100%; box-sizing: border-box; margin: 0 auto; padding: 2rem 1rem; }}
    h1 {{ margin-top: 0; }}
    .brand {{ color: inherit; text-decoration: none; }}
    .library {{ display: grid; grid-template-columns: repeat(auto-fill, minmax({}px, 1fr)); align-items: start; gap: 1rem; }}
    .media-card {{ position: relative; min-width: 0; padding: .7rem; border: 1px solid transparent; border-radius: .6rem; background: #1f2937; transition: border-color .15s, transform .15s; }}
    .media-card:hover {{ border-color: #374151; transform: translateY(-2px); }}
    .media-card-artwork {{ position: relative; }}
    .media-card-poster, .media-card-poster img {{ display: block; width: 100%; aspect-ratio: 2 / 3; }}
    .media-card-poster img {{ object-fit: cover; border-radius: .35rem; background: #111827; }}
    .media-card-placeholder {{ display: grid; width: 100%; height: 100%; place-items: center; border-radius: .35rem; color: #bfdbfe; background: linear-gradient(135deg, #1e3a8a, #312e81 55%, #111827); font-size: 2rem; }}
    .media-card-placeholder {{ font-size: .9rem; padding: .7rem; text-align: center; overflow-wrap: anywhere; }}
    .media-card-info {{ display: grid; gap: .12rem; padding: .55rem 0 0; color: inherit; text-align: center; text-decoration: none; }}
    .media-card-title {{ display: block; height: 2.4em; overflow: hidden; margin: .25rem 0 .15rem; font-size: .8rem; font-weight: 500; line-height: 1.2; }}
    .media-card-tv .media-card-title {{ height: auto; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .media-card-subtitle, .media-card-details {{ overflow: hidden; color: #9ca3af; font-size: .75rem; line-height: 1.35; text-overflow: ellipsis; white-space: nowrap; }}
    .media-card-progress-row {{ display: flex; align-items: center; gap: .45rem; margin: .3rem 0 0; }}
    .media-card-progress {{ flex: 1 1 auto; min-width: 0; height: .3rem; border-radius: 999px; background: #374151; overflow: hidden; }}
    .media-card-progress span {{ display: block; height: 100%; border-radius: inherit; background: linear-gradient(90deg, #2563eb, #22c55e); }}
    .media-card-percent {{ flex: 0 0 auto; color: #93c5fd; font-size: .68rem; line-height: 1; }}
    .media-card-menu {{ position: absolute; right: .45rem; bottom: .45rem; z-index: 4; }}
    .media-card-menu summary {{ display: grid; width: 1.65rem; height: 1.65rem; place-items: center; border: 1px solid rgba(255,255,255,.25); border-radius: 50%; color: #f9fafb; background: rgba(17,24,39,.58); backdrop-filter: blur(3px); font-size: 1rem; font-weight: 800; line-height: 1; list-style: none; cursor: pointer; }}
    .media-card-menu summary::-webkit-details-marker {{ display: none; }}
    .media-card-menu-items {{ position: absolute; right: 0; bottom: calc(100% + .35rem); display: grid; min-width: 12.5rem; gap: .2rem; padding: .35rem; border: 1px solid #374151; border-radius: .5rem; background: #111827; box-shadow: 0 8px 24px #0008; }}
    .media-card-menu-items button {{ padding: .55rem .65rem; border: 0; border-radius: .3rem; color: #e5e7eb; background: transparent; font: inherit; font-size: .78rem; text-align: left; cursor: pointer; }}
    .media-card-menu-items button:hover {{ background: #263244; }}
    .tv-season {{ min-width: 0; }}
    .tv-season h2 {{ margin: .75rem 0 .6rem; }}
    .tv-season-card {{ width: 100%; max-width: 100%; }}
    .breadcrumb {{ grid-column: 1 / -1; }}
    article, .folder-card {{ width: 100%; box-sizing: border-box; }}
    article {{ grid-column: span 4; min-width: 0; }}
    article {{ overflow: hidden; border: 1px solid #374151; border-radius: .75rem; background: #1f2937; }}
    video {{ display: block; width: 100%; aspect-ratio: 16 / 9; margin: 0 auto; background: #000; }}
    .details {{ padding: .9rem 1rem 1rem; }}
    h2 {{ font-size: 1rem; margin: 0 0 .4rem; overflow-wrap: anywhere; }}
    .play-actions {{ display: flex; flex-wrap: wrap; align-items: center; gap: .5rem; margin: .7rem 0; }}
    .play-actions button, .subtitle-picker select {{ padding: .45rem .65rem; }}
    .match-form {{ display: flex; flex-wrap: wrap; gap: .4rem; margin-top: .6rem; }} .match-form input {{ min-width: 0; flex: 1 1 12rem; padding: .45rem .6rem; border: 1px solid #4b5563; border-radius: .35rem; color: #fff; background: #111827; }} .match-form button {{ padding: .45rem .65rem; }}
    .info-button {{ background: #374151; }}
    dialog {{ position: relative; max-width: min(760px, calc(100vw - 2rem)); width: 100%; color: #f9fafb; border: 1px solid #4b5563; border-radius: .75rem; background: #111827; }} dialog::backdrop {{ background: rgba(0,0,0,.7); }} dialog form {{ position: absolute; top: .75rem; right: .75rem; margin: 0; }} dialog form button {{ padding: .35rem .6rem; }} .info-grid {{ display: grid; gap: .45rem; max-height: 65vh; overflow: auto; }} .info-row {{ display: grid; grid-template-columns: minmax(140px, .4fr) 1fr; gap: .8rem; padding: .45rem 0; border-bottom: 1px solid #263244; }} .info-key {{ color: #9ca3af; }} .info-value {{ overflow-wrap: anywhere; }}
    .theatre-button {{ background: #4b5563; }}
    #tv-fix-match-dialog {{ max-height: calc(100vh - 2rem); padding: 1.25rem; overflow: auto; }}
    #tv-fix-match-dialog h2 {{ padding-right: 2.5rem; }}
    .fix-match-close {{ position: absolute; top: .65rem; right: .65rem; min-width: 2.4rem; padding: .3rem .55rem; }}
    .fix-match-search-row {{ display: flex; flex-wrap: wrap; gap: .5rem; margin: .9rem 0; }}
    .fix-match-search-row input {{ flex: 1 1 16rem; min-width: 0; padding: .6rem .7rem; border: 1px solid #4b5563; border-radius: .4rem; color: #fff; background: #0b1220; font: inherit; }}
    .fix-match-results {{ display: grid; gap: .65rem; }}
    .fix-match-result {{ display: flex; align-items: flex-start; gap: .9rem; padding: .75rem; border: 1px solid #374151; border-radius: .5rem; background: #1f2937; }}
    .fix-match-poster {{ flex: 0 0 76px; width: 76px; height: 114px; object-fit: cover; border-radius: .3rem; background: #111827; }}
    .fix-match-result-content {{ min-width: 0; }}
    .fix-match-result h3 {{ margin: 0; }}
    @media (max-width: 420px) {{ .fix-match-poster {{ flex-basis: 60px; width: 60px; height: 90px; }} .fix-match-result {{ gap: .6rem; }} }}
    .fix-match-result p {{ margin: .35rem 0 .65rem; color: #cbd5e1; font-size: .85rem; line-height: 1.45; }}
    .fix-match-status {{ min-height: 1.5rem; color: #cbd5e1; }}

    body.theatre-mode main {{ max-width: none; padding: 0; }}
    body.theatre-mode .library {{ display: block; }}
    body.theatre-mode article:not(.theatre-card), body.theatre-mode .breadcrumb, body.theatre-mode .folder-card {{ display: none; }}
    body.theatre-mode .theatre-card {{ width: 100%; min-height: 100vh; border: 0; border-radius: 0; }}
    body.theatre-mode .theatre-card video {{ width: 100%; max-height: calc(100vh - 8rem); aspect-ratio: 16 / 9; object-fit: contain; }}
    .subtitle-picker {{ display: inline-flex; align-items: center; gap: .35rem; color: #cbd5e1; font-size: .9rem; }}
    .subtitle-picker select {{ border: 1px solid #4b5563; border-radius: .35rem; color: #fff; background: #111827; }}
    .subtitle-note {{ color: #9ca3af; font-size: .8rem; }}
    .breadcrumb {{ display: flex; flex-wrap: wrap; align-items: center; gap: .35rem; box-sizing: border-box; color: #9ca3af; }}
    .breadcrumb a {{ padding: .35rem .55rem; border-radius: .35rem; text-decoration: none; }}
    .breadcrumb a:hover {{ background: #24344d; }}
    .breadcrumb-separator {{ color: #6b7280; }}
    .metadata-panel {{ grid-column: 1 / -1; padding: 1rem 1.2rem; border: 1px solid #374151; border-radius: .75rem; background: rgba(17,24,39,.62); backdrop-filter: blur(8px); }}
    .metadata-panel h2 {{ margin: 0 0 .45rem; }} .metadata-description {{ max-width: 75ch; color: #d1d5db; line-height: 1.5; }} .metadata-details {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: .6rem 1.2rem; margin-top: .8rem; }} .metadata-detail strong {{ display: block; color: #9ca3af; font-size: .75rem; text-transform: uppercase; }}
    .show-overview-summary {{ display: flex; align-items: flex-start; gap: 1rem; }}
    .show-overview-cover {{ width: 120px; max-height: 180px; object-fit: cover; border-radius: .5rem; box-shadow: 0 3px 12px #0008; }}
    .show-overview-summary > div {{ min-width: 0; }}
    @media (max-width: 600px) {{ .show-overview-summary {{ gap: .75rem; }} .show-overview-cover {{ width: 88px; }} }}
    .folder-card {{ display: block; width: 100%; overflow: hidden; color: inherit; text-decoration: none; border: 1px solid #374151; border-radius: .75rem; background: #1f2937; transition: border-color .15s, transform .15s; }}
    .folder-card:hover {{ border-color: #60a5fa; transform: translateY(-2px); }}
    .folder-preview {{ position: relative; display: grid; place-items: center; overflow: hidden; aspect-ratio: 2 / 3; color: #bfdbfe; font-size: 3rem; background: linear-gradient(135deg, #1e3a8a, #312e81 55%, #111827); }}
    .folder-preview-video {{ width: 100%; height: 100%; object-fit: cover; opacity: .85; }}
    .folder-preview-icon {{ position: absolute; inset: 0; display: grid; place-items: center; text-shadow: 0 2px 8px #000; pointer-events: none; }}
    .folder-cover {{ position: absolute; inset: 0; z-index: 1; width: 100%; height: 100%; object-fit: cover; }}
    .folder-info {{ padding: .9rem 1rem 1rem; }}
    .folder-info h2 {{ margin: 0 0 .25rem; }}
    a {{ color: #93c5fd; }}
    .note {{ color: #9ca3af; }}
    @media (max-width: 700px) {{
      .app-shell {{ display: block; }}
      .sidebar {{ position: static; height: auto; padding: .7rem; border-right: 0; border-bottom: 1px solid #263244; }}
      .sidebar-top {{ padding-bottom: .35rem; }}
      .sidebar-brand {{ display: inline-block; padding: .35rem .55rem; }}
      .sidebar-toggle {{ display: none; }}
      .sidebar-label {{ display: none; }}
      .sidebar-nav {{ display: flex; overflow-x: auto; }}
      .sidebar-link {{ white-space: nowrap; }}
      main {{ padding-top: 1.25rem; }}
      article {{ grid-column: 1 / -1; }}
    }}
  </style>
</head>
<body>
  <div class="app-shell">
    {}
    <main{}>
    <h1>{}</h1>
    {}
    <section class="library">{}</section>
    <dialog id="info-modal"><form method="dialog"><button type="submit" aria-label="Close dialog" title="Close">×</button></form><h2>Technical media information</h2><div id="info-content" class="info-grid">Loading…</div></dialog>
    <dialog id="tv-fix-match-dialog" aria-labelledby="tv-fix-match-heading"><button class="fix-match-close" type="button" aria-label="Close Fix Match dialog" title="Close">×</button><h2 id="tv-fix-match-heading">Fix TV Show Match</h2><p id="tv-fix-match-show" class="note"></p><div class="fix-match-search-row"><input id="tv-fix-match-query" type="search" aria-label="Search TV shows" autocomplete="off"><button id="tv-fix-match-search" type="button">Search</button></div><p id="tv-fix-match-status" class="fix-match-status" role="status" aria-live="polite"></p><div id="tv-fix-match-results" class="fix-match-results"></div></dialog>
    </main>
  </div>
  <script>
    const shell = window.document.querySelector(".app-shell");
    const sidebarToggle = window.document.querySelector(".sidebar-toggle");
    if (window.localStorage.getItem("mplex-sidebar-collapsed") === "true") shell.classList.add("sidebar-collapsed");
    sidebarToggle?.addEventListener("click", () => {{
      const collapsed = shell.classList.toggle("sidebar-collapsed");
      window.localStorage.setItem("mplex-sidebar-collapsed", collapsed ? "true" : "false");
      sidebarToggle.setAttribute("aria-expanded", String(!collapsed));
    }});
    const infoModal = document.querySelector('#info-modal');
    const infoContent = document.querySelector('#info-content');
    if (!window.__mplexInfoEscapeBound) {{
      window.__mplexInfoEscapeBound = true;
      document.addEventListener("keydown", event => {{
        if (event.key === "Escape") {{
          document.querySelector('#info-modal')?.close();
          document.querySelector('#tv-fix-match-dialog')?.close();
        }}
      }});
    }}
    function renderTechnicalInfo(value) {{
      infoContent.replaceChildren();
      const add = (parent, key, item) => {{
        if (item && typeof item === "object") {{
          if (Array.isArray(item)) item.forEach((entry, index) => add(parent, `${{key}} ${{index + 1}}`, entry));
          else Object.entries(item).forEach(([childKey, childValue]) => add(parent, key ? `${{key}} / ${{childKey}}` : childKey, childValue));
          return;
        }}
        const row = document.createElement("div");
        row.className = "info-row";
        const label = document.createElement("span");
        label.className = "info-key";
        label.textContent = key.replaceAll("_", " ");
        const content = document.createElement("span");
        content.className = "info-value";
        content.textContent = String(item ?? "");
        row.append(label, content);
        parent.append(row);
      }};
      add(infoContent, "", value);
    }}
    document.querySelectorAll(".info-button").forEach(button => {{
      button.addEventListener("click", async () => {{
        infoContent.textContent = "Loading…";
        infoModal.showModal();
        try {{
          const response = await fetch(button.dataset.infoUrl, {{ cache: "no-store" }});
          if (response.ok) renderTechnicalInfo(await response.json());
          else infoContent.textContent = "No cached technical analysis found. Run Scan metadata or Force rescan in Settings.";
        }} catch (_) {{
          infoContent.textContent = "Media analysis could not be loaded.";
        }}
      }});
    }});
    document.querySelectorAll("video").forEach(video => {{
      video.volume = 1;
      video.muted = !video.controls;
      const capturePoster = () => {{
        if (video.poster || video.videoWidth === 0 || video.videoHeight === 0) return;
        const canvas = document.createElement("canvas");
        canvas.width = video.videoWidth;
        canvas.height = video.videoHeight;
        const context = canvas.getContext("2d");
        if (!context) return;
        context.drawImage(video, 0, 0, canvas.width, canvas.height);
        try {{ video.poster = canvas.toDataURL("image/jpeg", 0.78); }} catch (_) {{}}
      }};
      if (video.controls) {{
        video.addEventListener("loadeddata", capturePoster, {{ once: true }});
      }} else {{
        video.addEventListener("loadedmetadata", () => {{
          if (Number.isFinite(video.duration) && video.duration > 0) {{
            video.currentTime = video.duration * 0.1;
            video.addEventListener("seeked", capturePoster, {{ once: true }});
          }}
        }}, {{ once: true }});
      }}
      const path = video.dataset.progressPath;
      const subtitleSelect = video.closest("article")?.querySelector(".subtitle-select");
      const applySubtitle = selected => {{
        [...video.textTracks].forEach((track, index) => {{
          track.mode = index === selected ? "showing" : "disabled";
        }});
        if (subtitleSelect) subtitleSelect.value = String(selected);
      }};
      const subtitleOptionForStream = streamId => [...(subtitleSelect?.options || [])]
        .find(option => Number(option.dataset.streamId || 0) === streamId);
      const applyServerSubtitle = streamId => {{
        const option = subtitleOptionForStream(streamId);
        if (!option) {{
          applySubtitle(-1);
          subtitleSelect.dataset.selectedStreamId = String(streamId);
          return;
        }}
        applySubtitle(Number(option.value));
        subtitleSelect.dataset.selectedStreamId = String(streamId);
      }};
      if (subtitleSelect) {{
        applyServerSubtitle(Number(subtitleSelect.dataset.selectedStreamId || 0));
        const library = subtitleSelect.dataset.library;
        const subtitleQuery = `${{library ? `library=${{encodeURIComponent(library)}}&` : ""}}path=${{encodeURIComponent(path)}}`;
        subtitleSelect.addEventListener("change", event => {{
          const select = event.currentTarget;
          const option = select.selectedOptions[0];
          const streamId = Number(option?.dataset.streamId || 0);
          applySubtitle(Number(select.value));
          select.dataset.selectedStreamId = String(streamId);
          fetch("/web/subtitle-selection", {{
            method: "POST", headers: {{ "content-type": "application/json" }},
            body: JSON.stringify({{ path, library: library ? Number(library) : null, stream_id: streamId }})
          }}).catch(() => {{}});
        }});
        const syncSubtitleSelection = () => fetch(`/web/subtitle-selection?${{subtitleQuery}}`, {{ cache: "no-store" }})
          .then(response => response.ok ? response.json() : null)
          .then(selection => {{
            if (selection && Number(selection.streamId) !== Number(subtitleSelect.dataset.selectedStreamId || 0)) {{
              applyServerSubtitle(Number(selection.streamId));
            }}
          }}).catch(() => {{}});
        syncSubtitleSelection();
        window.setInterval(syncSubtitleSelection, 3000);
      }}
      if (!path) return;
      const article = video.closest("article");
      const resumeButton = article?.querySelector(".resume-button");
      const startButton = article?.querySelector(".start-button");
      const theatreButton = article?.querySelector(".theatre-button");
      theatreButton?.addEventListener("click", () => {{
        const active = document.body.classList.toggle("theatre-mode");
        article.classList.toggle("theatre-card", active);
        theatreButton.textContent = active ? "▣ Exit theatre" : "▣ Theatre";
        theatreButton.setAttribute("aria-label", active ? "Exit theatre mode" : "Theatre mode");
      }});
      const playAt = position => {{
        const start = () => {{
          video.currentTime = position;
          video.play().catch(() => {{}});
        }};
        if (video.readyState >= 1) start();
        else video.addEventListener("loadedmetadata", start, {{ once: true }});
      }};
      startButton?.addEventListener("click", () => playAt(0));
      resumeButton?.addEventListener("click", () => playAt(Number(resumeButton.dataset.position || 0)));
      fetch(`/web/progress?path=${{encodeURIComponent(path)}}`, {{ cache: "no-store" }})
        .then(response => response.ok ? response.json() : null)
        .then(progress => {{
          if (!progress || progress.watched || !Number.isFinite(progress.positionMs) || progress.positionMs <= 2) return;
          const position = progress.positionMs / 1000;
          if (resumeButton) {{
            resumeButton.hidden = false;
            resumeButton.dataset.position = String(position);
          }}
          const resume = () => {{ if (position < video.duration - 5) video.currentTime = position; }};
          if (video.readyState >= 1) resume();
          else video.addEventListener("loadedmetadata", resume, {{ once: true }});
        }}).catch(() => {{}});
      let saveTimer = null;
      let lastProgressSave = 0;
      const sendProgress = watched => {{
        lastProgressSave = Date.now();
        fetch("/web/progress", {{
          method: "POST", headers: {{ "content-type": "application/json" }},
          body: JSON.stringify({{ path, position_ms: Math.round(video.currentTime * 1000), duration_ms: Number.isFinite(video.duration) ? Math.round(video.duration * 1000) : null, watched }})
        }}).catch(() => {{}});
        if (resumeButton && video.currentTime > 2) {{
          resumeButton.hidden = false;
          resumeButton.dataset.position = String(video.currentTime);
        }}
      }};
      const save = watched => {{
        if (watched || video.paused) {{
          clearTimeout(saveTimer);
          saveTimer = null;
          sendProgress(watched);
          return;
        }}
        if (saveTimer !== null) return;
        const delay = Math.max(0, 5000 - (Date.now() - lastProgressSave));
        saveTimer = setTimeout(() => {{
          saveTimer = null;
          if (!video.paused) sendProgress(false);
        }}, delay);
      }};
      video.addEventListener("timeupdate", () => {{ if (!video.paused) save(false); }});
      video.addEventListener("pause", () => save(false));
      video.addEventListener("ended", () => save(true));
    }});
    if (!window.__mplexMediaHotkeysBound) {{
      window.__mplexMediaHotkeysBound = true;
      window.__mplexSelectedVideo = null;
      document.addEventListener("pointerover", event => {{
        if (event.target instanceof HTMLVideoElement) window.__mplexSelectedVideo = event.target;
      }});
      document.addEventListener("focusin", event => {{
        if (event.target instanceof HTMLVideoElement) window.__mplexSelectedVideo = event.target;
      }});
      document.addEventListener("keydown", event => {{
      const fullscreen = event.key.toLowerCase() === "f";
      const theatre = event.key.toLowerCase() === "t";
      if (event.key !== "ArrowLeft" && event.key !== "ArrowRight" && event.code !== "Space" && !fullscreen && !theatre) return;
      const video = window.__mplexSelectedVideo || window.document.querySelector("video");
      if (!video) return;
      event.preventDefault();
      if (theatre) {{
        video.closest("article")?.querySelector(".theatre-button")?.click();
        return;
      }}
      if (fullscreen) {{
        if (window.document.fullscreenElement) window.document.exitFullscreen();
        else video.requestFullscreen().catch(() => {{}});
        return;
      }}
      if (event.code === "Space") {{
        if (video.paused) video.play().catch(() => {{}});
        else video.pause();
        return;
      }}
      if (!Number.isFinite(video.duration)) return;
      const offset = event.key === "ArrowLeft" ? -5 : 5;
      video.currentTime = Math.max(0, Math.min(video.duration, video.currentTime + offset));
      }});
    }}
    let refreshing = false;
    async function refreshLibrary() {{
      if (refreshing) return;
      refreshing = true;
      try {{
        const response = await fetch(window.location.href, {{ cache: "no-store" }});
        if (!response.ok) return;
        const document = new DOMParser().parseFromString(await response.text(), "text/html");
        const current = window.document.querySelector(".library");
        const next = document.querySelector(".library");
        const hasPlaybackState = [...window.document.querySelectorAll("video")]
          .some(video => !video.paused && !video.ended || video.currentTime > 0);
        if (!hasPlaybackState && current && next && current.innerHTML !== next.innerHTML) {{
          current.replaceWith(next);
          window.__mplexSelectedVideo = null;
        }}
      }} finally {{
        refreshing = false;
      }}
    }}
    if (window.__mplexLibraryRefreshTimer) clearInterval(window.__mplexLibraryRefreshTimer);
    window.__mplexLibraryRefreshTimer = setInterval(refreshLibrary, 5000);
  </script>
  {}{}
</body>
</html>"#,
        POSTER_CARD_MIN_WIDTH,
        sidebar,
        pane_banner,
        page_heading,
        info_panel,
        items,
        media_card_action_script(),
        spa_navigation_script()
        )),
    )
        .into_response()
}

struct SubtitleTrack {
    label: String,
    url: String,
    stream_id: u64,
}

async fn external_subtitle_files(video: &std::path::Path) -> Vec<PathBuf> {
    let Some(stem) = video.file_stem().and_then(|value| value.to_str()) else {
        return Vec::new();
    };
    let Ok(mut entries) =
        tokio::fs::read_dir(video.parent().unwrap_or(std::path::Path::new("."))).await
    else {
        return Vec::new();
    };
    let mut tracks = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        let extension = path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        let Some(candidate_stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        let candidate_stem_lower = candidate_stem.to_ascii_lowercase();
        let stem_lower = stem.to_ascii_lowercase();
        if matches!(extension.as_str(), "srt" | "ass" | "vtt")
            && entry.file_type().await.is_ok_and(|kind| kind.is_file())
            && (candidate_stem_lower == stem_lower
                || candidate_stem_lower.starts_with(&format!("{stem_lower}.")))
        {
            tracks.push(path);
        }
    }
    tracks.sort_by_key(|path| path.file_name().unwrap_or_default().to_ascii_lowercase());
    tracks
}

fn external_subtitle_label(video: &std::path::Path, subtitle: &std::path::Path) -> String {
    let stem = video
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    let candidate = subtitle
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    let (_, language) = plex_language(&external_subtitle_language_token(stem, candidate));
    let format = subtitle
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_uppercase();
    if language == "Unknown" {
        format!("{format} (External)")
    } else {
        format!("{language} ({format} External)")
    }
}

fn external_subtitle_language(
    video: &std::path::Path,
    subtitle: &std::path::Path,
) -> (&'static str, &'static str) {
    let stem = video
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    let candidate = subtitle
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    plex_language(&external_subtitle_language_token(stem, candidate))
}

fn external_subtitle_language_token<'a>(stem: &str, candidate: &'a str) -> String {
    let suffix = candidate
        .get(stem.len()..)
        .filter(|_| {
            candidate
                .get(..stem.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(stem))
        })
        .unwrap_or_default()
        .trim_start_matches(['.', '_']);
    suffix
        .split(['.', '_', ' '])
        .find(|token| {
            !token.is_empty()
                && !matches!(
                    token.to_ascii_lowercase().as_str(),
                    "forced" | "default" | "sdh" | "hi"
                )
        })
        .unwrap_or_default()
        .to_owned()
}

async fn subtitle_tracks(
    media: &Media,
    relative: &std::path::Path,
    library: Option<usize>,
) -> Vec<SubtitleTrack> {
    let mut tracks = Vec::new();
    for path in external_subtitle_files(&media.path).await {
        tracks.push(SubtitleTrack {
            label: format!("External: {}", external_subtitle_label(&media.path, &path)),
            url: format!(
                "/web/subtitle?{}path={}",
                library
                    .map(|index| format!("library={index}&"))
                    .unwrap_or_default(),
                url_encode(
                    &relative
                        .parent()
                        .unwrap_or(std::path::Path::new("."))
                        .join(path.file_name().unwrap_or_default())
                        .to_string_lossy()
                )
            ),
            stream_id: plex_external_subtitle_id(media, &path),
        });
    }
    tracks.sort_by_key(|track| track.label.to_ascii_lowercase());
    tracks
}

async fn web_subtitle(State(s): State<AppState>, Query(query): Query<WebPath>) -> Response {
    let Some(relative) = query.path.as_deref().and_then(clean_relative_path) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if contains_hidden_component(&relative) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let root = selected_library_root(&s, query.library);
    let path = match web_path(&s, &root, &relative).await {
        Ok(path) => path,
        Err(status) => return status.into_response(),
    };
    if query.stream.is_some() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Ok(contents) = tokio::fs::read_to_string(&path).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let vtt = match extension.as_str() {
        "vtt" => contents,
        "srt" => srt_to_vtt(&contents),
        "ass" => ass_to_vtt(&contents),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    ([(header::CONTENT_TYPE, "text/vtt; charset=utf-8")], vtt).into_response()
}

async fn media_info_panel(
    directory: &std::path::Path,
    database_metadata: Option<&serde_json::Value>,
    hide_episode_fields: bool,
    cover_artwork: Option<&str>,
) -> String {
    if let Some(value) = read_sidecar_info(directory).await {
        let title = value
            .get("title")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("Media information");
        let description = value
            .get("description")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .or_else(|| {
                value
                    .get("metadata_json")
                    .and_then(|metadata| metadata.get("overview"))
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.is_empty())
            })
            .map(|value| format!(r#"<p class="metadata-description">{}</p>"#, html(value)))
            .unwrap_or_default();
        let overview = if hide_episode_fields {
            let artwork = cover_artwork
                .map(|url| {
                    format!(
                        r#"<img class="show-overview-cover" src="{}" alt="Cover for {}">"#,
                        html(url),
                        html(title)
                    )
                })
                .unwrap_or_default();
            format!(
                r#"<div class="show-overview-summary">{}<div>{}</div></div>"#,
                artwork, description
            )
        } else {
            description
        };
        let mut details = render_sidecar_details(&value, hide_episode_fields);
        if hide_episode_fields {
            if let Some(metadata) = value.get("metadata_json") {
                details.push_str(&render_tv_show_metadata_details(metadata));
            }
        }
        return format!(
            r#"<section class="metadata-panel"><h2>{}</h2>{}<div class="metadata-details">{}</div></section>"#,
            html(title),
            overview,
            details
        );
    }
    database_metadata
        .map(|metadata| render_database_info_panel(metadata, None))
        .unwrap_or_default()
}

fn render_tv_show_metadata_details(metadata: &serde_json::Value) -> String {
    let mut details = Vec::new();
    if let Some(genres) = metadata
        .get("genres")
        .and_then(serde_json::Value::as_array)
        .map(|genres| {
            genres
                .iter()
                .filter_map(|genre| {
                    genre
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| genre.as_str())
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .filter(|genres| !genres.is_empty())
    {
        details.push(("genres", genres));
    }
    let content_rating = metadata
        .get("content_ratings")
        .and_then(|ratings| ratings.get("results"))
        .and_then(serde_json::Value::as_array)
        .and_then(|ratings| {
            ratings
                .iter()
                .find(|rating| {
                    rating.get("iso_3166_1").and_then(serde_json::Value::as_str) == Some("US")
                })
                .or_else(|| ratings.first())
        })
        .and_then(|rating| rating.get("rating"))
        .and_then(serde_json::Value::as_str)
        .filter(|rating| !rating.is_empty());
    if let Some(rating) = content_rating {
        details.push(("rating", rating.to_owned()));
    }
    details
        .into_iter()
        .map(|(key, value)| {
            format!(
                r#"<div class="metadata-detail"><strong>{}</strong><span>{}</span></div>"#,
                html(key),
                html(&value)
            )
        })
        .collect()
}

fn render_database_info_panel(
    metadata: &serde_json::Value,
    episode: Option<&TvEpisodeInfo>,
) -> String {
    let episode_metadata = episode.and_then(|episode| {
        metadata.get("mplex_episode_metadata").or_else(|| {
            metadata
                .get("episodes")
                .and_then(serde_json::Value::as_array)
                .and_then(|episodes| {
                    episodes.iter().find(|item| {
                        item.get("episode_number")
                            .and_then(serde_json::Value::as_u64)
                            == Some(episode.episode as u64)
                    })
                })
        })
    });
    let title = episode_metadata
        .and_then(|value| value.get("name").and_then(serde_json::Value::as_str))
        .or_else(|| {
            episode.and_then(|_| {
                metadata
                    .get("mplex_episode_title")
                    .and_then(serde_json::Value::as_str)
            })
        })
        .or_else(|| metadata.get("title").and_then(serde_json::Value::as_str))
        .or_else(|| metadata.get("name").and_then(serde_json::Value::as_str))
        .unwrap_or("Media information");
    let episode_description = episode_metadata
        .and_then(|value| value.get("overview").and_then(serde_json::Value::as_str))
        .filter(|value| !value.is_empty());
    let season_description = metadata
        .get("mplex_season_description")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty());
    let show_description = metadata
        .get("mplex_show_description")
        .or_else(|| metadata.get("overview"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty());
    let (description_label, description) = if episode.is_some() {
        episode_description
            .map(|value| ("Episode description", value))
            .or_else(|| season_description.map(|value| ("Season description", value)))
            .or_else(|| show_description.map(|value| ("Show description", value)))
            .unwrap_or(("", ""))
    } else {
        show_description
            .map(|value| ("Show description", value))
            .unwrap_or(("", ""))
    };
    let description = if description.is_empty() {
        String::new()
    } else {
        format!(
            r#"<p class="metadata-description"><strong>{}</strong>: {}</p>"#,
            html(description_label),
            html(description)
        )
    };
    let mut details = Vec::new();
    if let Some(episode) = episode {
        details.push(("season", episode.season.to_string()));
        details.push(("episode", episode.episode.to_string()));
        if let Some(end) = episode.episode_end {
            details.push(("episode end", end.to_string()));
        }
    }
    for (key, label) in [
        ("release_date", "release date"),
        ("first_air_date", "first air date"),
        ("air_date", "air date"),
        ("runtime", "runtime"),
        ("vote_average", "rating"),
    ] {
        let value = episode_metadata
            .and_then(|episode| episode.get(key))
            .or_else(|| metadata.get(key));
        if let Some(value) = value {
            let rendered = match value {
                serde_json::Value::String(value) => value.clone(),
                serde_json::Value::Number(value) => value.to_string(),
                _ => String::new(),
            };
            if !rendered.is_empty() {
                details.push((label, rendered));
            }
        }
    }
    let details = details
        .into_iter()
        .map(|(key, value)| {
            format!(
                r#"<div class="metadata-detail"><strong>{}</strong><span>{}</span></div>"#,
                html(key),
                html(&value)
            )
        })
        .collect::<String>();
    format!(
        r#"<section class="metadata-panel"><h2>{}</h2>{}<div class="metadata-details">{}</div></section>"#,
        html(title),
        description,
        details
    )
}

fn matched_tv_show_metadata(
    state: &AppState,
    media_index: &[Media],
    show_directory: &std::path::Path,
) -> Option<serde_json::Value> {
    read_sidecar_metadata(show_directory).or_else(|| {
        media_index
            .iter()
            .filter(|media| media.path.starts_with(show_directory))
            .find_map(|media| {
                let (library_id, _) = media_library(state, media)?;
                read_database_metadata(state, &format!("{library_id}-{}", media.id))
            })
    })
}

fn render_season_description_panel(
    metadata: &serde_json::Value,
    season: u32,
    show_metadata: Option<&serde_json::Value>,
) -> String {
    let season_description = metadata
        .get("overview")
        .and_then(serde_json::Value::as_str)
        .filter(|description| !description.is_empty());
    let show_description = show_metadata
        .and_then(|metadata| metadata.get("overview"))
        .and_then(serde_json::Value::as_str)
        .filter(|description| !description.is_empty());
    let (label, description) = season_description
        .map(|description| ("Season description", description))
        .or_else(|| show_description.map(|description| ("Show description", description)))
        .unwrap_or_default();
    if description.is_empty() {
        return String::new();
    }
    let title = metadata
        .get("name")
        .and_then(serde_json::Value::as_str)
        .filter(|title| !title.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("Season {season}"));
    let details = format!(
        r#"<div class="metadata-detail"><strong>season</strong><span>{season}</span></div>"#
    );
    format!(
        r#"<section class="metadata-panel"><h2>{}</h2><p class="metadata-description"><strong>{}</strong>: {}</p><div class="metadata-details">{}</div></section>"#,
        html(&title),
        html(label),
        html(description),
        details
    )
}

async fn episode_info_panel(
    state: &AppState,
    root: &std::path::Path,
    relative: &std::path::Path,
) -> String {
    let path = root.join(relative);
    let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
        return String::new();
    };
    let Some((season_from_name, (episode_number, episode_end), marker_end)) =
        parse_tv_episode_marker(stem)
    else {
        return String::new();
    };
    let episode_info = TvEpisodeInfo {
        season: path
            .components()
            .filter_map(|component| component.as_os_str().to_str())
            .find_map(parse_tv_season_directory)
            .or(season_from_name)
            .unwrap_or(1),
        episode: episode_number,
        episode_end,
        episode_title: clean_episode_title(&stem[marker_end..]),
    };
    let media_index = media_snapshot(state);
    let show_directory = relative
        .components()
        .next()
        .map(|component| root.join(component.as_os_str()));
    let show_metadata = show_directory
        .as_deref()
        .and_then(|directory| matched_tv_show_metadata(state, &media_index, directory));
    let sidecar_mode = state
        .config
        .lock()
        .is_ok_and(|config| config.metadata_storage == "sidecar");
    let database_metadata = if sidecar_mode {
        None
    } else {
        media_index
            .iter()
            .find(|media| media.path == path)
            .and_then(|media| {
                let (library_id, _) = media_library(state, media)?;
                read_database_metadata(state, &format!("{library_id}-{}", media.id))
            })
    };
    let season_directory = show_directory
        .as_deref()
        .map(|directory| tv_season_directory(directory, episode_info.season, &path))
        .unwrap_or_else(|| path.parent().unwrap_or(root).to_path_buf());
    let season_metadata = if sidecar_mode {
        read_sidecar_info(&season_directory)
            .await
            .and_then(|info| info.get("metadata_json").cloned())
    } else {
        None
    };
    let stored_metadata = season_metadata.as_ref().or(database_metadata.as_ref());
    let episode_metadata = stored_metadata.and_then(|metadata| {
        metadata.get("mplex_episode_metadata").cloned().or_else(|| {
            metadata
                .get("episodes")
                .and_then(serde_json::Value::as_array)
                .and_then(|episodes| {
                    episodes.iter().find(|item| {
                        item.get("episode_number")
                            .and_then(serde_json::Value::as_u64)
                            == Some(episode_number as u64)
                    })
                })
                .cloned()
        })
    });
    let season_description = season_metadata
        .as_ref()
        .and_then(|metadata| metadata.get("overview"))
        .cloned()
        .or_else(|| {
            database_metadata
                .as_ref()
                .and_then(|metadata| metadata.get("mplex_season_description"))
                .cloned()
        });
    let mut panel_metadata = show_metadata.unwrap_or_else(|| serde_json::json!({}));
    if let Some(object) = panel_metadata.as_object_mut() {
        if let Some(description) = &season_description {
            object.insert("mplex_season_description".to_owned(), description.clone());
        }
        if let Some(episode_metadata) = episode_metadata {
            object.insert("mplex_episode_metadata".to_owned(), episode_metadata);
        }
    }
    render_database_info_panel(&panel_metadata, Some(&episode_info))
}

fn render_sidecar_details(value: &serde_json::Value, hide_episode_fields: bool) -> String {
    let mut display_value = value.clone();
    if let Some(object) = display_value.as_object_mut() {
        object.remove("technical");
    }
    toml::Value::try_from(display_value)
        .ok()
        .and_then(|value| {
            value
                .as_table()
                .map(|table| render_toml_details(table, hide_episode_fields))
        })
        .unwrap_or_default()
}

fn render_toml_details(
    table: &toml::map::Map<String, toml::Value>,
    hide_episode_fields: bool,
) -> String {
    table
        .iter()
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "metadata_json" | "description" | "technical" | "path" | "media_path"
            ) && !(hide_episode_fields
                && matches!(
                    key.as_str(),
                    "season" | "episode" | "episode_end" | "episode_title"
                ))
        })
        .map(|(key, value)| {
            let rendered = match value {
                toml::Value::Table(table) => render_toml_details(table, hide_episode_fields),
                toml::Value::Array(values) => values
                    .iter()
                    .map(render_toml_value)
                    .collect::<Vec<_>>()
                    .join(", "),
                _ => render_toml_value(value),
            };
            format!(
                r#"<div class="metadata-detail"><strong>{}</strong><span>{}</span></div>"#,
                html(&key.replace('_', " ")),
                rendered
            )
        })
        .collect()
}

fn render_toml_value(value: &toml::Value) -> String {
    match value {
        toml::Value::String(value) => html(value),
        toml::Value::Integer(value) => value.to_string(),
        toml::Value::Float(value) => value.to_string(),
        toml::Value::Boolean(value) => value.to_string(),
        toml::Value::Datetime(value) => html(&value.to_string()),
        toml::Value::Array(_) | toml::Value::Table(_) => html(&value.to_string()),
    }
}

async fn web_artwork(State(s): State<AppState>, Query(query): Query<WebPath>) -> Response {
    let Some(relative) = query.path.as_deref().and_then(clean_relative_path) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if contains_hidden_component(&relative)
        || !matches!(query.kind.as_deref(), Some("cover" | "banner"))
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    if !s
        .config
        .lock()
        .is_ok_and(|config| config.metadata_storage == "sidecar")
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let root = selected_library_root(&s, query.library);
    let directory = match web_path(&s, &root, &relative).await {
        Ok(path) if path.is_dir() => path,
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    let path = directory
        .join(".mplex")
        .join(format!("{}.jpg", query.kind.unwrap()));
    let Ok(bytes) = tokio::fs::read(path).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    ([(header::CONTENT_TYPE, "image/jpeg")], bytes).into_response()
}

async fn web_info(State(s): State<AppState>, Query(query): Query<WebPath>) -> Response {
    let Some(relative) = query.path.as_deref().and_then(clean_relative_path) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if contains_hidden_component(&relative) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let root = selected_library_root(&s, query.library);
    let path = match web_path(&s, &root, &relative).await {
        Ok(path) => path,
        Err(status) => return status.into_response(),
    };
    let directory = if path.is_dir() {
        path
    } else {
        path.parent()
            .unwrap_or(std::path::Path::new("."))
            .to_path_buf()
    };
    let Some(info) = read_sidecar_info(&directory).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(technical) = info.get("technical") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    Json(technical).into_response()
}

fn artwork_url(relative: &std::path::Path, library: Option<usize>, kind: &str) -> String {
    format!(
        "/web/artwork?{}path={}&kind={}",
        library
            .map(|index| format!("library={index}&"))
            .unwrap_or_default(),
        url_encode(&relative.to_string_lossy()),
        kind
    )
}

fn srt_to_vtt(contents: &str) -> String {
    let mut output = String::from("WEBVTT\n\n");
    for block in contents.replace("\r\n", "\n").split("\n\n") {
        let mut lines = block.lines();
        let first = lines.next().unwrap_or_default();
        let timing = if first.contains(" --> ") {
            first
        } else {
            lines.next().unwrap_or_default()
        };
        if !timing.contains(" --> ") {
            continue;
        }
        output.push_str(&timing.replace(',', "."));
        output.push('\n');
        for line in lines {
            output.push_str(line);
            output.push('\n');
        }
        output.push('\n');
    }
    output
}

fn ass_to_vtt(contents: &str) -> String {
    let mut output = String::from("WEBVTT\n\n");
    for line in contents
        .lines()
        .filter(|line| line.starts_with("Dialogue:"))
    {
        let fields = line
            .strip_prefix("Dialogue:")
            .unwrap_or_default()
            .trim()
            .splitn(10, ',')
            .collect::<Vec<_>>();
        if fields.len() < 10 {
            continue;
        }
        let start = ass_timestamp(fields[1]);
        let end = ass_timestamp(fields[2]);
        let text = fields[9].replace("\\N", "\n").replace("\\n", "\n");
        let text = strip_ass_tags(&text);
        output.push_str(&format!("{} --> {}\n{}\n\n", start, end, text));
    }
    output
}

fn ass_timestamp(value: &str) -> String {
    let parts = value.trim().split(':').collect::<Vec<_>>();
    if parts.len() != 3 {
        return "00:00:00.000".to_owned();
    }
    let seconds = parts[2].parse::<f32>().unwrap_or_default();
    format!(
        "{:02}:{:02}:{:06.3}",
        parts[0].parse::<u32>().unwrap_or_default(),
        parts[1].parse::<u32>().unwrap_or_default(),
        seconds
    )
}

fn strip_ass_tags(value: &str) -> String {
    let mut output = String::new();
    let mut in_tag = false;
    for character in value.chars() {
        match character {
            '{' => in_tag = true,
            '}' => in_tag = false,
            _ if !in_tag => output.push(character),
            _ => {}
        }
    }
    output
}

fn tv_episode_display_name(name: &str) -> String {
    let stem = std::path::Path::new(name)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(name);
    let Some((_, (episode, episode_end), marker_end)) = parse_tv_episode_marker(stem) else {
        return name.to_owned();
    };
    let title = clean_episode_title(&stem[marker_end..]);
    let episode_label = episode_end
        .map(|end| format!("Episodes {episode}–{end}"))
        .unwrap_or_else(|| format!("Episode {episode}"));
    title
        .map(|title| format!("{episode_label} · {title}"))
        .unwrap_or(episode_label)
}

fn web_client_item(
    name: &str,
    relative: &str,
    mime: &str,
    library: Option<usize>,
    subtitles: &[SubtitleTrack],
    artwork: Option<String>,
    selected_subtitle_stream_id: Option<u64>,
) -> String {
    let stream_path = url_encode(relative);
    let library_query = library
        .map(|index| format!("library={index}&"))
        .unwrap_or_default();
    let tracks = subtitles
        .iter()
        .map(|track| {
            format!(
                "<track kind=\"subtitles\" label=\"{}\" src=\"{}\" />",
                html(&track.label),
                html(&track.url)
            )
        })
        .collect::<String>();
    let selected_subtitle_stream_id = selected_subtitle_stream_id.unwrap_or_default();
    let subtitle_options = subtitles
        .iter()
        .enumerate()
        .map(|(index, track)| {
            format!(
                r#"<option value="{}" data-stream-id="{}"{}>{}</option>"#,
                index,
                track.stream_id,
                if track.stream_id == selected_subtitle_stream_id {
                    " selected"
                } else {
                    ""
                },
                html(&track.label)
            )
        })
        .collect::<String>();
    let subtitle_selector = if subtitles.is_empty() {
        r#"<span class="subtitle-note">No external or embedded subtitles detected.</span>"#
            .to_owned()
    } else {
        format!(
            r#"<label class="subtitle-picker">Subtitles <select class="subtitle-select" data-progress-path="{}" data-library="{}" data-selected-stream-id="{}"><option value="-1" data-stream-id="0"{}>Off</option>{}</select></label><span class="subtitle-note">External subtitle selection syncs with Plex clients.</span>"#,
            html(relative),
            library.map(|index| index.to_string()).unwrap_or_default(),
            selected_subtitle_stream_id,
            if selected_subtitle_stream_id == 0 {
                " selected"
            } else {
                ""
            },
            subtitle_options
        )
    };
    let relative_path = PathBuf::from(relative);
    let info_path = relative_path.parent().unwrap_or(std::path::Path::new(""));
    let info_url = format!(
        "/web/info?{}path={}",
        library_query,
        url_encode(&info_path.to_string_lossy())
    );
    let poster = artwork
        .map(|url| format!(r#" poster="{}""#, html(&url)))
        .unwrap_or_default();
    format!(
        r#"<article>
  <video controls preload="auto" data-progress-path="{}"{} src="/web/stream?{}path={}" type="{}">
    {}
    Your browser does not support HTML video. <a href="/web/stream?{}path={}">Open the stream</a>.
  </video>
  <div class="details">
    <h2>{}</h2>
    <div class="play-actions"><button type="button" class="resume-button" title="Resume from saved position" aria-label="Resume from saved position" hidden>↪ Resume</button><button type="button" class="start-button" title="Play from beginning" aria-label="Play from beginning">▶ Play from beginning</button><button type="button" class="theatre-button" title="Theatre mode" aria-label="Theatre mode">▣ Theatre</button><button type="button" class="info-button" data-info-url="{}" title="Show media information" aria-label="Show media information">ⓘ Info</button>{}</div>
    <form class="match-form" method="post" action="/settings/media/scan"><input type="hidden" name="path" value="{}"><input type="hidden" name="library" value="{}"><input name="title" value="{}" aria-label="Metadata match title"><button type="submit">↻ Match metadata</button></form>
    <a href="/web/stream?{}path={}">Open stream</a>
  </div>
</article>"#,
        html(relative),
        poster,
        library_query,
        stream_path,
        mime,
        tracks,
        library
            .map(|index| format!("library={index}&"))
            .unwrap_or_default(),
        url_encode(relative),
        html(name),
        html(&info_url),
        subtitle_selector,
        html(relative),
        library.map(|index| index.to_string()).unwrap_or_default(),
        html(name),
        library
            .map(|index| format!("library={index}&"))
            .unwrap_or_default(),
        url_encode(relative)
    )
}

fn sidebar_nav_for(
    state: &AppState,
    relative: &std::path::Path,
    selected_library: Option<usize>,
) -> String {
    let library_links = sidebar_library_links(state, selected_library);
    sidebar_nav_with_library_links(
        selected_library.is_none() && relative.as_os_str().is_empty(),
        false,
        &library_links,
    )
}

fn sidebar_nav_for_settings(state: &AppState) -> String {
    let library_links = sidebar_library_links(state, None);
    if library_links.is_empty() {
        sidebar_nav_with_settings(true)
    } else {
        sidebar_nav_with_library_links(false, true, &library_links)
    }
}

fn sidebar_library_links(state: &AppState, selected_library: Option<usize>) -> String {
    state
        .config
        .lock()
        .ok()
        .map(|config| {
            config
                .libraries
                .iter()
                .enumerate()
                .map(|(index, library)| {
                    format!(
                        "<a class=\"sidebar-link sidebar-library {}\" data-spa-link href=\"/?library={}\"><span class=\"sidebar-icon\">{}</span>{}</a>",
                        if selected_library == Some(index) {
                            "active"
                        } else {
                            ""
                        },
                        index,
                        if library.kind == "tv" { "▣" } else { "▤" },
                        html(&library.name)
                    )
                })
                .collect::<String>()
        })
        .unwrap_or_default()
}

fn sidebar_nav_with_settings(settings_active: bool) -> String {
    sidebar_nav_with_library_links(!settings_active, settings_active, "")
}

fn sidebar_nav_with_library_links(
    home_active: bool,
    settings_active: bool,
    library_links: &str,
) -> String {
    format!(
        r#"<aside class="sidebar">
  <div class="sidebar-top">
    <a class="sidebar-brand" data-spa-link href="/">harbringer</a>
    <button class="sidebar-toggle" type="button" aria-label="Collapse sidebar" aria-expanded="true" title="Collapse sidebar">‹</button>
  </div>
  <div class="sidebar-label">Browse</div>
  <nav class="sidebar-nav" aria-label="Main navigation">
    <a class="sidebar-link {}" data-spa-link href="/"><span class="sidebar-icon">⌂</span>Home</a>
    {}
    <a class="sidebar-link {}" href="/settings"><span class="sidebar-icon">⚙</span>Settings</a>
  </nav>
</aside>"#,
        if home_active && !settings_active {
            "active"
        } else {
            ""
        },
        library_links,
        if settings_active { "active" } else { "" },
    )
}

fn breadcrumb_nav(relative: &std::path::Path, library: Option<usize>, files_view: bool) -> String {
    if relative.as_os_str().is_empty() {
        return String::new();
    }
    let mut nav = String::from(r#"<nav class="breadcrumb" aria-label="Breadcrumb">"#);
    let prefix = library
        .map(|index| format!("library={index}&"))
        .unwrap_or_default();
    let view = if files_view { "view=files&" } else { "" };
    let mut current = PathBuf::new();
    for (index, component) in relative.components().enumerate() {
        let Component::Normal(name) = component else {
            continue;
        };
        current.push(name);
        let label = name.to_string_lossy();
        if index > 0 {
            nav.push_str(r#"<span class="breadcrumb-separator">/</span>"#);
        }
        nav.push_str(&format!(
            r#"<a data-spa-link href="/?{}{}path={}">{}</a>"#,
            prefix,
            view,
            url_encode(&current.to_string_lossy()),
            html(&label)
        ));
    }
    nav.push_str("</nav>");
    nav
}

fn folder_card(
    name: &str,
    relative: &std::path::Path,
    library: Option<usize>,
    preview: Option<&std::path::Path>,
    cover: bool,
    banner: bool,
    progress: Option<f64>,
) -> String {
    folder_card_with_badge(
        name, relative, library, preview, cover, banner, progress, None,
    )
}

fn media_thumbnail_url(media_id: &str) -> String {
    format!("/library/metadata/{}/thumb", url_encode(media_id))
}

fn media_card(
    media: &Media,
    library_config: &ConfigLibrary,
    metadata: Option<&serde_json::Value>,
    root: &std::path::Path,
    folder: &std::path::Path,
    media_path: &std::path::Path,
    library: usize,
    show_remove_from_continue: bool,
) -> String {
    let href = format!(
        "/?library={library}&path={}&play={}",
        url_encode(&folder.to_string_lossy()),
        url_encode(&media_path.to_string_lossy())
    );
    let tv = library_config.kind == "tv";
    let title = if tv {
        metadata
            .and_then(|item| item.get("name"))
            .and_then(serde_json::Value::as_str)
            .filter(|title| !title.is_empty())
            .map(str::to_owned)
            .or_else(|| {
                media
                    .path
                    .strip_prefix(root)
                    .ok()?
                    .components()
                    .next()
                    .map(|component| component.as_os_str().to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| media.title.clone())
    } else {
        metadata
            .and_then(|item| item.get("title"))
            .and_then(serde_json::Value::as_str)
            .filter(|title| !title.is_empty())
            .unwrap_or(&media.title)
            .to_owned()
    };
    let (subtitle, details) = if tv {
        tv_episode_card_labels(root, &media.path, metadata)
    } else {
        metadata
            .and_then(|item| item.get("release_date"))
            .and_then(serde_json::Value::as_str)
            .and_then(release_year)
            .or_else(|| title_year(&media.title))
            .map(|year| year.to_string())
            .map(|year| (Some(year), None))
            .unwrap_or_default()
    };
    let progress = metadata.map(media_card_progress).unwrap_or_default();
    media_poster_card(
        Some(&media.id),
        &href,
        &title,
        subtitle.as_deref(),
        details.as_deref(),
        progress,
        media_path,
        library,
        tv,
        library_config.kind != "other",
        "item",
        show_remove_from_continue,
    )
}

fn tv_episode_card_labels(
    root: &std::path::Path,
    media_path: &std::path::Path,
    metadata: Option<&serde_json::Value>,
) -> (Option<String>, Option<String>) {
    let episode = tv_episode_info(&root.to_path_buf(), media_path);
    let episode_title = episode
        .as_ref()
        .and_then(|episode| episode.episode_title.clone())
        .or_else(|| {
            let metadata_matches = episode.as_ref().is_some_and(|episode| {
                metadata.is_some_and(|item| {
                    item.get("mplex_season").and_then(serde_json::Value::as_u64)
                        == Some(episode.season as u64)
                        && item
                            .get("mplex_episode")
                            .and_then(serde_json::Value::as_u64)
                            == Some(episode.episode as u64)
                })
            });
            metadata_matches
                .then(|| {
                    metadata
                        .and_then(|item| item.get("mplex_episode_title"))
                        .and_then(serde_json::Value::as_str)
                        .filter(|title| !title.is_empty())
                        .map(str::to_owned)
                })
                .flatten()
        });
    let episode_number =
        episode.map(|episode| format!("S{} · E{}", episode.season, episode.episode));
    (episode_title, episode_number)
}

fn release_year(date: &str) -> Option<u32> {
    let year = date.get(..4)?.parse::<u32>().ok()?;
    (1888..=2100).contains(&year).then_some(year)
}

fn media_poster_card(
    media_id: Option<&str>,
    href: &str,
    title: &str,
    subtitle: Option<&str>,
    details: Option<&str>,
    progress: f64,
    media_path: &std::path::Path,
    library: usize,
    tv_card: bool,
    allow_metadata_actions: bool,
    watch_scope: &str,
    show_remove_from_continue: bool,
) -> String {
    let subtitle = subtitle
        .map(|value| format!("<span class=\"media-card-subtitle\">{}</span>", html(value)))
        .unwrap_or_default();
    let details = details
        .map(|value| format!("<span class=\"media-card-details\">{}</span>", html(value)))
        .unwrap_or_default();
    let poster = media_id
        .map(|id| {
            format!(
                r#"<img src="{}" alt="Poster for {}" loading="lazy">"#,
                html(&media_thumbnail_url(id)),
                html(title)
            )
        })
        .unwrap_or_else(|| {
            format!(
                r#"<div class="media-card-placeholder" role="img" aria-label="Poster unavailable for {}">No poster available</div>"#,
                html(title)
            )
        });
    let path = html(&media_path.to_string_lossy());
    let watch_key = if watch_scope == "show" {
        media_path
            .components()
            .next()
            .map(|component| html(&component.as_os_str().to_string_lossy()))
            .unwrap_or_else(|| path.clone())
    } else {
        path.clone()
    };
    let card_class = if tv_card {
        "media-card media-card-tv"
    } else {
        "media-card"
    };
    let card_style = "align-self:start;background:rgba(31,41,55,.3);padding:.7rem";
    let card_info_style = " style=\"box-sizing:border-box;height:4rem;padding-top:0;gap:0;grid-template-rows:1rem 1rem 1rem 1rem\"";
    let title_style = " style=\"height:auto;margin:0;white-space:nowrap;text-overflow:ellipsis\"";
    let remove_action = format!(
        r#"<button type="button" data-watch-action="remove_from_continue" data-path="{path}" data-library="{library}" data-watch-scope="{watch_scope}" data-watch-key="{watch_key}">Remove from Continue Watching</button>"#
    );
    let fix_match_action = if allow_metadata_actions {
        let action_path = if tv_card || watch_scope == "show" {
            media_path
                .components()
                .next()
                .map(|component| PathBuf::from(component.as_os_str()))
                .unwrap_or_default()
        } else {
            media_path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| media_path.to_path_buf())
        };
        let action_path = html(&action_path.to_string_lossy());
        format!(
            "<button type=\"button\" data-refresh-metadata data-library=\"{library}\" data-path=\"{action_path}\">Refresh metadata</button><button type=\"button\" data-fix-match data-library=\"{library}\" data-path=\"{action_path}\">Fix Match</button>"
        )
    } else {
        String::new()
    };
    let card = format!(
        r#"<div class="{}" style="{}" data-media-path="{}" data-library="{}" data-watch-scope="{}" data-watch-key="{}"><div class="media-card-artwork"><a class="media-card-poster" data-spa-link href="{}">{}</a><details class="media-card-menu"><summary aria-label="More options" title="More options">⋮</summary><div class="media-card-menu-items"><button type="button" data-watch-action="watched">Mark as watched</button><button type="button" data-watch-action="unwatched" data-path="{}" data-library="{}" data-watch-scope="{}" data-watch-key="{}">Mark as unwatched</button><button type="button" data-watch-action="remove_from_continue" data-path="{}" data-library="{}" data-watch-scope="{}" data-watch-key="{}">Remove from Continue Watching</button>{}</div></details></div><a class="media-card-info" data-spa-link href="{}"{}><span class="media-card-title" title="{}"{}>{}</span>{}{}{}</a></div>"#,
        card_class,
        card_style,
        path,
        library,
        watch_scope,
        watch_key,
        html(href),
        poster,
        path,
        library,
        watch_scope,
        watch_key,
        path,
        library,
        watch_scope,
        watch_key,
        fix_match_action,
        html(href),
        card_info_style,
        html(title),
        title_style,
        html(title),
        subtitle,
        details,
        media_progress_markup(progress),
    );
    if show_remove_from_continue {
        card
    } else {
        card.replace(&remove_action, "")
    }
}

fn media_progress_markup(progress: f64) -> String {
    let progress = progress.clamp(0.0, 1.0);
    let percent = (progress * 100.0).round() as u32;
    format!(
        r#"<div class="media-card-progress-row" style="display:flex;align-items:center;gap:.45rem;grid-row:4;height:1rem"><div class="media-card-progress" role="progressbar" aria-label="Playback progress" aria-valuemin="0" aria-valuemax="100" aria-valuenow="{percent}" style="flex:1;height:5px;margin:0"><span style="width:{:.2}%"></span></div><span class="media-card-percent" style="flex:0 0 auto;color:#9ca3af;font-size:.68rem;line-height:1">{percent}%</span></div>"#,
        progress * 100.0
    )
}

fn media_card_progress(metadata: &serde_json::Value) -> f64 {
    if metadata
        .get("mplex_view_count")
        .and_then(serde_json::Value::as_i64)
        .is_some_and(|count| count > 0)
    {
        return 1.0;
    }
    let offset = metadata
        .get("mplex_view_offset")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_default();
    let duration = metadata
        .get("mplex_duration")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_default();
    if duration > 0 {
        (offset as f64 / duration as f64).clamp(0.0, 1.0)
    } else {
        0.0
    }
}

fn media_card_action_script() -> &'static str {
    r##"<script>
if (!window.__mplexCardActionsBound) {
window.__mplexCardActionsBound = true;
document.addEventListener("error", event => {
  const image = event.target;
  if (!(image instanceof HTMLImageElement) || !image.matches(".media-card-poster img")) return;
  const placeholder = document.createElement("div");
  const title = image.alt.replace(/^Poster for /, "");
  placeholder.className = "media-card-placeholder";
  placeholder.setAttribute("role", "img");
  placeholder.setAttribute("aria-label", `Poster unavailable for ${title}`);
  placeholder.textContent = "No poster available";
  image.replaceWith(placeholder);
}, true);
const fixMatchDialog = document.querySelector("#tv-fix-match-dialog");
const fixMatchQuery = document.querySelector("#tv-fix-match-query");
const fixMatchSearchButton = document.querySelector("#tv-fix-match-search");
const fixMatchStatus = document.querySelector("#tv-fix-match-status");
const fixMatchResults = document.querySelector("#tv-fix-match-results");
const searchTvMatches = async () => {
  fixMatchResults.replaceChildren();
  const query = fixMatchQuery.value.trim();
  if (!query) {
    fixMatchStatus.textContent = "Enter a show title to search.";
    return;
  }
  fixMatchStatus.textContent = "Searching TMDB…";
  fixMatchSearchButton.disabled = true;
  try {
    const params = new URLSearchParams({
      library: fixMatchDialog.dataset.library,
      path: fixMatchDialog.dataset.path,
      query
    });
    const response = await fetch(`/web/tv-match/search?${params}`, { cache: "no-store" });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "TMDB search failed.");
    if (!payload.results.length) {
      fixMatchStatus.textContent = "No matches found. Try another search.";
      return;
    }
    fixMatchStatus.textContent = `${payload.results.length} results. Select the correct match.`;
    payload.results.forEach(result => {
      const item = document.createElement("div");
      item.className = "fix-match-result";
      const poster = document.createElement("img");
      poster.className = "fix-match-poster";
      poster.alt = "";
      poster.loading = "lazy";
      if (result.poster_path) poster.src = `https://image.tmdb.org/t/p/w185${result.poster_path}`;
      const content = document.createElement("div");
      content.className = "fix-match-result-content";
      const heading = document.createElement("h3");
      const year = result.first_air_date ? ` (${result.first_air_date.slice(0, 4)})` : "";
      heading.textContent = `${result.title}${year}`;
      const overview = document.createElement("p");
      overview.textContent = result.overview || "No summary available.";
      const select = document.createElement("button");
      select.type = "button";
      select.dataset.matchId = String(result.id);
      select.textContent = "Use this match";
      content.append(heading, overview, select);
      item.append(poster, content);
      fixMatchResults.append(item);
    });
  } catch (error) {
    fixMatchStatus.textContent = error.message || "TMDB search failed.";
  } finally {
    fixMatchSearchButton.disabled = false;
  }
};
fixMatchSearchButton.addEventListener("click", searchTvMatches);
fixMatchQuery.addEventListener("keydown", event => {
  if (event.key === "Enter") {
    event.preventDefault();
    searchTvMatches();
  }
});
fixMatchResults.addEventListener("click", async event => {
  const select = event.target.closest("[data-match-id]");
  if (!select || select.disabled) return;
  select.disabled = true;
  fixMatchStatus.textContent = "Saving match and artwork…";
  try {
    const response = await fetch("/web/tv-match/select", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        library: Number(fixMatchDialog.dataset.library),
        path: fixMatchDialog.dataset.path,
        tmdb_id: Number(select.dataset.matchId)
      })
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "Could not save this match.");
    fixMatchDialog.close();
    window.location.reload();
  } catch (error) {
    select.disabled = false;
    fixMatchStatus.textContent = error.message || "Could not save this match.";
  }
});
document.querySelector(".fix-match-close")?.addEventListener("click", () => fixMatchDialog.close());
fixMatchDialog.addEventListener("click", event => {
  if (event.target === fixMatchDialog) fixMatchDialog.close();
});
document.addEventListener("click", async event => {
  const menu = event.target.closest(".media-card-menu");
  if (menu && event.target.closest("summary")) {
    document.querySelectorAll(".media-card-menu[open]").forEach(openMenu => {
      if (openMenu !== menu) openMenu.removeAttribute("open");
    });
  } else if (!menu) {
    document.querySelectorAll(".media-card-menu[open]").forEach(openMenu => openMenu.removeAttribute("open"));
  }
  const button = event.target.closest("[data-watch-action]");
  const fixMatchButton = event.target.closest("[data-fix-match]");
  const refreshMetadataButton = event.target.closest("[data-refresh-metadata]");
  if (refreshMetadataButton) {
    event.preventDefault();
    const originalLabel = refreshMetadataButton.textContent;
    refreshMetadataButton.disabled = true;
    refreshMetadataButton.textContent = "Refreshing…";
    try {
      const response = await fetch(
        `/settings/libraries/${refreshMetadataButton.dataset.library}/scan?path=${encodeURIComponent(refreshMetadataButton.dataset.path)}&refresh=true`,
        { method: "POST", redirect: "follow" }
      );
      const resultUrl = new URL(response.url);
      const error = resultUrl.searchParams.get("error");
      if (!response.ok || error) throw new Error(error || "Could not refresh metadata.");
      window.location.reload();
    } catch (error) {
      refreshMetadataButton.disabled = false;
      refreshMetadataButton.textContent = originalLabel;
      let status = refreshMetadataButton.closest(".media-card-menu-items")?.querySelector(".media-card-menu-status");
      if (!status) {
        status = document.createElement("span");
        status.className = "media-card-menu-status";
        status.setAttribute("role", "status");
        status.style.cssText = "padding:.45rem .65rem;color:#fca5a5;font-size:.72rem";
        refreshMetadataButton.closest(".media-card-menu-items")?.append(status);
      }
      status.textContent = error.message || "Could not refresh metadata. Try again.";
    }
    return;
  }

  if (fixMatchButton) {
    event.preventDefault();
    document.querySelectorAll(".media-card-menu[open]").forEach(openMenu => openMenu.removeAttribute("open"));
    const card = fixMatchButton.closest(".media-card");
    const title = card?.querySelector(".media-card-title")?.textContent.trim() || "Media item";
    document.querySelector("#tv-fix-match-heading").textContent = "Fix Match";
    fixMatchQuery.setAttribute("aria-label", "Search movies and TV shows");
    fixMatchDialog.dataset.library = fixMatchButton.dataset.library;
    fixMatchDialog.dataset.path = fixMatchButton.dataset.path;
    document.querySelector("#tv-fix-match-show").textContent = `Search for the correct match for ${title}.`;
    fixMatchQuery.value = title;
    fixMatchStatus.textContent = "";
    fixMatchResults.replaceChildren();
    fixMatchDialog.showModal();
    searchTvMatches();
    return;
  }
  if (!button || button.disabled) return;
  event.preventDefault();
  button.disabled = true;
  const card = button.closest(".media-card");
  const { watchAction: action } = button.dataset;
  const path = button.dataset.path || card?.dataset.mediaPath;
  const library = button.dataset.library || card?.dataset.library;
  const watchScope = button.dataset.watchScope || card?.dataset.watchScope;
  const watchKey = button.dataset.watchKey || card?.dataset.watchKey;
  const showFailure = () => {
    let status = button.closest(".media-card-menu")?.querySelector(".media-card-menu-status");
    if (!status) {
      status = document.createElement("span");
      status.className = "media-card-menu-status";
      status.setAttribute("role", "status");
      status.style.cssText = "padding:.45rem .65rem;color:#fca5a5;font-size:.72rem";
      button.closest(".media-card-menu-items")?.append(status);
    }
    status.textContent = "Could not update watch status. Try again.";
  };
  try {
    const response = await fetch("/web/watch-action", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, path, library: Number(library), scope: watchScope })
    });
    if (!response.ok) {
      button.disabled = false;
      showFailure();
      return;
    }
    button.closest(".media-card-menu")?.querySelector(".media-card-menu-status")?.remove();
    button.closest("details")?.removeAttribute("open");
    const matchesItem = item => item.dataset.library === library && (
      watchScope === "show"
        ? item.dataset.watchKey === watchKey
        : item.dataset.mediaPath === path
    );
    if (action === "unwatched" || action === "watched") {
      const percentValue = action === "watched" ? "100" : "0";
      document.querySelectorAll(".media-card").forEach(item => {
        if (!matchesItem(item)) return;
        const progress = item.querySelector('[role="progressbar"]');
        const fill = progress?.querySelector("span");
        const percent = item.querySelector(".media-card-percent");
        progress?.setAttribute("aria-valuenow", percentValue);
        if (fill) fill.style.width = `${percentValue}%`;
        if (percent) percent.textContent = `${percentValue}%`;
        if (action === "watched" && item.closest(".continue-grid")) item.remove();
      });
    } else if (action === "remove_from_continue") {
      document.querySelectorAll(".continue-grid .media-card").forEach(item => {
        if (matchesItem(item)) item.remove();
      });
    }
    const continueGrid = document.querySelector(".continue-grid");
    if (continueGrid && !continueGrid.querySelector(".media-card")) {
      continueGrid.innerHTML = '<p class="note empty-state">Nothing to resume yet. Start watching something and it will appear here.</p>';
    }
    if (action === "watched" && continueGrid) window.location.reload();
  } catch (_) {
    button.disabled = false;
    showFailure();
  }
});
}
</script>"##
}

fn spa_navigation_script() -> &'static str {
    r#"<style>body { background: radial-gradient(ellipse at 75% -20%, #1b3151 0, transparent 44rem), #0b1020; }</style><script>
if (!window.__mplexSpaBound) {
window.__mplexSpaBound = true;
let navigationSequence = 0;
async function navigateMplex(url, addHistory) {
  const sequence = ++navigationSequence;
  const response = await fetch(url, { headers: { "X-Mplex-Web-Navigation": "1" }, cache: "no-store" });
  if (!response.ok) { window.location.assign(url); return; }
  const parsed = new DOMParser().parseFromString(await response.text(), "text/html");
  if (sequence !== navigationSequence) return;
  if (!parsed.querySelector(".library") && window.__mplexLibraryRefreshTimer) {
    clearInterval(window.__mplexLibraryRefreshTimer);
    window.__mplexLibraryRefreshTimer = null;
  }
  const scripts = [...parsed.body.querySelectorAll("script")].map(script => script.textContent || "");
  parsed.body.querySelectorAll("script").forEach(script => script.remove());
  document.title = parsed.title;
  document.head.innerHTML = parsed.head.innerHTML;
  document.body.innerHTML = parsed.body.innerHTML;
  if (addHistory) history.pushState({}, "", url);
  scripts.forEach(source => {
    const script = document.createElement("script");
    script.textContent = `(() => { ${source}\n })()`;
    document.body.append(script);
  });
  window.scrollTo(0, 0);
}
document.addEventListener("click", event => {
  const link = event.target.closest("a[data-spa-link]");
  if (!link || event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
  const url = new URL(link.href);
  if (url.origin !== location.origin || url.pathname !== "/") return;
  event.preventDefault();
  navigateMplex(url.pathname + url.search + url.hash, true).catch(() => window.location.assign(url.href));
});
window.addEventListener("popstate", () => navigateMplex(location.pathname + location.search + location.hash, false));
}
</script>"#
}

fn average_media_progress(media: &[&Media], metadata: &HashMap<String, serde_json::Value>) -> f64 {
    if media.is_empty() {
        return 0.0;
    }
    media
        .iter()
        .map(|item| {
            metadata
                .get(&item.id)
                .map(media_card_progress)
                .unwrap_or_default()
        })
        .sum::<f64>()
        / media.len() as f64
}

fn folder_card_with_badge(
    name: &str,
    relative: &std::path::Path,
    library: Option<usize>,
    preview: Option<&std::path::Path>,
    cover: bool,
    banner: bool,
    progress: Option<f64>,
    badge: Option<&str>,
) -> String {
    let preview_markup = if cover { None } else { preview }
        .map(|path| {
            format!(
                r#"<video class="folder-preview-video" muted playsinline preload="auto" src="/web/stream?{}path={}" aria-label="Preview of {}"></video>"#,
                library
                    .map(|index| format!("library={index}&"))
                    .unwrap_or_default(),
                url_encode(&path.to_string_lossy()),
                html(name)
            )
        })
        .unwrap_or_default();
    let artwork_markup = if cover {
        format!(
            r#"<img class="folder-cover" src="{}" alt="Cover for {}">"#,
            artwork_url(relative, library, "cover"),
            html(name)
        )
    } else {
        String::new()
    };
    let fallback_icon = if cover || preview.is_some() {
        String::new()
    } else {
        r#"<span class="folder-preview-icon" aria-hidden="true">📁</span>"#.to_owned()
    };
    let banner_style = if banner {
        format!(
            r#" style="background-image: linear-gradient(90deg, rgba(17,24,39,.45), rgba(17,24,39,.8)), url('{}')""#,
            artwork_url(relative, library, "banner")
        )
    } else {
        String::new()
    };
    let progress_markup = progress.map(media_progress_markup).unwrap_or_default();
    let badge_markup = badge
        .filter(|value| !value.is_empty())
        .map(|value| {
            format!(
                r#"<span style="position:absolute;top:.55rem;right:.55rem;z-index:2;min-width:1.7rem;padding:.25rem .45rem;border-radius:.35rem;color:#fff;background:rgba(17,24,39,.9);font-weight:700;text-align:center">{}</span>"#,
                html(value)
            )
        })
        .unwrap_or_default();
    format!(
        r#"<a class="folder-card" data-spa-link style="display:block;overflow:hidden;padding:0;color:inherit;text-decoration:none;border:1px solid #374151;border-radius:.75rem;background:#1f2937" href="/?{}path={}">
  <div class="folder-preview" style="position:relative;display:grid;place-items:center;overflow:hidden;aspect-ratio:2/3;background:linear-gradient(135deg,#1e3a8a,#312e81 55%,#111827)"{}>{}{}{}{}</div>
  <div class="folder-info" style="padding:.9rem 1rem 1rem"><h2 style="font-size:1rem;line-height:1.25;margin:0;overflow-wrap:anywhere">{}</h2>{}</div>
</a>"#,
        library
            .map(|index| format!("library={index}&"))
            .unwrap_or_default(),
        url_encode(&relative.to_string_lossy()),
        banner_style,
        preview_markup,
        badge_markup,
        fallback_icon,
        artwork_markup,
        html(name),
        progress_markup,
    )
}

async fn first_media_file(directory: &std::path::Path) -> Option<PathBuf> {
    let mut pending = vec![directory.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let mut entries = tokio::fs::read_dir(directory).await.ok()?;
        let mut children = Vec::new();
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            let path = entry.path();
            let file_type = entry.file_type().await.ok()?;
            if file_type.is_file() && media_type(&path).is_some() {
                return Some(path);
            }
            if file_type.is_dir() {
                children.push(path);
            }
        }
        children.sort();
        pending.extend(children.into_iter().rev());
    }
    None
}

fn file_item(name: &str) -> String {
    format!(
        r#"<div class="file-item">📄 <span>{}</span></div>"#,
        html(name)
    )
}

async fn media_file_count(directory: &std::path::Path) -> usize {
    let mut pending = vec![directory.to_path_buf()];
    let mut count = 0;
    while let Some(directory) = pending.pop() {
        let Ok(mut entries) = tokio::fs::read_dir(directory).await else {
            continue;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            let path = entry.path();
            let Ok(file_type) = entry.file_type().await else {
                continue;
            };
            if file_type.is_dir() {
                pending.push(path);
            } else if file_type.is_file() && media_type(&path).is_some() {
                count += 1;
            }
        }
    }
    count
}

async fn web_stream(
    State(s): State<AppState>,
    Query(query): Query<WebPath>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let Some(relative) = query.path.as_deref().and_then(clean_relative_path) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if contains_hidden_component(&relative) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let library_root = selected_library_root(&s, query.library);
    let path = match web_path(&s, &library_root, &relative).await {
        Ok(path) => path,
        Err(status) => return status.into_response(),
    };
    let Ok(metadata) = tokio::fs::metadata(&path).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some((container, mime)) = media_type(&path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !metadata.is_file() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let media = Media {
        id: relative.to_string_lossy().into_owned(),
        title: path
            .file_stem()
            .and_then(|name| name.to_str())
            .unwrap_or("Video")
            .to_owned(),
        path,
        size: metadata.len(),
        container: container.to_owned(),
        mime: mime.to_owned(),
        metadata_revision: 0,
    };
    stream_file(&media, method, headers, false).await
}

async fn web_path(
    _state: &AppState,
    root: &std::path::Path,
    relative: &std::path::Path,
) -> Result<PathBuf, StatusCode> {
    let root = tokio::fs::canonicalize(root)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let path = tokio::fs::canonicalize(root.join(relative))
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    if path.starts_with(&root) {
        Ok(path)
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

fn clean_relative_path(value: &str) -> Option<PathBuf> {
    let mut result = PathBuf::new();
    for component in PathBuf::from(value).components() {
        match component {
            Component::Normal(part) => result.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(result)
}

fn is_hidden_path(path: &std::path::Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with('.'))
}

fn contains_hidden_component(path: &std::path::Path) -> bool {
    path.components().any(|component| {
        matches!(component, Component::Normal(name) if name.to_str().is_some_and(|name| name.starts_with('.')))
    })
}

fn url_encode(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (*byte as char).to_string()
            }
            _ => format!("%{:02X}", byte),
        })
        .collect()
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

async fn log_not_found(State(state): State<AppState>, req: Request<Body>) -> Response {
    if req.uri().path() == "/video/:/transcode/universal/decision" {
        let query = req
            .uri()
            .query()
            .and_then(|query| serde_urlencoded::from_str::<BTreeMap<String, String>>(query).ok());
        let Some(query) = query else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        info!(
            protocol = ?query.get("protocol"),
            direct_play = ?query.get("directPlay"),
            direct_stream = ?query.get("directStream"),
            direct_stream_audio = ?query.get("directStreamAudio"),
            media_index = ?query.get("mediaIndex"),
            part_index = ?query.get("partIndex"),
            media_path = ?query.get("path"),
            plex_product = ?query.get("X-Plex-Product"),
            client_profile = ?query.get("X-Plex-Client-Profile-Name"),
            client_platform = ?query.get("X-Plex-Platform"),
            "Plex universal playback decision requested"
        );
        return plex_playback_decision(State(state), Query(query)).await;
    }
    if let Some(extension) = req
        .uri()
        .path()
        .strip_prefix("/video/:/transcode/universal/start.")
    {
        let query = req
            .uri()
            .query()
            .and_then(|query| serde_urlencoded::from_str::<BTreeMap<String, String>>(query).ok())
            .unwrap_or_default();
        info!(
            extension,
            protocol = ?query.get("protocol"),
            direct_play = ?query.get("directPlay"),
            direct_stream = ?query.get("directStream"),
            direct_stream_audio = ?query.get("directStreamAudio"),
            media_path = ?query.get("path"),
            plex_product = ?query.get("X-Plex-Product"),
            client_profile = ?query.get("X-Plex-Client-Profile-Name"),
            client_platform = ?query.get("X-Plex-Platform"),
            "Plex universal stream start requested"
        );
        if extension.eq_ignore_ascii_case("mpd") {
            warn!(
                extension,
                reason = "transcoding_unavailable",
                "Plex DASH playback rejected"
            );
            return (
                StatusCode::NOT_IMPLEMENTED,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                "DASH transcoding is not supported.",
            )
                .into_response();
        }
        if extension.eq_ignore_ascii_case("m3u8") {
            warn!(extension, "HLS playback is not supported");
            return (
                StatusCode::NOT_IMPLEMENTED,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                "HLS playback is not supported.",
            )
                .into_response();
        }
        let Some(media) = plex_playback_media_from_query(&state, &query) else {
            warn!(
                extension,
                reason = "media_not_found",
                "Plex passthrough stream rejected"
            );
            return StatusCode::NOT_FOUND.into_response();
        };
        return plex_protocol_response(
            stream_file(&media, req.method().clone(), req.headers().clone(), false).await,
        );
    }
    if req.uri().path() == "/photo/:/transcode" {
        let query = req
            .uri()
            .query()
            .and_then(|query| serde_urlencoded::from_str::<BTreeMap<String, String>>(query).ok());
        let Some(source) = query.and_then(|query| query.get("url").cloned()) else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        let Some((id, filename)) = plex_photo_source(&source) else {
            let source_path = Uri::try_from(source.as_str())
                .ok()
                .map(|uri| uri.path().to_owned())
                .unwrap_or_else(|| "<invalid-uri>".to_owned());
            warn!(source_path, "unsupported Plex photo source");
            return StatusCode::NOT_FOUND.into_response();
        };
        return plex_artwork(&state, &id, filename).await;
    }
    if matches!(
        req.uri().path(),
        "/:/websockets/notifications" | "/:/websocket/notifications"
    ) {
        let (mut parts, _) = req.into_parts();
        return match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
            Ok(upgrade) => plex_notification_response(upgrade, state.clone()),
            Err(rejection) => rejection.into_response(),
        };
    }
    if req.uri().path() == "/:/timeline" {
        let query = req
            .uri()
            .query()
            .and_then(|query| serde_urlencoded::from_str::<PlexTimelineQuery>(query).ok());
        let Some(query) = query else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        return plex_timeline(&state, query);
    }
    if req.uri().path() == "/:/prefs" {
        return plex_prefs(&state);
    }
    if matches!(req.uri().path(), "/:/scrobble" | "/:/unscrobble") {
        let watched = req.uri().path() == "/:/scrobble";
        let query = req
            .uri()
            .query()
            .and_then(|query| serde_urlencoded::from_str::<PlexScrobbleQuery>(query).ok());
        let Some(query) = query else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        return plex_scrobble(&state, query, watched);
    }
    warn!(method = %req.method(), path = %req.uri().path(), "unsupported route");
    StatusCode::NOT_FOUND.into_response()
}

#[derive(Deserialize)]
struct PlexTimelineQuery {
    #[serde(rename = "ratingKey")]
    rating_key: Option<String>,
    key: Option<String>,
    time: Option<i64>,
    duration: Option<i64>,
}

#[derive(Deserialize)]
struct PlexScrobbleQuery {
    #[serde(rename = "ratingKey")]
    rating_key: Option<String>,
    key: Option<String>,
}

fn plex_query_media_id(rating_key: Option<&str>, key: Option<&str>) -> Option<String> {
    rating_key
        .filter(|key| !key.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            let key = key?;
            key.rsplit("/library/metadata/")
                .next()
                .filter(|id| !id.is_empty() && !id.contains('/'))
                .map(str::to_owned)
        })
}

fn plex_playback_media_from_query(
    state: &AppState,
    query: &BTreeMap<String, String>,
) -> Option<Media> {
    let path = query.get("path").or_else(|| query.get("key"));
    path.and_then(|path| {
        let part_path = path
            .find("/library/parts/")
            .map(|index| &path[index + "/library/parts/".len()..]);
        part_path
            .and_then(|path| path.split('/').next())
            .and_then(|part_id| find_media_part(state, part_id))
            .or_else(|| {
                plex_query_media_id(None, Some(path))
                    .and_then(|media_id| find_media(state, &media_id))
            })
    })
    .or_else(|| {
        query
            .get("ratingKey")
            .or_else(|| query.get("mediaId"))
            .and_then(|media_id| find_media(state, media_id))
    })
}

fn plex_progress_path(state: &AppState, media_id: &str) -> Option<String> {
    let media = find_media(state, media_id)?;
    let (_, library) = media_library(state, &media)?;
    let root = library
        .folders
        .iter()
        .find(|folder| media.path.starts_with(folder))?;
    Some(
        media
            .path
            .strip_prefix(root)
            .ok()?
            .to_string_lossy()
            .into_owned(),
    )
}

fn plex_timeline(state: &AppState, query: PlexTimelineQuery) -> Response {
    let Some(media_id) = plex_query_media_id(query.rating_key.as_deref(), query.key.as_deref())
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(path) = plex_progress_path(state, &media_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let position = query.time.unwrap_or_default().max(0);
    let duration = query.duration.filter(|duration| *duration > 0);
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if connection.execute(
        "INSERT INTO watch_progress (user_id, media_key, position_ms, duration_ms, watched, hidden_from_continue, updated_at) VALUES (?1, ?2, ?3, ?4, 0, 0, ?5) ON CONFLICT(user_id, media_key) DO UPDATE SET position_ms = excluded.position_ms, duration_ms = COALESCE(excluded.duration_ms, watch_progress.duration_ms), watched = 0, hidden_from_continue = 0, updated_at = excluded.updated_at",
        (USER_ID, path, position, duration, updated_at),
    ).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    plex_json(serde_json::json!({"MediaContainer": {"size": 0}}))
}

fn plex_scrobble(state: &AppState, query: PlexScrobbleQuery, watched: bool) -> Response {
    let Some(media_id) = plex_query_media_id(query.rating_key.as_deref(), query.key.as_deref())
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(path) = plex_progress_path(state, &media_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let result = if watched {
        connection.execute(
            "INSERT INTO watch_progress (user_id, media_key, position_ms, duration_ms, watched, hidden_from_continue, updated_at) VALUES (?1, ?2, 0, NULL, 1, 0, ?3) ON CONFLICT(user_id, media_key) DO UPDATE SET position_ms = 0, watched = 1, hidden_from_continue = 0, updated_at = excluded.updated_at",
            (USER_ID, path, updated_at),
        )
    } else {
        connection.execute(
            "INSERT INTO watch_progress (user_id, media_key, position_ms, duration_ms, watched, hidden_from_continue, updated_at) VALUES (?1, ?2, 0, NULL, 0, 0, ?3) ON CONFLICT(user_id, media_key) DO UPDATE SET position_ms = 0, watched = 0, hidden_from_continue = 0, updated_at = excluded.updated_at",
            (USER_ID, path, updated_at),
        )
    };
    if result.is_err() {
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    } else {
        StatusCode::OK.into_response()
    }
}

async fn plex_remove_from_continue_watching(
    State(state): State<AppState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(media_id) = plex_query_media_id(
        query.get("ratingKey").map(String::as_str),
        query.get("key").map(String::as_str),
    ) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(path) = plex_progress_path(&state, &media_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    match connection.execute(
        "INSERT INTO watch_progress (user_id, media_key, position_ms, duration_ms, watched, hidden_from_continue, updated_at) VALUES (?1, ?2, 0, NULL, 0, 1, ?3) ON CONFLICT(user_id, media_key) DO UPDATE SET hidden_from_continue = 1, updated_at = excluded.updated_at",
        (USER_ID, path, updated_at),
    ) {
        Ok(_) => StatusCode::OK.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

fn plex_photo_source(source: &str) -> Option<(String, &'static str)> {
    let uri = Uri::try_from(source).ok()?;
    let segments = uri
        .path()
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let metadata_index = segments
        .windows(2)
        .position(|pair| pair == ["library", "metadata"])?;
    let id = *segments.get(metadata_index + 2)?;
    let filename = match *segments.get(metadata_index + 3)? {
        "thumb" => "cover.jpg",
        "art" => "banner.jpg",
        _ => return None,
    };
    Some((id.to_owned(), filename))
}

async fn plex_cors(request: Request<Body>, next: Next) -> Response {
    if !plex_cors_path(request.uri().path()) {
        return next.run(request).await;
    }

    let origin = request.headers().get(header::ORIGIN).cloned();
    let preflight = request.method() == Method::OPTIONS
        && request
            .headers()
            .contains_key("access-control-request-method");
    if preflight {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::OK;
        let headers = response.headers_mut();
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
        headers.insert("x-plex-protocol", HeaderValue::from_static("1.0"));
        headers.insert("cache-control", HeaderValue::from_static("no-cache"));
        if let Some(origin) = origin {
            headers.insert("access-control-allow-origin", origin);
            headers.insert(
                "access-control-allow-methods",
                HeaderValue::from_static("GET, POST, PUT, DELETE, HEAD, OPTIONS"),
            );
            if let Some(asked_headers) = request.headers().get("access-control-request-headers") {
                headers.insert("access-control-allow-headers", asked_headers.clone());
            } else {
                headers.insert(
                    "access-control-allow-headers",
                    HeaderValue::from_static(
                        "Accept, Accept-Language, Content-Type, Range, X-Plex-Token, X-Plex-Client-Identifier, X-Plex-Product, X-Plex-Version, X-Plex-Platform, X-Plex-Platform-Version, X-Plex-Device, X-Plex-Device-Name, X-Plex-Device-Vendor, X-Plex-Model, X-Plex-Language, X-Plex-Features, X-Plex-Provides, X-Plex-Container-Start, X-Plex-Container-Size, X-Plex-Client-Profile-Extra, X-Plex-Client-Profile-Name, X-Plex-Session-Identifier, X-Plex-Marketplace, X-Plex-Sync-Version",
                    ),
                );
            }
            headers.insert(
                "access-control-max-age",
                HeaderValue::from_static("1209600"),
            );
            headers.insert("vary", HeaderValue::from_static("Origin, X-Plex-Token"));
        }
        return response;
    }

    let mut response = next.run(request).await;
    if let Some(origin) = origin {
        let headers = response.headers_mut();
        headers.insert("access-control-allow-origin", origin);
        headers.insert(
            "access-control-expose-headers",
            HeaderValue::from_static(
                "Content-Length, Content-Range, Content-Type, Date, Location, X-Plex-Protocol, X-Plex-Container-Start, X-Plex-Container-Total-Size, X-Plex-Content-Original-Length, X-Plex-Content-Compressed-Length, X-Plex-Activity",
            ),
        );
        headers.insert("vary", HeaderValue::from_static("Origin, X-Plex-Token"));
    }
    response
}

fn plex_cors_path(path: &str) -> bool {
    // Keep the built-in UI same-origin while allowing Plex Web's file://
    // origin to call every Plex API operation, including queues and transcode.
    path != "/library"
        && path != "/web"
        && !path.starts_with("/web/")
        && !path.starts_with("/settings")
}

async fn root_handler(
    State(s): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    Query(query): Query<WebPath>,
) -> Response {
    let is_plex_client = headers.contains_key("X-Plex-Product")
        || headers.contains_key("X-Plex-Client-Identifier")
        || headers.contains_key("X-Plex-Token");
    let is_plex_query = plex_query_value(&uri, "X-Plex-Product").is_some()
        || plex_query_value(&uri, "X-Plex-Client-Identifier").is_some()
        || plex_query_value(&uri, "X-Plex-Token").is_some();
    if is_plex_client || is_plex_query {
        plex_root(&s).await
    } else if query.library.is_some()
        || query.path.is_some()
        || query.play.is_some()
        || query.episode.is_some()
        || query.view.is_some()
    {
        web_client(State(s), Query(query)).await
    } else {
        home_client(State(s)).await
    }
}

async fn plex_root(state: &AppState) -> Response {
    const ROOT_DIRECTORIES: &[&str] = &[
        "actions",
        "activities",
        "butler",
        "channels",
        "clients",
        "devices",
        "diagnostics",
        "hubs",
        "library",
        "livetv",
        "media",
        "neighborhood",
        "playQueues",
        "playlists",
        "resources",
        "search",
        "server",
        "servers",
        "statistics",
        "system",
        "transcode",
        "updater",
        "user",
    ];
    let root_directories_json = ROOT_DIRECTORIES
        .iter()
        .map(|name| serde_json::json!({"count": 1, "key": name, "title": name}))
        .collect::<Vec<_>>();
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": ROOT_DIRECTORIES.len(),
            "allowCameraUpload": false,
            "allowChannelAccess": false,
            "allowMediaDeletion": false,
            "allowSharing": false,
            "allowSync": false,
            "allowTuners": false,
            "apiVersion": "1.2.3",
            "backgroundProcessing": false,
            "certificate": state.plex_tls.hash().is_some(),
            "companionProxy": false,
            "countryCode": "usa",
            "diagnostics": "",
            "eventStream": false,
            "friendlyName": server_name(state),
            "hubSearch": true,
            "itemClusters": false,
            "livetv": 0,
            "machineIdentifier": state.machine_identifier.as_ref(),
            "mediaProviders": true,
            "multiuser": false,
            "musicAnalysis": 0,
            "myPlex": true,
            "myPlexMappingState": "mapped",
            "myPlexSigninState": "ok",
            "myPlexSubscription": false,
            "myPlexUsername": "",
            "offlineTranscode": 0,
            "ownerFeatures": "",
            "platform": "MacOSX",
            "platformVersion": std::env::consts::ARCH,
            "pluginHost": false,
            "pushNotifications": false,
            "readOnlyLibraries": false,
            "streamingBrainABRVersion": 3,
            "streamingBrainVersion": 2,
            "sync": false,
            "transcoderActiveVideoSessions": 0,
            "transcoderAudio": false,
            "transcoderLyrics": false,
            "transcoderPhoto": false,
            "transcoderSubtitles": false,
            "transcoderVideo": true,
            "updatedAt": updated_at,
            "updater": false,
            "version": "1.43.4.10903-e5521bd8c",
            "voiceSearch": false,
            "Directory": root_directories_json
        }
    }))
}

fn plex_json(body: serde_json::Value) -> Response {
    plex_protocol_response(Json(body).into_response())
}

fn plex_protocol_response(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert("X-Plex-Protocol", HeaderValue::from_static("1.0"));
    response
}

fn plex_request_token<'a>(headers: &'a HeaderMap, uri: &'a Uri) -> Option<&'a str> {
    headers
        .get("X-Plex-Token")
        .and_then(|value| value.to_str().ok())
        .or_else(|| plex_query_value(uri, "X-Plex-Token"))
}

fn plex_query_value<'a>(uri: &'a Uri, name: &str) -> Option<&'a str> {
    uri.query()?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        key.eq_ignore_ascii_case(name).then_some(value)
    })
}

async fn plex_identity(State(state): State<AppState>) -> Response {
    let claimed = state
        .db
        .lock()
        .ok()
        .and_then(|connection| {
            connection
                .query_row(
                    "SELECT claimed FROM server_identity WHERE id = 1",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .ok()
        })
        .unwrap_or(false);
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": 0,
            "apiVersion": "1.2.3",
            "claimed": claimed,
            "friendlyName": server_name(&state),
            "machineIdentifier": state.machine_identifier.as_ref(),
            "version": "1.43.4.10903-e5521bd8c"
        }
    }))
}

async fn plex_sections(State(state): State<AppState>) -> Response {
    let sections = plex_section_directories(&state);
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": sections.0,
            "allowSync": false,
            "title1": "Plex Library",
            "Directory": sections.1
        }
    }))
}

async fn plex_delete_section(
    State(state): State<AppState>,
    Path(section): Path<usize>,
) -> Response {
    let Some(library_index) = section.checked_sub(1) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Some(library) = config.libraries.get(library_index) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let library_name = library.name.clone();
    let mut updated_config = config.clone();
    updated_config.libraries.remove(library_index);

    let Ok(mut connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Ok(transaction) = connection.transaction() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if transaction
        .execute(
            "DELETE FROM library_folders WHERE library_id = (SELECT id FROM library_groups WHERE name = ?1)",
            [&library_name],
        )
        .and_then(|_| {
            transaction.execute("DELETE FROM library_groups WHERE name = ?1", [&library_name])
        })
        .is_err()
        || update_library_catalog_timestamp(&transaction).is_err()
        || save_config(&state.config_path, &updated_config).is_err()
        || transaction.commit().is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    *config = updated_config;
    StatusCode::OK.into_response()
}

async fn plex_create_section_official(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let query = {
        let mut query = form_urlencoded::Serializer::new(String::new());
        for (key, value) in form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes()) {
            match key.as_ref() {
                "type" => {
                    let value = match value.as_ref() {
                        "1" => "movie",
                        "2" => "show",
                        other => other,
                    };
                    query.append_pair("type", value);
                }
                "locations" => {
                    query.append_pair("location", &value);
                }
                _ => {
                    query.append_pair(&key, &value);
                }
            };
        }
        query.finish()
    };
    plex_create_section(State(state), RawQuery(Some(query)), headers).await
}

async fn plex_create_section(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let mut name = None;
    let mut library_type = None;
    let mut agent = None;
    let mut scanner = None;
    let mut language = None;
    let mut provider_group_id = None;
    let mut locations = Vec::new();
    let mut preferences = BTreeMap::new();
    for (key, value) in form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes()) {
        match key.as_ref() {
            "name" => name = Some(value.into_owned()),
            "type" => library_type = Some(value.into_owned()),
            "agent" => agent = Some(value.into_owned()),
            "scanner" => scanner = Some(value.into_owned()),
            "language" => language = Some(value.into_owned()),
            "metadataAgentProviderGroupId" => {
                let Ok(id) = value.parse::<i64>() else {
                    return StatusCode::BAD_REQUEST.into_response();
                };
                provider_group_id = Some(id);
            }
            "location" | "location[]" | "locations" => locations.push(value.into_owned()),
            "prefs" => {
                let Ok(object) = serde_json::from_str::<serde_json::Value>(&value) else {
                    return StatusCode::BAD_REQUEST.into_response();
                };
                let Some(object) = object.as_object() else {
                    return StatusCode::BAD_REQUEST.into_response();
                };
                for (key, value) in object {
                    preferences.insert(
                        key.clone(),
                        value
                            .as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| value.to_string()),
                    );
                }
            }
            key => {
                if let Some(key) = key
                    .strip_prefix("prefs[")
                    .and_then(|key| key.strip_suffix(']'))
                    .filter(|key| !key.is_empty())
                {
                    preferences.insert(key.to_owned(), value.into_owned());
                }
            }
        }
    }

    let Some(name) = name
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let kind = match library_type.as_deref() {
        Some("movie") => "movie",
        Some("show" | "tv") => "tv",
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    let Some(agent) = agent.filter(|agent| !agent.is_empty()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let scanner = scanner
        .filter(|scanner| !scanner.is_empty())
        .unwrap_or_else(|| {
            if kind == "tv" {
                "Plex TV Series".to_owned()
            } else if agent == "tv.plex.agents.none" {
                "Plex Video Files".to_owned()
            } else {
                "Plex Movie".to_owned()
            }
        });
    let agents = if kind == "tv" {
        [
            "tv.plex.agents.series",
            "tv.plex.agents.none",
            "tv.plex.agents.nfo.tv",
        ]
    } else {
        [
            "tv.plex.agents.movie",
            "tv.plex.agents.none",
            "tv.plex.agents.nfo.movie",
        ]
    };
    let scanners = if kind == "tv" {
        ["Plex TV Series", "Plex Series Scanner"]
    } else {
        ["Plex Movie", "Plex Video Files"]
    };
    if !agents.contains(&agent.as_str()) || !scanners.contains(&scanner.as_str()) {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let mut canonical_locations = Vec::with_capacity(locations.len());
    for location in locations {
        let location = location.trim();
        let Ok(metadata) = tokio::fs::metadata(location).await else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        if !metadata.is_dir() {
            return StatusCode::BAD_REQUEST.into_response();
        }
        let Ok(location) = tokio::fs::canonicalize(location).await else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        if !canonical_locations.contains(&location) {
            canonical_locations.push(location);
        }
    }
    if canonical_locations.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let plex_language = Some(
        language
            .filter(|language| !language.trim().is_empty())
            .unwrap_or_else(|| "en-US".to_owned()),
    );
    let metadata_agent_provider_group_id = provider_group_id.or_else(|| {
        Some(if agent == "tv.plex.agents.none" {
            4
        } else if kind == "tv" {
            2
        } else {
            1
        })
    });
    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if let Some(existing_index) = config
        .libraries
        .iter()
        .position(|library| library.name == name)
    {
        let existing = &config.libraries[existing_index];
        let default_agent = if existing.kind == "tv" {
            "tv.plex.agents.series"
        } else {
            "tv.plex.agents.movie"
        };
        let default_scanner = if existing.kind == "tv" {
            "Plex TV Series"
        } else {
            "Plex Movie"
        };
        let default_group_id = if existing.kind == "tv" { 2 } else { 1 };
        let same_request = existing.kind == kind
            && existing.folders == canonical_locations
            && existing.plex_agent.as_deref().unwrap_or(default_agent) == agent
            && existing.plex_scanner.as_deref().unwrap_or(default_scanner) == scanner
            && existing.plex_language.as_deref().unwrap_or("en-US")
                == plex_language.as_deref().unwrap_or("en-US")
            && existing
                .metadata_agent_provider_group_id
                .unwrap_or(default_group_id)
                == metadata_agent_provider_group_id.unwrap_or(default_group_id)
            && existing
                .plex_preferences
                .as_ref()
                .map_or(preferences.is_empty(), |stored| stored == &preferences);
        if !same_request {
            return StatusCode::BAD_REQUEST.into_response();
        }
        drop(config);
        let directories = plex_section_directories(&state).1;
        let Some(directory) = directories.get(existing_index) else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        return plex_create_section_response(directory, &headers);
    }
    let mut updated_config = config.clone();
    updated_config.libraries.push(ConfigLibrary {
        name,
        kind: kind.to_owned(),
        folders: canonical_locations,
        show_recently_added: true,
        show_continue_watching: true,
        scan_enabled: true,
        scan_interval_value: default_scan_interval_value(),
        scan_interval_unit: default_scan_interval_unit(),
        plex_agent: Some(agent),
        plex_scanner: Some(scanner),
        plex_language,
        metadata_agent_provider_group_id,
        plex_preferences: (!preferences.is_empty()).then_some(preferences),
    });

    let Ok(mut connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Ok(transaction) = connection.transaction() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if sync_config_libraries(&transaction, &updated_config).is_err()
        || save_config(&state.config_path, &updated_config).is_err()
        || transaction.commit().is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    *config = updated_config;
    drop(connection);
    drop(config);

    let directories = plex_section_directories(&state).1;
    let Some(directory) = directories.last() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    plex_create_section_response(directory, &headers)
}

fn plex_create_section_response(directory: &serde_json::Value, headers: &HeaderMap) -> Response {
    let accepts_xml = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.to_ascii_lowercase().contains("application/xml"));
    if !accepts_xml {
        return plex_json(serde_json::json!({
            "MediaContainer": {
                "size": 1,
                "Directory": [directory]
            }
        }));
    }

    let body = plex_create_section_xml(directory);
    let mut response = body.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    plex_protocol_response(response)
}

fn plex_create_section_xml(directory: &serde_json::Value) -> String {
    let attributes = [
        "allowSync",
        "filters",
        "refreshing",
        "key",
        "type",
        "title",
        "agent",
        "scanner",
        "language",
        "metadataAgentProviderGroupId",
        "uuid",
        "updatedAt",
        "createdAt",
        "scannedAt",
        "content",
        "directory",
        "contentChangedAt",
        "hidden",
    ]
    .iter()
    .filter_map(|name| {
        directory.get(*name).map(|value| {
            let value = match value {
                serde_json::Value::String(value) => value.clone(),
                serde_json::Value::Bool(value) => if *value { "1" } else { "0" }.to_owned(),
                _ => value.to_string(),
            };
            format!(" {}=\"{}\"", name, xml_escape(&value))
        })
    })
    .collect::<String>();
    let locations = directory
        .get("Location")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .map(|location| {
            let attributes = ["id", "path"]
                .iter()
                .filter_map(|name| {
                    location.get(*name).map(|value| {
                        let value = match value {
                            serde_json::Value::String(value) => value.clone(),
                            _ => value.to_string(),
                        };
                        format!(" {}=\"{}\"", name, xml_escape(&value))
                    })
                })
                .collect::<String>();
            format!("<Location{attributes}/>")
        })
        .collect::<String>();

    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><MediaContainer size=\"1\"><Directory{attributes}>{locations}</Directory></MediaContainer>"
    )
}

async fn plex_system_scanners(
    State(state): State<AppState>,
    Path(section): Path<usize>,
) -> Response {
    let Some(library) = section
        .checked_sub(1)
        .and_then(|index| state.config.lock().ok()?.libraries.get(index).cloned())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let scanners = if library.kind == "tv" {
        ["Plex TV Series", "Plex Series Scanner"]
    } else {
        ["Plex Movie", "Plex Video Files"]
    };
    let scanner_rows = scanners
        .iter()
        .map(|name| format!("<Scanner name=\"{name}\" />"))
        .collect::<String>();
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><MediaContainer>{scanner_rows}</MediaContainer>"
    );
    let mut response = body.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    plex_protocol_response(response)
}

async fn plex_system_agents(RawQuery(raw_query): RawQuery) -> Response {
    let media_type = raw_query.as_deref().and_then(|query| {
        form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == "mediaType")
            .map(|(_, value)| value.into_owned())
    });
    if !matches!(media_type.as_deref(), Some("1" | "2")) {
        return StatusCode::BAD_REQUEST.into_response();
    }

    const LANGUAGES: &[(&str, &str)] = &[
        ("ar-SA", "Arabic (Saudi Arabia)"),
        ("bg-BG", "Bulgarian"),
        ("ca-ES", "Catalan"),
        ("zh-CN", "Chinese"),
        ("zh-HK", "Chinese (Hong Kong)"),
        ("zh-TW", "Chinese (Taiwan)"),
        ("hr-HR", "Croatian"),
        ("cs-CZ", "Czech"),
        ("da-DK", "Danish"),
        ("nl-NL", "Dutch"),
        ("en-US", "English"),
        ("en-AU", "English (Australia)"),
        ("en-CA", "English (Canada)"),
        ("en-GB", "English (UK)"),
        ("et-EE", "Estonian"),
        ("fi-FI", "Finnish"),
        ("fr-FR", "French"),
        ("fr-CA", "French (Canada)"),
        ("de-DE", "German"),
        ("el-GR", "Greek"),
        ("he-IL", "Hebrew"),
        ("hi-IN", "Hindi"),
        ("hu-HU", "Hungarian"),
        ("is-IS", "Icelandic"),
        ("id-ID", "Indonesian"),
        ("it-IT", "Italian"),
        ("ja-JP", "Japanese"),
        ("ko-KR", "Korean"),
        ("lv-LV", "Latvian"),
        ("lt-LT", "Lithuanian"),
        ("nb-NO", "Norwegian Bokm&#229;l"),
        ("fa-IR", "Persian"),
        ("pl-PL", "Polish"),
        ("pt-BR", "Portuguese"),
        ("pt-PT", "Portuguese (Portugal)"),
        ("ro-RO", "Romanian"),
        ("ru-RU", "Russian"),
        ("sk-SK", "Slovak"),
        ("es-ES", "Spanish"),
        ("es-MX", "Spanish (Mexico)"),
        ("sv-SE", "Swedish"),
        ("th-TH", "Thai"),
        ("tr-TR", "Turkish"),
        ("uk-UA", "Ukrainian"),
        ("vi-VN", "Vietnamese"),
    ];
    const AGENTS: &[(&str, &str, &str)] = &[
        ("tv.plex.agents.movie", "Plex Movie", "1"),
        ("tv.plex.agents.none", "Plex Personal Media", "4"),
        ("tv.plex.agents.nfo.movie", "Plex NFO Movie", "5"),
    ];

    let mut body =
        String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><MediaContainer size=\"3\">");
    for (identifier, name, id) in AGENTS {
        body.push_str(&format!(
            "<Agent identifier=\"{identifier}\" name=\"{name}\" hasPrefs=\"0\" id=\"{id}\">"
        ));
        for (code, title) in LANGUAGES {
            if *code == "en-US" {
                body.push_str(&format!(
                    "<Language code=\"{code}\" title=\"{title}\" default=\"1\" />"
                ));
            } else {
                body.push_str(&format!("<Language code=\"{code}\" title=\"{title}\" />"));
            }
        }
        body.push_str("</Agent>");
    }
    body.push_str("</MediaContainer>");

    let mut response = body.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    plex_protocol_response(response)
}

async fn plex_myplex_account(State(state): State<AppState>) -> Response {
    let profile = plex_cloud_profile(&state).await;
    let mut account = serde_json::json!({
        "mappingState": "mapped",
        "signInState": "ok",
        "subscriptionState": "Unknown"
    });
    if let Some((name, thumb)) = profile {
        account["username"] = serde_json::json!(name);
        if let Some(thumb) = thumb {
            account["thumb"] = serde_json::json!(thumb);
        }
    }
    plex_json(serde_json::json!({"MyPlexAccount": account}))
}

async fn plex_accounts(State(state): State<AppState>) -> Response {
    plex_account_value(plex_cloud_profile(&state).await)
}

async fn plex_account(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if id != "1" {
        return StatusCode::NOT_FOUND.into_response();
    }
    plex_account_value(plex_cloud_profile(&state).await)
}

async fn plex_cloud_profile(state: &AppState) -> Option<(String, Option<String>)> {
    let token = state.db.lock().ok().and_then(|connection| {
        connection
            .query_row(
                "SELECT plex_account_token FROM server_identity WHERE id = 1",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .ok()
            .flatten()
    })?;
    let response = reqwest::Client::new()
        .get("https://plex.tv/api/v2/user")
        .header("Accept", "application/json")
        .header("X-Plex-Product", "Plex Media Server")
        .header(
            "X-Plex-Client-Identifier",
            state.machine_identifier.as_ref(),
        )
        .header("X-Plex-Token", token)
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .json::<serde_json::Value>()
        .await
        .ok()?;
    let name = ["username", "title", "friendlyName"]
        .iter()
        .find_map(|key| {
            response
                .get(*key)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
        })
        .map(str::to_owned);
    let thumb = response
        .get("thumb")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    name.or_else(|| thumb.as_ref().map(|_| "harbringer".to_owned()))
        .map(|name| (name, thumb))
}

fn plex_account_value(profile: Option<(String, Option<String>)>) -> Response {
    let mut account = serde_json::json!({
        "id": 1,
        "key": "/accounts/1",
        "name": "harbringer",
            "defaultAudioLanguage": PLEX_DEFAULT_AUDIO_LANGUAGE,
        "autoSelectAudio": true,
        "defaultSubtitleLanguage": "en",
        "subtitleMode": 2
    });
    if let Some((name, thumb)) = profile {
        account["name"] = serde_json::json!(name);
        if let Some(thumb) = thumb {
            account["thumb"] = serde_json::json!(thumb);
        }
    }
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": 1,
            "identifier": "com.plexapp.system.accounts",
            "Account": [account]
        }
    }))
}

async fn plex_activities() -> Response {
    plex_json(serde_json::json!({"MediaContainer": {"size": 0}}))
}

async fn plex_sessions() -> Response {
    plex_json(serde_json::json!({"MediaContainer": {"size": 0}}))
}

async fn plex_devices() -> Response {
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": 0,
            "identifier": "com.plexapp.system.devices",
            "Device": []
        }
    }))
}

async fn plex_session_history() -> Response {
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": 0,
            "totalSize": 0,
            "offset": 0,
            "Metadata": []
        }
    }))
}

fn plex_prefs(state: &AppState) -> Response {
    let settings = serde_json::json!([
        {"id":"FriendlyName","label":"Friendly name","summary":"Other computers on your network see this server under this name.","type":"text","default":"harbringer","value":server_name(state),"hidden":false,"advanced":false,"group":"general"},
        {"id":"OnDeckLimit","label":"On Deck limit","summary":"The maximum number of items shown in Continue Watching.","type":"int","default":40,"value":40,"hidden":false,"advanced":false,"group":"library"},
        {"id":"ScannerLowPriority","label":"Run scans at a lower priority","summary":"","type":"bool","default":false,"value":false,"hidden":false,"advanced":false,"group":"library"},
        {"id":"AllowLegacyAgents","label":"Allow legacy agents","summary":"","type":"bool","default":true,"value":true,"hidden":true,"advanced":false,"group":"library"},
        {"id":"CLDriver","label":"","summary":"","type":"text","default":"","value":"","hidden":true,"advanced":false,"group":"transcoder"},
        {"id":"LanNetworksBandwidth","label":"LAN Networks","summary":"","type":"text","default":"","value":"","hidden":false,"advanced":true,"group":"network"},
        {"id":"secureConnections","label":"Secure connections","summary":"","type":"int","default":1,"value":1,"hidden":false,"advanced":true,"group":"network","enumValues":"0:Disabled|1:Preferred|2:Required"},
        {"id":"PublishServerOnPlexOnline","label":"Show your server on plex.tv","summary":"","type":"bool","default":true,"value":true,"hidden":false,"advanced":false,"group":"network"}
    ]);
    plex_json(serde_json::json!({
        "MediaContainer": {"size": settings.as_array().map_or(0, Vec::len), "Setting": settings}
    }))
}

async fn plex_servers(State(state): State<AppState>) -> Response {
    let bind = state
        .config
        .lock()
        .ok()
        .and_then(|config| config.bind.parse::<SocketAddr>().ok())
        .map(|address| address.port())
        .unwrap_or(32400);
    let host = plex_connection_urls(&state)
        .first()
        .and_then(|url| url.strip_prefix("http://"))
        .and_then(|url| url.rsplit_once(':'))
        .map(|(host, _)| host.to_owned())
        .unwrap_or_else(|| "127.0.0.1".to_owned());
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": 1,
            "Server": [{
                "name": server_name(&state),
                "address": host,
                "port": bind,
                "machineIdentifier": state.machine_identifier.as_ref(),
                "version": "1.43.4.10903-e5521bd8c"
            }]
        }
    }))
}

async fn plex_media_providers(State(state): State<AppState>) -> Response {
    let updated_at = library_catalog_updated_at(&state);
    let mut directories = state
        .config
        .lock()
        .ok()
        .map(|config| {
            config
                .libraries
                .iter()
                .enumerate()
                .map(|(index, library)| {
                    let section = index + 1;
                    let (_, scanned_at, library_updated_at) =
                        library_publication_state(&state, &library.name, section);
                    let pivots = serde_json::json!([
                        {"id": "recommended", "key": format!("/hubs/sections/{section}"), "type": "hub", "title": "Recommended", "context": "content.discover", "symbol": "star"},
                        {"id": "library", "key": format!("/library/sections/{section}/all?type={}", if library.kind == "tv" { 2 } else { 1 }), "type": "list", "title": "Library", "context": "content.library", "symbol": "library"},
                        {"id": "collections", "key": format!("/library/sections/{section}/collections"), "type": "list", "title": "Collections", "context": "content.collections", "symbol": "stack"}
                    ]);
                    serde_json::json!({
                        "agent": if library.kind == "tv" { "tv.plex.agents.series" } else { "tv.plex.agents.movie" },
                        "scanner": if library.kind == "tv" { "Plex TV Series" } else { "Plex Movie" },
                        "language": "en-US",
                        "uuid": plex_library_uuid(&state, &library.name, section),
                        "id": section.to_string(),
                        "key": format!("/library/sections/{section}"),
                        "hubKey": format!("/hubs/sections/{section}"),
                        "type": plex_kind(&library.kind),
                        "title": library.name,
                        "refreshing": false,
                        "scannedAt": scanned_at,
                        "updatedAt": library_updated_at,
                        "Pivot": pivots
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    directories.insert(0, serde_json::json!({"title": "Home", "hubKey": "/hubs"}));
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": 1,
            "allowCameraUpload": false,
            "allowChannelAccess": false,
            "allowMediaDeletion": false,
            "allowSharing": false,
            "allowSync": false,
            "allowTuners": false,
            "apiVersion": "1.2.3",
            "backgroundProcessing": false,
            "certificate": state.plex_tls.hash().is_some(),
            "companionProxy": false,
            "countryCode": "usa",
            "diagnostics": "",
            "eventStream": false,
            "friendlyName": server_name(&state),
            "hubSearch": true,
            "itemClusters": false,
            "livetv": 0,
            "machineIdentifier": state.machine_identifier.as_ref(),
            "mediaProviders": true,
            "multiuser": false,
            "musicAnalysis": 0,
            "myPlex": true,
            "myPlexMappingState": "mapped",
            "myPlexSigninState": "ok",
            "myPlexSubscription": false,
            "myPlexUsername": "",
            "offlineTranscode": 0,
            "ownerFeatures": "",
            "platform": "MacOSX",
            "platformVersion": std::env::consts::ARCH,
            "pluginHost": false,
            "pushNotifications": false,
            "readOnlyLibraries": false,
            "streamingBrainABRVersion": 3,
            "streamingBrainVersion": 2,
            "sync": false,
            "transcoderActiveVideoSessions": 0,
            "transcoderAudio": false,
            "transcoderLyrics": false,
            "transcoderSubtitles": false,
            "transcoderVideo": true,
            "updatedAt": updated_at,
            "updater": false,
            "version": "1.43.4.10903-e5521bd8c",
            "voiceSearch": false,
            "MediaProvider": [{
                "identifier": "com.plexapp.plugins.library",
                "title": "Library",
                "types": "video",
                "protocols": "stream,download",
                "Feature": plex_library_provider_features(directories)
            }]
        }
    }))
}

fn plex_library_provider_features(directories: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "key": "/library/sections",
            "type": "content",
            "Directory": directories
        }),
        serde_json::json!({"key": "/hubs/search", "type": "search"}),
        serde_json::json!({"key": "/library/metadata", "type": "metadata"}),
        serde_json::json!({"key": "/hubs/promoted", "type": "promoted"}),
        serde_json::json!({
            "key": "/library/collections",
            "type": "collection"
        }),
        serde_json::json!({
            "key": "/hubs/continueWatching",
            "type": "continuewatching"
        }),
        serde_json::json!({
            "key": "/actions",
            "type": "actions",
            "Action": [{
                "id": "removeFromContinueWatching",
                "key": "/actions/removeFromContinueWatching"
            }]
        }),
        serde_json::json!({
            "key": "/library/search",
            "type": "universalsearch"
        }),
        serde_json::json!({
            "key": "/playlists",
            "type": "playlist",
            "flavor": "universal"
        }),
        serde_json::json!({
            "key": "/playQueues",
            "type": "playqueue",
            "flavor": "universal"
        }),
        serde_json::json!({
            "key": "/:/timeline",
            "type": "timeline",
            "scrobbleKey": "/:/scrobble",
            "unscrobbleKey": "/:/unscrobble"
        }),
        serde_json::json!({"type": "manage"}),
    ]
}

async fn plex_security_resources() -> Response {
    plex_json(serde_json::json!({"MediaContainer": {"size": 0}}))
}

async fn plex_global_hubs(
    State(state): State<AppState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    plex_hubs_response(&state, None, query.get("count"), None).await
}

async fn plex_continue_watching_hub(
    State(state): State<AppState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let only_section = query
        .get("section")
        .and_then(|section| section.parse::<usize>().ok());
    if query.contains_key("section") && only_section.is_none() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let count = query
        .get("count")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10)
        .clamp(1, 50);
    let progress = {
        let Ok(connection) = state.db.lock() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        let mut statement = match connection.prepare(
            "SELECT media_key, position_ms, duration_ms FROM watch_progress WHERE user_id = ?1 AND watched = 0 AND hidden_from_continue = 0 AND position_ms > 0 ORDER BY updated_at DESC, media_key",
        ) {
            Ok(statement) => statement,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let rows = match statement.query_map([USER_ID], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        }) {
            Ok(rows) => rows,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        match rows.collect::<rusqlite::Result<Vec<_>>>() {
            Ok(progress) => progress,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    };
    let libraries = match state.config.lock() {
        Ok(config) => config.libraries.clone(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let media_index = media_snapshot(&state);
    if only_section.is_some_and(|section| section == 0 || section > libraries.len()) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let progress_state = {
        let Ok(connection) = state.db.lock() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        let mut statement = match connection.prepare(
            "SELECT media_key, watched, hidden_from_continue, updated_at FROM watch_progress WHERE user_id = ?1",
        ) {
            Ok(statement) => statement,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let rows = match statement.query_map([USER_ID], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, i64>(3)?,
            ))
        }) {
            Ok(rows) => rows,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        match rows.collect::<rusqlite::Result<Vec<_>>>() {
            Ok(progress) => progress,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    };
    let mut watched_paths = std::collections::HashSet::new();
    let mut hidden_paths = std::collections::HashSet::new();
    let mut last_activity = std::collections::HashMap::new();
    for (path, watched, hidden, updated_at) in progress_state {
        if watched {
            watched_paths.insert(path.clone());
        }
        if hidden {
            hidden_paths.insert(path);
            continue;
        }
        last_activity.insert(path, updated_at);
    }

    let mut tv_episode_paths = std::collections::HashSet::new();
    for library in libraries.iter().filter(|library| library.kind == "tv") {
        for root in &library.folders {
            for media in media_index.iter() {
                let Ok(relative) = media.path.strip_prefix(root) else {
                    continue;
                };
                if tv_episode_info(root, &media.path).is_some() {
                    tv_episode_paths.insert(relative.to_string_lossy().into_owned());
                }
            }
        }
    }
    let mut candidates = next_tv_episode_paths(
        &libraries,
        media_index.as_ref(),
        &watched_paths,
        &hidden_paths,
        &last_activity,
    );
    candidates.extend(
        progress
            .iter()
            .filter(|(path, _, _)| !tv_episode_paths.contains(path))
            .map(|(path, _, _)| {
                (
                    path.clone(),
                    last_activity.get(path).copied().unwrap_or_default(),
                )
            }),
    );
    candidates.sort_by(|(left_path, left_time), (right_path, right_time)| {
        right_time
            .cmp(left_time)
            .then_with(|| left_path.cmp(right_path))
    });
    let progress_by_path = progress
        .iter()
        .map(|(path, position, duration)| (path.clone(), (*position, *duration)))
        .collect::<std::collections::HashMap<_, _>>();
    let mut seen = std::collections::HashSet::new();
    let mut items = Vec::new();
    for (media_key, activity) in candidates {
        if !seen.insert(media_key.clone()) {
            continue;
        }
        let found = libraries.iter().enumerate().find_map(|(index, library)| {
            if only_section.is_some_and(|section| section != index + 1) {
                return None;
            }
            media_index.iter().find_map(|media| {
                if !media_in_library(media, library) {
                    return None;
                }
                let root = library
                    .folders
                    .iter()
                    .find(|folder| media.path.starts_with(folder))?;
                let relative = media.path.strip_prefix(root).ok()?;
                (relative.to_string_lossy() == media_key).then_some((media.id.clone(), index + 1))
            })
        });
        if let Some((media_id, section)) = found {
            let (position, duration) = progress_by_path
                .get(&media_key)
                .copied()
                .unwrap_or((0, None));
            items.push((media_id, position, duration, section, activity));
        }
    }
    let total_size = items.len();
    let mut metadata = Vec::new();
    for (media_id, position, duration, _, _) in items.into_iter().take(count) {
        if let Some(mut item) = plex_metadata_item_json(&state, &media_id).await {
            item["viewOffset"] = serde_json::json!(position);
            if let Some(duration) = duration {
                item["duration"] = serde_json::json!(duration);
            }
            metadata.push(item);
        }
    }
    let hub_key = only_section.map_or_else(
        || "/hubs/continueWatching".to_owned(),
        |section| format!("/hubs/sections/{section}/continueWatching"),
    );
    let hub = serde_json::json!({
        "key": hub_key,
        "hubKey": hub_key,
        "title": "Continue Watching",
        "type": "mixed",
        "hubIdentifier": "home.continue",
        "context": "hub.home.continue",
        "size": metadata.len(),
        "more": total_size > count,
        "style": "shelf",
        "promoted": true,
        "Metadata": metadata
    });
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": 1,
            "identifier": "com.plexapp.plugins.library",
            "allowSync": false,
            "Hub": [hub]
        }
    }))
}

fn plex_stale_media_notification(state: &AppState) -> Option<String> {
    let active_media_ids = media_snapshot(state)
        .iter()
        .map(|media| media.id.clone())
        .collect::<HashSet<_>>();
    let connection = state.db.lock().ok()?;
    let mut statement = connection
        .prepare("SELECT library_section, media_key, canonical_path FROM media_identity")
        .ok()?;
    let entries = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|(section, media_key, path)| {
            let section = u64::try_from(section).ok()?;
            if section == 0 || active_media_ids.contains(&media_key) {
                return None;
            }
            let item_id = media_key
                .split_once('-')
                .filter(|(media_section, _)| media_section.parse::<u64>().ok() == Some(section))
                .and_then(|(_, item_id)| item_id.parse::<u64>().ok())?;
            let rating_key = (section << 20) | item_id;
            let title = path.rsplit(['/', '\\']).next().unwrap_or(&path);
            Some(serde_json::json!({
                "identifier": "com.plexapp.plugins.library",
                "sectionID": section.to_string(),
                "itemID": rating_key.to_string(),
                "type": 1,
                "title": title,
                "state": 9,
                "updatedAt": SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            }))
        })
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return None;
    }
    Some(
        serde_json::json!({
            "NotificationContainer": {
                "type": "timeline",
                "size": entries.len(),
                "TimelineEntry": entries
            }
        })
        .to_string(),
    )
}

fn plex_notification_response(upgrade: WebSocketUpgrade, state: AppState) -> Response {
    upgrade
        .on_upgrade(move |mut socket| async move {
            let mut notifications = state.plex_notifications.subscribe();
            if let Some(notification) = plex_stale_media_notification(&state) {
                if socket
                    .send(Message::Text(notification.into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            let mut heartbeat = tokio::time::interval_at(
                tokio::time::Instant::now() + Duration::from_secs(10),
                Duration::from_secs(10),
            );
            loop {
                tokio::select! {
                    message = socket.recv() => match message {
                        Some(Ok(Message::Ping(payload))) => {
                            if socket.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            info!(?frame, "Plex notification socket closed by client");
                            break;
                        }
                        Some(Ok(Message::Text(_) | Message::Binary(_) | Message::Pong(_))) => {}
                        Some(Err(error)) => {
                            info!(%error, "Plex notification socket read failed");
                            break;
                        }
                        None => {
                            info!("Plex notification socket ended by client");
                            break;
                        }
                    },
                    notification = notifications.recv() => match notification {
                        Ok(notification) => {
                            if socket.send(Message::Text(notification.into())).await.is_err() {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    },
                    _ = heartbeat.tick() => {
                        if socket.send(Message::Ping(vec![b'?'].into())).await.is_err() {
                            break;
                        }
                    }
                }
            }
        })
        .into_response()
}

async fn plex_section_hubs(
    State(state): State<AppState>,
    Path(section): Path<usize>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    plex_hubs_response(&state, Some(section), query.get("count"), None).await
}

async fn plex_section_hub(
    State(state): State<AppState>,
    Path((section, hub)): Path<(usize, String)>,
    Query(mut query): Query<BTreeMap<String, String>>,
) -> Response {
    if hub == "continueWatching" {
        query.insert("section".to_owned(), section.to_string());
        return plex_continue_watching_hub(State(state), Query(query)).await;
    }
    if !matches!(hub.as_str(), "recommended" | "recentlyAdded") {
        return StatusCode::NOT_FOUND.into_response();
    }
    plex_hubs_response(&state, Some(section), query.get("count"), Some(&hub)).await
}

async fn plex_hubs_response(
    state: &AppState,
    only_section: Option<usize>,
    count: Option<&String>,
    only_hub: Option<&str>,
) -> Response {
    let count = count
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10)
        .clamp(1, 50);
    let libraries = state
        .config
        .lock()
        .map(|config| config.libraries.clone())
        .unwrap_or_default();
    if only_section.is_some_and(|section| section == 0 || section > libraries.len()) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let media_index = media_snapshot(state);
    if only_hub == Some("recommended") && only_section.is_some() {
        return StatusCode::NOT_FOUND.into_response();
    }

    let mut hubs = Vec::new();
    for (index, library) in libraries.iter().enumerate() {
        let section = index + 1;
        if only_section.is_some_and(|requested| requested != section) {
            continue;
        }
        let mut media = media_index
            .iter()
            .filter(|media| media_in_library(media, library))
            .collect::<Vec<_>>();
        if media.is_empty() {
            continue;
        }
        let metadata = plex_section_metadata(state, section);
        let mut recently_added = media.clone();
        recently_added.sort_by_key(|item| std::cmp::Reverse(media_modified_at(item)));

        let rated_count = media
            .iter()
            .filter(|item| metadata.get(&item.id).and_then(plex_rating).is_some())
            .count();
        if rated_count > 0 {
            media.sort_by(|left, right| {
                let left_rating = metadata
                    .get(&left.id)
                    .and_then(plex_rating)
                    .unwrap_or_default();
                let right_rating = metadata
                    .get(&right.id)
                    .and_then(plex_rating)
                    .unwrap_or_default();
                right_rating
                    .partial_cmp(&left_rating)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| media_modified_at(right).cmp(&media_modified_at(left)))
            });
        } else {
            media.clone_from(&recently_added);
        }

        let recommendation = if rated_count > 0 {
            &media
        } else {
            &recently_added
        };
        let categories = plex_hub_categories(rated_count > 0, only_hub);
        for category in categories {
            if category == "recommended" {
                hubs.push(
                    plex_recommendation_hub(
                        state,
                        section,
                        library,
                        recommendation,
                        &metadata,
                        count,
                        rated_count > 0,
                        true,
                    )
                    .await,
                );
            } else {
                hubs.push(
                    plex_recommendation_hub(
                        state,
                        section,
                        library,
                        &recently_added,
                        &metadata,
                        count,
                        false,
                        false,
                    )
                    .await,
                );
            }
        }
    }

    if let Some(section) = only_section.filter(|_| only_hub.is_none()) {
        let mut query = BTreeMap::new();
        query.insert("section".to_owned(), section.to_string());
        let response = plex_continue_watching_hub(State(state.clone()), Query(query)).await;
        if !response.status().is_success() {
            return response;
        }
        let body = match axum::body::to_bytes(response.into_body(), usize::MAX).await {
            Ok(body) => body,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let continue_hub = match serde_json::from_slice::<serde_json::Value>(&body) {
            Ok(body) => body["MediaContainer"]["Hub"][0].clone(),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        hubs.insert(0, continue_hub);
    }
    let mut container = serde_json::json!({"size": hubs.len(), "Hub": hubs});
    container["allowSync"] = serde_json::json!(false);
    container["identifier"] = serde_json::json!("com.plexapp.plugins.library");
    if let Some(section) = only_section {
        let library = &libraries[section - 1];
        container["librarySectionID"] = serde_json::json!(section);
        container["librarySectionTitle"] = serde_json::json!(library.name);
        container["librarySectionUUID"] =
            serde_json::json!(plex_library_uuid(&state, &library.name, section));
    }
    plex_json(serde_json::json!({"MediaContainer": container}))
}

fn plex_hub_categories(_has_rated_items: bool, requested: Option<&str>) -> Vec<&'static str> {
    match requested {
        Some("recentlyAdded") | None => vec!["recentlyAdded"],
        Some(_) => Vec::new(),
    }
}

async fn plex_recommendation_hub(
    state: &AppState,
    section: usize,
    library: &ConfigLibrary,
    media: &[&Media],
    metadata: &HashMap<String, serde_json::Value>,
    count: usize,
    rank_by_rating: bool,
    recommended: bool,
) -> serde_json::Value {
    let kind = plex_kind(&library.kind);
    let (title, identifier, category, sort_key) = match (recommended, rank_by_rating) {
        (true, true) => (
            format!("Recommended in {}", library.name),
            format!("{}.recommended.{section}", kind),
            "recommended",
            "audienceRating:desc",
        ),
        (true, false) => (
            format!("Recommended in {}", library.name),
            format!("{}.recommended.{section}", kind),
            "recommended",
            "addedAt:desc",
        ),
        (false, _) => (
            format!("Recently Added in {}", library.name),
            format!("{}.recentlyadded.{section}", kind),
            "recentlyAdded",
            "addedAt:desc",
        ),
    };
    let media_index = media_snapshot(state);
    let tv_shows = (library.kind == "tv").then(|| {
        plex_tv_show_groups(
            section,
            &library.folders,
            media_index
                .iter()
                .filter(|media| media_in_library(media, library)),
        )
    });
    if let Some(tv_shows) = tv_shows.as_ref() {
        let ranked_shows = plex_ranked_tv_shows(media, tv_shows);
        let items = ranked_shows
            .iter()
            .take(count)
            .map(|show| plex_tv_show_json(show, section, library, metadata))
            .collect::<Vec<_>>();
        return serde_json::json!({
            "hubKey": format!("/library/sections/{section}/all?sort={sort_key}"),
            "key": format!("/hubs/sections/{section}/{category}"),
            "title": title,
            "type": kind,
            "hubIdentifier": identifier,
            "context": format!("hub.{kind}.{category}"),
            "size": ranked_shows.len(),
            "more": ranked_shows.len() > count,
            "style": "shelf",
            "promoted": true,
            "Metadata": items
        });
    }
    let mut items = Vec::new();
    for item in media.iter().take(count) {
        let mut video = plex_video_json(item, section, library, metadata.get(&item.id));
        add_plex_technical_metadata(state, &mut video, item).await;
        items.push(video);
    }
    serde_json::json!({
        "hubKey": format!("/library/sections/{section}/all?sort={sort_key}"),
        "key": format!("/hubs/sections/{section}/{category}"),
        "title": title,
        "type": kind,
        "hubIdentifier": identifier,
        "context": format!("hub.{kind}.{category}"),
        "size": media.len(),
        "more": media.len() > count,
        "style": "shelf",
        "promoted": true,
        "Metadata": items
    })
}

fn media_modified_at(media: &Media) -> u64 {
    std::fs::metadata(&media.path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn plex_rating(metadata: &serde_json::Value) -> Option<f64> {
    metadata
        .get("vote_average")
        .and_then(serde_json::Value::as_f64)
}

fn plex_section_metadata(state: &AppState, section: usize) -> HashMap<String, serde_json::Value> {
    let mut metadata = HashMap::new();
    let Ok(config) = state.config.lock().map(|config| config.clone()) else {
        return metadata;
    };
    let Some(library) = config.libraries.get(section.saturating_sub(1)).cloned() else {
        return metadata;
    };
    let media_index = media_snapshot(state);
    let sidecar_mode = config.metadata_storage == "sidecar";
    if !sidecar_mode {
        if let Ok(connection) = state.db.lock() {
            if let Ok(mut statement) = connection.prepare(
            "SELECT media_key, metadata_json FROM media WHERE media_key LIKE ?1 AND metadata_json IS NOT NULL",
        ) {
            if let Ok(rows) = statement.query_map([format!("{section}-%")], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            }) {
                for row in rows.flatten() {
                    let (key, value) = row;
                    if let (Some(id), Ok(item)) = (
                        key.strip_prefix(&format!("{section}-")),
                        serde_json::from_str(&value),
                    ) {
                        metadata.insert(id.to_owned(), item);
                    }
                }
            }
        }
        }
    }

    for media in media_index
        .iter()
        .filter(|media| media_in_library(media, &library))
    {
        if !sidecar_mode {
            continue;
        }
        if metadata.contains_key(&media.id) {
            continue;
        }
        let Some(root) = library
            .folders
            .iter()
            .find(|folder| media.path.starts_with(folder))
        else {
            continue;
        };
        let group_path = metadata_group_path(root, &media.path, &library.kind);
        let Some(mut item) = read_sidecar_metadata(&group_path) else {
            continue;
        };
        if library.kind == "tv" {
            if let Some(episode) = tv_episode_info(root, &media.path) {
                if let Some(object) = item.as_object_mut() {
                    object.insert("mplex_season".to_owned(), serde_json::json!(episode.season));
                    object.insert(
                        "mplex_episode".to_owned(),
                        serde_json::json!(episode.episode),
                    );
                    if let Some(end) = episode.episode_end {
                        object.insert("mplex_episode_end".to_owned(), serde_json::json!(end));
                    }
                    if let Some(title) = episode.episode_title {
                        object.insert("mplex_episode_title".to_owned(), serde_json::json!(title));
                    }
                }
            }
        }
        metadata.insert(media.id.clone(), item);
    }
    if let Ok(connection) = state.db.lock() {
        if let Ok(mut statement) = connection.prepare(
            "SELECT media_key, position_ms, duration_ms, watched, updated_at FROM watch_progress WHERE user_id = ?1 AND (hidden_from_continue = 0 OR watched = 1)",
        ) {
            if let Ok(rows) = statement.query_map([USER_ID], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            }) {
                let progress = rows
                    .filter_map(Result::ok)
                    .map(|(path, position, duration, watched, updated_at)| {
                        (path, (position, duration, watched, updated_at))
                    })
                    .collect::<HashMap<_, _>>();
                for media in media_index
                    .iter()
                    .filter(|media| media_in_library(media, &library))
                {
                    let Some(root) = library
                        .folders
                        .iter()
                        .find(|folder| media.path.starts_with(folder))
                    else {
                        continue;
                    };
                    let Some(path) = media.path.strip_prefix(root).ok() else {
                        continue;
                    };
                    let Some((position, duration, watched, updated_at)) =
                        progress.get(path.to_string_lossy().as_ref())
                    else {
                        continue;
                    };
                    let item = metadata
                        .entry(media.id.clone())
                        .or_insert_with(|| serde_json::json!({}));
                    if let Some(object) = item.as_object_mut() {
                        object.insert("mplex_view_offset".to_owned(), serde_json::json!(position));
                        object.insert("mplex_view_count".to_owned(), serde_json::json!(i64::from(*watched)));
                        object.insert(
                            "mplex_last_viewed_at".to_owned(),
                            serde_json::json!(if *watched { *updated_at } else { 0 }),
                        );
                        if let Some(duration) = duration {
                            object.insert("mplex_duration".to_owned(), serde_json::json!(duration));
                        }
                    }
                }
            }
        }
    }
    metadata
}

fn plex_genre_tokens(metadata: Option<&serde_json::Value>) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    if let Some(genres) = metadata
        .and_then(|metadata| metadata.get("genres"))
        .and_then(serde_json::Value::as_array)
    {
        tokens.extend(genres.iter().filter_map(|genre| {
            genre
                .get("id")
                .and_then(serde_json::Value::as_i64)
                .map(|id| format!("id:{id}"))
                .or_else(|| {
                    genre
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .map(|name| format!("name:{}", name.to_lowercase()))
                })
        }));
    }
    if let Some(ids) = metadata
        .and_then(|metadata| metadata.get("genre_ids"))
        .and_then(serde_json::Value::as_array)
    {
        tokens.extend(
            ids.iter()
                .filter_map(serde_json::Value::as_i64)
                .map(|id| format!("id:{id}")),
        );
    }
    tokens
}

fn plex_library_for_metadata(state: &AppState, id: &str) -> Option<(usize, ConfigLibrary)> {
    let media_index = media_snapshot(state);
    find_media(state, id)
        .and_then(|media| media_library(state, &media))
        .or_else(|| {
            find_plex_tv_location(state, media_index.as_ref(), id)
                .map(|(section, library, _, _)| (section, library))
        })
}

async fn plex_related_hubs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some((section, library)) = plex_library_for_metadata(&state, &id) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let count = query
        .get("count")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10)
        .min(50);
    let media_index = media_snapshot(&state);
    let metadata = plex_section_metadata(&state, section);
    let source_groups = (library.kind == "tv").then(|| {
        plex_tv_show_groups(
            section,
            &library.folders,
            media_index
                .iter()
                .filter(|media| media_in_library(media, &library)),
        )
    });
    let source_group = source_groups.as_ref().and_then(|groups| {
        groups.iter().find(|show| {
            show.rating_key == id
                || show.episodes.iter().any(|episode| episode.id == id)
                || plex_tv_season_groups(show, &library.folders)
                    .keys()
                    .any(|season| plex_tv_season_key(&show.rating_key, *season) == id)
        })
    });
    let source_media = source_group
        .and_then(|show| show.episodes.first().copied())
        .or_else(|| media_index.iter().find(|media| media.id == id));
    let target_genres = plex_genre_tokens(source_media.and_then(|media| metadata.get(&media.id)));
    let source_group_key = source_group.map(|show| show.rating_key.clone());

    let (total, items) = if library.kind == "tv" {
        let groups = source_groups.unwrap_or_default();
        let mut candidates = groups
            .iter()
            .filter(|show| Some(show.rating_key.as_str()) != source_group_key.as_deref())
            .filter_map(|show| {
                let representative = show.episodes.first().copied()?;
                let item_metadata = metadata.get(&representative.id);
                let overlap = plex_genre_tokens(item_metadata)
                    .intersection(&target_genres)
                    .count();
                Some((
                    show,
                    representative,
                    overlap,
                    item_metadata.and_then(plex_rating),
                ))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .2
                .cmp(&left.2)
                .then_with(|| {
                    right
                        .3
                        .partial_cmp(&left.3)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| media_modified_at(right.1).cmp(&media_modified_at(left.1)))
        });
        let total = candidates.len();
        let mut related_items = Vec::new();
        for (show, _, _, _) in candidates.iter().take(count) {
            related_items.push(plex_tv_show_json(show, section, &library, &metadata));
        }
        (total, related_items)
    } else {
        let mut candidates = media_index
            .iter()
            .filter(|media| media_in_library(media, &library) && media.id != id)
            .map(|media| {
                let item_metadata = metadata.get(&media.id);
                let overlap = plex_genre_tokens(item_metadata)
                    .intersection(&target_genres)
                    .count();
                (media, overlap, item_metadata.and_then(plex_rating))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .1
                .cmp(&left.1)
                .then_with(|| {
                    right
                        .2
                        .partial_cmp(&left.2)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| media_modified_at(right.0).cmp(&media_modified_at(left.0)))
        });
        let total = candidates.len();
        let mut related_items = Vec::new();
        for (media, _, _) in candidates.iter().take(count) {
            let mut item = plex_video_json(*media, section, &library, metadata.get(&media.id));
            add_plex_technical_metadata(&state, &mut item, media).await;
            related_items.push(item);
        }
        (total, related_items)
    };
    let hubs = if query.get("onlyTransient").is_some_and(|value| value == "1") {
        Vec::new()
    } else {
        vec![serde_json::json!({
            "hubKey": format!("/library/sections/{section}/all"),
            "key": format!("/hubs/metadata/{id}/related"),
            "title": "Related",
            "type": plex_kind(&library.kind),
            "hubIdentifier": format!("{}.related.{section}", plex_kind(&library.kind)),
            "context": "hub.metadata.related",
            "size": items.len(),
            "more": total > items.len(),
            "style": "shelf",
            "promoted": false,
            "Metadata": items
        })]
    };
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": hubs.len(),
            "allowSync": false,
            "identifier": "com.plexapp.plugins.library",
            "librarySectionID": section,
            "librarySectionTitle": library.name,
            "librarySectionUUID": plex_library_uuid(&state, &library.name, section),
            "Hub": hubs
        }
    }))
}

fn plex_search_rank(query: &str, candidate: &str) -> Option<u8> {
    let query = query.trim().to_lowercase();
    let candidate = candidate.to_lowercase();
    if query.is_empty() {
        return None;
    }
    if candidate.contains(&query) {
        return Some(if candidate == query {
            0
        } else if candidate.starts_with(&query) {
            1
        } else {
            2
        });
    }

    let query_words = query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    let candidate_words = candidate
        .split(|character: char| !character.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    if query_words.is_empty() || candidate_words.is_empty() {
        return None;
    }

    let mut fuzzy_match = false;
    for query_word in query_words {
        if candidate_words
            .iter()
            .any(|candidate_word| candidate_word.contains(query_word))
        {
            continue;
        }
        let max_distance = if query_word.chars().count() >= 10 {
            2
        } else {
            1
        };
        if candidate_words.iter().any(|candidate_word| {
            plex_edit_distance(query_word, candidate_word).is_some_and(|distance| {
                distance <= max_distance
                    && (query_word.chars().count() as isize
                        - candidate_word.chars().count() as isize)
                        .unsigned_abs()
                        <= max_distance
            })
        }) {
            fuzzy_match = true;
        } else {
            return None;
        }
    }
    Some(if fuzzy_match { 3 } else { 2 })
}

fn plex_edit_distance(left: &str, right: &str) -> Option<usize> {
    let left = left.chars().collect::<Vec<_>>();
    let right = right.chars().collect::<Vec<_>>();
    if left.len().abs_diff(right.len()) > 2 {
        return None;
    }
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    let mut current = vec![0; right.len() + 1];
    for (left_index, left_character) in left.iter().enumerate() {
        current[0] = left_index + 1;
        let mut row_minimum = current[0];
        for (right_index, right_character) in right.iter().enumerate() {
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(previous[right_index] + usize::from(left_character != right_character));
            row_minimum = row_minimum.min(current[right_index + 1]);
        }
        if row_minimum > 2 {
            return None;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    Some(previous[right.len()])
}

fn plex_episode_search_rank(
    query: &str,
    episode_title: &str,
    filename_title: &str,
    show_rank: Option<u8>,
) -> Option<u8> {
    [
        plex_search_rank(query, episode_title),
        plex_search_rank(query, filename_title),
        show_rank,
    ]
    .into_iter()
    .flatten()
    .min()
}

fn plex_search_type_matches(query: &BTreeMap<String, String>, kind: &str) -> bool {
    let Some(type_filter) = query.get("type") else {
        return true;
    };
    type_filter.split(',').any(|value| {
        matches!(
            (value.trim(), kind),
            ("1" | "movie", "movie") | ("2" | "show", "show") | ("4" | "episode", "episode")
        )
    })
}

async fn plex_search_items(
    state: &AppState,
    query: &BTreeMap<String, String>,
) -> Vec<serde_json::Value> {
    let Some(term) = query.get("query").map(|value| value.trim()) else {
        return Vec::new();
    };
    if term.is_empty() {
        return Vec::new();
    }
    let section_filter = query
        .get("sectionId")
        .or_else(|| query.get("sectionID"))
        .and_then(|section| section.parse::<usize>().ok());
    let limit = query
        .get("limit")
        .and_then(|limit| limit.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 100);
    let libraries = state
        .config
        .lock()
        .map(|config| config.libraries.clone())
        .unwrap_or_default();
    let media_index = media_snapshot(state);
    let mut results = Vec::<(u8, String, serde_json::Value)>::new();

    for (index, library) in libraries.iter().enumerate() {
        let section = index + 1;
        if section_filter.is_some_and(|requested| requested != section) {
            continue;
        }
        let metadata = plex_section_metadata(state, section);
        if library.kind == "tv" {
            let shows = plex_tv_show_groups(
                section,
                &library.folders,
                media_index
                    .iter()
                    .filter(|media| media_in_library(media, library)),
            );
            for show in shows {
                let show_guid = plex_tv_show_guid(&show, &metadata);
                let representative = show.episodes.first().copied();
                let show_metadata = representative.and_then(|media| metadata.get(&media.id));
                let show_title = show_metadata
                    .and_then(|value| value.get("name"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(&show.title);
                let show_rank = [
                    show_title,
                    show.title.as_str(),
                    show_metadata
                        .and_then(|value| value.get("original_name"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default(),
                ]
                .into_iter()
                .filter_map(|candidate| plex_search_rank(term, candidate))
                .min();
                if plex_search_type_matches(query, "show") {
                    if let Some(rank) = show_rank {
                        results.push((
                            rank,
                            show_title.to_lowercase(),
                            plex_tv_show_json(&show, section, library, &metadata),
                        ));
                    }
                }
                if !plex_search_type_matches(query, "episode") {
                    continue;
                }
                for (media_index, media) in show.episodes.iter().enumerate() {
                    let episode_metadata = metadata.get(&media.id);
                    let episode_filename_title = tv_episode_display_name(&media.title);
                    let episode_title = episode_metadata
                        .and_then(|value| value.get("mplex_episode_title"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or(&episode_filename_title);
                    let Some(rank) = plex_episode_search_rank(
                        term,
                        episode_title,
                        episode_filename_title.as_str(),
                        show_rank,
                    ) else {
                        continue;
                    };
                    let episode_info = library
                        .folders
                        .iter()
                        .find(|root| media.path.starts_with(root))
                        .and_then(|root| tv_episode_info(root, &media.path));
                    let season = episode_info
                        .as_ref()
                        .map(|episode| episode.season)
                        .unwrap_or(1);
                    let episode = episode_info
                        .as_ref()
                        .map(|episode| episode.episode)
                        .unwrap_or(media_index as u32 + 1);
                    let mut item = plex_video_json(media, section, library, episode_metadata);
                    plex_tv_episode_parents(&mut item, &show, season, episode, &show_guid);
                    add_plex_technical_metadata(state, &mut item, media).await;
                    results.push((rank, episode_title.to_lowercase(), item));
                }
            }
        } else if plex_search_type_matches(query, "movie") {
            for media in media_index
                .iter()
                .filter(|media| media_in_library(media, library))
            {
                let item_metadata = metadata.get(&media.id);
                let rank = [
                    media.title.as_str(),
                    item_metadata
                        .and_then(|value| value.get("title"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default(),
                    item_metadata
                        .and_then(|value| value.get("original_title"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default(),
                ]
                .into_iter()
                .filter_map(|candidate| plex_search_rank(term, candidate))
                .min();
                if let Some(rank) = rank {
                    let mut item = plex_video_json(media, section, library, item_metadata);
                    add_plex_technical_metadata(state, &mut item, media).await;
                    results.push((
                        rank,
                        item["title"].as_str().unwrap_or_default().to_lowercase(),
                        item,
                    ));
                }
            }
        }
    }

    results.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    results
        .into_iter()
        .take(limit)
        .map(|(_, _, item)| item)
        .collect()
}

fn plex_metadata_guid(media_type: &str, rating_key: &str) -> String {
    format!("com.plexapp.plugins.library://{media_type}/{rating_key}")
}

fn plex_tv_show_guid(
    show: &PlexTvShowGroup<'_>,
    metadata: &HashMap<String, serde_json::Value>,
) -> String {
    show.episodes
        .first()
        .and_then(|episode| metadata.get(&episode.id))
        .and_then(|metadata| metadata.get("id"))
        .and_then(serde_json::Value::as_i64)
        .map(|id| format!("tmdb://tv/{id}"))
        .unwrap_or_else(|| plex_metadata_guid("show", &show.rating_key))
}

async fn plex_library_search(
    State(state): State<AppState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    if !query.contains_key("query") {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let items = plex_search_items(&state, &query).await;
    let hubs = plex_search_hubs(items.clone(), 3);
    // Plezy consumes the local-library response as SearchResult records whose
    // Metadata member is the actual playable library item. Keep the simpler
    // Metadata and Hub forms too for clients using those documented shapes.
    let search_results = plex_search_result_entries(&items);
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": items.len(),
            "totalSize": items.len(),
            "offset": 0,
            "identifier": "com.plexapp.plugins.library",
            "Hub": hubs,
            "SearchResult": search_results,
            "Metadata": items
        }
    }))
}

fn plex_search_result_entries(items: &[serde_json::Value]) -> Vec<serde_json::Value> {
    items
        .iter()
        .cloned()
        .map(|item| serde_json::json!({"Metadata": item, "score": 1.0}))
        .collect()
}

fn plex_search_hubs(items: Vec<serde_json::Value>, limit: usize) -> Vec<serde_json::Value> {
    let mut grouped = BTreeMap::<String, Vec<serde_json::Value>>::new();
    for item in items {
        let kind = item
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("movie")
            .to_owned();
        grouped.entry(kind).or_default().push(item);
    }
    grouped
        .into_iter()
        .map(|(kind, items)| {
            let title = match kind.as_str() {
                "show" => "Shows",
                "episode" => "Episodes",
                _ => "Movies",
            };
            let more = items.len() > limit;
            let items = items.into_iter().take(limit).collect::<Vec<_>>();
            let mut hub = serde_json::json!({
                "hubIdentifier": kind,
                "title": title,
                "type": kind,
                "size": items.len(),
                "more": more
            });
            let item_field = if kind == "show" {
                "Directory"
            } else {
                "Metadata"
            };
            hub[item_field] = serde_json::json!(items);
            hub
        })
        .collect()
}

async fn plex_hub_search(
    State(state): State<AppState>,
    Query(mut query): Query<BTreeMap<String, String>>,
) -> Response {
    if !query.contains_key("query") {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let limit = query
        .get("limit")
        .and_then(|limit| limit.parse::<usize>().ok())
        .unwrap_or(3)
        .clamp(1, 50);
    query.insert("limit".to_owned(), "100".to_owned());
    let items = plex_search_items(&state, &query).await;
    let hubs = plex_search_hubs(items, limit);
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": hubs.len(),
            "Hub": hubs
        }
    }))
}

async fn plex_section(State(state): State<AppState>, Path(section): Path<usize>) -> Response {
    let Some(library) = state
        .config
        .lock()
        .ok()
        .and_then(|config| config.libraries.get(section.saturating_sub(1)).cloned())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let uuid = plex_library_uuid(&state, &library.name, section);
    let (_, scanned_at, updated_at) = library_publication_state(&state, &library.name, section);
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": 1,
            "updatedAt": library_catalog_updated_at(&state),
            "librarySectionID": section,
            "librarySectionTitle": library.name,
            "Directory": [{
                "key": format!("/library/sections/{section}/all"),
                "type": plex_kind(&library.kind),
                "title": library.name,
                "uuid": uuid,
                "scannedAt": scanned_at,
                "updatedAt": updated_at
            }]
        }
    }))
}

fn plex_section_update_response(state: &AppState, index: usize, headers: &HeaderMap) -> Response {
    let directories = plex_section_directories(state).1;
    let Some(directory) = directories.get(index) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    plex_create_section_response(directory, headers)
}

async fn plex_update_section(
    State(state): State<AppState>,
    Path(section): Path<usize>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let mut agent = None;
    let mut name = None;
    let mut scanner = None;
    let mut language = None;
    let mut provider_group_id = None;
    let mut locations = Vec::new();
    let mut preferences = BTreeMap::new();
    for (key, value) in form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes()) {
        let value = value.into_owned();
        match key.as_ref() {
            "agent" => agent = Some(value),
            "name" => name = Some(value),
            "scanner" => scanner = Some(value),
            "language" => language = Some(value),
            "metadataAgentProviderGroupId" => {
                let Ok(id) = value.parse::<i64>() else {
                    return StatusCode::BAD_REQUEST.into_response();
                };
                provider_group_id = Some(id);
            }
            "locations" | "location" | "location[]" => locations.push(value),
            key => {
                if let Some(key) = key
                    .strip_prefix("prefs[")
                    .and_then(|key| key.strip_suffix(']'))
                    .filter(|key| !key.is_empty())
                {
                    preferences.insert(key.to_owned(), value);
                }
            }
        }
    }
    let Some(agent) = agent.filter(|agent| !agent.trim().is_empty()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let name = match name {
        Some(name) if !name.trim().is_empty() && name.trim().chars().count() <= 64 => {
            Some(name.trim().to_owned())
        }
        Some(_) => return StatusCode::BAD_REQUEST.into_response(),
        None => None,
    };
    let scanner = match scanner {
        Some(scanner) if !scanner.trim().is_empty() => Some(scanner.trim().to_owned()),
        Some(_) => return StatusCode::BAD_REQUEST.into_response(),
        None => None,
    };
    let language = match language {
        Some(language) if !language.trim().is_empty() => Some(language.trim().to_owned()),
        Some(_) => return StatusCode::BAD_REQUEST.into_response(),
        None => None,
    };
    let Some(index) = section.checked_sub(1) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(library) = state
        .config
        .lock()
        .ok()
        .and_then(|config| config.libraries.get(index).cloned())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let tv_library = library.kind == "tv";
    let allowed_agents = if tv_library {
        [
            "tv.plex.agents.series",
            "tv.plex.agents.none",
            "tv.plex.agents.nfo.tv",
        ]
    } else {
        [
            "tv.plex.agents.movie",
            "tv.plex.agents.none",
            "tv.plex.agents.nfo.movie",
        ]
    };
    let allowed_scanners = if tv_library {
        ["Plex TV Series", "Plex Series Scanner"]
    } else {
        ["Plex Movie", "Plex Video Files"]
    };
    if !allowed_agents.contains(&agent.as_str())
        || scanner
            .as_deref()
            .is_some_and(|scanner| !allowed_scanners.contains(&scanner))
    {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let mut requested_locations = Vec::new();
    for location in locations {
        let requested = PathBuf::from(location.trim());
        let Ok(metadata) = tokio::fs::metadata(&requested).await else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        if !metadata.is_dir() {
            return StatusCode::BAD_REQUEST.into_response();
        }
        let Ok(canonical) = tokio::fs::canonicalize(requested).await else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        if !requested_locations.contains(&canonical) {
            requested_locations.push(canonical);
        }
    }

    let mut existing_locations = Vec::new();
    for folder in &library.folders {
        existing_locations.push(
            tokio::fs::canonicalize(folder)
                .await
                .unwrap_or_else(|_| folder.clone()),
        );
    }

    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Some(current_library) = config.libraries.get(index) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if current_library.name != library.name
        || current_library.kind != library.kind
        || current_library.folders != library.folders
    {
        return StatusCode::CONFLICT.into_response();
    }
    if name.as_ref().is_some_and(|name| {
        config
            .libraries
            .iter()
            .enumerate()
            .any(|(other_index, other)| {
                other_index != index && other.name.eq_ignore_ascii_case(name)
            })
    }) {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let mut updated_config = config.clone();
    let updated_library = &mut updated_config.libraries[index];
    let mut changed = false;
    if updated_library.plex_agent.as_deref() != Some(agent.as_str()) {
        updated_library.plex_agent = Some(agent);
        changed = true;
    }
    if let Some(name) = name {
        if updated_library.name != name {
            updated_library.name = name;
            changed = true;
        }
    }
    if let Some(scanner) = scanner {
        if updated_library.plex_scanner.as_deref() != Some(scanner.as_str()) {
            updated_library.plex_scanner = Some(scanner);
            changed = true;
        }
    }
    if let Some(language) = language {
        if updated_library.plex_language.as_deref() != Some(language.as_str()) {
            updated_library.plex_language = Some(language);
            changed = true;
        }
    }
    if let Some(provider_group_id) = provider_group_id {
        if updated_library.metadata_agent_provider_group_id != Some(provider_group_id) {
            updated_library.metadata_agent_provider_group_id = Some(provider_group_id);
            changed = true;
        }
    }
    for location in requested_locations {
        if !existing_locations.contains(&location) {
            updated_library.folders.push(location.clone());
            existing_locations.push(location);
            changed = true;
        }
    }
    if !preferences.is_empty() {
        let stored_preferences = updated_library
            .plex_preferences
            .get_or_insert_with(BTreeMap::new);
        for (key, value) in preferences {
            if stored_preferences.get(&key) != Some(&value) {
                stored_preferences.insert(key, value);
                changed = true;
            }
        }
    }
    if !changed {
        drop(config);
        return plex_section_update_response(&state, index, &headers);
    }

    let old_name = library.name;
    let new_name = updated_library.name.clone();
    let Ok(mut connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Ok(transaction) = connection.transaction() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if (old_name != new_name
        && transaction
            .execute(
                "UPDATE library_groups SET name = ?1, updated_at = MAX(updated_at + 1, CAST(strftime('%s', 'now') AS INTEGER)) WHERE name = ?2",
                (&new_name, &old_name),
            )
            .is_err())
        || sync_config_libraries(&transaction, &updated_config).is_err()
        || update_library_catalog_timestamp(&transaction).is_err()
        || save_config(&state.config_path, &updated_config).is_err()
        || transaction.commit().is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    drop(connection);
    *config = updated_config;
    drop(config);
    plex_section_update_response(&state, index, &headers)
}

fn plex_section_preference_settings(
    kind: &str,
    preferences: Option<&BTreeMap<String, String>>,
) -> Vec<serde_json::Value> {
    let make_setting = |id: &str, setting_type: &str, default: &str| {
        let value = preferences
            .and_then(|preferences| preferences.get(id))
            .map(String::as_str)
            .unwrap_or(default);
        let mut setting = serde_json::json!({
            "id": id,
            "label": id,
            "summary": "",
            "type": setting_type,
            "default": default,
            "value": value,
            "hidden": false,
            "advanced": false,
            "group": ""
        });
        let enum_values = match id {
            "hidden" => Some("0:Include on home screen and in global search|1:Exclude from home screen|2:Exclude from home screen and global search"),
            "episodeSort" => Some("-1:Oldest first|1:Newest first"),
            "showOrdering" => Some("tmdbAiring:The Movie Database|aired:TheTVDB"),
            "collectionMode" => Some("0:Hide collections and show items|1:Hide items in collections|2:Show both"),
            "flattenSeasons" => Some("0:Show|2:Hide for single-season series|1:Hide"),
            "enableAdMarkerGeneration" => Some("0:Disabled|1:For recorded items|2:For all items"),
            _ => None,
        };
        if let Some(enum_values) = enum_values {
            setting["enumValues"] = serde_json::json!(enum_values);
        }
        setting
    };
    let common_settings = [
        ("hidden", "int", "0"),
        ("country", "text", "US"),
        ("originalTitles", "bool", "false"),
        ("localizedArtwork", "bool", "true"),
        ("useLocalAssets", "bool", "true"),
        ("respectTags", "bool", "false"),
        ("useExternalExtras", "bool", "true"),
        ("skipNonTrailerExtras", "bool", "false"),
        ("useRedbandTrailers", "bool", "false"),
        ("includeExtrasWithLocalizedSubtitles", "bool", "false"),
        ("includeAdultContent", "bool", "false"),
        ("enableBIFGeneration", "bool", "true"),
        ("collectionMode", "int", "2"),
        ("enableIntroMarkerGeneration", "bool", "true"),
        ("enableCreditsMarkerGeneration", "bool", "true"),
        ("enableAdMarkerGeneration", "int", "1"),
        ("enableVoiceActivityGeneration", "bool", "true"),
        ("enableLoudnessAnalysis", "bool", "true"),
    ];
    let mut settings: Vec<serde_json::Value> = common_settings
        .iter()
        .map(|(id, setting_type, value)| make_setting(id, setting_type, value))
        .collect();
    if kind == "tv" {
        for (id, setting_type, value) in [
            ("episodeSort", "text", "-1"),
            ("showOrdering", "text", "tmdbAiring"),
            ("useSeasonTitles", "bool", "false"),
            ("flattenSeasons", "int", "0"),
        ] {
            settings.push(make_setting(id, setting_type, value));
        }
    }
    settings
}

fn plex_preferences_response(container: serde_json::Value, headers: &HeaderMap) -> Response {
    let accepts_xml = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.to_ascii_lowercase().contains("application/xml"));
    if !accepts_xml {
        return plex_json(serde_json::json!({"MediaContainer": container}));
    }
    let root_attributes = ["size", "librarySectionID", "librarySectionTitle"]
        .iter()
        .filter_map(|name| {
            container.get(*name).map(|value| {
                let value = value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string());
                format!(" {}=\"{}\"", name, xml_escape(&value))
            })
        })
        .collect::<String>();
    let settings = container
        .get("Setting")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .map(|setting| {
            let attributes = setting
                .as_object()
                .into_iter()
                .flat_map(|setting| setting.iter())
                .map(|(name, value)| {
                    let value = value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string());
                    format!(" {name}=\"{}\"", xml_escape(&value))
                })
                .collect::<String>();
            format!("<Setting{attributes}/>")
        })
        .collect::<String>();
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><MediaContainer{root_attributes}>{settings}</MediaContainer>"
    );
    let mut response = body.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    plex_protocol_response(response)
}

async fn plex_library_section_prefs(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let mut media_type = None;
    let mut agent = None;
    for (key, value) in form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes()) {
        match key.as_ref() {
            "type" => {
                let Ok(value) = value.parse::<i64>() else {
                    return StatusCode::BAD_REQUEST.into_response();
                };
                media_type = Some(value);
            }
            "agent" => agent = Some(value.into_owned()),
            _ => {}
        }
    }
    let Some(media_type) = media_type else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let kind = match media_type {
        1 => "movie",
        2 => "tv",
        _ => {
            return plex_preferences_response(
                serde_json::json!({"size": 0, "Setting": []}),
                &headers,
            )
        }
    };
    let libraries = match state.config.lock() {
        Ok(config) => config.libraries.clone(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let library = libraries.iter().find(|library| {
        let library_kind_matches = if kind == "movie" {
            library.kind != "tv"
        } else {
            library.kind == "tv"
        };
        let default_agent = if library.kind == "tv" {
            "tv.plex.agents.series"
        } else {
            "tv.plex.agents.movie"
        };
        library_kind_matches
            && agent
                .as_deref()
                .is_none_or(|agent| library.plex_agent.as_deref().unwrap_or(default_agent) == agent)
    });
    let settings = plex_section_preference_settings(
        kind,
        library.and_then(|library| library.plex_preferences.as_ref()),
    );
    plex_preferences_response(
        serde_json::json!({"size": settings.len(), "Setting": settings}),
        &headers,
    )
}

async fn plex_agent_prefs(
    Path((root, plugin)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if root != ":" {
        return StatusCode::NOT_FOUND.into_response();
    }
    if !matches!(
        plugin.as_str(),
        "tv.plex.agents.movie"
            | "tv.plex.agents.none"
            | "tv.plex.agents.series"
            | "tv.plex.agents.nfo.movie"
            | "tv.plex.agents.nfo.tv"
    ) {
        return StatusCode::NOT_FOUND.into_response();
    }
    plex_preferences_response(serde_json::json!({"size": 0, "Setting": []}), &headers)
}

fn plex_browse_include_files(raw_query: Option<&str>) -> Result<bool, StatusCode> {
    let include_files = raw_query
        .map(|query| {
            form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == "includeFiles")
                .map(|(_, value)| value.into_owned())
        })
        .flatten();
    match include_files.as_deref() {
        None | Some("0") => Ok(false),
        Some("1") => Ok(true),
        Some(_) => Err(StatusCode::BAD_REQUEST),
    }
}

async fn plex_service_browse_root(
    State(_state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let include_files = match plex_browse_include_files(raw_query.as_deref()) {
        Ok(include_files) => include_files,
        Err(status) => return status.into_response(),
    };
    plex_browse_directory(PathBuf::from("/"), include_files, &headers).await
}

async fn plex_service_browse_path(
    State(_state): State<AppState>,
    Path(encoded_path): Path<String>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let include_files = match plex_browse_include_files(raw_query.as_deref()) {
        Ok(include_files) => include_files,
        Err(status) => return status.into_response(),
    };
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded_path.as_bytes())
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(encoded_path.as_bytes()));
    let Ok(decoded) = decoded else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Ok(path) = String::from_utf8(decoded) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let path = PathBuf::from(path);
    if !path.is_absolute() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    plex_browse_directory(path, include_files, &headers).await
}

async fn plex_browse_directory(
    directory: PathBuf,
    include_files: bool,
    headers: &HeaderMap,
) -> Response {
    let Ok(metadata) = tokio::fs::metadata(&directory).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !metadata.is_dir() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(mut entries) = tokio::fs::read_dir(&directory).await else {
        return StatusCode::FORBIDDEN.into_response();
    };
    let mut paths = Vec::new();
    let mut files = Vec::new();
    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let Ok(file_type) = entry.file_type().await else {
            continue;
        };
        let child_path = entry.path();
        let encoded = URL_SAFE_NO_PAD.encode(child_path.to_string_lossy().as_bytes());
        let key = format!("/services/browse/{encoded}");
        let title = entry.file_name().to_string_lossy().into_owned();
        let item = serde_json::json!({
            "key": key,
            "path": child_path.to_string_lossy(),
            "title": title
        });
        if file_type.is_dir() {
            paths.push(item);
        } else if include_files && file_type.is_file() {
            files.push(item);
        }
    }
    paths.sort_by(|left, right| left["title"].as_str().cmp(&right["title"].as_str()));
    files.sort_by(|left, right| left["title"].as_str().cmp(&right["title"].as_str()));

    let size = paths.len() + files.len();
    let mut container = serde_json::json!({"size": size, "Path": paths});
    if !files.is_empty() {
        container["File"] = serde_json::json!(files);
    }
    let accepts_json = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.to_ascii_lowercase().contains("application/json"));
    if accepts_json {
        return plex_json(serde_json::json!({"MediaContainer": container}));
    }
    let mut body =
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><MediaContainer size=\"{size}\">");
    for (tag, items) in [
        ("Path", container["Path"].as_array()),
        ("File", container["File"].as_array()),
    ] {
        for item in items.into_iter().flatten() {
            let attributes = ["key", "path", "title"]
                .iter()
                .filter_map(|name| {
                    item.get(*name)
                        .and_then(serde_json::Value::as_str)
                        .map(|value| format!(" {name}=\"{}\"", xml_escape(value)))
                })
                .collect::<String>();
            body.push_str(&format!("<{tag}{attributes}/>"));
        }
    }
    body.push_str("</MediaContainer>");
    let mut response = body.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    plex_protocol_response(response)
}

fn plex_section_preferences_response(
    section: usize,
    library: &ConfigLibrary,
    headers: &HeaderMap,
) -> Response {
    let settings =
        plex_section_preference_settings(&library.kind, library.plex_preferences.as_ref());
    plex_preferences_response(
        serde_json::json!({
            "size": settings.len(),
            "librarySectionID": section,
            "librarySectionTitle": library.name,
            "Setting": settings
        }),
        headers,
    )
}

async fn plex_section_prefs(
    State(state): State<AppState>,
    Path(section): Path<usize>,
    headers: HeaderMap,
) -> Response {
    let Some(library) = section.checked_sub(1).and_then(|index| {
        state
            .config
            .lock()
            .ok()
            .and_then(|config| config.libraries.get(index).cloned())
    }) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    plex_section_preferences_response(section, &library, &headers)
}

async fn plex_update_section_prefs(
    State(state): State<AppState>,
    Path(section): Path<usize>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let mut preferences = BTreeMap::new();
    for (key, value) in form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes()) {
        let key = key.as_ref();
        let value = value.into_owned();
        if key == "prefs" {
            let Ok(object) = serde_json::from_str::<serde_json::Value>(&value) else {
                return StatusCode::BAD_REQUEST.into_response();
            };
            let Some(object) = object.as_object() else {
                return StatusCode::BAD_REQUEST.into_response();
            };
            for (key, value) in object {
                preferences.insert(
                    key.clone(),
                    value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string()),
                );
            }
        } else if let Some(key) = key
            .strip_prefix("prefs[")
            .and_then(|key| key.strip_suffix(']'))
            .filter(|key| !key.is_empty())
        {
            preferences.insert(key.to_owned(), value);
        } else if !key.starts_with("X-Plex-") && key != "token" {
            preferences.insert(key.to_owned(), value);
        }
    }
    if preferences.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(index) = section.checked_sub(1) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Some(library) = config.libraries.get(index).cloned() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut updated_config = config.clone();
    let stored_preferences = updated_config.libraries[index]
        .plex_preferences
        .get_or_insert_with(BTreeMap::new);
    let mut changed = false;
    for (key, value) in preferences {
        if stored_preferences.get(&key) != Some(&value) {
            stored_preferences.insert(key, value);
            changed = true;
        }
    }
    if changed {
        let Ok(mut connection) = state.db.lock() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        let Ok(transaction) = connection.transaction() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        if transaction
            .execute(
                "UPDATE library_groups SET updated_at = MAX(updated_at + 1, CAST(strftime('%s', 'now') AS INTEGER)) WHERE name = ?1",
                [&library.name],
            )
            .is_err()
            || update_library_catalog_timestamp(&transaction).is_err()
            || save_config(&state.config_path, &updated_config).is_err()
            || transaction.commit().is_err()
        {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        drop(connection);
        *config = updated_config;
    }
    let updated_library = config.libraries[index].clone();
    drop(config);
    plex_section_preferences_response(section, &updated_library, &headers)
}

async fn plex_section_collections(
    State(state): State<AppState>,
    Path(section): Path<usize>,
) -> Response {
    let libraries = match state.config.lock() {
        Ok(config) => config.libraries.clone(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if section == 0 || section > libraries.len() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let library = &libraries[section - 1];
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Ok(mut collections) = plex_collection_records(&connection) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    collections.retain(|collection| collection.section_id == section as i64);
    let Ok(mut body) = plex_collection_container(
        &connection,
        &collections,
        &libraries,
        &state.machine_identifier,
    ) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    body["MediaContainer"]["librarySectionID"] = serde_json::json!(section);
    body["MediaContainer"]["librarySectionTitle"] = serde_json::json!(library.name);
    let stable_id = connection
        .query_row(
            "SELECT id FROM library_groups WHERE name = ?1",
            [&library.name],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(section as i64);
    body["MediaContainer"]["librarySectionUUID"] = serde_json::json!(plex_library_uuid_for_id(
        &state.machine_identifier,
        stable_id
    ));
    body["MediaContainer"]["title1"] = serde_json::json!(library.name);
    body["MediaContainer"]["title2"] = serde_json::json!("Collections");
    body["MediaContainer"]["viewGroup"] = serde_json::json!(plex_kind(&library.kind));
    plex_json(body)
}

async fn plex_section_sorts(State(state): State<AppState>, Path(section): Path<usize>) -> Response {
    let Some(library) = state
        .config
        .lock()
        .ok()
        .and_then(|config| config.libraries.get(section.saturating_sub(1)).cloned())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": 1,
            "identifier": "com.plexapp.plugins.library",
            "librarySectionID": section,
            "librarySectionTitle": library.name,
            "librarySectionUUID": plex_library_uuid(&state, &library.name, section),
            "title1": library.name,
            "Directory": [{
                "key": "titleSort",
                "descKey": "titleSort:desc",
                "default": "asc",
                "defaultDirection": "asc",
                "firstCharacterKey": format!("/library/sections/{section}/firstCharacter"),
                "title": "Title"
            }]
        }
    }))
}

async fn plex_section_filters(
    State(state): State<AppState>,
    Path(section): Path<usize>,
) -> Response {
    let Some(library) = state
        .config
        .lock()
        .ok()
        .and_then(|config| config.libraries.get(section.saturating_sub(1)).cloned())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": 0,
            "identifier": "com.plexapp.plugins.library",
            "librarySectionID": section,
            "librarySectionTitle": library.name,
            "librarySectionUUID": plex_library_uuid(&state, &library.name, section),
            "title1": library.name,
            "viewGroup": "secondary",
            "Directory": []
        }
    }))
}

fn plex_first_character(title: &str) -> char {
    title
        .chars()
        .find(|character| character.is_alphanumeric())
        .map(|character| character.to_ascii_uppercase())
        .filter(char::is_ascii)
        .unwrap_or('#')
}

async fn plex_section_first_characters(
    State(state): State<AppState>,
    Path(section): Path<usize>,
) -> Response {
    let Some(library) = state
        .config
        .lock()
        .ok()
        .and_then(|config| config.libraries.get(section.saturating_sub(1)).cloned())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let metadata = plex_section_metadata(&state, section);
    let media_index = media_snapshot(&state);
    let mut initials = BTreeSet::new();
    if library.kind == "tv" {
        for show in plex_tv_show_groups(
            section,
            &library.folders,
            media_index
                .iter()
                .filter(|media| media_in_library(media, &library)),
        ) {
            let title = show
                .episodes
                .first()
                .and_then(|episode| metadata.get(&episode.id))
                .and_then(|item| item.get("name"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or(&show.title);
            initials.insert(plex_first_character(title));
        }
    } else {
        initials.extend(
            media_index
                .iter()
                .filter(|media| media_in_library(media, &library))
                .map(|media| plex_first_character(&media.title)),
        );
    }
    let directory = initials
        .into_iter()
        .map(|letter| {
            serde_json::json!({
                "key": format!("/library/sections/{section}/firstCharacter/{letter}"),
                "title": letter.to_string(),
                "type": "firstCharacter"
            })
        })
        .collect::<Vec<_>>();
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": directory.len(),
            "identifier": "com.plexapp.plugins.library",
            "librarySectionID": section,
            "librarySectionTitle": library.name,
            "librarySectionUUID": plex_library_uuid(&state, &library.name, section),
            "title1": library.name,
            "viewGroup": "secondary",
            "Directory": directory
        }
    }))
}

async fn plex_section_first_character_items(
    State(state): State<AppState>,
    Path((section, letter)): Path<(usize, String)>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let Some(library) = state
        .config
        .lock()
        .ok()
        .and_then(|config| config.libraries.get(section.saturating_sub(1)).cloned())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let metadata = plex_section_metadata(&state, section);
    let media_index = media_snapshot(&state);
    let initial = letter
        .chars()
        .next()
        .map(|character| character.to_ascii_uppercase())
        .unwrap_or('#');
    let mut items = Vec::new();
    if library.kind == "tv" {
        for show in plex_tv_show_groups(
            section,
            &library.folders,
            media_index
                .iter()
                .filter(|media| media_in_library(media, &library)),
        ) {
            let show_metadata = show
                .episodes
                .first()
                .and_then(|episode| metadata.get(&episode.id));
            let title = show_metadata
                .and_then(|item| item.get("name"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or(&show.title);
            if plex_first_character(title) == initial {
                items.push(plex_tv_show_json(&show, section, &library, &metadata));
            }
        }
    } else {
        for media in media_index
            .iter()
            .filter(|media| media_in_library(media, &library))
        {
            if plex_first_character(&media.title) == initial {
                let mut item = plex_video_json(media, section, &library, metadata.get(&media.id));
                add_plex_technical_metadata(&state, &mut item, media).await;
                items.push(item);
            }
        }
    }
    items.sort_by(|left, right| {
        left["title"]
            .as_str()
            .unwrap_or_default()
            .to_lowercase()
            .cmp(&right["title"].as_str().unwrap_or_default().to_lowercase())
    });
    let total = items.len();
    let (offset, items) = plex_paged_children(&items, &headers, &uri);
    let mut container = plex_library_media_container(
        &library,
        section,
        &plex_library_uuid(&state, &library.name, section),
        items.len(),
        offset,
        total,
    );
    container["Metadata"] = serde_json::json!(items);
    container["viewGroup"] = serde_json::json!(plex_kind(&library.kind));
    let mut response = plex_json(serde_json::json!({"MediaContainer": container}));
    response.headers_mut().insert(
        "X-Plex-Container-Start",
        HeaderValue::from_str(&offset.to_string()).expect("numeric header value"),
    );
    response.headers_mut().insert(
        "X-Plex-Container-Total-Size",
        HeaderValue::from_str(&total.to_string()).expect("numeric header value"),
    );
    response
}

async fn plex_library_all(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
    uri: Uri,
) -> Response {
    let Ok(config) = state.config.lock().map(|config| config.clone()) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let type_filter = query.get("type").map(String::as_str);
    let include_type = |numeric: &str, name: &str, default: bool| {
        type_filter.map_or(default, |filter| {
            filter
                .split(',')
                .any(|value| value.trim() == numeric || value.trim() == name)
        })
    };
    let mut items = Vec::new();
    let media_index = media_snapshot(&state);
    for (index, library) in config.libraries.iter().enumerate() {
        let section = index + 1;
        let metadata = plex_section_metadata(&state, section);
        if library.kind == "tv" {
            let shows = plex_tv_show_groups(
                section,
                &library.folders,
                media_index
                    .iter()
                    .filter(|media| media_in_library(media, library)),
            );
            for show in shows {
                let show_guid = plex_tv_show_guid(&show, &metadata);
                if include_type("2", "show", true) {
                    items.push(plex_tv_show_json(&show, section, library, &metadata));
                }
                let seasons = plex_tv_season_groups(&show, &library.folders);
                if include_type("3", "season", false) {
                    for (season, episodes) in &seasons {
                        items.push(plex_tv_season_json(
                            &show,
                            *season,
                            section,
                            library,
                            &metadata,
                            episodes.len(),
                        ));
                    }
                }
                if include_type("4", "episode", false) {
                    for (index, episode) in show.episodes.iter().enumerate() {
                        let episode_info = library
                            .folders
                            .iter()
                            .find(|root| episode.path.starts_with(root))
                            .and_then(|root| tv_episode_info(root, &episode.path));
                        let season = episode_info.as_ref().map(|info| info.season).unwrap_or(1);
                        let episode_number = episode_info
                            .as_ref()
                            .map(|info| info.episode)
                            .unwrap_or(index as u32 + 1);
                        let mut item =
                            plex_video_json(episode, section, library, metadata.get(&episode.id));
                        plex_tv_episode_parents(
                            &mut item,
                            &show,
                            season,
                            episode_number,
                            &show_guid,
                        );
                        add_plex_technical_metadata(&state, &mut item, episode).await;
                        items.push(item);
                    }
                }
            }
        } else if include_type("1", "movie", true) {
            for media in media_index
                .iter()
                .filter(|media| media_in_library(media, library))
            {
                let mut item = plex_video_json(media, section, library, metadata.get(&media.id));
                add_plex_technical_metadata(&state, &mut item, media).await;
                items.push(item);
            }
        }
    }

    if let Some(title_filter) = query.get("title") {
        let exact = title_filter.starts_with('=');
        let title_filter = title_filter
            .strip_prefix('=')
            .unwrap_or(title_filter)
            .to_lowercase();
        items.retain(|item| {
            item.get("title")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|title| {
                    let title = title.to_lowercase();
                    if exact {
                        title == title_filter
                    } else {
                        title.contains(&title_filter)
                    }
                })
        });
    }

    let requested_sort = query
        .get("sort")
        .map(|sort| sort.split(',').next().unwrap_or_default())
        .unwrap_or_default();
    let mut sort_parts = requested_sort.split(':');
    let sort_key = sort_parts.next().unwrap_or_default();
    let descending = sort_parts.next() == Some("desc");
    items.sort_by(|left, right| {
        let ordering = match sort_key {
            "duration" | "addedAt" | "rating" | "audienceRating" => left
                .get(sort_key)
                .and_then(serde_json::Value::as_f64)
                .unwrap_or_default()
                .partial_cmp(
                    &right
                        .get(sort_key)
                        .and_then(serde_json::Value::as_f64)
                        .unwrap_or_default(),
                )
                .unwrap_or(std::cmp::Ordering::Equal),
            _ => left
                .get("titleSort")
                .or_else(|| left.get("title"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_lowercase()
                .cmp(
                    &right
                        .get("titleSort")
                        .or_else(|| right.get("title"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_lowercase(),
                ),
        };
        if descending {
            ordering.reverse()
        } else {
            ordering
        }
    });

    let total = items.len();
    let (offset, end) = plex_container_page(&headers, &uri)
        .map(|(requested_offset, requested_size)| {
            let offset = requested_offset.min(total);
            (offset, offset.saturating_add(requested_size).min(total))
        })
        .unwrap_or((0, total));
    let metadata = items
        .into_iter()
        .skip(offset)
        .take(end.saturating_sub(offset))
        .collect::<Vec<_>>();
    let count = metadata.len();
    let mut response = plex_json(serde_json::json!({
        "MediaContainer": {
            "size": count,
            "totalSize": total,
            "offset": offset,
            "allowSync": false,
            "identifier": "com.plexapp.plugins.library",
            "content": "secondary",
            "title1": "Library",
            "title2": "All",
            "Metadata": metadata
        }
    }));
    response.headers_mut().insert(
        "X-Plex-Container-Start",
        HeaderValue::from_str(&offset.to_string()).expect("numeric header value"),
    );
    response.headers_mut().insert(
        "X-Plex-Container-Total-Size",
        HeaderValue::from_str(&total.to_string()).expect("numeric header value"),
    );
    response
}

async fn plex_all(
    State(s): State<AppState>,
    Path(section): Path<usize>,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
    uri: Uri,
) -> Response {
    let Some(library) = s
        .config
        .lock()
        .ok()
        .and_then(|config| config.libraries.get(section.saturating_sub(1)).cloned())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if library.kind == "tv" {
        return plex_tv_all(&s, section, library, &headers, &query, &uri).await;
    }
    let media_index = media_snapshot(&s);
    let mut media = media_index
        .iter()
        .filter(|media| media_in_library(media, &library))
        .collect::<Vec<_>>();
    let metadata_by_id = plex_section_metadata(&s, section);
    let requested_sort = query.get("sort").map(String::as_str).unwrap_or_default();
    let sort_key = requested_sort
        .split(',')
        .next()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default();
    let descending = requested_sort
        .split(',')
        .next()
        .is_some_and(|sort| sort.ends_with(":desc"));
    if requested_sort.is_empty() || sort_key == "titleSort" {
        media.sort_by(|left, right| {
            let ordering = left.title.to_lowercase().cmp(&right.title.to_lowercase());
            if descending {
                ordering.reverse()
            } else {
                ordering
            }
        });
    } else if sort_key == "addedAt" {
        media.sort_by_key(|item| {
            let time = media_modified_at(item);
            if descending {
                std::cmp::Reverse(time)
            } else {
                std::cmp::Reverse(u64::MAX - time)
            }
        });
    } else if sort_key == "audienceRating" || sort_key == "rating" {
        media.sort_by(|left, right| {
            let left_rating = metadata_by_id
                .get(&left.id)
                .and_then(plex_rating)
                .unwrap_or_default();
            let right_rating = metadata_by_id
                .get(&right.id)
                .and_then(plex_rating)
                .unwrap_or_default();
            let ordering = left_rating
                .partial_cmp(&right_rating)
                .unwrap_or(std::cmp::Ordering::Equal);
            if descending {
                ordering.reverse()
            } else {
                ordering
            }
        });
    }
    let total = media.len();
    let uuid = plex_library_uuid(&s, &library.name, section);
    let (offset, end) = plex_container_page(&headers, &uri)
        .map(|(requested_offset, requested_size)| {
            let offset = requested_offset.min(total);
            (offset, offset.saturating_add(requested_size).min(total))
        })
        .unwrap_or((0, total));
    let mut metadata = Vec::with_capacity(end.saturating_sub(offset));
    for item in media.iter().skip(offset).take(end.saturating_sub(offset)) {
        let mut video = plex_video_json(item, section, &library, metadata_by_id.get(&item.id));
        add_plex_technical_metadata(&s, &mut video, item).await;
        metadata.push(video);
    }
    let count = metadata.len();
    let mut container =
        plex_library_media_container(&library, section, &uuid, count, offset, total);
    container["Metadata"] = serde_json::json!(metadata);
    let mut response = plex_json(serde_json::json!({"MediaContainer": container}));
    response.headers_mut().insert(
        "X-Plex-Container-Start",
        HeaderValue::from_str(&offset.to_string()).expect("numeric header value"),
    );
    response.headers_mut().insert(
        "X-Plex-Container-Total-Size",
        HeaderValue::from_str(&total.to_string()).expect("numeric header value"),
    );
    response
}

async fn plex_tv_all(
    state: &AppState,
    section: usize,
    library: ConfigLibrary,
    headers: &HeaderMap,
    query: &BTreeMap<String, String>,
    uri: &Uri,
) -> Response {
    let metadata = plex_section_metadata(state, section);
    let media_index = media_snapshot(state);
    let mut shows = plex_tv_show_groups(
        section,
        &library.folders,
        media_index
            .iter()
            .filter(|media| media_in_library(media, &library)),
    );
    let requested_sort = query.get("sort").map(String::as_str).unwrap_or_default();
    let descending = requested_sort.ends_with(":desc");
    shows.sort_by(|left, right| {
        let ordering = left.title.to_lowercase().cmp(&right.title.to_lowercase());
        if descending {
            ordering.reverse()
        } else {
            ordering
        }
    });
    let total = shows.len();
    let (offset, end) = plex_container_page(headers, uri)
        .map(|(requested_offset, requested_size)| {
            let offset = requested_offset.min(total);
            (offset, offset.saturating_add(requested_size).min(total))
        })
        .unwrap_or((0, total));
    let items = shows
        .iter()
        .skip(offset)
        .take(end.saturating_sub(offset))
        .map(|show| plex_tv_show_json(show, section, &library, &metadata))
        .collect::<Vec<_>>();
    let mut container = plex_library_media_container(
        &library,
        section,
        &plex_library_uuid(state, &library.name, section),
        items.len(),
        offset,
        total,
    );
    container["Metadata"] = serde_json::json!(items);
    let mut response = plex_json(serde_json::json!({"MediaContainer": container}));
    response.headers_mut().insert(
        "X-Plex-Container-Start",
        HeaderValue::from_str(&offset.to_string()).expect("numeric header value"),
    );
    response.headers_mut().insert(
        "X-Plex-Container-Total-Size",
        HeaderValue::from_str(&total.to_string()).expect("numeric header value"),
    );
    response
}

fn plex_tv_season_groups<'a>(
    show: &PlexTvShowGroup<'a>,
    roots: &[PathBuf],
) -> BTreeMap<u32, Vec<&'a Media>> {
    let mut seasons = BTreeMap::<u32, Vec<&Media>>::new();
    for media in &show.episodes {
        let season = roots
            .iter()
            .find(|root| media.path.starts_with(root))
            .and_then(|root| tv_episode_info(root, &media.path))
            .map(|episode| episode.season)
            .unwrap_or(1);
        seasons.entry(season).or_default().push(media);
    }
    for episodes in seasons.values_mut() {
        episodes.sort_by_key(|media| {
            roots
                .iter()
                .find(|root| media.path.starts_with(root))
                .and_then(|root| tv_episode_info(root, &media.path))
                .map(|episode| (episode.episode, media.title.to_lowercase()))
                .unwrap_or((u32::MAX, media.title.to_lowercase()))
        });
    }
    seasons
}

fn plex_tv_show_json(
    show: &PlexTvShowGroup<'_>,
    section: usize,
    library: &ConfigLibrary,
    metadata: &HashMap<String, serde_json::Value>,
) -> serde_json::Value {
    let Some(representative) = show.episodes.first() else {
        return serde_json::Value::Null;
    };
    let item_metadata = metadata.get(&representative.id);
    let mut item = plex_video_json(representative, section, library, item_metadata);
    let title = item_metadata
        .and_then(|metadata| metadata.get("name"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or(&show.title);
    item["ratingKey"] = serde_json::json!(show.rating_key);
    item["key"] = serde_json::json!(format!("/library/metadata/{}/children", show.rating_key));
    item["type"] = serde_json::json!("show");
    item["title"] = serde_json::json!(title);
    item["titleSort"] = serde_json::json!(title.to_lowercase());
    item["originalTitle"] = serde_json::json!(title);
    item["childCount"] = serde_json::json!(plex_tv_season_groups(show, &library.folders).len());
    item["leafCount"] = serde_json::json!(show.episodes.len());
    let viewed_episodes = show
        .episodes
        .iter()
        .filter(|episode| {
            metadata
                .get(&episode.id)
                .and_then(|metadata| metadata.get("mplex_view_count"))
                .and_then(serde_json::Value::as_i64)
                .is_some_and(|view_count| view_count > 0)
        })
        .count();
    item["viewedLeafCount"] = serde_json::json!(viewed_episodes);
    item["guid"] = serde_json::json!(plex_tv_show_guid(show, metadata));
    if let Some(object) = item.as_object_mut() {
        object.remove("Media");
        object.remove("duration");
        object.remove("viewOffset");
        object.remove("viewCount");
        object.remove("lastViewedAt");
        object.remove("parentIndex");
        object.remove("parentTitle");
        object.remove("grandparentTitle");
    }
    item
}

fn plex_tv_season_json(
    show: &PlexTvShowGroup<'_>,
    season: u32,
    section: usize,
    library: &ConfigLibrary,
    metadata: &HashMap<String, serde_json::Value>,
    episode_count: usize,
) -> serde_json::Value {
    let season_key = plex_tv_season_key(&show.rating_key, season);
    let representative = show
        .episodes
        .iter()
        .find(|media| {
            library
                .folders
                .iter()
                .find(|root| media.path.starts_with(root))
                .and_then(|root| tv_episode_info(root, &media.path))
                .map(|episode| episode.season == season)
                .unwrap_or(season == 1)
        })
        .or_else(|| show.episodes.first());
    let mut item = representative
        .map(|media| plex_video_json(media, section, library, metadata.get(&media.id)))
        .unwrap_or_else(|| serde_json::json!({}));
    let title = if season == 0 {
        "Specials".to_owned()
    } else {
        format!("Season {season}")
    };
    item["ratingKey"] = serde_json::json!(season_key);
    item["key"] = serde_json::json!(format!("/library/metadata/{season_key}/children"));
    item["guid"] = serde_json::json!(plex_metadata_guid("season", &season_key));
    item["parentGuid"] = serde_json::json!(plex_tv_show_guid(show, metadata));
    item["type"] = serde_json::json!("season");
    item["title"] = serde_json::json!(title);
    item["titleSort"] = serde_json::json!(title.to_lowercase());
    item["index"] = serde_json::json!(season);
    item["parentRatingKey"] = serde_json::json!(show.rating_key);
    item["parentKey"] = serde_json::json!(format!("/library/metadata/{}", show.rating_key));
    item["parentType"] = serde_json::json!("show");
    item["parentTitle"] = serde_json::json!(show.title);
    item["parentIndex"] = serde_json::json!(season);
    item["childCount"] = serde_json::json!(episode_count);
    item["leafCount"] = serde_json::json!(episode_count);
    if let Some(object) = item.as_object_mut() {
        object.remove("Media");
        object.remove("duration");
        object.remove("viewOffset");
        object.remove("viewCount");
        object.remove("lastViewedAt");
        object.remove("grandparentTitle");
        object.remove("grandparentRatingKey");
    }
    item
}

fn plex_tv_episode_parents(
    item: &mut serde_json::Value,
    show: &PlexTvShowGroup<'_>,
    season: u32,
    episode: u32,
    show_guid: &str,
) {
    let season_key = plex_tv_season_key(&show.rating_key, season);
    item["parentGuid"] = serde_json::json!(plex_metadata_guid("season", &season_key));
    item["grandparentGuid"] = serde_json::json!(show_guid);
    item["grandparentRatingKey"] = serde_json::json!(show.rating_key);
    item["grandparentKey"] = serde_json::json!(format!("/library/metadata/{}", show.rating_key));
    item["grandparentTitle"] = serde_json::json!(show.title);
    item["grandparentType"] = serde_json::json!("show");
    item["parentRatingKey"] = serde_json::json!(season_key);
    item["parentKey"] = serde_json::json!(format!("/library/metadata/{season_key}"));
    item["parentTitle"] = serde_json::json!(if season == 0 {
        "Specials".to_owned()
    } else {
        format!("Season {season}")
    });
    item["parentType"] = serde_json::json!("season");
    item["parentIndex"] = serde_json::json!(season);
    item["index"] = serde_json::json!(episode);
    if let Some(representative) = show.episodes.first() {
        let representative_key = plex_rating_key(representative);
        item["grandparentThumb"] =
            serde_json::json!(format!("/library/metadata/{representative_key}/thumb"));
        item["grandparentArt"] =
            serde_json::json!(format!("/library/metadata/{representative_key}/art"));
    }
}

fn plex_library_media_container(
    library: &ConfigLibrary,
    section: usize,
    library_uuid: &str,
    size: usize,
    offset: usize,
    total_size: usize,
) -> serde_json::Value {
    let view_group = plex_kind(&library.kind);
    serde_json::json!({
        "size": size,
        "totalSize": total_size,
        "offset": offset,
        "allowSync": false,
        "identifier": "com.plexapp.plugins.library",
        "content": "secondary",
        "librarySectionID": section,
        "librarySectionTitle": library.name,
        "librarySectionUUID": library_uuid,
        "title1": library.name,
        "title2": format!("All {}", library.name),
        "viewGroup": view_group
    })
}

fn plex_library_children_container(
    library: &ConfigLibrary,
    section: usize,
    library_uuid: &str,
    size: usize,
    offset: usize,
    total_size: usize,
    view_group: &str,
) -> serde_json::Value {
    let mut container =
        plex_library_media_container(library, section, library_uuid, size, offset, total_size);
    container["viewGroup"] = serde_json::json!(view_group);
    container
}

fn plex_container_page(headers: &HeaderMap, uri: &Uri) -> Option<(usize, usize)> {
    fn value(headers: &HeaderMap, uri: &Uri, name: &'static str) -> Option<usize> {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .or_else(|| plex_query_value(uri, name))
            .and_then(|value| value.parse().ok())
    }

    Some((
        value(headers, uri, "X-Plex-Container-Start")?,
        value(headers, uri, "X-Plex-Container-Size")?,
    ))
}

fn plex_paged_children<T: Clone>(items: &[T], headers: &HeaderMap, uri: &Uri) -> (usize, Vec<T>) {
    let total = items.len();
    let (offset, size) = plex_container_page(headers, uri).unwrap_or((0, 20));
    let offset = offset.min(total);
    let end = offset.saturating_add(size).min(total);
    (offset, items[offset..end].to_vec())
}

fn plex_children_json(
    items: Vec<serde_json::Value>,
    total: usize,
    offset: usize,
) -> serde_json::Value {
    serde_json::json!({
        "size": items.len(),
        "totalSize": total,
        "offset": offset,
        "Metadata": items
    })
}

fn find_plex_tv_location<'a>(
    state: &AppState,
    media_items: &'a [Media],
    rating_key: &str,
) -> Option<(usize, ConfigLibrary, PlexTvShowGroup<'a>, Option<u32>)> {
    let libraries = state.config.lock().ok()?.libraries.clone();
    for (index, library) in libraries.into_iter().enumerate() {
        if library.kind != "tv" {
            continue;
        }
        let section = index + 1;
        let shows = plex_tv_show_groups(
            section,
            &library.folders,
            media_items
                .iter()
                .filter(|media| media_in_library(media, &library)),
        );
        for show in shows {
            if show.rating_key == rating_key {
                return Some((section, library, show, None));
            }
            for season in plex_tv_season_groups(&show, &library.folders)
                .keys()
                .copied()
            {
                if plex_tv_season_key(&show.rating_key, season) == rating_key {
                    return Some((section, library, show, Some(season)));
                }
            }
        }
    }
    None
}

async fn plex_item(
    State(s): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let wants_children = uri.path().ends_with("/children");
    let include_children = plex_query_value(&uri, "includeChildren") == Some("1");
    let media_index = media_snapshot(&s);
    if let Some((section, library, mut show, season)) =
        find_plex_tv_location(&s, media_index.as_ref(), &id)
    {
        let metadata = plex_section_metadata(&s, section);
        if let Some(show_title) = show
            .episodes
            .first()
            .and_then(|episode| metadata.get(&episode.id))
            .and_then(|metadata| metadata.get("name"))
            .and_then(serde_json::Value::as_str)
        {
            show.title = show_title.to_owned();
        }
        let show_guid = plex_tv_show_guid(&show, &metadata);
        let uuid = plex_library_uuid(&s, &library.name, section);
        if let Some(season) = season {
            let season_episodes = plex_tv_season_groups(&show, &library.folders)
                .remove(&season)
                .unwrap_or_default();
            let mut episodes = Vec::with_capacity(season_episodes.len());
            for (index, media) in season_episodes.iter().enumerate() {
                let episode_number = library
                    .folders
                    .iter()
                    .find(|root| media.path.starts_with(root))
                    .and_then(|root| tv_episode_info(root, &media.path))
                    .map(|episode| episode.episode)
                    .unwrap_or(index as u32 + 1);
                let mut item = plex_video_json(media, section, &library, metadata.get(&media.id));
                plex_tv_episode_parents(&mut item, &show, season, episode_number, &show_guid);
                add_plex_technical_metadata(&s, &mut item, media).await;
                episodes.push(item);
            }
            if wants_children {
                let (offset, episodes) = plex_paged_children(&episodes, &headers, &uri);
                let mut container = plex_library_children_container(
                    &library,
                    section,
                    &uuid,
                    episodes.len(),
                    offset,
                    season_episodes.len(),
                    "episode",
                );
                container["Metadata"] = serde_json::json!(episodes);
                container["offset"] = serde_json::json!(offset);
                container["title2"] = serde_json::json!(if season == 0 {
                    "Specials".to_owned()
                } else {
                    format!("Season {season}")
                });
                return plex_json(serde_json::json!({"MediaContainer": container}));
            }
            let mut item = plex_tv_season_json(
                &show,
                season,
                section,
                &library,
                &metadata,
                season_episodes.len(),
            );
            if include_children {
                let (offset, episodes) = plex_paged_children(&episodes, &headers, &uri);
                item["Children"] = plex_children_json(episodes, season_episodes.len(), offset);
            }
            let mut container = plex_library_media_container(&library, section, &uuid, 1, 0, 1);
            container["Metadata"] = serde_json::json!([item]);
            return plex_json(serde_json::json!({"MediaContainer": container}));
        }
        let seasons = plex_tv_season_groups(&show, &library.folders);
        let season_items = seasons
            .iter()
            .map(|(season, episodes)| {
                plex_tv_season_json(&show, *season, section, &library, &metadata, episodes.len())
            })
            .collect::<Vec<_>>();
        if wants_children {
            let (offset, items) = plex_paged_children(&season_items, &headers, &uri);
            let mut container = plex_library_children_container(
                &library,
                section,
                &uuid,
                items.len(),
                offset,
                season_items.len(),
                "season",
            );
            container["Metadata"] = serde_json::json!(items);
            container["offset"] = serde_json::json!(offset);
            container["title2"] = serde_json::json!(show.title);
            return plex_json(serde_json::json!({"MediaContainer": container}));
        }
        let mut item = plex_tv_show_json(&show, section, &library, &metadata);
        if include_children {
            let (offset, items) = plex_paged_children(&season_items, &headers, &uri);
            item["Children"] = plex_children_json(items, season_items.len(), offset);
        }
        let mut container = plex_library_media_container(&library, section, &uuid, 1, 0, 1);
        container["Metadata"] = serde_json::json!([item]);
        return plex_json(serde_json::json!({"MediaContainer": container}));
    }
    let Some(media) = find_media(&s, &id) else {
        let media_key = id
            .split_once('-')
            .filter(|(section, item)| {
                section.parse::<usize>().is_ok() && item.parse::<u64>().is_ok()
            })
            .map(|_| id.clone())
            .or_else(|| {
                id.parse::<u64>().ok().and_then(|rating_key| {
                    let section = rating_key >> 20;
                    let item = rating_key & ((1 << 20) - 1);
                    (section > 0 && item > 0).then(|| format!("{section}-{item}"))
                })
            });
        let known_stale_key = media_key.as_ref().is_some_and(|key| {
            s.db.lock()
                .ok()
                .and_then(|connection| {
                    connection
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM media_identity WHERE media_key = ?1)",
                            [key],
                            |row| row.get::<_, bool>(0),
                        )
                        .ok()
                })
                .unwrap_or(false)
        });
        let section = media_key
            .as_deref()
            .and_then(|key| key.split_once('-'))
            .and_then(|(section, _)| section.parse::<usize>().ok())
            .filter(|section| {
                *section > 0
                    && s.config
                        .lock()
                        .map(|config| *section <= config.libraries.len())
                        .unwrap_or(false)
            });
        if known_stale_key {
            if let Some(section) = section {
                if scan_library_inventory(&s, section - 1).await.is_ok() {
                    if let Ok(config) = s.config.lock().map(|config| config.clone()) {
                        if let Some(library) = config.libraries.get(section - 1) {
                            let mut container = plex_library_media_container(
                                library,
                                section,
                                &plex_library_uuid(&s, &library.name, section),
                                0,
                                0,
                                0,
                            );
                            container["Metadata"] = serde_json::json!([]);
                            return plex_json(serde_json::json!({"MediaContainer": container}));
                        }
                    }
                }
            }
        }
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some((section, library)) = media_library(&s, &media) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let metadata_by_id = plex_section_metadata(&s, section);
    let mut video = plex_video_json(&media, section, &library, metadata_by_id.get(&media.id));
    if library.kind == "tv" {
        let groups = plex_tv_show_groups(
            section,
            &library.folders,
            media_index
                .iter()
                .filter(|candidate| media_in_library(candidate, &library)),
        );
        if let Some(show) = groups
            .into_iter()
            .find(|show| show.episodes.iter().any(|episode| episode.id == media.id))
        {
            let episode_info = library
                .folders
                .iter()
                .find(|root| media.path.starts_with(root))
                .and_then(|root| tv_episode_info(root, &media.path));
            let season = episode_info
                .as_ref()
                .map(|episode| episode.season)
                .unwrap_or(1);
            let episode = episode_info
                .as_ref()
                .map(|episode| episode.episode)
                .unwrap_or(1);
            let show_guid = plex_tv_show_guid(&show, &metadata_by_id);
            plex_tv_episode_parents(&mut video, &show, season, episode, &show_guid);
            if let Some(show_title) = metadata_by_id
                .get(&media.id)
                .and_then(|metadata| metadata.get("name"))
                .and_then(serde_json::Value::as_str)
            {
                video["grandparentTitle"] = serde_json::json!(show_title);
            }
        }
    }
    add_plex_technical_metadata(&s, &mut video, &media).await;
    // Plex for Mac refetches the selected item after creating the queue and
    // bases its playability check on this metadata response as well.
    plex_mark_direct_play(&mut video);
    let mut container = plex_library_media_container(
        &library,
        section,
        &plex_library_uuid(&s, &library.name, section),
        1,
        0,
        1,
    );
    plex_mark_direct_play_decision(&mut container);
    container["Metadata"] = serde_json::json!([video]);
    plex_json(serde_json::json!({
        "MediaContainer": container
    }))
}

fn plex_empty_extras_response(
    library: &ConfigLibrary,
    section: usize,
    library_uuid: &str,
) -> serde_json::Value {
    let mut container = plex_library_media_container(library, section, library_uuid, 0, 0, 0);
    container["title2"] = serde_json::json!("Extras");
    container["Metadata"] = serde_json::json!([]);
    serde_json::json!({"MediaContainer": container})
}

async fn plex_extras(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let Some((section, library)) = plex_library_for_metadata(&state, &id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    plex_json(plex_empty_extras_response(
        &library,
        section,
        &plex_library_uuid(&state, &library.name, section),
    ))
}

async fn plex_part(
    State(s): State<AppState>,
    Path((id, _part)): Path<(String, String)>,
) -> Response {
    plex_item(
        State(s),
        Path(id),
        HeaderMap::new(),
        Uri::from_static("/library/metadata/item"),
    )
    .await
}

fn plex_rating_key_from_source(source: &str) -> Option<String> {
    let source = source.split('?').next().unwrap_or(source);
    let id = source
        .rsplit_once("/library/metadata/")?
        .1
        .split('/')
        .next()?;
    (!id.is_empty()).then(|| id.to_owned())
}

#[derive(Clone)]
struct PlexPlaylistRecord {
    id: i64,
    title: String,
    playlist_type: String,
    smart: bool,
    added_at: i64,
    updated_at: i64,
}

#[derive(Clone)]
struct PlexCollectionRecord {
    id: i64,
    title: String,
    section_id: i64,
    added_at: i64,
    updated_at: i64,
}

fn plex_collection_record(
    connection: &Connection,
    id: i64,
) -> rusqlite::Result<PlexCollectionRecord> {
    connection.query_row(
        "SELECT id, title, section_id, added_at, updated_at FROM collections WHERE id = ?1",
        [id],
        |row| {
            Ok(PlexCollectionRecord {
                id: row.get(0)?,
                title: row.get(1)?,
                section_id: row.get(2)?,
                added_at: row.get(3)?,
                updated_at: row.get(4)?,
            })
        },
    )
}

fn plex_collection_records(connection: &Connection) -> rusqlite::Result<Vec<PlexCollectionRecord>> {
    let mut statement = connection.prepare(
        "SELECT id, title, section_id, added_at, updated_at FROM collections ORDER BY title COLLATE NOCASE, id",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(PlexCollectionRecord {
            id: row.get(0)?,
            title: row.get(1)?,
            section_id: row.get(2)?,
            added_at: row.get(3)?,
            updated_at: row.get(4)?,
        })
    })?;
    rows.collect()
}

fn plex_collection_summary(
    connection: &Connection,
    collection: &PlexCollectionRecord,
    libraries: &[ConfigLibrary],
    machine_identifier: &str,
) -> rusqlite::Result<serde_json::Value> {
    let item_count = connection.query_row(
        "SELECT COUNT(*) FROM collection_items WHERE collection_id = ?1",
        [collection.id],
        |row| row.get::<_, i64>(0),
    )?;
    let mut summary = serde_json::json!({
        "ratingKey": collection.id.to_string(),
        "key": format!("/library/collections/{}/items", collection.id),
        "type": "collection",
        "title": collection.title,
        "titleSort": collection.title.to_lowercase(),
        "summary": "",
        "smart": false,
        "childCount": item_count,
        "leafCount": item_count,
        "addedAt": collection.added_at,
        "updatedAt": collection.updated_at
    });
    if let Ok(section) = usize::try_from(collection.section_id) {
        if let Some(library) = section
            .checked_sub(1)
            .and_then(|index| libraries.get(index))
        {
            summary["librarySectionID"] = serde_json::json!(section);
            summary["librarySectionTitle"] = serde_json::json!(library.name);
            let stable_id = connection
                .query_row(
                    "SELECT id FROM library_groups WHERE name = ?1",
                    [&library.name],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap_or(section as i64);
            summary["librarySectionUUID"] =
                serde_json::json!(plex_library_uuid_for_id(machine_identifier, stable_id));
            summary["librarySectionKey"] =
                serde_json::json!(format!("/library/sections/{section}"));
        }
    }
    Ok(summary)
}

fn plex_collection_container(
    connection: &Connection,
    collections: &[PlexCollectionRecord],
    libraries: &[ConfigLibrary],
    machine_identifier: &str,
) -> rusqlite::Result<serde_json::Value> {
    let metadata = collections
        .iter()
        .map(|collection| {
            plex_collection_summary(connection, collection, libraries, machine_identifier)
        })
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(serde_json::json!({
        "MediaContainer": {
            "size": metadata.len(),
            "totalSize": metadata.len(),
            "offset": 0,
            "identifier": "com.plexapp.plugins.library",
            "Metadata": metadata
        }
    }))
}

async fn plex_collections(
    State(state): State<AppState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let libraries = match state.config.lock() {
        Ok(config) => config.libraries.clone(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let requested_section = match query.get("sectionId") {
        Some(value) => match value.parse::<usize>() {
            Ok(section) if section > 0 && section <= libraries.len() => Some(section as i64),
            Ok(_) => return StatusCode::NOT_FOUND.into_response(),
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        },
        None => None,
    };
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Ok(mut collections) = plex_collection_records(&connection) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if let Some(section) = requested_section {
        collections.retain(|collection| collection.section_id == section);
    }
    match plex_collection_container(
        &connection,
        &collections,
        &libraries,
        &state.machine_identifier,
    ) {
        Ok(body) => plex_json(body),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn plex_create_collection(
    State(state): State<AppState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(title) = query
        .get("title")
        .map(|title| title.trim())
        .filter(|title| !title.is_empty())
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if query
        .get("smart")
        .is_some_and(|smart| !matches!(smart.as_str(), "0" | "false"))
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let libraries = match state.config.lock() {
        Ok(config) => config.libraries.clone(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let section = match query.get("sectionId") {
        Some(value) => match value.parse::<usize>() {
            Ok(section) if section > 0 && section <= libraries.len() => section,
            Ok(_) => return StatusCode::NOT_FOUND.into_response(),
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        },
        None if !libraries.is_empty() => 1,
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if connection
        .execute(
            "INSERT INTO collections (title, section_id, added_at, updated_at) VALUES (?1, ?2, ?3, ?3)",
            (title, section as i64, updated_at),
        )
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let Ok(collection) = plex_collection_record(&connection, connection.last_insert_rowid()) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Ok(body) = plex_collection_container(
        &connection,
        &[collection],
        &libraries,
        &state.machine_identifier,
    ) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let mut response = plex_json(body);
    *response.status_mut() = StatusCode::CREATED;
    response
}

async fn plex_collection_items(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    let entries = {
        let Ok(connection) = state.db.lock() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        if plex_collection_record(&connection, id).is_err() {
            return StatusCode::NOT_FOUND.into_response();
        }
        let mut statement = match connection.prepare(
            "SELECT id, media_key FROM collection_items WHERE collection_id = ?1 ORDER BY position, id",
        ) {
            Ok(statement) => statement,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let rows = match statement.query_map([id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        }) {
            Ok(rows) => rows,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        match rows.collect::<rusqlite::Result<Vec<_>>>() {
            Ok(entries) => entries,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    };
    let mut metadata = Vec::new();
    for (item_id, media_key) in entries {
        if let Some(mut item) = plex_metadata_item_json(&state, &media_key).await {
            item["collectionItemID"] = serde_json::json!(item_id);
            metadata.push(item);
        }
    }
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": metadata.len(),
            "totalSize": metadata.len(),
            "offset": 0,
            "identifier": "com.plexapp.plugins.library",
            "Metadata": metadata
        }
    }))
}

async fn plex_add_collection_item(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let collection = {
        let Ok(connection) = state.db.lock() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        match plex_collection_record(&connection, id) {
            Ok(collection) => collection,
            Err(_) => return StatusCode::NOT_FOUND.into_response(),
        }
    };
    let Some(source) = query.get("uri").or_else(|| query.get("key")) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(media_id) = plex_rating_key_from_source(source)
        .or_else(|| source.parse::<u64>().ok().map(|key| key.to_string()))
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(media) = find_media(&state, &media_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some((section, _)) = media_library(&state, &media) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if collection.section_id != section as i64 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let libraries = match state.config.lock() {
        Ok(config) => config.libraries.clone(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let media_key = plex_rating_key(&media).to_string();
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let next_position = match connection.query_row(
        "SELECT COALESCE(MAX(position), -1) + 1 FROM collection_items WHERE collection_id = ?1",
        [id],
        |row| row.get::<_, i64>(0),
    ) {
        Ok(position) => position,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if connection
        .execute(
            "INSERT OR IGNORE INTO collection_items (collection_id, media_key, position, added_at) VALUES (?1, ?2, ?3, ?4)",
            (id, media_key, next_position, updated_at),
        )
        .and_then(|_| {
            connection.execute(
                "UPDATE collections SET updated_at = ?1 WHERE id = ?2",
                (updated_at, id),
            )
        })
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let Ok(collection) = plex_collection_record(&connection, id) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    match plex_collection_container(
        &connection,
        &[collection],
        &libraries,
        &state.machine_identifier,
    ) {
        Ok(body) => plex_json(body),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn plex_remove_collection_item(
    State(state): State<AppState>,
    Path((id, item_id)): Path<(i64, String)>,
) -> Response {
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if plex_collection_record(&connection, id).is_err() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let media_key = find_media(&state, &item_id)
        .map(|media| plex_rating_key(&media).to_string())
        .unwrap_or_else(|| item_id.clone());
    let mut result = connection.execute(
        "DELETE FROM collection_items WHERE collection_id = ?1 AND media_key = ?2",
        (id, &media_key),
    );
    if matches!(result, Ok(0)) {
        if let Ok(item_id) = item_id.parse::<i64>() {
            result = connection.execute(
                "DELETE FROM collection_items WHERE collection_id = ?1 AND id = ?2",
                (id, item_id),
            );
        }
    }
    match result {
        Ok(0) => StatusCode::NOT_FOUND.into_response(),
        Ok(_) => {
            let updated_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs() as i64)
                .unwrap_or_default();
            if connection
                .execute(
                    "UPDATE collections SET updated_at = ?1 WHERE id = ?2",
                    (updated_at, id),
                )
                .is_err()
            {
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            } else {
                StatusCode::OK.into_response()
            }
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

fn plex_playlist_record(connection: &Connection, id: i64) -> rusqlite::Result<PlexPlaylistRecord> {
    connection.query_row(
        "SELECT id, title, playlist_type, smart, added_at, updated_at FROM playlists WHERE id = ?1",
        [id],
        |row| {
            Ok(PlexPlaylistRecord {
                id: row.get(0)?,
                title: row.get(1)?,
                playlist_type: row.get(2)?,
                smart: row.get(3)?,
                added_at: row.get(4)?,
                updated_at: row.get(5)?,
            })
        },
    )
}

fn plex_playlist_records(connection: &Connection) -> rusqlite::Result<Vec<PlexPlaylistRecord>> {
    let mut statement = connection.prepare(
        "SELECT id, title, playlist_type, smart, added_at, updated_at FROM playlists ORDER BY title COLLATE NOCASE, id",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(PlexPlaylistRecord {
            id: row.get(0)?,
            title: row.get(1)?,
            playlist_type: row.get(2)?,
            smart: row.get(3)?,
            added_at: row.get(4)?,
            updated_at: row.get(5)?,
        })
    })?;
    rows.collect()
}

fn plex_playlist_summary(
    connection: &Connection,
    playlist: &PlexPlaylistRecord,
) -> rusqlite::Result<serde_json::Value> {
    let leaf_count = connection.query_row(
        "SELECT COUNT(*) FROM playlist_items WHERE playlist_id = ?1",
        [playlist.id],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(serde_json::json!({
        "ratingKey": playlist.id.to_string(),
        "key": format!("/playlists/{}", playlist.id),
        "type": "playlist",
        "title": playlist.title,
        "titleSort": playlist.title.to_lowercase(),
        "playlistType": playlist.playlist_type,
        "smart": playlist.smart,
        "leafCount": leaf_count,
        "childCount": leaf_count,
        "addedAt": playlist.added_at,
        "updatedAt": playlist.updated_at
    }))
}

fn plex_playlist_container(
    connection: &Connection,
    playlists: &[PlexPlaylistRecord],
) -> rusqlite::Result<serde_json::Value> {
    let metadata = playlists
        .iter()
        .map(|playlist| plex_playlist_summary(connection, playlist))
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(serde_json::json!({
        "MediaContainer": {
            "size": metadata.len(),
            "totalSize": metadata.len(),
            "identifier": "com.plexapp.plugins.library",
            "Metadata": metadata
        }
    }))
}

async fn plex_playlists(
    State(state): State<AppState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Ok(mut playlists) = plex_playlist_records(&connection) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if let Some(playlist_type) = query.get("playlistType") {
        playlists.retain(|playlist| &playlist.playlist_type == playlist_type);
    }
    if let Some(smart) = query.get("smart") {
        let wants_smart = matches!(smart.as_str(), "1" | "true");
        playlists.retain(|playlist| playlist.smart == wants_smart);
    }
    match plex_playlist_container(&connection, &playlists) {
        Ok(body) => plex_json(body),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn plex_create_playlist(
    State(state): State<AppState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(title) = query
        .get("title")
        .map(|title| title.trim())
        .filter(|title| !title.is_empty())
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let playlist_type = query
        .get("playlistType")
        .or_else(|| query.get("type"))
        .map(String::as_str)
        .unwrap_or("video");
    if playlist_type != "video"
        || query
            .get("smart")
            .is_some_and(|smart| !matches!(smart.as_str(), "0" | "false"))
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if connection
        .execute(
            "INSERT INTO playlists (title, playlist_type, smart, added_at, updated_at) VALUES (?1, 'video', 0, ?2, ?2)",
            (title, updated_at),
        )
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let playlist_id = connection.last_insert_rowid();
    let Ok(playlist) = plex_playlist_record(&connection, playlist_id) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Ok(body) = plex_playlist_container(&connection, &[playlist]) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let mut response = plex_json(body);
    *response.status_mut() = StatusCode::CREATED;
    response
}

async fn plex_get_playlist(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Ok(playlist) = plex_playlist_record(&connection, id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match plex_playlist_container(&connection, &[playlist]) {
        Ok(body) => plex_json(body),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn plex_update_playlist(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if plex_playlist_record(&connection, id).is_err() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Some(title) = query
        .get("title")
        .map(|title| title.trim())
        .filter(|title| !title.is_empty())
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();
    if connection
        .execute(
            "UPDATE playlists SET title = ?1, updated_at = ?2 WHERE id = ?3",
            (title, updated_at, id),
        )
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let Ok(playlist) = plex_playlist_record(&connection, id) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    match plex_playlist_container(&connection, &[playlist]) {
        Ok(body) => plex_json(body),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn plex_delete_playlist(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if plex_playlist_record(&connection, id).is_err() {
        return StatusCode::NOT_FOUND.into_response();
    }
    if connection
        .execute("DELETE FROM playlist_items WHERE playlist_id = ?1", [id])
        .and_then(|_| connection.execute("DELETE FROM playlists WHERE id = ?1", [id]))
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn plex_metadata_item_json(state: &AppState, rating_key: &str) -> Option<serde_json::Value> {
    let uri = Uri::try_from(format!("/library/metadata/{rating_key}")).ok()?;
    let response = plex_item(
        State(state.clone()),
        Path(rating_key.to_owned()),
        HeaderMap::new(),
        uri,
    )
    .await;
    if response.status() != StatusCode::OK {
        return None;
    }
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .ok()?;
    serde_json::from_slice::<serde_json::Value>(&body)
        .ok()?
        .pointer("/MediaContainer/Metadata/0")
        .cloned()
}

async fn plex_playlist_items(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    let entries = {
        let Ok(connection) = state.db.lock() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        if plex_playlist_record(&connection, id).is_err() {
            return StatusCode::NOT_FOUND.into_response();
        }
        let mut statement = match connection.prepare(
            "SELECT id, media_key FROM playlist_items WHERE playlist_id = ?1 ORDER BY position, id",
        ) {
            Ok(statement) => statement,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let rows = match statement.query_map([id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        }) {
            Ok(rows) => rows,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        match rows.collect::<rusqlite::Result<Vec<_>>>() {
            Ok(entries) => entries,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    };
    let mut metadata = Vec::new();
    for (item_id, rating_key) in entries {
        if let Some(mut item) = plex_metadata_item_json(&state, &rating_key).await {
            item["playlistItemID"] = serde_json::json!(item_id);
            metadata.push(item);
        }
    }
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": metadata.len(),
            "totalSize": metadata.len(),
            "offset": 0,
            "identifier": "com.plexapp.plugins.library",
            "Metadata": metadata
        }
    }))
}

async fn plex_add_playlist_item(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    {
        let Ok(connection) = state.db.lock() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        if plex_playlist_record(&connection, id).is_err() {
            return StatusCode::NOT_FOUND.into_response();
        }
    }
    let Some(source) = query.get("uri").or_else(|| query.get("key")) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(rating_key) = plex_rating_key_from_source(source)
        .or_else(|| source.parse::<u64>().ok().map(|key| key.to_string()))
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(media) = find_media(&state, &rating_key) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let rating_key = plex_rating_key(&media).to_string();
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let next_position = match connection.query_row(
        "SELECT COALESCE(MAX(position), -1) + 1 FROM playlist_items WHERE playlist_id = ?1",
        [id],
        |row| row.get::<_, i64>(0),
    ) {
        Ok(position) => position,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let added_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();
    if connection
        .execute(
            "INSERT INTO playlist_items (playlist_id, media_key, position, added_at) VALUES (?1, ?2, ?3, ?4)",
            (id, rating_key, next_position, added_at),
        )
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let Ok(playlist) = plex_playlist_record(&connection, id) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    match plex_playlist_container(&connection, &[playlist]) {
        Ok(body) => plex_json(body),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn plex_clear_playlist_items(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if plex_playlist_record(&connection, id).is_err() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let result = if let Some(item_id) = query.get("playlistItemID") {
        let Ok(item_id) = item_id.parse::<i64>() else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        connection.execute(
            "DELETE FROM playlist_items WHERE id = ?1 AND playlist_id = ?2",
            (item_id, id),
        )
    } else {
        connection.execute("DELETE FROM playlist_items WHERE playlist_id = ?1", [id])
    };
    match result {
        Ok(0) if query.contains_key("playlistItemID") => StatusCode::NOT_FOUND.into_response(),
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn plex_delete_playlist_item(
    State(state): State<AppState>,
    Path((playlist_id, item_id)): Path<(i64, i64)>,
) -> Response {
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if plex_playlist_record(&connection, playlist_id).is_err() {
        return StatusCode::NOT_FOUND.into_response();
    }
    match connection.execute(
        "DELETE FROM playlist_items WHERE playlist_id = ?1 AND id = ?2",
        (playlist_id, item_id),
    ) {
        Ok(0) => StatusCode::NOT_FOUND.into_response(),
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn plex_move_playlist_item(
    State(state): State<AppState>,
    Path((playlist_id, item_id)): Path<(i64, i64)>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Ok(connection) = state.db.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if plex_playlist_record(&connection, playlist_id).is_err() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let mut statement = match connection
        .prepare("SELECT id FROM playlist_items WHERE playlist_id = ?1 ORDER BY position, id")
    {
        Ok(statement) => statement,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let rows = match statement.query_map([playlist_id], |row| row.get::<_, i64>(0)) {
        Ok(rows) => rows,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let mut item_ids = match rows.collect::<rusqlite::Result<Vec<_>>>() {
        Ok(item_ids) => item_ids,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let Some(current_index) = item_ids.iter().position(|id| *id == item_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let after = match query.get("after") {
        Some(after) => match after.parse::<i64>() {
            Ok(after) => after,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        },
        None => -1,
    };
    item_ids.remove(current_index);
    let insertion_index = if after < 0 {
        0
    } else if let Some(index) = item_ids.iter().position(|id| *id == after) {
        index + 1
    } else {
        return StatusCode::NOT_FOUND.into_response();
    };
    item_ids.insert(insertion_index, item_id);
    for (position, item_id) in item_ids.iter().enumerate() {
        if connection
            .execute(
                "UPDATE playlist_items SET position = ?1 WHERE id = ?2 AND playlist_id = ?3",
                (position as i64, item_id, playlist_id),
            )
            .is_err()
        {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

fn plex_play_queue_source_uri(library_uuid: &str, rating_key: &str) -> String {
    let item_path = format!("/library/metadata/{rating_key}");
    format!("library://{library_uuid}/item/{}", url_encode(&item_path))
}

fn plex_play_queue_response(
    id: u64,
    source_uri: &str,
    items: Vec<serde_json::Value>,
    selected_index: usize,
    include_direct_play_decision: bool,
) -> serde_json::Value {
    let selected = items.get(selected_index);
    let selected_item_id = selected
        .and_then(|item| item.get("playQueueItemID"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let selected_metadata_item_id = selected
        .and_then(|item| item.get("ratingKey"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let media_tag_version = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    let mut response = serde_json::json!({
        "MediaContainer": {
            "identifier": "com.plexapp.plugins.library",
            "mediaTagPrefix": "/system/bundle/media/flags/",
            "mediaTagVersion": media_tag_version,
            "playQueueID": id,
            "playQueueSelectedItemID": selected_item_id,
            "playQueueSelectedItemOffset": 0,
            "playQueueSelectedMetadataItemID": selected_metadata_item_id,
            "playQueueShuffled": false,
            "playQueueSourceURI": source_uri,
            "playQueueTotalCount": items.len(),
            "playQueueVersion": 1,
            "size": items.len(),
            "Metadata": items
        }
    });
    if include_direct_play_decision {
        plex_mark_direct_play_decision(&mut response["MediaContainer"]);
    }
    response
}

fn plex_queue_item_without_playback_decision(item: &mut serde_json::Value) {
    let Some(media) = item
        .get_mut("Media")
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|media| media.first_mut())
    else {
        return;
    };
    for field in ["protocol", "decision", "selected"] {
        media[field] = serde_json::Value::Null;
    }
    let Some(part) = media
        .get_mut("Part")
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|parts| parts.first_mut())
    else {
        return;
    };
    for field in ["protocol", "decision", "selected"] {
        part[field] = serde_json::Value::Null;
    }
    if let Some(streams) = part
        .get_mut("Stream")
        .and_then(serde_json::Value::as_array_mut)
    {
        for stream in streams {
            stream["decision"] = serde_json::Value::Null;
            if stream["streamType"] == 1 || stream["selected"] == false {
                stream["selected"] = serde_json::Value::Null;
            }
        }
    }
}

async fn plex_create_play_queue(
    State(state): State<AppState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(source_uri) = query.get("uri") else {
        warn!(reason = "missing_uri", "Plex play queue request rejected");
        return StatusCode::BAD_REQUEST.into_response();
    };
    if !matches!(query.get("type").map(String::as_str), Some("video")) {
        warn!(
            queue_type = query.get("type").map(String::as_str).unwrap_or("<missing>"),
            reason = "unsupported_type",
            "Plex play queue request rejected"
        );
        return StatusCode::BAD_REQUEST.into_response();
    }
    let rating_key = query
        .get("key")
        .and_then(|key| plex_rating_key_from_source(key))
        .or_else(|| plex_rating_key_from_source(source_uri));
    let Some(rating_key) = rating_key else {
        warn!(
            reason = "unrecognized_item_uri",
            "Plex play queue request rejected"
        );
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(media) = find_media(&state, &rating_key) else {
        warn!(
            rating_key,
            reason = "item_not_found",
            "Plex play queue request rejected"
        );
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some((section, library)) = media_library(&state, &media) else {
        warn!(
            rating_key,
            reason = "item_has_no_library",
            "Plex play queue request rejected"
        );
        return StatusCode::NOT_FOUND.into_response();
    };
    let metadata = plex_section_metadata(&state, section);
    let mut item = plex_video_json(&media, section, &library, metadata.get(&media.id));
    add_plex_technical_metadata(&state, &mut item, &media).await;
    let direct_play = query.get("directPlay").map(String::as_str) != Some("0");
    if direct_play {
        plex_mark_direct_play(&mut item);
    } else {
        plex_queue_item_without_playback_decision(&mut item);
    }
    let queue_id = state
        .next_plex_play_queue_id
        .fetch_add(1, Ordering::Relaxed);
    // This queue currently contains one item, so its server-unique queue ID
    // also serves as the compact integer playQueueItemID expected by PMS clients.
    let queue_item_id = queue_id;
    item["playQueueItemID"] = serde_json::json!(queue_item_id);
    let library_uuid = plex_library_uuid(&state, &library.name, section);
    let plex_rating_key = plex_rating_key(&media).to_string();
    let normalized_source_uri = plex_play_queue_source_uri(&library_uuid, &plex_rating_key);
    let selected_metadata_item_id = plex_rating_key.parse::<u64>().unwrap_or_default();
    info!(
        queue_id,
        incoming_key = %rating_key,
        public_rating_key = %plex_rating_key,
        selected_metadata_item_id,
        queue_item_id,
        source_uri = %normalized_source_uri,
        "Plex play queue created"
    );
    let response =
        plex_play_queue_response(queue_id, &normalized_source_uri, vec![item], 0, direct_play);
    if let Ok(mut queues) = state.plex_play_queues.lock() {
        queues.insert(queue_id, response.clone());
    } else {
        warn!(queue_id, "Plex play queue response was not retained");
    }
    plex_json(response)
}

async fn plex_get_play_queue(State(state): State<AppState>, Path(id): Path<u64>) -> Response {
    state
        .plex_play_queues
        .lock()
        .ok()
        .and_then(|queues| queues.get(&id).cloned())
        .map(plex_json)
        .unwrap_or_else(|| StatusCode::NOT_FOUND.into_response())
}

async fn plex_playback_decision(
    State(state): State<AppState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let media_id = query
        .get("path")
        .or_else(|| query.get("key"))
        .and_then(|path| plex_query_media_id(None, Some(path)))
        .or_else(|| query.get("ratingKey").cloned())
        .or_else(|| query.get("mediaId").cloned());
    let Some(media_id) = media_id else {
        warn!(
            reason = "missing_media_id",
            "Plex playback decision rejected"
        );
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(media) = find_media(&state, &media_id) else {
        warn!(
            media_id,
            reason = "item_not_found",
            "Plex playback decision rejected"
        );
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some((section, library)) = media_library(&state, &media) else {
        warn!(
            media_id,
            reason = "item_has_no_library",
            "Plex playback decision rejected"
        );
        return StatusCode::NOT_FOUND.into_response();
    };

    let metadata_by_id = plex_section_metadata(&state, section);
    let mut video = plex_video_json(&media, section, &library, metadata_by_id.get(&media.id));
    add_plex_technical_metadata(&state, &mut video, &media).await;
    let library_uuid = plex_library_uuid(&state, &library.name, section);
    if query.get("directPlay").map(String::as_str) != Some("0") {
        plex_mark_direct_play(&mut video);
        info!(media_id, section, "Plex direct-play decision returned");
        return plex_json(plex_direct_play_decision_response(
            video,
            section,
            &library,
            &library_uuid,
        ));
    }

    let protocol = query.get("protocol").map(String::as_str).unwrap_or("http");
    warn!(
        media_id,
        section,
        protocol,
        reason = "transcoding_unavailable",
        "Plex playback conversion rejected"
    );
    StatusCode::NOT_IMPLEMENTED.into_response()
}

fn plex_direct_play_decision_response(
    video: serde_json::Value,
    section: usize,
    library: &ConfigLibrary,
    library_uuid: &str,
) -> serde_json::Value {
    let mut container = plex_library_media_container(library, section, library_uuid, 1, 0, 1);
    plex_mark_direct_play_decision(&mut container);
    // The client uses the overall decision fields to choose its playback
    // path. Returning a conversion failure here overrides the successful
    // direct-play result and produces Plex's "server is not powerful enough"
    // error before it requests the original media part. This response is a
    // direct-play decision; advertise that same successful result consistently.
    container["transcodeDecisionCode"] = serde_json::json!(1000);
    container["transcodeDecisionText"] = serde_json::json!("Direct play OK.");
    container["Metadata"] = serde_json::json!([video]);
    serde_json::json!({"MediaContainer": container})
}

fn plex_mark_direct_play_decision(container: &mut serde_json::Value) {
    container["directPlayDecisionCode"] = serde_json::json!(1000);
    container["directPlayDecisionText"] = serde_json::json!("Direct play OK.");
    container["generalDecisionCode"] = serde_json::json!(1000);
    container["generalDecisionText"] = serde_json::json!("Direct play OK.");
}

fn plex_mark_direct_play(video: &mut serde_json::Value) {
    let Some(media) = video
        .get_mut("Media")
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|media| media.first_mut())
    else {
        return;
    };
    media["decision"] = serde_json::json!("directplay");
    media["selected"] = serde_json::json!(true);
    media["protocol"] = serde_json::json!("http");
    if let Some(part) = media
        .get_mut("Part")
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|parts| parts.first_mut())
    {
        part["decision"] = serde_json::json!("directplay");
        part["selected"] = serde_json::json!(true);
        part["protocol"] = serde_json::json!("http");
        if let Some(streams) = part
            .get_mut("Stream")
            .and_then(serde_json::Value::as_array_mut)
        {
            for stream in streams {
                stream["decision"] = serde_json::json!("directplay");
                if stream.get("streamType").and_then(serde_json::Value::as_i64) == Some(3) {
                    continue;
                }
                if stream.get("selected").is_none() {
                    stream["selected"] = serde_json::json!(true);
                }
            }
        }
    }
}

#[derive(Clone, Copy, Deserialize, Default)]
struct PlexStreamSelection {
    #[serde(rename = "audioStreamID")]
    audio_stream_id: Option<u64>,
    #[serde(rename = "subtitleStreamID")]
    subtitle_stream_id: Option<u64>,
}

async fn plex_set_streams(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(selection): Query<PlexStreamSelection>,
) -> Response {
    let Some(media) = find_media_part(&state, &id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if selection.audio_stream_id.is_none() && selection.subtitle_stream_id.is_none() {
        return plex_json(serde_json::json!({"MediaContainer": {"size": 0}}));
    }
    let info = state
        .media_info
        .lock()
        .ok()
        .and_then(|cache| cache.get(&media.id).cloned());
    let info = match info {
        Some(info) => info,
        None => match inspect_media(&media.path).await {
            Some(info) => {
                if let Ok(mut cache) = state.media_info.lock() {
                    cache.insert(media.id.clone(), info.clone());
                }
                info
            }
            None => serde_json::json!({"media": {"track": []}}),
        },
    };
    let subtitle_paths = external_subtitle_files(&media.path).await;
    let audio_valid = selection.audio_stream_id.is_none_or(|selected| {
        technical_tracks(&info).iter().any(|track| {
            technical_text(track, "@type") == Some("Audio")
                && technical_track_id(track).saturating_add(1) == selected
        })
    });
    let subtitle_valid = selection.subtitle_stream_id.is_none_or(|selected| {
        selected == 0
            || technical_tracks(&info).iter().any(|track| {
                technical_text(track, "@type") == Some("Text")
                    && technical_track_id(track).saturating_add(1) == selected
            })
            || subtitle_paths
                .iter()
                .any(|path| plex_external_subtitle_id(&media, path) == selected)
    });
    if !audio_valid || !subtitle_valid {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if let Ok(mut selections) = state.plex_stream_selections.lock() {
        let current = selections.entry(plex_part_id(&media)).or_default();
        if selection.audio_stream_id.is_some() {
            current.audio_stream_id = selection.audio_stream_id;
        }
        if selection.subtitle_stream_id.is_some() {
            current.subtitle_stream_id = selection.subtitle_stream_id;
        }
    }
    plex_json(serde_json::json!({"MediaContainer": {"size": 0}}))
}

async fn plex_part_file(
    State(state): State<AppState>,
    Path((id, part_path)): Path<(String, String)>,
    uri: Uri,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let Some(media) = find_media_part(&state, &id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let expected = format!("file.{}", media.container);
    if part_path.rsplit('/').next() != Some(expected.as_str()) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let download = plex_query_value(&uri, "download") == Some("1");
    stream_file(&media, method, headers, download).await
}

async fn plex_stream(
    State(state): State<AppState>,
    Path(stream): Path<String>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some((stream_id, extension)) = plex_stream_id_and_format(&stream) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let media_index = media_snapshot(&state);
    for media in media_index.iter() {
        for subtitle_path in external_subtitle_files(&media.path).await {
            if plex_external_subtitle_id(media, &subtitle_path) != stream_id {
                continue;
            }
            let Ok(contents) = tokio::fs::read_to_string(&subtitle_path).await else {
                return StatusCode::NOT_FOUND.into_response();
            };
            let subtitle_extension = subtitle_path
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            let requested_format = query
                .get("format")
                .map(|format| format.to_ascii_lowercase())
                .unwrap_or_else(|| extension.to_owned());
            let Some((content_type, contents)) =
                external_subtitle_content(&contents, &subtitle_extension, &requested_format)
            else {
                return StatusCode::NOT_FOUND.into_response();
            };
            return ([(header::CONTENT_TYPE, content_type)], contents).into_response();
        }
    }
    StatusCode::NOT_FOUND.into_response()
}

fn plex_stream_id_and_format(stream: &str) -> Option<(u64, &str)> {
    let (id, extensions) = stream.split_once('.')?;
    let stream_id = id.parse::<u64>().ok()?;
    let mut requested_format = None;
    for extension in extensions.split('.') {
        if !matches!(extension, "srt" | "ass" | "vtt") {
            return None;
        }
        requested_format = Some(extension);
    }
    Some((stream_id, requested_format?))
}

fn external_subtitle_content(
    contents: &str,
    source_format: &str,
    requested_format: &str,
) -> Option<(&'static str, String)> {
    match requested_format {
        "srt" => Some((
            "text/srt; charset=utf-8",
            match source_format {
                "srt" => contents.to_owned(),
                "vtt" => vtt_to_srt(contents),
                "ass" => vtt_to_srt(&ass_to_vtt(contents)),
                _ => return None,
            },
        )),
        "vtt" | "webvtt" => Some((
            "text/vtt; charset=utf-8",
            match source_format {
                "srt" => srt_to_vtt(contents),
                "ass" => ass_to_vtt(contents),
                "vtt" => contents.to_owned(),
                _ => return None,
            },
        )),
        "ass" | "ssa" if source_format == "ass" => {
            Some(("text/x-ssa; charset=utf-8", contents.to_owned()))
        }
        _ => None,
    }
}

fn vtt_to_srt(contents: &str) -> String {
    let mut output = String::new();
    let mut cue = 0;
    for block in contents.replace("\r\n", "\n").split("\n\n") {
        let mut lines = block.lines();
        let first = lines.next().unwrap_or_default();
        if first.trim_start().starts_with("WEBVTT") {
            continue;
        }
        let (identifier, timing) = if first.contains(" --> ") {
            (None, first)
        } else {
            let second = lines.next().unwrap_or_default();
            (Some(first), second)
        };
        if !timing.contains(" --> ") {
            continue;
        }
        cue += 1;
        output.push_str(
            identifier
                .map(str::to_owned)
                .unwrap_or_else(|| cue.to_string())
                .as_str(),
        );
        output.push('\n');
        output.push_str(&timing.replace('.', ","));
        output.push('\n');
        for line in lines {
            output.push_str(line);
            output.push('\n');
        }
        output.push('\n');
    }
    output
}

async fn plex_thumb(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    plex_artwork(&state, &id, "cover.jpg").await
}

async fn plex_art(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    plex_artwork(&state, &id, "banner.jpg").await
}

async fn plex_artwork(state: &AppState, id: &str, filename: &str) -> Response {
    let Some(media) = find_media(state, id) else {
        info!(
            media_id = id,
            artwork = filename,
            "Plex artwork references unknown media"
        );
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some((library_id, library)) = media_library(state, &media) else {
        info!(
            media_id = id,
            artwork = filename,
            "Plex artwork media has no library"
        );
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(root) = library
        .folders
        .iter()
        .find(|folder| media.path.starts_with(folder))
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let sidecar_mode = state
        .config
        .lock()
        .is_ok_and(|config| config.metadata_storage == "sidecar");
    if sidecar_mode {
        let group_path = metadata_group_path(root, &media.path, &library.kind);
        let group_directory = if group_path.is_file() {
            group_path.parent().unwrap_or(root)
        } else {
            group_path.as_path()
        };
        let season_directory = media.path.parent().unwrap_or(root);
        let artwork_paths = if library.kind == "tv" && filename == "cover.jpg" {
            [
                season_directory.join(".mplex").join(filename),
                group_directory.join(".mplex").join(filename),
            ]
        } else {
            [
                group_directory.join(".mplex").join(filename),
                season_directory.join(".mplex").join(filename),
            ]
        };
        for artwork in artwork_paths {
            if let Ok(bytes) = tokio::fs::read(artwork).await {
                return (
                    [(header::CONTENT_TYPE, HeaderValue::from_static("image/jpeg"))],
                    bytes,
                )
                    .into_response();
            }
        }
    }
    if !sidecar_mode {
        let kind = match filename {
            "cover.jpg" => "cover",
            "banner.jpg" => "banner",
            _ => return StatusCode::NOT_FOUND.into_response(),
        };
        let media_key = database_media_key(library_id, &media.id);
        let artwork_path = state.db.lock().ok().and_then(|connection| {
            connection
                .query_row(
                    "SELECT local_path FROM artwork WHERE media_key = ?1 AND kind = ?2",
                    (&media_key, kind),
                    |row| row.get::<_, String>(0),
                )
                .ok()
        });
        if let Some(artwork_path) = artwork_path {
            if let Ok(bytes) = tokio::fs::read(artwork_path).await {
                return (
                    [(header::CONTENT_TYPE, HeaderValue::from_static("image/jpeg"))],
                    bytes,
                )
                    .into_response();
            }
        }
    }
    if filename == "cover.jpg" {
        if let Some(bytes) = plex_frame_cover(state, &media).await {
            return (
                [(header::CONTENT_TYPE, HeaderValue::from_static("image/jpeg"))],
                bytes,
            )
                .into_response();
        }
    }
    info!(
        media_id = id,
        artwork = filename,
        "Plex artwork unavailable"
    );
    StatusCode::NOT_FOUND.into_response()
}

async fn plex_frame_cover(state: &AppState, media: &Media) -> Option<Vec<u8>> {
    let cache_dir = state
        .config_path
        .parent()
        .unwrap_or(std::path::Path::new("config"))
        .join("artwork-cache");
    let cache_key = format!(
        "{:x}",
        Sha256::digest(format!(
            "{}:{}:{}",
            media.id,
            media.size,
            media_modified_at(media)
        ))
    );
    let cache_path = cache_dir.join(format!("{cache_key}.jpg"));
    if let Ok(bytes) = tokio::fs::read(&cache_path).await {
        return Some(bytes);
    }

    warn!(media_id = %media.id, "no cached cover; frame extraction requires an in-process decoder");
    None
}

fn plex_section_directories(state: &AppState) -> (usize, Vec<serde_json::Value>) {
    let libraries = state
        .config
        .lock()
        .map(|config| config.libraries.clone())
        .unwrap_or_default();
    let content_changed_at = library_catalog_updated_at(state);
    let directories_json = libraries
        .iter()
        .enumerate()
        .map(|(index, library)| {
            let section = index + 1;
            let (stable_id, scanned_at, updated_at) =
                library_publication_state(state, &library.name, section);
            let (created_at, locations) = plex_library_section_details(state, stable_id);
            serde_json::json!({
                "allowSync": true,
                "filters": true,
                "refreshing": false,
                "key": section.to_string(),
                "type": plex_kind(&library.kind),
                "title": library.name,
                "agent": library.plex_agent.as_deref().unwrap_or(if library.kind == "tv" { "tv.plex.agents.series" } else { "tv.plex.agents.movie" }),
                "scanner": library.plex_scanner.as_deref().unwrap_or(if library.kind == "tv" { "Plex TV Series" } else { "Plex Movie" }),
                "language": library.plex_language.as_deref().unwrap_or("en-US"),
                "metadataAgentProviderGroupId": library.metadata_agent_provider_group_id.unwrap_or(if library.kind == "tv" { 2 } else { 1 }),
                "uuid": plex_library_uuid(state, &library.name, section),
                "updatedAt": updated_at,
                "createdAt": created_at,
                "scannedAt": scanned_at,
                "content": true,
                "directory": true,
                "contentChangedAt": content_changed_at,
                "hidden": 0,
                "Location": locations
            })
        })
        .collect::<Vec<_>>();
    (libraries.len(), directories_json)
}

fn plex_kind(kind: &str) -> &'static str {
    match kind {
        "tv" => "show",
        "movie" => "movie",
        _ => "movie",
    }
}

fn media_in_library(media: &Media, library: &ConfigLibrary) -> bool {
    library
        .folders
        .iter()
        .any(|folder| media.path.starts_with(folder))
}

fn media_library(state: &AppState, media: &Media) -> Option<(usize, ConfigLibrary)> {
    state
        .config
        .lock()
        .ok()?
        .libraries
        .iter()
        .enumerate()
        .find(|(_, library)| media_in_library(media, library))
        .map(|(index, library)| (index + 1, library.clone()))
}

fn plex_video_json(
    media: &Media,
    section: usize,
    library: &ConfigLibrary,
    metadata: Option<&serde_json::Value>,
) -> serde_json::Value {
    let rating_key = plex_rating_key(media).to_string();
    let media_type = if library.kind == "tv" {
        "episode"
    } else {
        plex_kind(&library.kind)
    };
    let playback_duration = metadata
        .and_then(|metadata| metadata.get("mplex_duration"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_default();
    let title = if media_type == "episode" {
        tv_episode_display_name(&media.title)
    } else {
        media.title.clone()
    };
    let modified_at = media_modified_at(media);
    let mut item = serde_json::json!({
        "ratingKey": rating_key,
        "key": format!("/library/metadata/{rating_key}"),
        "type": media_type,
        "guid": plex_metadata_guid(media_type, &rating_key),
        "title": title,
        "thumb": format!("/library/metadata/{rating_key}/thumb"),
        "art": format!("/library/metadata/{rating_key}/art"),
        "librarySectionID": section,
        "librarySectionKey": format!("/library/sections/{section}"),
        "librarySectionTitle": library.name,
        "originalTitle": media.title,
        "titleSort": media.title.to_lowercase(),
        "updatedAt": modified_at.max(media.metadata_revision),
        "addedAt": modified_at,
        "duration": playback_duration,
        "viewOffset": 0,
        "viewCount": 0,
        "lastViewedAt": 0,
        "Media": [{
            "id": plex_media_id(media),
            "duration": playback_duration,
            "container": media.container,
            "protocol": "http",
            "has64bitOffsets": media.size > i32::MAX as u64,
            "optimizedForStreaming": false,
            "videoResolution": "unknown",
            "Part": [{
                "id": plex_part_id(media),
                "key": format!("/library/parts/{}/{}/file.{}", plex_part_id(media), modified_at, media.container),
                "duration": playback_duration,
                "file": media.path,
                "size": media.size,
                "container": media.container,
                "exists": true,
                "accessible": true,
                "has64bitOffsets": media.size > i32::MAX as u64,
                "optimizedForStreaming": false
            }]
        }]
    });
    if let Some(metadata) = metadata {
        if let Some(position) = metadata
            .get("mplex_view_offset")
            .and_then(serde_json::Value::as_i64)
        {
            item["viewOffset"] = serde_json::json!(position);
        }
        if let Some(count) = metadata
            .get("mplex_view_count")
            .and_then(serde_json::Value::as_i64)
        {
            item["viewCount"] = serde_json::json!(count);
        }
        if let Some(last_viewed) = metadata
            .get("mplex_last_viewed_at")
            .and_then(serde_json::Value::as_i64)
        {
            item["lastViewedAt"] = serde_json::json!(last_viewed);
        }
    }
    if let Some(year) = title_year(&media.title) {
        item["year"] = serde_json::json!(year);
    }
    if let Some(metadata) = metadata {
        let metadata_title = match media_type {
            "show" => metadata.get("name").and_then(serde_json::Value::as_str),
            "episode" => metadata
                .get("mplex_episode_title")
                .and_then(serde_json::Value::as_str),
            _ => metadata.get("title").and_then(serde_json::Value::as_str),
        };
        if let Some(title) = metadata_title.filter(|title| !title.is_empty()) {
            item["title"] = serde_json::json!(title);
        }
        if let Some(summary) = metadata
            .get("overview")
            .and_then(serde_json::Value::as_str)
            .filter(|summary| !summary.is_empty())
        {
            item["summary"] = serde_json::json!(summary);
        }
        let date = match media_type {
            "show" => metadata.get("first_air_date"),
            "movie" => metadata.get("release_date"),
            _ => None,
        }
        .and_then(serde_json::Value::as_str)
        .filter(|date| !date.is_empty());
        if let Some(date) = date {
            item["originallyAvailableAt"] = serde_json::json!(date);
            if let Some(year) = date.get(..4).and_then(|year| year.parse::<i64>().ok()) {
                item["year"] = serde_json::json!(year);
            }
        }
        let original_title = metadata
            .get("original_title")
            .or_else(|| metadata.get("original_name"))
            .and_then(serde_json::Value::as_str);
        if let Some(original_title) = original_title.filter(|title| !title.is_empty()) {
            item["originalTitle"] = serde_json::json!(original_title);
        }
        if let Some(rating) = plex_rating(metadata) {
            item["rating"] = serde_json::json!(rating);
            item["audienceRating"] = serde_json::json!(rating);
        }
        let genre_names = metadata
            .get("genres")
            .and_then(serde_json::Value::as_array)
            .map(|genres| {
                genres
                    .iter()
                    .filter_map(|genre| genre.get("name").and_then(serde_json::Value::as_str))
                    .map(|name| serde_json::json!({"tag": name}))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| {
                metadata
                    .get("genre_ids")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(serde_json::Value::as_i64)
                    .filter_map(|id| {
                        tmdb_genre_name(
                            if media_type == "episode" {
                                "show"
                            } else {
                                media_type
                            },
                            id,
                        )
                    })
                    .map(|name| serde_json::json!({"tag": name}))
                    .collect()
            });
        if !genre_names.is_empty() {
            item["Genre"] = serde_json::json!(genre_names);
        }
        if media_type != "episode" {
            if let Some(tmdb_id) = metadata.get("id").and_then(serde_json::Value::as_i64) {
                item["guid"] = serde_json::json!(format!(
                    "tmdb://{}/{}",
                    if media_type == "show" { "tv" } else { "movie" },
                    tmdb_id
                ));
            }
        }
        if media_type == "show" {
            if let Some(show_title) = metadata.get("name").and_then(serde_json::Value::as_str) {
                item["grandparentTitle"] = serde_json::json!(show_title);
            }
            if let Some(season) = metadata
                .get("mplex_season")
                .and_then(serde_json::Value::as_u64)
            {
                item["parentIndex"] = serde_json::json!(season);
                item["parentTitle"] = serde_json::json!(format!("Season {season:02}"));
            }
            if let Some(episode) = metadata
                .get("mplex_episode")
                .and_then(serde_json::Value::as_u64)
            {
                item["index"] = serde_json::json!(episode);
            }
        }
    }
    item
}

fn title_year(title: &str) -> Option<u32> {
    title
        .split(|character: char| !character.is_ascii_digit())
        .find_map(|part| {
            (part.len() == 4)
                .then(|| part.parse::<u32>().ok())
                .flatten()
                .filter(|year| (1888..=2100).contains(year))
        })
}

fn tmdb_genre_name(media_type: &str, id: i64) -> Option<&'static str> {
    let name = match id {
        12 => "Adventure",
        14 => "Fantasy",
        16 => "Animation",
        18 => "Drama",
        27 => "Horror",
        28 => "Action",
        35 => "Comedy",
        36 => "History",
        37 => "Western",
        53 => "Thriller",
        80 => "Crime",
        99 => "Documentary",
        878 => "Science Fiction",
        9648 => "Mystery",
        10402 => "Music",
        10749 => "Romance",
        10751 => "Family",
        10752 => "War",
        10759 if media_type == "show" => "Action & Adventure",
        10762 if media_type == "show" => "Kids",
        10763 if media_type == "show" => "News",
        10764 if media_type == "show" => "Reality",
        10765 if media_type == "show" => "Sci-Fi & Fantasy",
        10766 if media_type == "show" => "Soap",
        10767 if media_type == "show" => "Talk",
        10768 if media_type == "show" => "War & Politics",
        10770 => "TV Movie",
        _ => return None,
    };
    Some(name)
}

async fn stream_media(
    State(s): State<AppState>,
    Path(id): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    stream_for_id(s, id, method, headers).await
}

async fn stream_for_id(s: AppState, id: String, method: Method, headers: HeaderMap) -> Response {
    let Some(media) = find_media(&s, &id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    stream_file(&media, method, headers, false).await
}

async fn stream_file(
    media: &Media,
    method: Method,
    headers: HeaderMap,
    download: bool,
) -> Response {
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let (start, end) = match parse_range(range, media.size) {
        Ok(value) => value,
        Err(()) => {
            return (
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(header::CONTENT_RANGE, format!("bytes */{}", media.size))],
                Body::empty(),
            )
                .into_response()
        }
    };
    let length = end - start + 1;
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&media.mime).unwrap(),
    );
    response_headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&length.to_string()).unwrap(),
    );
    response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if download {
        let filename = media
            .path
            .file_name()
            .map(|name| name.to_string_lossy())
            .unwrap_or_else(|| std::borrow::Cow::Borrowed("media"));
        let fallback = filename
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, ' ' | '.' | '-' | '_') {
                    character
                } else {
                    '_'
                }
            })
            .collect::<String>();
        let encoded = filename
            .as_bytes()
            .iter()
            .map(|byte| {
                if byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'.' | b'_') {
                    char::from(*byte).to_string()
                } else {
                    format!("%{byte:02X}")
                }
            })
            .collect::<String>();
        let value = format!("attachment; filename=\"{fallback}\"; filename*=UTF-8''{encoded}");
        response_headers.insert(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_str(&value).unwrap(),
        );
    }
    if range.is_some() {
        response_headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {}-{}/{}", start, end, media.size)).unwrap(),
        );
    }
    if method == Method::HEAD {
        return (
            if range.is_some() {
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            },
            response_headers,
            Body::empty(),
        )
            .into_response();
    }
    let mut file = match File::open(&media.path).await {
        Ok(file) => file,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    if file.seek(SeekFrom::Start(start)).await.is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let stream = ReaderStream::new(file.take(length));
    (
        if range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        },
        response_headers,
        Body::from_stream(stream),
    )
        .into_response()
}

fn parse_range(value: Option<&str>, size: u64) -> Result<(u64, u64), ()> {
    if size == 0 {
        return Err(());
    }
    let Some(value) = value else {
        return Ok((0, size - 1));
    };
    let value = value.strip_prefix("bytes=").ok_or(())?;
    if value.contains(',') {
        return Err(());
    }
    let (left, right) = value.split_once('-').ok_or(())?;
    let (start, end) = if left.is_empty() {
        let suffix: u64 = right.parse().map_err(|_| ())?;
        if suffix == 0 {
            return Err(());
        }
        (size.saturating_sub(suffix), size - 1)
    } else {
        let start: u64 = left.parse().map_err(|_| ())?;
        if start >= size {
            return Err(());
        }
        let end = if right.is_empty() {
            size - 1
        } else {
            right.parse::<u64>().map_err(|_| ())?.min(size - 1)
        };
        if end < start {
            return Err(());
        }
        (start, end)
    };
    Ok((start, end))
}

fn html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
#[path = "../tests/unit/plex_helpers.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/unit/plex_endpoints.rs"]
mod plex_endpoint_tests;
