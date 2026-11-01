#![recursion_limit = "256"]

mod plex_tls;

use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocketUpgrade},
        Form, FromRequestParts, Path, Query, RawForm, State,
    },
    http::{header, HeaderMap, HeaderValue, Method, Request, StatusCode, Uri},
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{any, get, post, put},
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
    collections::{BTreeMap, BTreeSet, HashMap},
    net::SocketAddr,
    path::{Component, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
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

const USER_ID: &str = "mplex-user";
const POSTER_CARD_MIN_WIDTH: usize = 140;
const PLEX_DEFAULT_AUDIO_LANGUAGE: &str = "en";

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, value_name = "DIRECTORY")]
    config: Option<PathBuf>,
    #[arg(long)]
    token: Option<String>,
    #[arg(long)]
    bind: Option<SocketAddr>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Config {
    #[serde(default = "default_bind")]
    bind: String,
    #[serde(default = "default_token")]
    token: String,
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
}

fn default_bind() -> String {
    "0.0.0.0:32400".to_owned()
}
fn default_token() -> String {
    "development-token".to_owned()
}
fn default_metadata_storage() -> String {
    "database".to_owned()
}
fn default_true() -> bool {
    true
}

fn plex_machine_identifier(seed: &str) -> String {
    let digest = Sha256::digest(seed.as_bytes());
    digest[..20]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn plex_library_uuid(machine_identifier: &str, section: usize) -> String {
    let digest = Sha256::digest(format!("{machine_identifier}:library:{section}").as_bytes());
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
    machine_identifier: Arc<str>,
    root: Arc<PathBuf>,
    media: Arc<Vec<Media>>,
    media_info: Arc<Mutex<HashMap<String, serde_json::Value>>>,
    plex_stream_selections: Arc<Mutex<HashMap<u64, PlexStreamSelection>>>,
    plex_play_queues: Arc<Mutex<HashMap<u64, serde_json::Value>>>,
    next_plex_play_queue_id: Arc<AtomicU64>,
    token: Arc<str>,
    plex_tls: PlexTls,
}

struct Media {
    id: String,
    title: String,
    path: PathBuf,
    size: u64,
    container: String,
    mime: String,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_timer(LocalSecondTimer)
        .init();
    let args = Args::parse();
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
        config_dir.join("mplex.toml")
    };
    let mut config = load_config(&config_path)?;
    if let Some(bind) = args.bind {
        config.bind = bind.to_string();
    }
    if let Some(token) = args.token {
        config.token = token;
    }
    save_config(&config_path, &config)?;
    let (db, machine_identifier) = open_database(&config_path)?;
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
            let Ok(folder) = tokio::fs::canonicalize(folder).await else {
                continue;
            };
            let mut library_media = scan_media(&folder).await?;
            for item in &mut library_media {
                item.id = format!("{}-{}", library_index + 1, item.id);
            }
            persist_media_records(&db, &folder, &library_media)?;
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
    let state = AppState {
        config: Arc::new(Mutex::new(config.clone())),
        config_path: Arc::new(config_path),
        db: Arc::new(Mutex::new(db)),
        machine_identifier: Arc::from(machine_identifier),
        root: Arc::new(root),
        media: Arc::new(media),
        media_info: Arc::new(Mutex::new(HashMap::new())),
        plex_stream_selections: Arc::new(Mutex::new(HashMap::new())),
        plex_play_queues: Arc::new(Mutex::new(HashMap::new())),
        next_plex_play_queue_id: Arc::new(AtomicU64::new(1)),
        token: Arc::from(config.token),
        plex_tls,
    };
    let server_tls = state.plex_tls.clone();
    let publication_state = state.clone();
    tokio::spawn(async move {
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
    let app = Router::new()
        .route("/", get(root_handler))
        .route("/library", get(web_client))
        .route("/web", get(web_client))
        .route("/web/stream", any(web_stream))
        .route("/web/progress", get(web_progress).post(update_web_progress))
        .route("/web/subtitle", get(web_subtitle))
        .route("/web/artwork", get(web_artwork))
        .route("/web/info", get(web_info))
        .route("/settings", get(settings_page))
        .route("/settings/libraries", post(create_library))
        .route("/settings/folders", post(add_library_folder))
        .route("/settings/libraries/display", post(update_library_display))
        .route("/settings/preferences", post(update_preferences))
        .route("/settings/plex/start", get(start_plex_auth))
        .route("/settings/plex/status", get(plex_auth_status))
        .route("/settings/plex/callback", get(plex_auth_callback))
        .route("/settings/plex/disconnect", post(disconnect_plex))
        .route("/settings/libraries/:id/scan", post(scan_library_metadata))
        .route("/settings/media/scan", post(scan_media_metadata))
        .route("/settings/browse", get(settings_browse))
        .route("/identity", get(plex_identity))
        .route("/myplex/account", get(plex_myplex_account))
        .route("/accounts", get(plex_accounts))
        .route("/accounts/:id", get(plex_account))
        .route("/activities", get(plex_activities))
        .route("/status/sessions", get(plex_sessions))
        .route("/servers", get(plex_servers))
        .route("/media/providers", get(plex_media_providers))
        .route("/security/resources", get(plex_security_resources))
        .route("/hubs", get(plex_global_hubs))
        .route("/hubs/promoted", get(plex_global_hubs))
        .route("/hubs/search", get(plex_hub_search))
        .route("/hubs/sections/:section", get(plex_section_hubs))
        .route("/hubs/metadata/:id/related", get(plex_related_hubs))
        .route(
            "/hubs/sections/:section/:hub",
            get(plex_section_hub),
        )
        .route("/library/sections", get(plex_sections))
        .route("/library/sections/:section", get(plex_section))
        .route(
            "/library/sections/:section/collections",
            get(plex_section_collections),
        )
        .route(
            "/library/sections/:section/prefs",
            get(plex_section_prefs).put(plex_section_prefs),
        )
        .route("/library/sections/:section/sorts", get(plex_section_sorts))
        .route("/library/sections/:section/filters", get(plex_section_filters))
        .route("/library/sections/:section/firstCharacter", get(plex_section_first_characters))
        .route("/library/sections/:section/firstCharacter/:letter", get(plex_section_first_character_items))
        .route("/library/sections/:section/firstCharacters", get(plex_section_first_characters))
        .route("/library/search", get(plex_library_search))
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
        .route("/library/metadata/:id/thumb", get(plex_thumb))
        .route("/library/metadata/:id/art", get(plex_art))
        .route("/media/:id", any(stream_media))
        .route("/System/Info/Public", get(jellyfin_public_system))
        .route("/Users/Public", get(jellyfin_public_users))
        .route("/Users/AuthenticateByName", post(jellyfin_authenticate))
        .route("/Users/:user/Views", get(jellyfin_views))
        .route("/Users/:user/Items", get(jellyfin_items))
        .route("/Items/:id", get(jellyfin_item))
        .route("/Items/:id/PlaybackInfo", get(jellyfin_playback_info))
        .route("/Videos/:id/stream", any(jellyfin_stream))
        .route("/Videos/:id/stream.mp4", any(jellyfin_stream))
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
                    info!(
                        "event=http_request remote_ip={} method={} path={} accept={:?} plex_product={:?} token_present={}",
                        remote_ip,
                        request.method(),
                        request.uri().path(),
                        request.headers().get(header::ACCEPT),
                        request.headers().get("X-Plex-Product"),
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
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(config.bind.parse::<SocketAddr>()?).await?;
    let address = listener.local_addr()?;
    tokio::spawn(run_gdm_responder(gdm_machine_identifier, address.port()));
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
    server_tls.serve(listener, app).await?;
    Ok(())
}

async fn run_gdm_responder(machine_identifier: String, port: u16) {
    let multicast = std::net::Ipv4Addr::new(239, 0, 0, 250);
    let response = gdm_message("HTTP/1.0 200 OK", &machine_identifier, port);
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
        let response = response.clone();
        tokio::spawn(async move {
            let mut buffer = [0u8; 2048];
            loop {
                let Ok((size, peer)) = socket.recv_from(&mut buffer).await else {
                    return;
                };
                if buffer[..size].starts_with(b"M-SEARCH * HTTP/1.") {
                    let _ = socket.send_to(response.as_bytes(), peer).await;
                }
            }
        });
    }

    let announcement = gdm_message("HELLO", &machine_identifier, port);
    let announcement_socket = std::net::UdpSocket::bind((std::net::Ipv4Addr::UNSPECIFIED, 0));
    if let Ok(announcement_socket) = announcement_socket {
        let _ = announcement_socket.send_to(announcement.as_bytes(), (multicast, 32413));
    }
}

fn gdm_message(prefix: &str, machine_identifier: &str, port: u16) -> String {
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    format!(
        "{prefix}\r\nContent-Type: plex/media-server\r\nResource-Identifier: {machine_identifier}\r\nName: harbringer\r\nPort: {port}\r\nUpdated-At: {updated_at}\r\nVersion: 1.43.4.10903-e5521bd8c\r\n\r\n"
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

fn find_media<'a>(state: &'a AppState, id: &str) -> Option<&'a Media> {
    state.media.iter().find(|item| item.id == id)
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

fn load_config(path: &std::path::Path) -> Result<Config, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(Config {
            bind: default_bind(),
            token: default_token(),
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
        connection.execute(
            "INSERT OR IGNORE INTO library_groups (name, kind, added_at) VALUES (?1, ?2, strftime('%s', 'now'))",
            (&library.name, &library.kind),
        )?;
        let id: i64 = connection.query_row(
            "SELECT id FROM library_groups WHERE name = ?1",
            [&library.name],
            |row| row.get(0),
        )?;
        connection.execute(
            "UPDATE library_groups SET kind = ?1 WHERE id = ?2",
            (&library.kind, id),
        )?;
        for folder in &library.folders {
            connection.execute(
                "INSERT OR IGNORE INTO library_folders (library_id, path, added_at) VALUES (?1, ?2, strftime('%s', 'now'))",
                (id, folder.to_string_lossy().as_ref()),
            )?;
        }
    }
    Ok(())
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
         CREATE TABLE IF NOT EXISTS watch_progress (
             user_id TEXT NOT NULL,
             media_key TEXT NOT NULL,
             position_ms INTEGER NOT NULL DEFAULT 0,
             duration_ms INTEGER,
             watched INTEGER NOT NULL DEFAULT 0,
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
             added_at INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS library_folders (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             library_id INTEGER NOT NULL REFERENCES library_groups(id) ON DELETE CASCADE,
             path TEXT NOT NULL UNIQUE,
             added_at INTEGER NOT NULL
         );",
    )?;
    let _ = connection.execute(
        "ALTER TABLE server_identity ADD COLUMN plex_last_error TEXT",
        [],
    );
    let _ = connection.execute(
        "ALTER TABLE libraries ADD COLUMN kind TEXT NOT NULL DEFAULT 'other'",
        [],
    );
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
    let _ = connection.execute(
        "UPDATE server_identity SET server_name = 'harbringer' WHERE id = 1",
        [],
    );
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
struct LibraryDisplayForm {
    library_id: usize,
    show_recently_added: Option<String>,
    show_continue_watching: Option<String>,
}

#[derive(Deserialize)]
struct PreferencesForm {
    metadata_storage: Option<String>,
    #[serde(default)]
    metadata_in_media_folder: Option<String>,
    tmdb_bearer_token: Option<String>,
}

#[derive(Deserialize, Default)]
struct SettingsQuery {
    folder: Option<String>,
    error: Option<String>,
    metadata_matched: Option<usize>,
    metadata_status: Option<String>,
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
            "INSERT INTO watch_progress (user_id, media_key, position_ms, duration_ms, watched, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(user_id, media_key) DO UPDATE SET position_ms = excluded.position_ms, duration_ms = excluded.duration_ms, watched = excluded.watched, updated_at = excluded.updated_at",
            (USER_ID, path.to_string_lossy().as_ref(), update.position_ms.max(0), update.duration_ms, update.watched, updated_at),
        )
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::NO_CONTENT.into_response()
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
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><MediaContainer size=\"{}\"><Server name=\"harbringer\" localAddresses=\"{}\" port=\"{}\" machineIdentifier=\"{}\" version=\"1.43.4.10903-e5521bd8c\" date=\"{}\" relay=\"0\" />{directories}</MediaContainer>",
        libraries.len() + 1,
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
    let libraries = {
        let Ok(config) = state.config.lock() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        config
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
                )
            })
            .collect::<Vec<_>>()
    };
    let list = libraries
        .iter()
        .map(|(id, name, kind, folders, show_recently_added, show_continue_watching)| {
            format!(
                "<li><strong>{}</strong> <span class=\"note\">({})</span><ul>{}</ul><details><summary>＋ Add folder</summary><form method=\"post\" action=\"/settings/folders\"><input type=\"hidden\" name=\"library_id\" value=\"{}\"><input name=\"path\" type=\"text\" placeholder=\"/another/folder\" required><button type=\"submit\">Add folder</button></form></details><form method=\"post\" action=\"/settings/libraries/{}/scan\" style=\"display:inline-flex\"><button type=\"submit\">Scan metadata</button><button type=\"submit\" formaction=\"/settings/libraries/{}/scan?force=true\">Force rescan</button></form><form method=\"post\" action=\"/settings/libraries/display\" class=\"library-display\"><input type=\"hidden\" name=\"library_id\" value=\"{}\"><label><input type=\"checkbox\" name=\"show_recently_added\" value=\"true\" {}> Recently Added</label><label><input type=\"checkbox\" name=\"show_continue_watching\" value=\"true\" {}> Continue Watching</label><button type=\"submit\">Save Home sections</button></form></li>",
                html(name), html(library_kind_label(kind)), folders.iter().map(|path| format!("<li>{}</li>", html(path))).collect::<String>(), id, id, id, id, if *show_recently_added { "checked" } else { "" }, if *show_continue_watching { "checked" } else { "" }
            )
        })
        .collect::<String>();
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
        .unwrap_or_default();
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
                "<h2>Plex.tv</h2><p class=\"flash-error\" role=\"status\">Signed in, but secure Plex client publication failed: {}. HTTP access remains published.</p><form method=\"get\" action=\"/settings/plex/start\"><button type=\"submit\">Reconnect to Plex.tv</button></form><form method=\"post\" action=\"/settings/plex/disconnect\"><button type=\"submit\">Disconnect and revoke</button></form>",
                html(error)
            )
        } else {
            r#"<h2>Plex.tv <span style="color:#4ade80" title="Connected and published" aria-label="Connected and published">✓</span></h2><p class="note">Connected to Plex.tv and published for Plex clients.</p><form method="get" action="/settings/plex/start"><button type="submit">Reconnect to Plex.tv</button></form><form method="post" action="/settings/plex/disconnect" onsubmit="return confirm('Disconnect and revoke harbringer from Plex.tv?')"><button type="submit">Disconnect and revoke</button></form>"#.to_owned()
        }
    } else if plex_status.0 {
        match plex_status.2.as_deref() {
            Some(error) => format!(
                "<h2>Plex.tv</h2><p class=\"flash-error\" role=\"status\">harbringer is signed in to Plex.tv, but server claiming or resource publication failed: {}.</p><form method=\"get\" action=\"/settings/plex/start\"><button type=\"submit\">Connect to Plex.tv again</button></form><form method=\"post\" action=\"/settings/plex/disconnect\"><button type=\"submit\">Disconnect and revoke</button></form>",
                html(error)
            ),
            None => r#"<h2>Plex.tv</h2><p class="flash-error" role="status">harbringer has an older Plex.tv account token but has not completed server claiming or resource publication.</p><form method="get" action="/settings/plex/start"><button type="submit">Connect to Plex.tv again</button></form><form method="post" action="/settings/plex/disconnect"><button type="submit">Disconnect and revoke</button></form>"#.to_owned(),
        }
    } else {
        r#"<h2>Plex.tv</h2><p class="note">Connect harbringer to your Plex account so Plex clients can discover and authenticate with this server.</p><form method="get" action="/settings/plex/start"><button type="submit">Connect to Plex.tv</button></form>"#.to_owned()
    };
    Html(format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>harbringer settings</title><style>
body {{ margin: 0; color: #f9fafb; background: #111827; font-family: system-ui, sans-serif; }}
.app-shell {{ display: grid; grid-template-columns: 240px minmax(0, 1fr); min-height: 100vh; transition: grid-template-columns .2s ease; }} .app-shell.sidebar-collapsed {{ grid-template-columns: 68px minmax(0, 1fr); }}
.sidebar {{ position: sticky; top: 0; height: 100vh; box-sizing: border-box; padding: 1.25rem .9rem; border-right: 1px solid #263244; background: #0d1421; }}
.sidebar-brand {{ display: block; padding: .5rem .7rem 1.5rem; color: #f9fafb; font-size: 1.25rem; font-weight: 700; text-decoration: none; }}
.sidebar-top {{ display: flex; align-items: center; justify-content: space-between; padding-bottom: 1rem; }}
.sidebar-top .sidebar-brand {{ padding-bottom: 0; }} .sidebar-toggle {{ width: 2rem; height: 2rem; border: 0; border-radius: .4rem; color: #cbd5e1; background: transparent; cursor: pointer; font-size: 1.15rem; }} .sidebar-collapsed .sidebar-top {{ justify-content: center; }} .sidebar-collapsed .sidebar-brand, .sidebar-collapsed .sidebar-label, .sidebar-collapsed .sidebar-link {{ font-size: 0; }} .sidebar-collapsed .sidebar-link {{ justify-content: center; }} .sidebar-collapsed .sidebar-icon {{ font-size: 1.1rem; }}
.sidebar-label {{ padding: 0 .7rem .45rem; color: #6b7280; font-size: .7rem; font-weight: 700; letter-spacing: .1em; text-transform: uppercase; }}
.sidebar-nav {{ display: grid; gap: .25rem; }} .sidebar-link {{ display: flex; align-items: center; gap: .7rem; padding: .65rem .7rem; border-radius: .5rem; color: #cbd5e1; text-decoration: none; }}
.sidebar-link:hover {{ background: #17243a; color: #fff; }} .sidebar-link.active {{ background: #1d4ed8; color: #fff; }} .sidebar-icon {{ width: 1.3rem; text-align: center; font-size: 1.1rem; }}
main {{ min-width: 0; max-width: 760px; width: 100%; box-sizing: border-box; margin: 0 auto; padding: 2rem 1rem; }} a {{ color: #93c5fd; }}
form {{ display: flex; gap: .6rem; margin: 1.5rem 0; }} input, select {{ flex: 1; padding: .7rem; border: 1px solid #4b5563; border-radius: .4rem; color: #fff; background: #1f2937; }} .path-list {{ display: grid; gap: .55rem; flex: 2; }} .path-row {{ display: flex; gap: .5rem; }} .path-row input {{ min-width: 0; }} .metadata-options {{ display: grid; gap: .6rem; margin: .7rem 0 1rem; }} .metadata-option {{ display: flex; align-items: flex-start; gap: .6rem; padding: .8rem; border: 1px solid #374151; border-radius: .5rem; background: #172033; }} .metadata-option input {{ flex: 0; margin-top: .2rem; }}
button {{ padding: .7rem 1rem; border: 0; border-radius: .4rem; color: #fff; background: #2563eb; cursor: pointer; }} li {{ margin: .65rem 0; overflow-wrap: anywhere; }} .note {{ color: #9ca3af; }}
 .flash-error {{ margin: 1rem 0; padding: .8rem 1rem; border: 1px solid #ef4444; border-radius: .5rem; color: #fecaca; background: #451a1a; }} .flash-notice {{ margin: 1rem 0; padding: .8rem 1rem; border: 1px solid #22c55e; border-radius: .5rem; color: #bbf7d0; background: #14532d; }}
details {{ margin-top: .7rem; }} summary {{ display: inline-block; padding: .45rem .7rem; border: 1px solid #3b82f6; border-radius: .4rem; color: #bfdbfe; cursor: pointer; list-style: none; }} summary::-webkit-details-marker {{ display: none; }} details form {{ margin: .7rem 0 0; }}
@media (max-width: 700px) {{ .app-shell {{ display: block; }} .sidebar {{ position: static; height: auto; padding: .7rem; border-right: 0; border-bottom: 1px solid #263244; }} .sidebar-top {{ padding-bottom: .35rem; }} .sidebar-brand {{ padding: .35rem .55rem; }} .sidebar-toggle {{ display: none; }} .sidebar-label {{ display: none; }} .sidebar-nav {{ display: flex; overflow-x: auto; }} .sidebar-link {{ white-space: nowrap; }} }}
</style></head><body><div class="app-shell">{}
<main><h1>Settings</h1>{}{}{}
<p class="note">A library is a named collection with one type and one or more monitored folders. Create it with its first folder, then add more folders to that same library below.</p>
<h2>Metadata storage</h2><p class="note">Choose one place for matched movie and TV metadata. SQLite mode stores matches in this server’s database. Sidecar mode stores matches and artwork beside the media, so another mplex server can reuse them without its database or a new TMDB lookup. SQLite still stores server identity, library configuration, and playback progress.</p><form method="post" action="/settings/preferences"><div class="metadata-options"><label class="metadata-option"><input type="radio" name="metadata_storage" value="database" {}><span><strong>SQLite database {}</strong><br><span class="note">Matched metadata stays in this server’s database.</span></span></label><label class="metadata-option"><input type="radio" name="metadata_storage" value="sidecar" {}><span><strong>.mplex files in media folders {}</strong><br><span class="note">Matched metadata and artwork live in <code>.mplex/cover.jpg</code>, <code>banner.jpg</code>, and <code>info.toml</code> beside the media.</span></span></label></div><input name="tmdb_bearer_token" type="password" placeholder="TMDB bearer token (leave blank to keep current)" value=""><button type="submit">Save preferences</button></form>
<details open><summary>＋ Add library</summary><form method="post" action="/settings/libraries"><input name="name" type="text" placeholder="Library name" required><select name="kind"><option value="movie">Movies</option><option value="tv">TV Shows</option><option value="other">Other</option></select><div class="path-list" id="new-library-paths"><div class="path-row"><input name="path[]" type="text" placeholder="/path/to/folder" value="{}" required><a href="/settings/browse">Browse</a><button type="button" class="remove-path" disabled>−</button></div></div><button type="button" id="add-path">＋</button><button type="submit">Create library</button></form></details>
<h2>Libraries</h2><ul>{}</ul>
 </main></div><script>
const shell = document.querySelector('.app-shell'); const toggle = document.querySelector('.sidebar-toggle');
if (localStorage.getItem('mplex-sidebar-collapsed') === 'true') shell.classList.add('sidebar-collapsed');
toggle?.addEventListener('click', () => {{ const collapsed = shell.classList.toggle('sidebar-collapsed'); localStorage.setItem('mplex-sidebar-collapsed', collapsed ? 'true' : 'false'); }});
const pathList = document.querySelector('#new-library-paths'); document.querySelector('#add-path')?.addEventListener('click', () => {{ const row = document.createElement('div'); row.className = 'path-row'; row.innerHTML = '<input name="path[]" type="text" placeholder="/path/to/folder" required><a href="/settings/browse">Browse</a><button type="button" class="remove-path">−</button>'; row.querySelector('.remove-path').addEventListener('click', () => {{ row.remove(); }}); pathList.appendChild(row); }});
</script></body></html>"#,
        sidebar_nav_with_settings(false, true),
        error,
        notice,
        plex_auth,
        if metadata_enabled { "" } else { "checked" },
        if metadata_enabled { "" } else { "(current)" },
        if metadata_enabled { "checked" } else { "" },
        if metadata_enabled { "(current)" } else { "" },
        html(query.folder.as_deref().unwrap_or("")),
        list
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
    let client = reqwest::Client::new();
    let token = config
        .tmdb_bearer_token
        .as_deref()
        .filter(|token| !token.is_empty());
    let sidecar_mode = config.metadata_storage == "sidecar";
    let kind = if library.kind == "tv" { "tv" } else { "movie" };
    let mut matched = 0;
    let forced = scan_query.force.unwrap_or(false);
    for folder in &library.folders {
        let Ok(files) = scan_media(folder).await else {
            continue;
        };
        let mut groups = BTreeMap::<PathBuf, Vec<Media>>::new();
        for media in files {
            let group_path = metadata_group_path(folder, &media.path, kind);
            groups.entry(group_path).or_default().push(media);
        }
        for (group_path, group) in groups {
            let group_title = metadata_group_title(&group_path, folder);
            if !forced {
                let media_key = format!("{}-{}", library_id + 1, group[0].id);
                let selected = if sidecar_mode {
                    read_sidecar_metadata(&group_path)
                } else {
                    read_database_metadata(&state, &media_key)
                };
                if selected
                    .as_ref()
                    .is_some_and(|metadata| metadata.get("id").is_some())
                {
                    if sidecar_mode {
                        clear_group_database_metadata(&state, library_id, &group);
                    }
                    matched += 1;
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
                            )
                            .await;
                        }
                    } else {
                        for media in &group {
                            let metadata =
                                tv_metadata_for_media(&metadata, folder, &media.path, kind);
                            let key = format!("{}-{}", library_id + 1, media.id);
                            if !write_database_metadata(&state, &key, &metadata) {
                                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                            }
                        }
                    }
                    matched += 1;
                    continue;
                }
            }
            let Some(token) = token else {
                return settings_error(
                    "Add a TMDB bearer token before matching media without stored metadata",
                );
            };
            let title = metadata_search_title(&group_title);
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
            let Ok(response) = response else { continue };
            let Ok(payload) = response.json::<serde_json::Value>().await else {
                continue;
            };
            let Some(result) = payload
                .get("results")
                .and_then(|results| results.as_array())
                .and_then(|results| results.first())
            else {
                continue;
            };
            for media in &group {
                let media_metadata = tv_metadata_for_media(result, folder, &media.path, kind);
                if !sidecar_mode {
                    let metadata_json =
                        serde_json::to_string(&media_metadata).unwrap_or_else(|_| "{}".to_owned());
                    let media_key = format!("{}-{}", library_id + 1, media.id);
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
                let media_key = format!("{}-{}", library_id + 1, representative.id);
                let _ = download_tmdb_artwork(
                    &client,
                    token,
                    folder,
                    &group_path,
                    &media_key,
                    result,
                    &state,
                )
                .await;
                if kind == "tv" {
                    let mut seasons = BTreeMap::<PathBuf, (u32, String)>::new();
                    for media in &group {
                        let Some(episode) = tv_episode_info(folder, &media.path) else {
                            continue;
                        };
                        let Some(season_path) = media.path.parent() else {
                            continue;
                        };
                        seasons.entry(season_path.to_path_buf()).or_insert_with(|| {
                            (episode.season, format!("{}-{}", library_id + 1, media.id))
                        });
                    }
                    for (season_path, (season, media_key)) in seasons {
                        let _ = download_tmdb_season_artwork(
                            &client,
                            token,
                            &season_path,
                            result,
                            season,
                            &media_key,
                            &state,
                        )
                        .await;
                    }
                }
            }
            matched += 1;
        }
    }
    let suffix = if forced { "&metadata_forced=true" } else { "" };
    Redirect::to(&format!("/settings?metadata_matched={matched}{suffix}")).into_response()
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
    let media_key = format!("{}-{}", library_id + 1, media.id);
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
        )
        .await;
    }
    let parent = relative.parent().unwrap_or(std::path::Path::new(""));
    Redirect::to(&format!(
        "/library?library={library_id}&path={}",
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
    directory.join(".mplex/info.toml")
}

fn parse_sidecar_metadata(contents: &str) -> Option<serde_json::Value> {
    toml::from_str::<toml::Value>(contents)
        .ok()?
        .get("metadata_json")?
        .as_str()
        .and_then(|metadata| serde_json::from_str::<serde_json::Value>(metadata).ok())
        .filter(|metadata| metadata.is_object())
}

fn read_sidecar_metadata(group_path: &std::path::Path) -> Option<serde_json::Value> {
    let contents = std::fs::read_to_string(metadata_sidecar_info_path(group_path)).ok()?;
    parse_sidecar_metadata(&contents)
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
        let media_key = format!("{}-{}", library_id + 1, media.id);
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
    title
        .replace('.', " ")
        .replace('_', " ")
        .replace('-', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
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

fn tv_metadata_for_media(
    metadata: &serde_json::Value,
    root: &PathBuf,
    media: &std::path::Path,
    kind: &str,
) -> serde_json::Value {
    if kind != "tv" {
        return metadata.clone();
    }
    let Some(episode) = tv_episode_info(root, media) else {
        return metadata.clone();
    };
    let mut enriched = metadata.clone();
    if let Some(object) = enriched.as_object_mut() {
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
    let path = media
        .path
        .strip_prefix(folder)
        .unwrap_or(&media.path)
        .to_string_lossy();
    let episode = tv_episode_info(folder, &media.path);
    let info = MediaInfo {
        title: title.to_owned(),
        media_path: path.into_owned(),
        description: json_string(metadata, "overview"),
        release_date: json_string(metadata, "release_date"),
        first_air_date: json_string(metadata, "first_air_date"),
        number_of_seasons: json_i64(metadata, "number_of_seasons"),
        number_of_episodes: json_i64(metadata, "number_of_episodes"),
        season: episode.as_ref().map(|value| value.season),
        episode: episode.as_ref().map(|value| value.episode),
        episode_end: episode.as_ref().and_then(|value| value.episode_end),
        episode_title: episode.and_then(|value| value.episode_title),
        metadata_json: serde_json::to_string_pretty(metadata).unwrap_or_default(),
        technical: inspect_media(&media.path).await,
    };
    let contents = toml::to_string_pretty(&info).map_err(std::io::Error::other)?;
    tokio::fs::write(directory.join("info.toml"), contents).await
}

#[derive(Serialize)]
struct MediaInfo {
    title: String,
    media_path: String,
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
    metadata_json: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    technical: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct SeasonInfo {
    title: String,
    description: String,
    season: u32,
    metadata_json: String,
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

fn find_media_part<'a>(state: &'a AppState, id: &str) -> Option<&'a Media> {
    let id = id.parse::<u64>().ok()?;
    state.media.iter().find(|media| plex_part_id(media) == id)
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
        let (mut language_code, _) = plex_track_language(track);
        let fallback_audio_language =
            (stream_type == 2 && audio_track_count == 1 && language_code == "und")
                .then_some(plex_language(PLEX_DEFAULT_AUDIO_LANGUAGE).1);
        if fallback_audio_language.is_some() {
            language_code = plex_language(PLEX_DEFAULT_AUDIO_LANGUAGE).0;
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
            "language": language_code,
            "languageCode": language_code,
            "displayTitle": display_title,
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
            "language": external_subtitle_language(&media.path, &subtitle_path).0,
            "languageCode": external_subtitle_language(&media.path, &subtitle_path).0,
            "displayTitle": label,
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
            continue;
        };
        let response = client
            .get(format!("https://image.tmdb.org/t/p/w780{}", path))
            .bearer_auth(token)
            .send()
            .await?;
        if !response.status().is_success() {
            continue;
        }
        let bytes = response.bytes().await?;
        let local = artwork.join(format!("{kind}.jpg"));
        tokio::fs::write(&local, &bytes).await?;
        record_artwork(&state.db, media_key, kind, &local);
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
) -> Result<(), Box<dyn std::error::Error>> {
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
        metadata_json: serde_json::to_string_pretty(&metadata).unwrap_or_default(),
    };
    tokio::fs::write(artwork.join("info.toml"), toml::to_string_pretty(&info)?).await?;
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
    Html(format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Choose folder · harbringer</title><style>body {{ margin: 0; padding: 2rem; color: #f9fafb; background: #111827; font-family: system-ui, sans-serif; }} main {{ max-width: 820px; margin: auto; }} a {{ color: #93c5fd; }} .toolbar {{ display: flex; flex-wrap: wrap; gap: .6rem; margin: 1.2rem 0; }} .shortcut, .action {{ display: inline-block; padding: .6rem .8rem; border: 1px solid #374151; border-radius: .5rem; text-decoration: none; background: #1f2937; }} .folder-list {{ padding: 0; list-style: none; border: 1px solid #374151; border-radius: .6rem; overflow: hidden; }} .folder {{ display: flex; justify-content: space-between; gap: 1rem; padding: .8rem 1rem; border-bottom: 1px solid #374151; }} .folder:last-child {{ border-bottom: 0; }} .open {{ margin-right: 1rem; }} code, .note {{ color: #9ca3af; overflow-wrap: anywhere; }} </style></head><body><main><p><a href="/settings">← Back to Settings</a></p><h1>Choose a media folder</h1><p class="note">Pick a folder on the harbringer server. You can add more folders to the same library after this.</p><p><strong>Current folder:</strong> <code>{}</code></p><div class="toolbar"><a class="action" href="/settings/browse?path={}">⬆ Parent</a><a class="action" href="/settings?folder={}">Choose this folder</a></div>{}<ul class="folder-list">{}</ul></main></body></html>"#,
        html(&directory.to_string_lossy()),
        url_encode(&parent.to_string_lossy()),
        url_encode(&directory.to_string_lossy()),
        shortcuts,
        items
    )).into_response()
}

async fn create_library(State(state): State<AppState>, RawForm(raw): RawForm) -> Response {
    let fields = match serde_urlencoded::from_bytes::<Vec<(String, String)>>(&raw) {
        Ok(fields) => fields,
        Err(_) => return settings_error("Invalid library form"),
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
        _ => return settings_error("Invalid library type"),
    };
    let name = name.trim();
    if name.is_empty() {
        return settings_error("Library name is required");
    }
    if paths.is_empty() {
        return settings_error("At least one folder is required");
    }
    let mut canonical_paths = Vec::with_capacity(paths.len());
    for value in paths
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        let Ok(path_metadata) = tokio::fs::metadata(value).await else {
            return settings_error(&format!("Directory does not exist: {}", value));
        };
        if !path_metadata.is_dir() {
            return settings_error(&format!("Not a directory: {}", value));
        }
        let Ok(path) = tokio::fs::canonicalize(value).await else {
            return settings_error(&format!("Directory could not be resolved: {}", value));
        };
        if !canonical_paths.contains(&path) {
            canonical_paths.push(path);
        }
    }
    if canonical_paths.is_empty() {
        return settings_error("At least one folder is required");
    }
    let Ok(mut config) = state.config.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if config.libraries.iter().any(|library| library.name == name) {
        return settings_error("A library with that name already exists");
    }
    config.libraries.push(ConfigLibrary {
        name: name.to_owned(),
        kind,
        folders: canonical_paths,
        show_recently_added: true,
        show_continue_watching: true,
    });
    if save_config(&state.config_path, &config).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    Redirect::to("/settings").into_response()
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
        return Redirect::to("/settings").into_response();
    }
    library.folders.push(path);
    if save_config(&state.config_path, &config).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    Redirect::to("/settings").into_response()
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

#[derive(Deserialize, Default)]
struct WebPath {
    path: Option<String>,
    library: Option<usize>,
    view: Option<String>,
    kind: Option<String>,
    stream: Option<usize>,
    play: Option<String>,
    episode: Option<String>,
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
            "<a class=\"library-card\" style=\"overflow:hidden;padding:0\" href=\"/library?library={}\">{}<span class=\"library-card-info\" style=\"display:flex;flex-direction:column;gap:.25rem;padding:.85rem 1rem 1rem\"><strong>{}</strong><span class=\"note\">{}</span></span></a>",
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
    let continue_watching = state
        .db
        .lock()
        .ok()
        .and_then(|connection| {
            let mut statement = connection.prepare("SELECT media_key, position_ms, duration_ms FROM watch_progress WHERE user_id = ?1 AND watched = 0 AND position_ms > 0 ORDER BY updated_at DESC LIMIT 12").ok()?;
            let rows = statement.query_map([USER_ID], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, Option<i64>>(2)?))).ok()?;
            Some(rows.filter_map(Result::ok).collect::<Vec<_>>())
        })
        .unwrap_or_default()
        ;
    let mut continue_cards = String::new();
    let mut continue_folders = Vec::new();
    for (path, position, duration) in continue_watching {
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
                        state
                            .media
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
        if continue_folders.contains(&folder.to_path_buf()) {
            continue;
        }
        continue_folders.push(folder.to_path_buf());
        let Ok(relative) = folder.strip_prefix(root) else {
            continue;
        };
        let Some(preview) = first_media_file(folder)
            .await
            .and_then(|file| file.strip_prefix(root).ok().map(PathBuf::from))
        else {
            continue;
        };
        let name = folder
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or(&media.title);
        let cover = config.metadata_storage == "sidecar"
            && tokio::fs::metadata(folder.join(".mplex/cover.jpg"))
                .await
                .is_ok();
        let banner = config.metadata_storage == "sidecar"
            && tokio::fs::metadata(folder.join(".mplex/banner.jpg"))
                .await
                .is_ok();
        let progress = duration
            .filter(|duration| *duration > 0)
            .map(|duration| (position as f64 / duration as f64).clamp(0.0, 1.0));
        continue_cards.push_str(&folder_card(
            name,
            relative,
            Some(library_index),
            Some(&preview),
            cover,
            banner,
            progress,
        ));
    }
    let continue_watching = if continue_cards.is_empty() {
        "<p class=\"note empty-state\">Nothing to resume yet. Start watching something and it will appear here.</p>".to_owned()
    } else {
        continue_cards
    };
    let mut recent = String::new();
    let mut recent_folders = Vec::new();
    for media in state.media.iter().take(12) {
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
        if recent_folders.contains(&folder.to_path_buf()) {
            continue;
        }
        recent_folders.push(folder.to_path_buf());
        let Ok(relative) = folder.strip_prefix(root) else {
            continue;
        };
        let preview = media.path.strip_prefix(root).ok();
        let cover = config.metadata_storage == "sidecar"
            && tokio::fs::metadata(folder.join(".mplex/cover.jpg"))
                .await
                .is_ok();
        let banner = config.metadata_storage == "sidecar"
            && tokio::fs::metadata(folder.join(".mplex/banner.jpg"))
                .await
                .is_ok();
        let name = folder
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or(&media.title);
        recent.push_str(&folder_card(
            name,
            relative,
            Some(library_index),
            preview,
            cover,
            banner,
            None,
        ));
    }
    if recent.is_empty() {
        recent = "<p class=\"note empty-state\">No recently added media yet.</p>".to_owned();
    }
    Html(format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>harbringer</title><style>body {{ margin: 0; color: #f9fafb; background: #111827; font-family: system-ui, sans-serif; }} .app-shell {{ display: grid; grid-template-columns: 240px minmax(0, 1fr); min-height: 100vh; }} .sidebar {{ position: sticky; top: 0; height: 100vh; box-sizing: border-box; padding: 1.25rem .9rem; border-right: 1px solid #263244; background: #0d1421; }} .sidebar-brand {{ display: block; padding: .5rem .7rem 1.5rem; color: #f9fafb; font-size: 1.25rem; font-weight: 700; text-decoration: none; }} .sidebar-top {{ display: flex; justify-content: space-between; align-items: center; }} .sidebar-toggle {{ background: none; border: 0; color: #cbd5e1; font-size: 1.3rem; cursor: pointer; }} .sidebar-label {{ padding: 1rem .7rem .4rem; color: #6b7280; font-size: .7rem; text-transform: uppercase; }} .sidebar-nav {{ display: grid; gap: .25rem; }} .sidebar-link {{ display: flex; gap: .7rem; padding: .65rem .7rem; border-radius: .5rem; color: #cbd5e1; text-decoration: none; }} .sidebar-link.active, .sidebar-link:hover {{ background: #1d4ed8; color: #fff; }} .sidebar-icon {{ width: 1.3rem; text-align: center; }} main {{ max-width: 1100px; width: 100%; box-sizing: border-box; margin: auto; padding: 2rem 1rem; }} a {{ color: #93c5fd; text-decoration: none; }} h1 {{ margin-top: 0; }} .section {{ margin: 2rem 0; }} .library-grid, .recent-grid, .continue-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax({}px, 1fr)); gap: 1rem; }} .library-card, .recent-item, .continue-item {{ display: flex; flex-direction: column; gap: .4rem; padding: 1rem; border: 1px solid #374151; border-radius: .7rem; background: #1f2937; }} .continue-item {{ min-height: 130px; justify-content: flex-end; border-color: #2563eb; background: linear-gradient(135deg, #1e3a8a, #1f2937); font-size: 1.05rem; }} .library-card:hover, .recent-item:hover, .continue-item:hover {{ border-color: #60a5fa; }} .library-icon {{ font-size: 2rem; }} .note {{ color: #9ca3af; }} .empty-state {{ padding: 1.5rem; border: 1px dashed #4b5563; border-radius: .7rem; }} @media (max-width: 700px) {{ .app-shell {{ display: block; }} .sidebar {{ position: static; height: auto; width: 100%; }} .sidebar-nav {{ display: flex; overflow-x: auto; }} .sidebar-link {{ white-space: nowrap; }} .sidebar-label {{ display: none; }} .sidebar-toggle {{ display: none; }} }}</style></head><body><div class="app-shell">{}<main><h1>Home</h1><section class="section"><h2>Continue watching</h2><div class="continue-grid">{}</div></section><section class="section"><h2>Recently added</h2><div class="recent-grid">{}</div></section><section class="section"><h2>Your libraries</h2><div class="library-grid">{}</div></section></main></div><script>const shell=document.querySelector('.app-shell'), toggle=document.querySelector('.sidebar-toggle'); if(localStorage.getItem('mplex-sidebar-collapsed')==='true') shell.classList.add('sidebar-collapsed'); toggle?.addEventListener('click',()=>{{shell.classList.toggle('sidebar-collapsed');localStorage.setItem('mplex-sidebar-collapsed',shell.classList.contains('sidebar-collapsed')?'true':'false')}});</script></body></html>"#,
        POSTER_CARD_MIN_WIDTH, sidebar_nav_for(&state, std::path::Path::new(""), None), continue_watching, recent, libraries
    )).into_response()
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

async fn web_client(State(s): State<AppState>, Query(query): Query<WebPath>) -> Response {
    let relative = match clean_relative_path(query.path.as_deref().unwrap_or("")) {
        Some(path) => path,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };
    if contains_hidden_component(&relative) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let library_root = selected_library_root(&s, query.library);
    let folder_view = query.view.as_deref() == Some("folders");
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
    let info_panel = if let Some(selected_relative) = selected_relative.as_deref() {
        episode_info_panel(&s, &library_root, selected_relative).await
    } else if sidecar_mode {
        media_info_panel(&directory).await
    } else {
        String::new()
    };
    let mut entries = match tokio::fs::read_dir(&directory).await {
        Ok(entries) => entries,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    let mut directories = Vec::new();
    let mut files = Vec::new();
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
            && (folder_view || media_type(&path).is_some())
            && selected_relative
                .as_deref()
                .is_none_or(|selected| selected == child_relative)
        {
            files.push((name, child_relative, path));
        }
    }
    directories.sort_by_key(|(name, _)| name.to_ascii_lowercase());
    files.sort_by_key(|(name, _, _)| name.to_ascii_lowercase());

    let mut items = String::new();
    items.push_str(&breadcrumb_nav(&relative, query.library));
    let sidebar_library = query.library.or_else(|| {
        s.config
            .lock()
            .ok()
            .and_then(|config| (!config.libraries.is_empty()).then_some(0))
    });
    let sidebar = sidebar_nav_for(&s, &relative, sidebar_library);
    let mut folder_cards = String::new();
    let mut season_sections = String::new();
    let mut empty_folders = String::new();
    let mut has_content = false;
    let tv_season_view = !folder_view
        && library_kind.as_deref() == Some("tv")
        && relative
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(parse_tv_season_directory)
            .is_some();
    for (name, child_relative) in directories {
        has_content = true;
        let directory = directory.join(&name);
        if !folder_view
            && library_kind.as_deref() == Some("tv")
            && parse_tv_season_directory(&name).is_some()
        {
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
                let season_count = episode_count.to_string();
                let season_card = folder_card_with_badge(
                    &format!("Season {season_number:02}"),
                    &child_relative,
                    query.library,
                    preview.as_deref(),
                    cover,
                    banner,
                    None,
                    Some(&season_count),
                );
                season_sections.push_str(&format!(
                    r#"<section class="tv-season"><h2>Season {season_number:02}</h2><div class="tv-season-card">{season_card}</div></section>"#
                ));
                continue;
            }
        }
        let media_count = media_file_count(&directory).await;
        if media_count == 0 {
            empty_folders.push_str(&empty_folder_item(&name, &child_relative, query.library));
        } else {
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
        if let Some((_, mime)) = media_type(&path) {
            if tv_season_view && selected_relative.is_none() {
                items.push_str(&tv_episode_card(&name, &child_relative, query.library));
                continue;
            }
            let artwork = if sidecar_mode
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
            items.push_str(&web_client_item(
                &name,
                &child_relative.to_string_lossy(),
                mime,
                query.library,
                &subtitle_tracks(&path, &child_relative, query.library).await,
                artwork,
            ));
        } else {
            items.push_str(&file_item(&name));
        }
    }
    items.push_str(&folder_cards);
    items.push_str(&season_sections);
    if folder_view && !empty_folders.is_empty() {
        items.push_str(&format!(
            r#"<section class="empty-folders"><h2>Empty folders</h2>{}</section>"#,
            empty_folders
        ));
    }
    if !has_content {
        items.push_str(r#"<p class="note">No supported media or subfolders here.</p>"#);
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
    .sidebar-libraries {{ display: grid; gap: .15rem; margin: .2rem 0 .45rem 2rem; }}
    .sidebar-library {{ padding: .35rem .55rem; border-radius: .35rem; color: #94a3b8; font-size: .9rem; text-decoration: none; overflow-wrap: anywhere; }}
    .sidebar-library:hover, .sidebar-library.active {{ color: #fff; background: #17243a; }}
    .sidebar-collapsed .sidebar-top {{ justify-content: center; }}
    .sidebar-collapsed .sidebar-brand, .sidebar-collapsed .sidebar-label, .sidebar-collapsed .sidebar-link {{ font-size: 0; }}
    .sidebar-collapsed .sidebar-toggle {{ transform: rotate(180deg); }}
    .sidebar-collapsed .sidebar-link {{ justify-content: center; padding-left: 0; padding-right: 0; }}
    .sidebar-collapsed .sidebar-icon {{ font-size: 1.1rem; }}
    main {{ min-width: 0; max-width: 1100px; width: 100%; box-sizing: border-box; margin: 0 auto; padding: 2rem 1rem; }}
    h1 {{ margin-top: 0; }}
    .brand {{ color: inherit; text-decoration: none; }}
    .library {{ display: grid; grid-template-columns: repeat(auto-fill, minmax({}px, 1fr)); align-items: start; gap: 1rem; }}
    .tv-season {{ grid-column: 1 / -1; }}
    .tv-season h2 {{ margin: .75rem 0 .6rem; }}
    .tv-season-card {{ width: 180px; max-width: 100%; }}
    .tv-season .media-list {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); align-items: start; gap: 1rem; }}
    .tv-episode-card {{ display: grid; grid-template-columns: 5rem minmax(0, 1fr) auto; align-items: center; gap: 1rem; grid-column: 1 / -1; padding: .9rem 1rem; color: inherit; text-decoration: none; border: 1px solid #374151; border-radius: .65rem; background: #1f2937; }}
    .tv-episode-card:hover {{ border-color: #60a5fa; background: #24344d; }}
    .tv-episode-number {{ color: #93c5fd; font-weight: 700; }}
    .tv-episode-title {{ overflow-wrap: anywhere; }}
    .tv-episode-action {{ color: #93c5fd; white-space: nowrap; }}
    .breadcrumb, .empty-folders {{ grid-column: 1 / -1; }}
    article, .folder-card, .empty-folders {{ width: 100%; box-sizing: border-box; }}
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
    body.theatre-mode main {{ max-width: none; padding: 0; }}
    body.theatre-mode .library {{ display: block; }}
    body.theatre-mode article:not(.theatre-card), body.theatre-mode .breadcrumb, body.theatre-mode .folder-card, body.theatre-mode .empty-folders {{ display: none; }}
    body.theatre-mode .theatre-card {{ width: 100%; min-height: 100vh; border: 0; border-radius: 0; }}
    body.theatre-mode .theatre-card video {{ width: 100%; max-height: calc(100vh - 8rem); aspect-ratio: 16 / 9; object-fit: contain; }}
    .subtitle-picker {{ display: inline-flex; align-items: center; gap: .35rem; color: #cbd5e1; font-size: .9rem; }}
    .subtitle-picker select {{ border: 1px solid #4b5563; border-radius: .35rem; color: #fff; background: #111827; }}
    .subtitle-note {{ color: #9ca3af; font-size: .8rem; }}
    .breadcrumb {{ display: flex; flex-wrap: wrap; align-items: center; gap: .35rem; box-sizing: border-box; color: #9ca3af; }}
    .breadcrumb a {{ padding: .35rem .55rem; border-radius: .35rem; text-decoration: none; }}
    .breadcrumb a:hover {{ background: #24344d; }}
    .breadcrumb-separator {{ color: #6b7280; }}
    .view-toggle {{ display: inline-flex; gap: .2rem; margin: .25rem 0 1rem; padding: .2rem; border: 1px solid #374151; border-radius: .5rem; background: #172033; }}
    .view-toggle a {{ padding: .4rem .75rem; border-radius: .35rem; text-decoration: none; color: #cbd5e1; }}
    .view-toggle a.active, .view-toggle a:hover {{ color: #fff; background: #2563eb; }}
    .metadata-panel {{ grid-column: 1 / -1; padding: 1rem 1.2rem; border: 1px solid #374151; border-radius: .75rem; background: rgba(17,24,39,.62); backdrop-filter: blur(8px); }}
    .metadata-panel h2 {{ margin: 0 0 .45rem; }} .metadata-description {{ max-width: 75ch; color: #d1d5db; line-height: 1.5; }} .metadata-details {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: .6rem 1.2rem; margin-top: .8rem; }} .metadata-detail strong {{ display: block; color: #9ca3af; font-size: .75rem; text-transform: uppercase; }}
    .folder-card {{ display: block; width: 100%; overflow: hidden; color: inherit; text-decoration: none; border: 1px solid #374151; border-radius: .75rem; background: #1f2937; transition: border-color .15s, transform .15s; }}
    .folder-card:hover {{ border-color: #60a5fa; transform: translateY(-2px); }}
    .folder-preview {{ position: relative; display: grid; place-items: center; overflow: hidden; aspect-ratio: 2 / 3; color: #bfdbfe; font-size: 3rem; background: linear-gradient(135deg, #1e3a8a, #312e81 55%, #111827); }}
    .folder-preview-video {{ width: 100%; height: 100%; object-fit: cover; opacity: .85; }}
    .folder-preview-icon {{ position: absolute; inset: 0; display: grid; place-items: center; text-shadow: 0 2px 8px #000; pointer-events: none; }}
    .folder-cover {{ position: absolute; inset: 0; z-index: 1; width: 100%; height: 100%; object-fit: cover; }}
    .folder-info {{ padding: .9rem 1rem 1rem; }}
    .folder-info h2 {{ margin: 0 0 .25rem; }}
    .empty-folders {{ padding: 1rem; border: 1px solid #374151; border-radius: .75rem; background: #172033; }}
    .empty-folders h2 {{ margin-top: 0; }}
    .empty-folder {{ padding: .35rem 0; }}
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
    <h1>Library</h1>
    <nav class="view-toggle" aria-label="Library view"><a class="{}" href="/library?{}{}">Media</a><a class="{}" href="/library?{}{}view=folders">Folders</a></nav>
    {}
    <section class="library">{}</section>
    <dialog id="info-modal"><form method="dialog"><button type="submit" aria-label="Close dialog" title="Close">×</button></form><h2>Technical media information</h2><div id="info-content" class="info-grid">Loading…</div></dialog>
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
    document.addEventListener("keydown", event => {{
      if (event.key === "Escape" && infoModal.open) infoModal.close();
    }});
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
      const subtitleKey = `mplex-subtitle:${{path || video.dataset.progressPath || video.currentSrc}}`;
      const applySubtitle = selected => {{
        [...video.textTracks].forEach((track, index) => {{
          track.mode = index === selected ? "showing" : "disabled";
        }});
        if (subtitleSelect) subtitleSelect.value = String(selected);
      }};
      const savedSubtitle = Number.parseInt(window.localStorage.getItem(subtitleKey) || "-1", 10);
      if (Number.isInteger(savedSubtitle) && savedSubtitle >= 0) applySubtitle(savedSubtitle);
      subtitleSelect?.addEventListener("change", event => {{
        const selected = Number(event.target.value);
        applySubtitle(selected);
        window.localStorage.setItem(subtitleKey, String(selected));
      }});
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
      let saveTimer;
      const save = watched => {{
        clearTimeout(saveTimer);
        saveTimer = setTimeout(() => fetch("/web/progress", {{
          method: "POST", headers: {{ "content-type": "application/json" }},
          body: JSON.stringify({{ path, position_ms: Math.round(video.currentTime * 1000), duration_ms: Number.isFinite(video.duration) ? Math.round(video.duration * 1000) : null, watched }})
        }}).catch(() => {{}}), 500);
      }};
      video.addEventListener("timeupdate", () => {{ if (!video.paused) save(false); }});
      video.addEventListener("pause", () => save(false));
      video.addEventListener("ended", () => save(true));
    }});
    let selectedVideo = null;
    document.addEventListener("pointerover", event => {{
      if (event.target instanceof HTMLVideoElement) selectedVideo = event.target;
    }});
    document.addEventListener("focusin", event => {{
      if (event.target instanceof HTMLVideoElement) selectedVideo = event.target;
    }});
    document.addEventListener("keydown", event => {{
      const fullscreen = event.key.toLowerCase() === "f";
      const theatre = event.key.toLowerCase() === "t";
      if (event.key !== "ArrowLeft" && event.key !== "ArrowRight" && event.code !== "Space" && !fullscreen && !theatre) return;
      const video = selectedVideo || window.document.querySelector("video");
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
          selectedVideo = null;
        }}
      }} finally {{
        refreshing = false;
      }}
    }}
    setInterval(refreshLibrary, 5000);
  </script>
</body>
</html>"#,
        POSTER_CARD_MIN_WIDTH,
        sidebar,
        pane_banner,
        if !folder_view { "active" } else { "" },
        query
            .library
            .map(|index| format!("library={index}&"))
            .unwrap_or_default(),
        query
            .path
            .as_deref()
            .map(|path| format!("path={}&", url_encode(path)))
            .unwrap_or_default(),
        if folder_view { "active" } else { "" },
        query
            .library
            .map(|index| format!("library={index}&"))
            .unwrap_or_default(),
        query
            .path
            .as_deref()
            .map(|path| format!("path={}&", url_encode(path)))
            .unwrap_or_default(),
        info_panel,
        items
        )),
    )
        .into_response()
}

struct SubtitleTrack {
    label: String,
    url: String,
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
    video: &PathBuf,
    relative: &std::path::Path,
    library: Option<usize>,
) -> Vec<SubtitleTrack> {
    let mut tracks = Vec::new();
    for path in external_subtitle_files(video).await {
        tracks.push(SubtitleTrack {
            label: format!("External: {}", external_subtitle_label(video, &path)),
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

async fn media_info_panel(directory: &std::path::Path) -> String {
    let Ok(contents) = tokio::fs::read_to_string(directory.join(".mplex/info.toml")).await else {
        return String::new();
    };
    let Ok(value) = contents.parse::<toml::Value>() else {
        return String::new();
    };
    let title = value
        .get("title")
        .and_then(toml::Value::as_str)
        .unwrap_or("Media information");
    let description = value
        .get("description")
        .and_then(toml::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(|value| format!(r#"<p class="metadata-description">{}</p>"#, html(value)))
        .unwrap_or_default();
    let details = value
        .as_table()
        .map(render_toml_details)
        .unwrap_or_default();
    format!(
        r#"<section class="metadata-panel"><h2>{}</h2>{}<div class="metadata-details">{}</div></section>"#,
        html(title),
        description,
        details
    )
}

async fn episode_info_panel(
    state: &AppState,
    root: &std::path::Path,
    relative: &std::path::Path,
) -> String {
    if !state
        .config
        .lock()
        .is_ok_and(|config| config.metadata_storage == "sidecar")
    {
        return String::new();
    }
    let path = root.join(relative);
    let season_directory = path.parent().unwrap_or(root);
    let Ok(contents) = tokio::fs::read_to_string(season_directory.join(".mplex/info.toml")).await
    else {
        return String::new();
    };
    let Ok(info) = contents.parse::<toml::Value>() else {
        return String::new();
    };
    let Some(metadata) = info
        .get("metadata_json")
        .and_then(toml::Value::as_str)
        .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
    else {
        return String::new();
    };
    let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
        return String::new();
    };
    let Some((_, (episode, _), _)) = parse_tv_episode_marker(stem) else {
        return String::new();
    };
    let episode_metadata = metadata
        .get("episodes")
        .and_then(|episodes| episodes.as_array())
        .and_then(|episodes| {
            episodes.iter().find(|item| {
                item.get("episode_number").and_then(|value| value.as_u64()) == Some(episode as u64)
            })
        });
    let Some(episode_metadata) = episode_metadata else {
        return String::new();
    };
    let title = episode_metadata
        .get("name")
        .and_then(|value| value.as_str())
        .unwrap_or("Episode");
    let description = episode_metadata
        .get("overview")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(|value| format!(r#"<p class="metadata-description">{}</p>"#, html(value)))
        .unwrap_or_default();
    let details = [
        ("episode", format!("{}", episode)),
        (
            "air date",
            episode_metadata
                .get("air_date")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_owned(),
        ),
        (
            "runtime",
            episode_metadata
                .get("runtime")
                .and_then(|value| value.as_i64())
                .map(|value| format!("{value} minutes"))
                .unwrap_or_default(),
        ),
    ]
    .into_iter()
    .filter(|(_, value)| !value.is_empty())
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

fn render_toml_details(table: &toml::map::Map<String, toml::Value>) -> String {
    table
        .iter()
        .filter(|(key, _)| {
            key.as_str() != "metadata_json"
                && key.as_str() != "description"
                && key.as_str() != "technical"
        })
        .map(|(key, value)| {
            let rendered = match value {
                toml::Value::Table(table) => render_toml_details(table),
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
    let Ok(contents) = tokio::fs::read_to_string(directory.join(".mplex/info.toml")).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(info) = contents.parse::<toml::Value>() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(technical) = info.get("technical") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(technical) = serde_json::to_value(technical) else {
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

fn tv_episode_card(name: &str, relative: &std::path::Path, library: Option<usize>) -> String {
    let library_query = library
        .map(|index| format!("library={index}&"))
        .unwrap_or_default();
    let path = url_encode(&relative.to_string_lossy());
    let folder = relative.parent().unwrap_or(std::path::Path::new(""));
    format!(
        r#"<a class="tv-episode-card" href="/library?{}path={}&episode={}"><span class="tv-episode-number">{}</span><span class="tv-episode-title">{}</span><span class="tv-episode-action">▶ Play episode</span></a>"#,
        library_query,
        url_encode(&folder.to_string_lossy()),
        path,
        html(&tv_episode_number(name)),
        html(&tv_episode_display_name(name)),
    )
}

fn tv_episode_number(name: &str) -> String {
    let stem = std::path::Path::new(name)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(name);
    parse_tv_episode_marker(stem)
        .map(|(_, (episode, episode_end), _)| {
            episode_end
                .map(|end| format!("E{episode:02}–E{end:02}"))
                .unwrap_or_else(|| format!("E{episode:02}"))
        })
        .unwrap_or_else(|| "Episode".to_owned())
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
    let subtitle_options = subtitles
        .iter()
        .enumerate()
        .map(|(index, track)| {
            format!(
                r#"<option value="{}">{}</option>"#,
                index,
                html(&track.label)
            )
        })
        .collect::<String>();
    let subtitle_selector = if subtitles.is_empty() {
        r#"<span class="subtitle-note">No external or embedded subtitles detected.</span>"#
            .to_owned()
    } else {
        format!(
            r#"<label class="subtitle-picker">Subtitles <select class="subtitle-select"><option value="-1">Off</option>{}</select></label><span class="subtitle-note">External and embedded subtitles</span>"#,
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
    let library_links = state
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
                        "<a class=\"sidebar-library {}\" href=\"/library?library={}\">{}</a>",
                        if selected_library == Some(index) {
                            "active"
                        } else {
                            ""
                        },
                        index,
                        html(&library.name)
                    )
                })
                .collect::<String>()
        })
        .unwrap_or_default();
    sidebar_nav_with_library_links(
        relative.as_os_str().is_empty() == false,
        false,
        &library_links,
    )
}

fn sidebar_nav_with_settings(relative_is_nested: bool, settings_active: bool) -> String {
    sidebar_nav_with_library_links(relative_is_nested, settings_active, "")
}

fn sidebar_nav_with_library_links(
    relative_is_nested: bool,
    settings_active: bool,
    library_links: &str,
) -> String {
    let library_active = relative_is_nested && !settings_active;
    let home_active = !relative_is_nested && !settings_active;
    format!(
        r#"<aside class="sidebar">
  <div class="sidebar-top">
    <a class="sidebar-brand" href="/">harbringer</a>
    <button class="sidebar-toggle" type="button" aria-label="Collapse sidebar" aria-expanded="true" title="Collapse sidebar">‹</button>
  </div>
  <div class="sidebar-label">Browse</div>
  <nav class="sidebar-nav" aria-label="Main navigation">
    <a class="sidebar-link {}" href="/"><span class="sidebar-icon">⌂</span>Home</a>
    <a class="sidebar-link {}" href="/library"><span class="sidebar-icon">▦</span>Library</a>
    <div class="sidebar-libraries">{}</div>
    <a class="sidebar-link {}" href="/settings"><span class="sidebar-icon">⚙</span>Settings</a>
  </nav>
</aside>"#,
        if home_active { "active" } else { "" },
        if library_active { "active" } else { "" },
        library_links,
        if settings_active { "active" } else { "" },
    )
}

fn breadcrumb_nav(relative: &std::path::Path, library: Option<usize>) -> String {
    if relative.as_os_str().is_empty() {
        return String::new();
    }
    let mut nav = String::from(r#"<nav class="breadcrumb" aria-label="Breadcrumb">"#);
    let prefix = library
        .map(|index| format!("library={index}&"))
        .unwrap_or_default();
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
            r#"<a href="/library?{}path={}">{}</a>"#,
            prefix,
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
    let progress_markup = progress
        .map(|progress| {
            format!(
                r#"<div aria-label="{} percent watched" style="height:4px;margin-top:.7rem;border-radius:999px;background:#374151;overflow:hidden"><span style="display:block;width:{}%;height:100%;background:#3b82f6"></span></div>"#,
                (progress * 100.0).round() as u32,
                progress * 100.0
            )
        })
        .unwrap_or_default();
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
        r#"<a class="folder-card" style="display:block;overflow:hidden;padding:0;color:inherit;text-decoration:none;border:1px solid #374151;border-radius:.75rem;background:#1f2937" href="/library?{}path={}">
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

fn empty_folder_item(name: &str, relative: &std::path::Path, library: Option<usize>) -> String {
    format!(
        r#"<div class="empty-folder"><a href="/library?{}path={}">📂 {}</a></div>"#,
        library
            .map(|index| format!("library={index}&"))
            .unwrap_or_default(),
        url_encode(&relative.to_string_lossy()),
        html(name)
    )
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
    };
    stream_file(&media, method, headers).await
}

async fn web_path(
    _state: &AppState,
    root: &std::path::Path,
    relative: &std::path::Path,
) -> Result<PathBuf, StatusCode> {
    let path = tokio::fs::canonicalize(root.join(relative))
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    if path.starts_with(root) {
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
        return plex_playback_decision(State(state), Query(query)).await;
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
            Ok(upgrade) => plex_notification_response(upgrade),
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
        return plex_prefs();
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

fn plex_progress_path(state: &AppState, media_id: &str) -> Option<String> {
    let media = find_media(state, media_id)?;
    let (_, library) = media_library(state, media)?;
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
        "INSERT INTO watch_progress (user_id, media_key, position_ms, duration_ms, watched, updated_at) VALUES (?1, ?2, ?3, ?4, 0, ?5) ON CONFLICT(user_id, media_key) DO UPDATE SET position_ms = excluded.position_ms, duration_ms = COALESCE(excluded.duration_ms, watch_progress.duration_ms), watched = 0, updated_at = excluded.updated_at",
        (USER_ID, path, position, duration, updated_at),
    ).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::OK.into_response()
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
            "INSERT INTO watch_progress (user_id, media_key, position_ms, duration_ms, watched, updated_at) VALUES (?1, ?2, 0, NULL, 1, ?3) ON CONFLICT(user_id, media_key) DO UPDATE SET position_ms = 0, watched = 1, updated_at = excluded.updated_at",
            (USER_ID, path, updated_at),
        )
    } else {
        connection.execute(
            "INSERT INTO watch_progress (user_id, media_key, position_ms, duration_ms, watched, updated_at) VALUES (?1, ?2, 0, NULL, 0, ?3) ON CONFLICT(user_id, media_key) DO UPDATE SET position_ms = 0, watched = 0, updated_at = excluded.updated_at",
            (USER_ID, path, updated_at),
        )
    };
    if result.is_err() {
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    } else {
        StatusCode::OK.into_response()
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
    path == "/"
        || matches!(
            path,
            "/identity"
                | "/myplex/account"
                | "/accounts"
                | "/activities"
                | "/servers"
                | "/media/providers"
                | "/security/resources"
        )
        || [
            "/:/",
            "/hubs",
            "/library/",
            "/photo/",
            "/media/",
            "/accounts/",
            "/status/",
        ]
        .iter()
        .any(|prefix| path.starts_with(prefix))
}

async fn root_handler(State(s): State<AppState>, headers: HeaderMap, uri: Uri) -> Response {
    let is_plex_client = headers.contains_key("X-Plex-Product")
        || headers.contains_key("X-Plex-Client-Identifier")
        || headers.contains_key("X-Plex-Token");
    let is_plex_query = plex_query_value(&uri, "X-Plex-Product").is_some()
        || plex_query_value(&uri, "X-Plex-Client-Identifier").is_some()
        || plex_query_value(&uri, "X-Plex-Token").is_some();
    if is_plex_client || is_plex_query {
        plex_root(&s).await
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
            "friendlyName": "harbringer",
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
            "transcoderVideo": false,
            "transcoderVideoBitrates": "64,96,208,320,720,1500,2000,3000,4000,8000,10000,12000,20000",
            "transcoderVideoQualities": "0,1,2,3,4,5,6,7,8,9,10,11,12",
            "transcoderVideoResolutions": "128,128,160,240,320,480,768,720,720,1080,1080,1080,1080",
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
            "Directory": sections.1
        }
    }))
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

fn plex_prefs() -> Response {
    let settings = serde_json::json!([
        {"id":"FriendlyName","label":"Friendly name","summary":"Other computers on your network see this server under this name.","type":"text","default":"","value":"harbringer","hidden":false,"advanced":false,"group":"general"},
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
                "name": "harbringer",
                "address": host,
                "port": bind,
                "machineIdentifier": state.machine_identifier.as_ref(),
                "version": "1.43.4.10903-e5521bd8c"
            }]
        }
    }))
}

async fn plex_media_providers(State(state): State<AppState>) -> Response {
    let updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
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
                    serde_json::json!({
                        "agent": if library.kind == "tv" { "tv.plex.agents.series" } else { "tv.plex.agents.movie" },
                        "scanner": if library.kind == "tv" { "Plex TV Series" } else { "Plex Movie" },
                        "language": "en-US",
                        "uuid": plex_library_uuid(&state.machine_identifier, section),
                        "id": section.to_string(),
                        "key": format!("/library/sections/{section}"),
                        "hubKey": format!("/hubs/sections/{section}"),
                        "type": plex_kind(&library.kind),
                        "title": library.name,
                        "refreshing": false,
                        "scannedAt": updated_at,
                        "updatedAt": updated_at,
                        "Pivot": [
                            {"id": "recommended", "key": format!("/hubs/sections/{section}"), "type": "hub", "title": "Recommended", "context": "content.discover", "symbol": "star"},
                            {"id": "library", "key": format!("/library/sections/{section}/all?type={}", if library.kind == "tv" { 2 } else { 1 }), "type": "list", "title": "Library", "context": "content.library", "symbol": "library"},
                            {"id": "collections", "key": format!("/library/sections/{section}/collections"), "type": "list", "title": "Collections", "context": "content.collections", "symbol": "stack"}
                        ]
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
            "friendlyName": "harbringer",
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
            "transcoderVideo": false,
            "transcoderVideoBitrates": "64,96,208,320,720,1500,2000,3000,4000,8000,10000,12000,20000",
            "transcoderVideoQualities": "0,1,2,3,4,5,6,7,8,9,10,11,12",
            "transcoderVideoResolutions": "128,128,160,240,320,480,768,720,720,1080,1080,1080,1080",
            "updatedAt": updated_at,
            "updater": false,
            "version": "1.43.4.10903-e5521bd8c",
            "voiceSearch": false,
            "MediaProvider": [{
                "identifier": "com.plexapp.plugins.library",
                "title": "Library",
                "types": "video",
                "protocols": "stream,download",
                "Feature": [{"key": "/library/sections", "type": "content", "Directory": directories},
                             {"key": "/hubs/search", "type": "search"},
                             {"key": "/library/metadata", "type": "metadata"},
                             {"key": "/hubs/promoted", "type": "promoted"}]
            }]
        }
    }))
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

fn plex_notification_response(upgrade: WebSocketUpgrade) -> Response {
    upgrade
        .on_upgrade(|mut socket| async move {
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
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
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

    let mut hubs = Vec::new();
    for (index, library) in libraries.iter().enumerate() {
        let section = index + 1;
        if only_section.is_some_and(|requested| requested != section) {
            continue;
        }
        let mut media = state
            .media
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
        for category in plex_hub_categories(rated_count > 0, only_hub) {
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

    let mut container = serde_json::json!({"size": hubs.len(), "Hub": hubs});
    container["allowSync"] = serde_json::json!(false);
    container["identifier"] = serde_json::json!("com.plexapp.plugins.library");
    if let Some(section) = only_section {
        let library = &libraries[section - 1];
        container["librarySectionID"] = serde_json::json!(section);
        container["librarySectionTitle"] = serde_json::json!(library.name);
        container["librarySectionUUID"] =
            serde_json::json!(plex_library_uuid(&state.machine_identifier, section));
    }
    plex_json(serde_json::json!({"MediaContainer": container}))
}

fn plex_hub_categories(has_rated_items: bool, requested: Option<&str>) -> Vec<&'static str> {
    match requested {
        Some("recommended") => vec!["recommended"],
        Some("recentlyAdded") => vec!["recentlyAdded"],
        Some(_) => Vec::new(),
        None if has_rated_items => vec!["recommended", "recentlyAdded"],
        None => vec!["recommended"],
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
    let tv_shows = (library.kind == "tv").then(|| {
        plex_tv_show_groups(
            section,
            &library.folders,
            state
                .media
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

    for media in state
        .media
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
            "SELECT media_key, position_ms, duration_ms, watched, updated_at FROM watch_progress WHERE user_id = ?1",
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
                for media in state
                    .media
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
                        object.insert("mplex_last_viewed_at".to_owned(), serde_json::json!(updated_at));
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
    find_media(state, id)
        .and_then(|media| media_library(state, media))
        .or_else(|| {
            find_plex_tv_location(state, id).map(|(section, library, _, _)| (section, library))
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
    let metadata = plex_section_metadata(&state, section);
    let source_groups = (library.kind == "tv").then(|| {
        plex_tv_show_groups(
            section,
            &library.folders,
            state
                .media
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
        .or_else(|| find_media(&state, &id));
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
        let mut candidates = state
            .media
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
            "librarySectionUUID": plex_library_uuid(&state.machine_identifier, section),
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
                state
                    .media
                    .iter()
                    .filter(|media| media_in_library(media, library)),
            );
            for show in shows {
                let representative = show.episodes.first().copied();
                let show_metadata = representative.and_then(|media| metadata.get(&media.id));
                let show_title = show_metadata
                    .and_then(|value| value.get("name"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(&show.title);
                if plex_search_type_matches(query, "show") {
                    if let Some(rank) = [
                        show_title,
                        show.title.as_str(),
                        show_metadata
                            .and_then(|value| value.get("original_name"))
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default(),
                    ]
                    .into_iter()
                    .filter_map(|candidate| plex_search_rank(term, candidate))
                    .min()
                    {
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
                    let Some(rank) = [episode_title, episode_filename_title.as_str()]
                        .into_iter()
                        .filter_map(|candidate| plex_search_rank(term, candidate))
                        .min()
                    else {
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
                    plex_tv_episode_parents(&mut item, &show, season, episode);
                    add_plex_technical_metadata(state, &mut item, media).await;
                    results.push((rank, episode_title.to_lowercase(), item));
                }
            }
        } else if plex_search_type_matches(query, "movie") {
            for media in state
                .media
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
    let uuid = plex_library_uuid(&state.machine_identifier, section);
    plex_json(serde_json::json!({
        "MediaContainer": {
            "size": 1,
            "librarySectionID": section,
            "librarySectionTitle": library.name,
            "Directory": [{
                "key": format!("/library/sections/{section}/all"),
                "type": plex_kind(&library.kind),
                "title": library.name,
                "uuid": uuid
            }]
        }
    }))
}

async fn plex_section_prefs(State(state): State<AppState>, Path(section): Path<usize>) -> Response {
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
            "librarySectionID": section,
            "librarySectionTitle": library.name,
            "Setting": []
        }
    }))
}

async fn plex_section_collections(
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
            "totalSize": 0,
            "offset": 0,
            "identifier": "com.plexapp.plugins.library",
            "librarySectionID": section,
            "librarySectionTitle": library.name,
            "librarySectionUUID": plex_library_uuid(&state.machine_identifier, section),
            "title1": library.name,
            "title2": "Collections",
            "viewGroup": plex_kind(&library.kind),
            "Metadata": []
        }
    }))
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
            "librarySectionUUID": plex_library_uuid(&state.machine_identifier, section),
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
            "librarySectionUUID": plex_library_uuid(&state.machine_identifier, section),
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
    let mut initials = BTreeSet::new();
    if library.kind == "tv" {
        for show in plex_tv_show_groups(
            section,
            &library.folders,
            state
                .media
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
            state
                .media
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
            "librarySectionUUID": plex_library_uuid(&state.machine_identifier, section),
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
            state
                .media
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
        for media in state
            .media
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
        &plex_library_uuid(&state.machine_identifier, section),
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
    let mut media = s
        .media
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
    let uuid = plex_library_uuid(&s.machine_identifier, section);
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
    let mut shows = plex_tv_show_groups(
        section,
        &library.folders,
        state
            .media
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
        &plex_library_uuid(&state.machine_identifier, section),
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
    if let Some(tmdb_id) = item_metadata
        .and_then(|metadata| metadata.get("id"))
        .and_then(serde_json::Value::as_i64)
    {
        item["guid"] = serde_json::json!(format!("tmdb://tv/{tmdb_id}"));
    }
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
) {
    let season_key = plex_tv_season_key(&show.rating_key, season);
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
    state: &'a AppState,
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
            state
                .media
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
    if let Some((section, library, mut show, season)) = find_plex_tv_location(&s, &id) {
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
        let uuid = plex_library_uuid(&s.machine_identifier, section);
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
                plex_tv_episode_parents(&mut item, &show, season, episode_number);
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
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some((section, library)) = media_library(&s, media) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let metadata_by_id = plex_section_metadata(&s, section);
    let mut video = plex_video_json(media, section, &library, metadata_by_id.get(&media.id));
    if library.kind == "tv" {
        let groups = plex_tv_show_groups(
            section,
            &library.folders,
            s.media
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
            plex_tv_episode_parents(&mut video, &show, season, episode);
            if let Some(show_title) = metadata_by_id
                .get(&media.id)
                .and_then(|metadata| metadata.get("name"))
                .and_then(serde_json::Value::as_str)
            {
                video["grandparentTitle"] = serde_json::json!(show_title);
            }
        }
    }
    add_plex_technical_metadata(&s, &mut video, media).await;
    let mut container = plex_library_media_container(
        &library,
        section,
        &plex_library_uuid(&s.machine_identifier, section),
        1,
        0,
        1,
    );
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
        &plex_library_uuid(&state.machine_identifier, section),
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

fn plex_play_queue_response(
    id: u64,
    source_uri: &str,
    items: Vec<serde_json::Value>,
    selected_index: usize,
) -> serde_json::Value {
    let selected = items.get(selected_index);
    let selected_item_id = selected
        .and_then(|item| item.get("playQueueItemID"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let selected_metadata_item_id = selected
        .and_then(|item| item.get("Media"))
        .and_then(serde_json::Value::as_array)
        .and_then(|media| media.first())
        .and_then(|media| media.get("id"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    serde_json::json!({
        "MediaContainer": {
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
    })
}

async fn plex_create_play_queue(
    State(state): State<AppState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(source_uri) = query.get("uri") else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if !matches!(query.get("type").map(String::as_str), Some("video")) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let rating_key = query
        .get("key")
        .and_then(|key| plex_rating_key_from_source(key))
        .or_else(|| plex_rating_key_from_source(source_uri));
    let Some(rating_key) = rating_key else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(media) = find_media(&state, &rating_key) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some((section, library)) = media_library(&state, media) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let metadata = plex_section_metadata(&state, section);
    let mut item = plex_video_json(media, section, &library, metadata.get(&media.id));
    add_plex_technical_metadata(&state, &mut item, media).await;
    let queue_id = state
        .next_plex_play_queue_id
        .fetch_add(1, Ordering::Relaxed);
    let queue_item_id = plex_safe_numeric_id(&format!("play-queue:{queue_id}:{}", media.id));
    item["playQueueItemID"] = serde_json::json!(queue_item_id);
    let response = plex_play_queue_response(queue_id, source_uri, vec![item], 0);
    if let Ok(mut queues) = state.plex_play_queues.lock() {
        queues.insert(queue_id, response.clone());
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
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(media) = find_media(&state, &media_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some((section, library)) = media_library(&state, media) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let metadata_by_id = plex_section_metadata(&state, section);
    let mut video = plex_video_json(media, section, &library, metadata_by_id.get(&media.id));
    add_plex_technical_metadata(&state, &mut video, media).await;
    plex_mark_direct_play(&mut video);
    let library_uuid = plex_library_uuid(&state.machine_identifier, section);
    plex_json(plex_direct_play_decision_response(
        video,
        section,
        &library,
        &library_uuid,
    ))
}

fn plex_direct_play_decision_response(
    video: serde_json::Value,
    section: usize,
    library: &ConfigLibrary,
    library_uuid: &str,
) -> serde_json::Value {
    let mut container = plex_library_media_container(library, section, library_uuid, 1, 0, 1);
    container["directPlayDecisionCode"] = serde_json::json!(1000);
    container["directPlayDecisionText"] = serde_json::json!("Direct play OK.");
    container["generalDecisionCode"] = serde_json::json!(1000);
    container["generalDecisionText"] = serde_json::json!("Direct play OK.");
    container["transcodeDecisionCode"] = serde_json::json!(1000);
    container["transcodeDecisionText"] = serde_json::json!("Direct play OK.");
    container["Metadata"] = serde_json::json!([video]);
    serde_json::json!({"MediaContainer": container})
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
                .any(|path| plex_external_subtitle_id(media, path) == selected)
    });
    if !audio_valid || !subtitle_valid {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if let Ok(mut selections) = state.plex_stream_selections.lock() {
        let current = selections.entry(plex_part_id(media)).or_default();
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
    stream_file(media, method, headers).await
}

async fn plex_stream(
    State(state): State<AppState>,
    Path(stream): Path<String>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some((stream_id, extension)) = stream.rsplit_once('.') else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(stream_id) = stream_id.parse::<u64>() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !matches!(extension, "srt" | "ass" | "vtt") {
        return StatusCode::NOT_FOUND.into_response();
    }
    for media in state.media.iter() {
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
    let Some((_, library)) = media_library(state, media) else {
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
    if filename == "cover.jpg" {
        if let Some(bytes) = plex_frame_cover(state, media).await {
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
    let directories_json = libraries
        .iter()
        .enumerate()
        .map(|(index, library)| {
            let section = index + 1;
            serde_json::json!({
                "key": format!("/library/sections/{section}"),
                "type": plex_kind(&library.kind),
                "title": library.name,
                "agent": if library.kind == "tv" { "tv.plex.agents.series" } else { "tv.plex.agents.movie" },
                "scanner": if library.kind == "tv" { "Plex TV Series" } else { "Plex Movie" },
                "language": "en-US",
                "uuid": plex_library_uuid(&state.machine_identifier, section),
                "refreshing": false
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
        "ratingKey": media.id,
        "key": format!("/library/metadata/{}", media.id),
        "type": media_type,
        "title": title,
        "thumb": format!("/library/metadata/{}/thumb", media.id),
        "art": format!("/library/metadata/{}/art", media.id),
        "librarySectionID": section,
        "librarySectionKey": format!("/library/sections/{section}"),
        "librarySectionTitle": library.name,
        "originalTitle": media.title,
        "titleSort": media.title.to_lowercase(),
        "updatedAt": modified_at,
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

async fn jellyfin_public_system() -> Json<serde_json::Value> {
    Json(serde_json::json!({"LocalAddress":"","ServerName":"mplex","Version":"0.1.0","Id":"mplex"}))
}

async fn jellyfin_public_users() -> Json<serde_json::Value> {
    Json(serde_json::json!([{"Name":"mplex","Id":USER_ID,"HasPassword":false}]))
}

#[derive(Deserialize)]
struct Login {
    #[serde(rename = "Username")]
    _username: Option<String>,
    #[serde(rename = "Pw")]
    _password: Option<String>,
}

async fn jellyfin_authenticate(
    State(s): State<AppState>,
    Json(_login): Json<Login>,
) -> Json<serde_json::Value> {
    Json(
        serde_json::json!({"User":{"Name":"mplex","Id":USER_ID},"AccessToken":s.token.as_ref(),"ServerId":"mplex"}),
    )
}

async fn jellyfin_views() -> Json<serde_json::Value> {
    Json(
        serde_json::json!({"Items":[{"Name":"Movies","Id":"library","CollectionType":"movies","ServerId":"mplex"}],"TotalRecordCount":1}),
    )
}

async fn jellyfin_items(State(s): State<AppState>) -> Json<serde_json::Value> {
    Json(
        serde_json::json!({"Items":s.media.iter().map(jellyfin_item_value).collect::<Vec<_>>(),"TotalRecordCount":s.media.len()}),
    )
}

async fn jellyfin_item(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    Json(
        serde_json::json!({"Items":find_media(&s, &id).map(jellyfin_item_value).into_iter().collect::<Vec<_>>(),"TotalRecordCount":if find_media(&s, &id).is_some() { 1 } else { 0 }}),
    )
}

fn jellyfin_item_value(media: &Media) -> serde_json::Value {
    let path = media.path.to_string_lossy().to_string();
    serde_json::json!({"Name":media.title,"Id":media.id,"Type":"Movie","MediaType":"Video","RunTimeTicks":0,"Container":media.container,"Path":path,"ImageTags":{},"MediaSources":[{"Id":media.id,"Path":path,"DirectStreamUrl":format!("/Videos/{}/stream", media.id),"Container":media.container,"Size":media.size,"SupportsDirectPlay":true,"SupportsDirectStream":false,"SupportsTranscoding":false,"MediaStreams":[]}]})
}

async fn jellyfin_playback_info(State(s): State<AppState>, Path(id): Path<String>) -> Response {
    let Some(media) = find_media(&s, &id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let path = media.path.to_string_lossy().to_string();
    Json(serde_json::json!({"MediaSources":[{"Id":media.id,"Path":path,"DirectStreamUrl":format!("/Videos/{}/stream", media.id),"Container":media.container,"Size":media.size,"SupportsDirectPlay":true,"SupportsDirectStream":false,"SupportsTranscoding":false,"MediaStreams":[]}]})).into_response()
}

async fn jellyfin_stream(
    State(s): State<AppState>,
    Path(id): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    stream_for_id(s, id, method, headers).await
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
    stream_file(media, method, headers).await
}

async fn stream_file(media: &Media, method: Method, headers: HeaderMap) -> Response {
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
mod tests {
    use super::{
        apply_plex_technical_metadata, ass_to_vtt, clean_relative_path, contains_hidden_component,
        external_subtitle_content, external_subtitle_language_token, extract_xml_token, html,
        parse_range, parse_sidecar_metadata, parse_tv_episode_marker, parse_tv_season_directory,
        plex_children_json, plex_codec, plex_connectivity_url, plex_device_announce_query,
        plex_device_announce_url, plex_direct_play_decision_response, plex_empty_extras_response,
        plex_external_subtitle_id, plex_genre_tokens, plex_hub_categories, plex_language,
        plex_library_children_container, plex_library_media_container, plex_mark_direct_play,
        plex_media_id, plex_paged_children, plex_part_id, plex_photo_source,
        plex_play_queue_response, plex_presence_url, plex_publish_urls, plex_query_media_id,
        plex_ranked_tv_shows, plex_rating_key_from_source, plex_safe_numeric_id, plex_search_hubs,
        plex_search_rank, plex_search_result_entries, plex_search_type_matches, plex_stream_id,
        plex_track_label, plex_tv_episode_parents, plex_tv_season_groups, plex_tv_season_json,
        plex_tv_show_groups, plex_tv_show_json, plex_video_json, plex_video_resolution,
        sidebar_nav_with_settings, srt_to_vtt, technical_duration_ms, technical_tracks,
        tv_show_title, vtt_to_srt, ConfigLibrary, HeaderMap, HeaderValue, Media, PathBuf,
        PlexStreamSelection, Uri,
    };
    #[test]
    fn plex_playback_ids_resolve_rating_keys_and_metadata_paths() {
        assert_eq!(
            plex_query_media_id(Some("2-42"), Some("/library/metadata/other")),
            Some("2-42".to_owned())
        );
        assert_eq!(
            plex_query_media_id(None, Some("/library/metadata/2-42")),
            Some("2-42".to_owned())
        );
        assert_eq!(plex_query_media_id(None, None), None);
    }
    #[test]
    fn plex_section_hubs_include_explicit_recently_added_even_without_ratings() {
        assert_eq!(
            plex_hub_categories(false, Some("recentlyAdded")),
            vec!["recentlyAdded"]
        );
        assert_eq!(plex_hub_categories(false, None), vec!["recommended"]);
        assert_eq!(
            plex_hub_categories(true, None),
            vec!["recommended", "recentlyAdded"]
        );
    }
    #[test]
    fn plex_photo_sources_resolve_thumb_and_art_routes() {
        assert_eq!(
            plex_photo_source("/library/metadata/2-42/thumb/1715112705"),
            Some(("2-42".to_owned(), "cover.jpg"))
        );
        assert_eq!(
            plex_photo_source("https://server.example/library/metadata/2-42/art"),
            Some(("2-42".to_owned(), "banner.jpg"))
        );
        assert_eq!(plex_photo_source("/library/metadata/2-42/poster"), None);
    }
    #[test]
    fn media_info_json_duration_and_tracks_map_to_plex_fields() {
        let info = serde_json::json!({
            "media": {"track": [
                {"@type":"General", "Duration":"120.5"},
                {"@type":"Video", "StreamOrder":"0", "Format":"AVC", "Width":"1920", "Height":"1080"},
                {"@type":"Audio", "StreamOrder":"1", "Format":"AC-3", "Channels":"6", "Language":"English"},
                {"@type":"Text", "StreamOrder":"2", "Format":"UTF-8", "Language":"English"}
            ]}
        });
        assert_eq!(technical_duration_ms(&info), Some(120_500));
        let tracks = technical_tracks(&info);
        assert_eq!(tracks.len(), 4);
        assert_eq!(plex_video_resolution(1080), "1080");
        assert_eq!(plex_codec("AC-3"), "ac3");
        assert_eq!(plex_track_label(&tracks[2], 2), "English (AC3 5.1)");
    }
    #[test]
    fn plex_media_advertises_playable_part_audio_video_and_external_subtitle() {
        let media = Media {
            id: "1-7".to_owned(),
            title: "Film".to_owned(),
            path: PathBuf::from("/media/Film.mkv"),
            size: 9_000_000,
            container: "mkv".to_owned(),
            mime: "video/x-matroska".to_owned(),
        };
        let library = ConfigLibrary {
            name: "Movies".to_owned(),
            kind: "movie".to_owned(),
            folders: Vec::new(),
            show_recently_added: true,
            show_continue_watching: true,
        };
        let info = serde_json::json!({"media":{"track":[
            {"@type":"General", "Duration":"120.5"},
            {"@type":"Video", "StreamOrder":"0", "Format":"AVC", "Width":"1280", "Height":"720"},
            {"@type":"Audio", "StreamOrder":"1", "Format":"AC-3", "Channels":"6", "Language":"eng"},
            {"@type":"Text", "StreamOrder":"2", "Format":"UTF-8", "Language":"rum"}
        ]}});
        let subtitle = PathBuf::from("/media/Film.rum.srt");
        let mut item = plex_video_json(&media, 1, &library, None);
        apply_plex_technical_metadata(&mut item, &media, &info, &[subtitle], None);

        assert_eq!(item["duration"], 120_500);
        assert!(item.get("id").is_none());
        assert_eq!(item["ratingKey"], "1-7");
        assert_eq!(item["librarySectionKey"], "/library/sections/1");
        assert_eq!(item["Media"][0]["videoResolution"], "720");
        assert_eq!(item["Media"][0]["videoCodec"], "h264");
        assert_eq!(item["Media"][0]["audioCodec"], "ac3");
        assert_eq!(item["Media"][0]["audioChannels"], 6);
        assert_eq!(item["Media"][0]["id"], plex_media_id(&media));
        assert_ne!(item["Media"][0]["id"], item["Media"][0]["Part"][0]["id"]);
        assert!(plex_media_id(&media) <= (1_u64 << 53) - 1);
        assert_eq!(item["Media"][0]["Part"][0]["id"], plex_part_id(&media));
        assert!(plex_part_id(&media) <= (1_u64 << 53) - 1);
        assert!(item["Media"][0]["Part"][0]["key"]
            .as_str()
            .unwrap()
            .ends_with("/file.mkv"));
        let streams = item["Media"][0]["Part"][0]["Stream"].as_array().unwrap();
        assert_eq!(streams.len(), 4);
        assert_eq!(streams[1]["type"], 2);
        assert_eq!(streams[1]["channels"], 6);
        assert_eq!(streams[1]["selected"], true);
        assert_eq!(streams[2]["language"], "ro");
        assert_eq!(streams[3]["displayTitle"], "Romanian (SRT External)");
        assert!(streams[3]["id"].as_u64().unwrap() <= (1_u64 << 53) - 1);
        assert!(streams[3]["key"]
            .as_str()
            .unwrap()
            .starts_with("/library/streams/"));
        assert!(streams[3]["key"].as_str().unwrap().ends_with(".srt"));

        let subtitle_id = plex_external_subtitle_id(&media, &PathBuf::from("/media/Film.rum.srt"));
        apply_plex_technical_metadata(
            &mut item,
            &media,
            &info,
            &[PathBuf::from("/media/Film.rum.srt")],
            Some(PlexStreamSelection {
                audio_stream_id: Some(2),
                subtitle_stream_id: Some(subtitle_id),
            }),
        );
        let streams = item["Media"][0]["Part"][0]["Stream"].as_array().unwrap();
        assert_eq!(streams[1]["selected"], true);
        assert_eq!(streams[2]["selected"], false);
        assert_eq!(streams[3]["selected"], true);

        let container = plex_library_media_container(&library, 1, "library-uuid", 1, 0, 1);
        assert_eq!(container["identifier"], "com.plexapp.plugins.library");
        assert_eq!(container["librarySectionUUID"], "library-uuid");
        assert_eq!(container["viewGroup"], "movie");

        plex_mark_direct_play(&mut item);
        let decision = plex_direct_play_decision_response(item, 1, &library, "library-uuid");
        let decision_container = &decision["MediaContainer"];
        assert_eq!(decision_container["directPlayDecisionCode"], 1000);
        assert_eq!(decision_container["generalDecisionText"], "Direct play OK.");
        assert_eq!(
            decision_container["Metadata"][0]["Media"][0]["Part"][0]["decision"],
            "directplay"
        );
    }

    #[test]
    fn plex_numeric_stream_ids_are_javascript_safe_stable_and_distinct() {
        let first = Media {
            id: "1-7".to_owned(),
            title: "Film".to_owned(),
            path: PathBuf::from("/media/Film.mkv"),
            size: 9_000_000,
            container: "mkv".to_owned(),
            mime: "video/x-matroska".to_owned(),
        };
        let subtitle = PathBuf::from("/media/Film.en.srt");
        let id = plex_external_subtitle_id(&first, &subtitle);
        assert!((1..=(1_u64 << 53) - 1).contains(&id));
        assert_eq!(id, plex_stream_id(&first, &subtitle));
        assert_eq!(id, plex_external_subtitle_id(&first, &subtitle));
        assert_ne!(id, plex_part_id(&first));
        assert_ne!(id, plex_safe_numeric_id("stream:1-7:/media/Film.fr.srt"));
    }

    #[test]
    fn romanian_iso_language_codes_are_friendly_and_normalized() {
        assert_eq!(plex_language("rum"), ("ro", "Romanian"));
        assert_eq!(plex_language("ron"), ("ro", "Romanian"));
        assert_eq!(plex_language("deu"), ("de", "German"));
        assert_eq!(plex_language("fry"), ("fy", "Western Frisian"));
        assert_eq!(plex_language("Northern Sami"), ("se", "Northern Sami"));
        assert_eq!(plex_language("en-US"), ("en", "English"));
        assert_eq!(plex_language("vi"), ("vi", "Vietnamese"));
        assert_eq!(plex_language("vie"), ("vi", "Vietnamese"));
        assert_eq!(plex_language("se"), ("se", "Northern Sami"));
        assert_eq!(plex_language("sme"), ("se", "Northern Sami"));
        assert_eq!(plex_language("sv"), ("sv", "Swedish"));
        assert_eq!(plex_language("Zaza"), ("zza", "Zaza"));
        assert_eq!(
            external_subtitle_language_token("Film", "Film.RUM.default"),
            "RUM"
        );
        assert_eq!(
            super::external_subtitle_label(
                std::path::Path::new("Film.mkv"),
                std::path::Path::new("Film.vi.srt")
            ),
            "Vietnamese (SRT External)"
        );
        assert_eq!(
            super::external_subtitle_label(
                std::path::Path::new("Film.mkv"),
                std::path::Path::new("Film.se.srt")
            ),
            "Northern Sami (SRT External)"
        );
    }

    #[test]
    fn untagged_audio_track_still_has_a_readable_label() {
        let track = serde_json::json!({
            "@type":"Audio", "Format":"AAC", "Channels":"2", "ChannelLayout":"Stereo (2.0)"
        });
        assert_eq!(plex_track_label(&track, 2), "AAC Stereo");
    }

    #[test]
    fn sidecar_metadata_json_loads_without_database_state() {
        let info = r#"metadata_json = '{"id":42,"title":"Portable match"}'"#;
        let metadata = parse_sidecar_metadata(info).unwrap();
        assert_eq!(metadata["id"], 42);
        assert_eq!(metadata["title"], "Portable match");
        assert_eq!(parse_sidecar_metadata("title = 'Unmatched media'"), None);
    }
    #[test]
    fn full_file() {
        assert_eq!(parse_range(None, 10), Ok((0, 9)));
    }
    #[test]
    fn valid_range() {
        assert_eq!(parse_range(Some("bytes=2-5"), 10), Ok((2, 5)));
    }
    #[test]
    fn suffix_range() {
        assert_eq!(parse_range(Some("bytes=-3"), 10), Ok((7, 9)));
    }
    #[test]
    fn unsatisfiable_range() {
        assert!(parse_range(Some("bytes=10-"), 10).is_err());
    }
    #[test]
    fn html_escapes_titles() {
        assert_eq!(html("A & <B>"), "A &amp; &lt;B&gt;");
    }
    #[test]
    fn extracts_plex_server_token() {
        assert_eq!(
            extract_xml_token(
                "<user><authentication-token>server-token</authentication-token></user>"
            ),
            Some("server-token".to_owned())
        );
        assert_eq!(extract_xml_token("<user />"), None);
    }
    #[test]
    fn plex_publishes_lan_http_urls_for_plex_direct_discovery() {
        assert_eq!(
            plex_publish_urls(
                &[
                    "10.20.61.51".parse().unwrap(),
                    "192.168.1.10".parse().unwrap()
                ],
                32400
            ),
            ["http://10.20.61.51:32400", "http://192.168.1.10:32400"]
        );
    }
    #[test]
    fn plex_device_announce_matches_measured_pms_request_shape() {
        let urls = vec!["http://10.20.61.51:32400".to_owned()];
        assert_eq!(
            plex_device_announce_url("machine-id"),
            "https://servers.plex.tv/devices/machine-id"
        );
        assert_eq!(
            plex_device_announce_query(&urls, true, "server-token"),
            [
                ("Connection[][uri]", "http://10.20.61.51:32400"),
                ("httpsEnabled", "1"),
                ("httpsRequired", "0"),
                ("dnsRebindingProtection", "0"),
                ("X-Plex-Token", "server-token"),
            ]
        );
    }
    #[test]
    fn plex_presence_and_connectivity_urls_match_measured_pms_endpoints() {
        assert_eq!(
            plex_presence_url("12345", "machine-id"),
            "https://pubsub.plex.tv/sub/eventsource/12345/machine-id"
        );
        assert_eq!(
            plex_connectivity_url("machine-id"),
            "https://plex.tv/api/servers/machine-id/connectivity"
        );
    }
    #[test]
    fn converts_srt_to_webvtt() {
        let vtt = srt_to_vtt("1\n00:00:01,250 --> 00:00:03,000\nHello\n");
        assert!(vtt.starts_with("WEBVTT\n\n"));
        assert!(vtt.contains("00:00:01.250 --> 00:00:03.000\nHello"));
        let (content_type, served) =
            external_subtitle_content("1\n00:00:01,250 --> 00:00:03,000\nHello\n", "srt", "srt")
                .unwrap();
        assert_eq!(content_type, "text/srt; charset=utf-8");
        assert!(served.starts_with("1\n00:00:01,250"));
        let (_, served_vtt) =
            external_subtitle_content("1\n00:00:01,250 --> 00:00:03,000\nHello\n", "srt", "vtt")
                .unwrap();
        assert!(served_vtt.starts_with("WEBVTT\n\n"));
        assert!(vtt_to_srt(&served_vtt).contains("00:00:01,250 --> 00:00:03,000"));
    }
    #[test]
    fn converts_ass_dialogue_to_webvtt_and_removes_tags() {
        let vtt = ass_to_vtt(
            "[Events]\nDialogue: 0,0:01:02.50,0:01:04.00,Default,,0,0,0,,{\\i1}Hello\\Nworld",
        );
        assert!(vtt.contains("00:01:02.500 --> 00:01:04.000\nHello\nworld"));
    }
    #[test]
    fn web_paths_stay_relative() {
        assert_eq!(
            clean_relative_path("season one/episode.mp4").unwrap(),
            std::path::PathBuf::from("season one/episode.mp4")
        );
        assert!(clean_relative_path("../outside.mp4").is_none());
        assert!(clean_relative_path("/outside.mp4").is_none());
    }
    #[test]
    fn web_paths_hide_dotfiles() {
        assert!(contains_hidden_component(std::path::Path::new(
            ".secret/video.mp4"
        )));
        assert!(contains_hidden_component(std::path::Path::new(
            "folder/.video.mp4"
        )));
        assert!(!contains_hidden_component(std::path::Path::new(
            "folder/video.mp4"
        )));
    }
    #[test]
    fn sidebar_links_to_home_and_library() {
        let sidebar = sidebar_nav_with_settings(true, false);
        assert!(sidebar.contains("href=\"/\""));
        assert!(sidebar.contains("href=\"/library\""));
        assert!(sidebar.contains("class=\"sidebar-link active\""));
        assert!(sidebar.contains("sidebar-toggle"));
        let settings = sidebar_nav_with_settings(false, true);
        assert!(settings.contains("href=\"/settings\""));
        assert!(!settings.contains("<a class=\"sidebar-link active\" href=\"/\""));
    }
    #[test]
    fn parses_plex_season_and_episode_names() {
        assert_eq!(parse_tv_season_directory("Season 02"), Some(2));
        assert_eq!(parse_tv_season_directory("season.02"), Some(2));
        assert_eq!(parse_tv_season_directory("specials"), Some(0));
        let parsed = parse_tv_episode_marker("Show Name - s02e18-e19 - Finale").unwrap();
        assert_eq!(parsed.0, Some(2));
        assert_eq!(parsed.1, (18, Some(19)));
        assert_eq!(&"Show Name - s02e18-e19 - Finale"[parsed.2..], " - Finale");
    }

    #[test]
    fn plex_tv_library_groups_episode_files_under_show_and_season_items() {
        let root = PathBuf::from("/media/tv");
        let media = vec![
            Media {
                id: "3-1".to_owned(),
                title: "Example Show s01e01 Pilot.mkv".to_owned(),
                path: root.join("Example Show/Season 01/Example.Show.s01e01.Pilot.mkv"),
                size: 100,
                container: "mkv".to_owned(),
                mime: "video/x-matroska".to_owned(),
            },
            Media {
                id: "3-2".to_owned(),
                title: "Example Show s01e02 Arrival.mkv".to_owned(),
                path: root.join("Example Show/Season 01/Example.Show.s01e02.Arrival.mkv"),
                size: 100,
                container: "mkv".to_owned(),
                mime: "video/x-matroska".to_owned(),
            },
        ];
        let roots = vec![root.clone()];
        assert_eq!(tv_show_title(&root, &media[0].path), "Example Show");
        let mut shows = plex_tv_show_groups(3, &roots, media.iter());
        assert_eq!(shows.len(), 1);
        let show = shows.pop().unwrap();
        assert_eq!(show.title, "Example Show");
        assert_eq!(show.episodes.len(), 2);
        let ranked_shows =
            plex_ranked_tv_shows(&[&media[1], &media[0]], std::slice::from_ref(&show));
        assert_eq!(ranked_shows.len(), 1);
        assert_eq!(ranked_shows[0].episodes.len(), 2);
        let seasons = plex_tv_season_groups(&show, &roots);
        assert_eq!(seasons.len(), 1);
        assert_eq!(seasons[&1].len(), 2);

        let library = ConfigLibrary {
            name: "TV".to_owned(),
            kind: "tv".to_owned(),
            folders: roots,
            show_recently_added: true,
            show_continue_watching: true,
        };
        let metadata = std::collections::HashMap::from([(
            "3-1".to_owned(),
            serde_json::json!({"mplex_view_count": 1}),
        )]);
        let show_item = plex_tv_show_json(&show, 3, &library, &metadata);
        assert_eq!(show_item["type"], "show");
        assert_eq!(show_item["childCount"], 1);
        assert_eq!(show_item["leafCount"], 2);
        assert_eq!(show_item["viewedLeafCount"], 1);
        assert!(show_item.get("Media").is_none());

        let season_item = plex_tv_season_json(&show, 1, 3, &library, &metadata, 2);
        assert_eq!(season_item["type"], "season");
        assert_eq!(season_item["parentRatingKey"], show.rating_key);
        assert_eq!(season_item["childCount"], 2);
        let seasons_container =
            plex_library_children_container(&library, 3, "tv-uuid", 1, 0, 1, "season");
        let episodes_container =
            plex_library_children_container(&library, 3, "tv-uuid", 2, 0, 2, "episode");
        assert_eq!(seasons_container["viewGroup"], "season");
        assert_eq!(episodes_container["viewGroup"], "episode");
        let mut episode_item = plex_video_json(&media[0], 3, &library, None);
        plex_tv_episode_parents(&mut episode_item, &show, 1, 1);
        assert_eq!(episode_item["type"], "episode");
        assert!(!episode_item["key"].as_str().unwrap().ends_with("/children"));
        assert_eq!(episode_item["parentType"], "season");
        assert_eq!(episode_item["grandparentType"], "show");
        assert_eq!(
            episode_item["parentKey"],
            format!("/library/metadata/{}-season-1", show.rating_key)
        );
        assert_eq!(
            episode_item["grandparentKey"],
            format!("/library/metadata/{}", show.rating_key)
        );
        assert_eq!(episode_item["title"], "Episode 1 · Pilot");
    }

    #[test]
    fn plex_tv_children_include_nested_items_and_default_to_first_page() {
        let values = (0..25)
            .map(|index| serde_json::json!({"index": index}))
            .collect::<Vec<_>>();
        let uri = Uri::from_static("/library/metadata/show/children");
        let (offset, page) = plex_paged_children(&values, &HeaderMap::new(), &uri);
        assert_eq!(offset, 0);
        assert_eq!(page.len(), 20);
        let mut headers = HeaderMap::new();
        headers.insert("X-Plex-Container-Start", HeaderValue::from_static("20"));
        headers.insert("X-Plex-Container-Size", HeaderValue::from_static("10"));
        let (offset, page) = plex_paged_children(&values, &headers, &uri);
        assert_eq!(offset, 20);
        assert_eq!(page.len(), 5);
        assert_eq!(plex_children_json(page, 25, offset)["totalSize"], 25);
    }

    #[test]
    fn plex_related_genre_matching_uses_tmdb_ids_and_names() {
        let metadata = serde_json::json!({
            "genre_ids": [28, 878],
            "genres": [{"name": "Adventure"}, {"id": 12}]
        });
        let tokens = plex_genre_tokens(Some(&metadata));
        assert!(tokens.contains("id:28"));
        assert!(tokens.contains("id:878"));
        assert!(tokens.contains("id:12"));
        assert!(tokens.contains("name:adventure"));
    }

    #[test]
    fn plex_search_matches_titles_and_respects_type_filters() {
        assert_eq!(plex_search_rank("har", "Harbringer"), Some(1));
        assert_eq!(plex_search_rank("br", "Harbringer"), Some(2));
        assert_eq!(plex_search_rank("harbringer", "The Harbinger"), Some(3));
        assert_eq!(plex_search_rank("star trek", "Star Trek"), Some(0));
        assert_eq!(plex_search_rank("", "Harbringer"), None);
        assert_eq!(plex_search_rank("missing", "Harbringer"), None);

        let movie_query = std::collections::BTreeMap::from([("type".to_owned(), "1".to_owned())]);
        assert!(plex_search_type_matches(&movie_query, "movie"));
        assert!(!plex_search_type_matches(&movie_query, "show"));
        let episode_query =
            std::collections::BTreeMap::from([("type".to_owned(), "episode".to_owned())]);
        assert!(plex_search_type_matches(&episode_query, "episode"));
        assert!(!plex_search_type_matches(&episode_query, "movie"));
    }

    #[test]
    fn plex_search_returns_plex_typed_hubs_and_show_directories() {
        let hubs = plex_search_hubs(
            vec![
                serde_json::json!({"type": "show", "title": "Example Show"}),
                serde_json::json!({"type": "movie", "title": "Example Movie"}),
            ],
            3,
        );
        let show_hub = hubs.iter().find(|hub| hub["type"] == "show").unwrap();
        assert_eq!(show_hub["Directory"][0]["title"], "Example Show");
        assert!(show_hub.get("Metadata").is_none());
        let movie_hub = hubs.iter().find(|hub| hub["type"] == "movie").unwrap();
        assert_eq!(movie_hub["Metadata"][0]["title"], "Example Movie");
        assert!(movie_hub.get("Directory").is_none());
    }

    #[test]
    fn plex_library_search_result_wraps_items_as_plezy_expects() {
        let entries = plex_search_result_entries(&[serde_json::json!({
            "ratingKey": "1-42",
            "type": "movie",
            "title": "Example Movie"
        })]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["Metadata"]["ratingKey"], "1-42");
        assert_eq!(entries[0]["Metadata"]["title"], "Example Movie");
        assert_eq!(entries[0]["score"], 1.0);
    }

    #[test]
    fn plex_extras_for_known_media_return_an_empty_json_container() {
        let library = ConfigLibrary {
            name: "Movies".to_owned(),
            kind: "movie".to_owned(),
            folders: Vec::new(),
            show_recently_added: true,
            show_continue_watching: true,
        };
        let response = plex_empty_extras_response(&library, 1, "library-uuid");
        assert_eq!(response["MediaContainer"]["size"], 0);
        assert_eq!(
            response["MediaContainer"]["Metadata"],
            serde_json::json!([])
        );
        assert_eq!(response["MediaContainer"]["title2"], "Extras");
    }

    #[test]
    fn plex_play_queue_source_and_response_follow_pms_shape() {
        assert_eq!(
            plex_rating_key_from_source("/library/metadata/2-42"),
            Some("2-42".to_owned())
        );
        assert_eq!(
            plex_rating_key_from_source(
                "library://library-id/item//library/metadata/2-42?X-Plex-Token=test"
            ),
            Some("2-42".to_owned())
        );
        let item = serde_json::json!({
            "ratingKey": "2-42",
            "playQueueItemID": 73,
            "Media": [{"id": 51}]
        });
        let queue = plex_play_queue_response(
            9,
            "library://library-id/item/%2Flibrary%2Fmetadata%2F2-42",
            vec![item],
            0,
        );
        assert_eq!(queue["MediaContainer"]["playQueueID"], 9);
        assert_eq!(queue["MediaContainer"]["playQueueSelectedItemID"], 73);
        assert_eq!(
            queue["MediaContainer"]["playQueueSelectedMetadataItemID"],
            51
        );
        assert_eq!(queue["MediaContainer"]["playQueueTotalCount"], 1);
    }
}
