use axum::{
    body::Body,
    extract::{Form, Path, Query, RawForm, State},
    http::{header, HeaderMap, HeaderValue, Method, Request, StatusCode},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{any, get, post},
    Json, Router,
};
use clap::Parser;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Component, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
};
use tokio_util::io::ReaderStream;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

const USER_ID: &str = "mplex-user";
const POSTER_CARD_MIN_WIDTH: usize = 140;

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
    #[serde(default)]
    metadata_in_media_folder: bool,
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
    "0.0.0.0:8096".to_owned()
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

#[derive(Clone)]
struct AppState {
    config: Arc<Mutex<Config>>,
    config_path: Arc<PathBuf>,
    db: Arc<Mutex<Connection>>,
    machine_identifier: Arc<str>,
    root: Arc<PathBuf>,
    media: Arc<Vec<Media>>,
    token: Arc<str>,
}

struct Media {
    id: String,
    title: String,
    path: PathBuf,
    size: u64,
    container: String,
    mime: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
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

    let state = AppState {
        config: Arc::new(Mutex::new(config.clone())),
        config_path: Arc::new(config_path),
        db: Arc::new(Mutex::new(db)),
        machine_identifier: Arc::from(machine_identifier),
        root: Arc::new(root),
        media: Arc::new(media),
        token: Arc::from(config.token),
    };
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
        .route("/settings/libraries/:id/scan", post(scan_library_metadata))
        .route("/settings/media/scan", post(scan_media_metadata))
        .route("/settings/browse", get(settings_browse))
        .route("/identity", get(plex_identity))
        .route("/library/sections", get(plex_sections))
        .route("/library/sections/1/all", get(plex_all))
        .route("/library/metadata/:id", get(plex_item))
        .route("/library/metadata/:id/children", get(plex_item))
        .route("/library/metadata/:id/media/:part", get(plex_part))
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
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(config.bind.parse::<SocketAddr>()?).await?;
    let address = listener.local_addr()?;
    let host = if address.ip().is_unspecified() {
        "127.0.0.1".to_owned()
    } else {
        format!("{}", address.ip())
    };
    println!(
        r#"
           ____  _
 _ __ ___ |  _ \| | _____  __
| '_ ` _ \| |_) | |/ _ \ \/ /
| | | | | |  __/| |  __/>  <
|_| |_| |_|_|   |_|\___/_/\_\

mPlex running on port {}
Web client: http://{}:{}/
"#,
        address.port(),
        host,
        address.port()
    );
    axum::serve(listener, app).await?;
    Ok(())
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
            metadata_in_media_folder: false,
            metadata_storage: default_metadata_storage(),
            tmdb_bearer_token: None,
        });
    }
    let mut config: Config = toml::from_str(&std::fs::read_to_string(path)?)?;
    if config.metadata_in_media_folder {
        config.metadata_storage = "sidecar".to_owned();
    } else if config.metadata_storage == "sidecar" {
        config.metadata_in_media_folder = true;
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
        .join(".mplex");
    std::fs::create_dir_all(&state_dir)?;
    let connection = Connection::open(state_dir.join("state.sqlite"))?;
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS server_identity (
             id INTEGER PRIMARY KEY CHECK (id = 1),
             machine_identifier TEXT NOT NULL UNIQUE,
             server_name TEXT NOT NULL,
             plex_account_token TEXT,
             claimed INTEGER NOT NULL DEFAULT 0
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
                "INSERT INTO server_identity (id, machine_identifier, server_name) VALUES (1, ?1, 'mPlex')",
                [&generated],
            )?;
            Ok(generated)
        })?;
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
        .map(|config| config.metadata_in_media_folder)
        .unwrap_or(false);
    Html(format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>mPlex settings</title><style>
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
<main><h1>Settings</h1>{}{}
<p class="note">A library is a named collection with one type and one or more monitored folders. Create it with its first folder, then add more folders to that same library below.</p>
<h2>Metadata storage</h2><p class="note">Choose where matched movie and TV metadata is stored. SQLite is the runtime database; sidecar mode also writes portable files beside the media.</p><form method="post" action="/settings/preferences"><div class="metadata-options"><label class="metadata-option"><input type="radio" name="metadata_storage" value="database" {}><span><strong>SQLite database {}</strong><br><span class="note">Store metadata in mPlex’s database only.</span></span></label><label class="metadata-option"><input type="radio" name="metadata_storage" value="sidecar" {}><span><strong>.mplex files in media folders {}</strong><br><span class="note">Also write <code>.mplex/cover.jpg</code>, <code>banner.jpg</code>, and <code>info.toml</code>.</span></span></label></div><input name="tmdb_bearer_token" type="password" placeholder="TMDB bearer token (leave blank to keep current)" value=""><button type="submit">Save preferences</button></form>
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
        config.metadata_in_media_folder = true;
    } else if form.metadata_storage.as_deref() == Some("database") {
        config.metadata_storage = "database".to_owned();
        config.metadata_in_media_folder = false;
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
    let Some(token) = config
        .tmdb_bearer_token
        .as_deref()
        .filter(|token| !token.is_empty())
    else {
        return settings_error("Add a TMDB bearer token in Settings before scanning metadata");
    };
    let client = reqwest::Client::new();
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
            if config.metadata_in_media_folder {
                let representative = &group[0];
                let _ = store_metadata_sidecar(
                    folder,
                    &group_path,
                    &group_title,
                    representative,
                    &tv_metadata_for_media(result, folder, &representative.path, kind),
                )
                .await;
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
    if config.metadata_in_media_folder {
        let group_path = metadata_group_path(root, &media.path, kind);
        let _ = store_metadata_sidecar(root, &group_path, &title, &media, result).await;
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
        technical: probe_media(&media.path).await,
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

async fn probe_media(path: &std::path::Path) -> Option<serde_json::Value> {
    let output = tokio::process::Command::new("ffprobe")
        .args([
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
        ])
        .arg(path)
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    serde_json::from_slice(&output.stdout).ok()
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
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Choose folder · mPlex</title><style>body {{ margin: 0; padding: 2rem; color: #f9fafb; background: #111827; font-family: system-ui, sans-serif; }} main {{ max-width: 820px; margin: auto; }} a {{ color: #93c5fd; }} .toolbar {{ display: flex; flex-wrap: wrap; gap: .6rem; margin: 1.2rem 0; }} .shortcut, .action {{ display: inline-block; padding: .6rem .8rem; border: 1px solid #374151; border-radius: .5rem; text-decoration: none; background: #1f2937; }} .folder-list {{ padding: 0; list-style: none; border: 1px solid #374151; border-radius: .6rem; overflow: hidden; }} .folder {{ display: flex; justify-content: space-between; gap: 1rem; padding: .8rem 1rem; border-bottom: 1px solid #374151; }} .folder:last-child {{ border-bottom: 0; }} .open {{ margin-right: 1rem; }} code, .note {{ color: #9ca3af; overflow-wrap: anywhere; }} </style></head><body><main><p><a href="/settings">← Back to Settings</a></p><h1>Choose a media folder</h1><p class="note">Pick a folder on the mPlex server. You can add more folders to the same library after this.</p><p><strong>Current folder:</strong> <code>{}</code></p><div class="toolbar"><a class="action" href="/settings/browse?path={}">⬆ Parent</a><a class="action" href="/settings?folder={}">Choose this folder</a></div>{}<ul class="folder-list">{}</ul></main></body></html>"#,
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
        let artwork = library_artwork(library, index).await;
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
        let cover = tokio::fs::metadata(folder.join(".mplex/cover.jpg"))
            .await
            .is_ok();
        let banner = tokio::fs::metadata(folder.join(".mplex/banner.jpg"))
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
        let cover = tokio::fs::metadata(folder.join(".mplex/cover.jpg"))
            .await
            .is_ok();
        let banner = tokio::fs::metadata(folder.join(".mplex/banner.jpg"))
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
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>mPlex</title><style>body {{ margin: 0; color: #f9fafb; background: #111827; font-family: system-ui, sans-serif; }} .app-shell {{ display: grid; grid-template-columns: 240px minmax(0, 1fr); min-height: 100vh; }} .sidebar {{ position: sticky; top: 0; height: 100vh; box-sizing: border-box; padding: 1.25rem .9rem; border-right: 1px solid #263244; background: #0d1421; }} .sidebar-brand {{ display: block; padding: .5rem .7rem 1.5rem; color: #f9fafb; font-size: 1.25rem; font-weight: 700; text-decoration: none; }} .sidebar-top {{ display: flex; justify-content: space-between; align-items: center; }} .sidebar-toggle {{ background: none; border: 0; color: #cbd5e1; font-size: 1.3rem; cursor: pointer; }} .sidebar-label {{ padding: 1rem .7rem .4rem; color: #6b7280; font-size: .7rem; text-transform: uppercase; }} .sidebar-nav {{ display: grid; gap: .25rem; }} .sidebar-link {{ display: flex; gap: .7rem; padding: .65rem .7rem; border-radius: .5rem; color: #cbd5e1; text-decoration: none; }} .sidebar-link.active, .sidebar-link:hover {{ background: #1d4ed8; color: #fff; }} .sidebar-icon {{ width: 1.3rem; text-align: center; }} main {{ max-width: 1100px; width: 100%; box-sizing: border-box; margin: auto; padding: 2rem 1rem; }} a {{ color: #93c5fd; text-decoration: none; }} h1 {{ margin-top: 0; }} .section {{ margin: 2rem 0; }} .library-grid, .recent-grid, .continue-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax({}px, 1fr)); gap: 1rem; }} .library-card, .recent-item, .continue-item {{ display: flex; flex-direction: column; gap: .4rem; padding: 1rem; border: 1px solid #374151; border-radius: .7rem; background: #1f2937; }} .continue-item {{ min-height: 130px; justify-content: flex-end; border-color: #2563eb; background: linear-gradient(135deg, #1e3a8a, #1f2937); font-size: 1.05rem; }} .library-card:hover, .recent-item:hover, .continue-item:hover {{ border-color: #60a5fa; }} .library-icon {{ font-size: 2rem; }} .note {{ color: #9ca3af; }} .empty-state {{ padding: 1.5rem; border: 1px dashed #4b5563; border-radius: .7rem; }} @media (max-width: 700px) {{ .app-shell {{ display: block; }} .sidebar {{ position: static; height: auto; width: 100%; }} .sidebar-nav {{ display: flex; overflow-x: auto; }} .sidebar-link {{ white-space: nowrap; }} .sidebar-label {{ display: none; }} .sidebar-toggle {{ display: none; }} }}</style></head><body><div class="app-shell">{}<main><h1>Home</h1><section class="section"><h2>Continue watching</h2><div class="continue-grid">{}</div></section><section class="section"><h2>Recently added</h2><div class="recent-grid">{}</div></section><section class="section"><h2>Your libraries</h2><div class="library-grid">{}</div></section></main></div><script>const shell=document.querySelector('.app-shell'), toggle=document.querySelector('.sidebar-toggle'); if(localStorage.getItem('mplex-sidebar-collapsed')==='true') shell.classList.add('sidebar-collapsed'); toggle?.addEventListener('click',()=>{{shell.classList.toggle('sidebar-collapsed');localStorage.setItem('mplex-sidebar-collapsed',shell.classList.contains('sidebar-collapsed')?'true':'false')}});</script></body></html>"#,
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
    let pane_banner = if tokio::fs::metadata(directory.join(".mplex/banner.jpg"))
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
    } else {
        media_info_panel(&directory).await
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
                let cover = tokio::fs::metadata(directory.join(".mplex/cover.jpg"))
                    .await
                    .is_ok();
                let banner = tokio::fs::metadata(directory.join(".mplex/banner.jpg"))
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
            let cover = tokio::fs::metadata(directory.join(".mplex/cover.jpg"))
                .await
                .is_ok();
            let banner = tokio::fs::metadata(directory.join(".mplex/banner.jpg"))
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
            let artwork = if tokio::fs::metadata(
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

async fn subtitle_tracks(
    video: &PathBuf,
    relative: &std::path::Path,
    library: Option<usize>,
) -> Vec<SubtitleTrack> {
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
        let Some(extension) = path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase())
        else {
            continue;
        };
        if !matches!(extension.as_str(), "srt" | "ass" | "vtt")
            || !entry.file_type().await.is_ok_and(|kind| kind.is_file())
        {
            continue;
        }
        let Some(candidate_stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        if candidate_stem != stem && !candidate_stem.starts_with(&format!("{}.", stem)) {
            continue;
        }
        let label = candidate_stem
            .strip_prefix(stem)
            .unwrap_or(candidate_stem)
            .trim_start_matches('.')
            .replace('.', " ")
            .replace('_', " ");
        tracks.push(SubtitleTrack {
            label: if label.is_empty() {
                format!("External: {}", extension.to_uppercase())
            } else {
                format!("External: {label}")
            },
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
    if let Ok(contents) = tokio::fs::read_to_string(
        video
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .join(".mplex/info.toml"),
    )
    .await
    {
        if let Ok(info) = contents.parse::<toml::Value>() {
            if let Some(streams) = info
                .get("technical")
                .and_then(|value| value.get("streams"))
                .and_then(toml::Value::as_array)
            {
                for stream in streams {
                    if stream.get("codec_type").and_then(toml::Value::as_str) != Some("subtitle") {
                        continue;
                    }
                    let Some(index) = stream.get("index").and_then(toml::Value::as_integer) else {
                        continue;
                    };
                    let language = stream
                        .get("tags")
                        .and_then(|tags| tags.get("language"))
                        .and_then(toml::Value::as_str)
                        .unwrap_or("unknown");
                    let title = stream
                        .get("tags")
                        .and_then(|tags| tags.get("title"))
                        .and_then(toml::Value::as_str)
                        .unwrap_or(language);
                    tracks.push(SubtitleTrack {
                        label: format!("Embedded: {title}"),
                        url: format!(
                            "/web/subtitle?{}path={}&stream={index}",
                            library
                                .map(|library| format!("library={library}&"))
                                .unwrap_or_default(),
                            url_encode(&relative.to_string_lossy())
                        ),
                    });
                }
            }
        }
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
    if let Some(stream) = query.stream {
        let Ok(output) = tokio::process::Command::new("ffmpeg")
            .args(["-v", "error", "-i"])
            .arg(&path)
            .args(["-map", &format!("0:{stream}"), "-f", "webvtt", "-"])
            .output()
            .await
        else {
            return StatusCode::NOT_FOUND.into_response();
        };
        if !output.status.success() {
            return StatusCode::NOT_FOUND.into_response();
        }
        return (
            [(header::CONTENT_TYPE, "text/vtt; charset=utf-8")],
            output.stdout,
        )
            .into_response();
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
    _state: &AppState,
    root: &std::path::Path,
    relative: &std::path::Path,
) -> String {
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
    <a class="sidebar-brand" href="/">mPlex</a>
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

async fn log_not_found(req: Request<Body>) -> StatusCode {
    warn!(method = %req.method(), path = %req.uri().path(), "unsupported route");
    StatusCode::NOT_FOUND
}

async fn root_handler(State(s): State<AppState>, headers: HeaderMap) -> Response {
    let is_plex_client = headers.contains_key("X-Plex-Product")
        || headers.contains_key("X-Plex-Client-Identifier")
        || headers.contains_key("X-Plex-Token");
    if is_plex_client {
        plex_root(&s).await
    } else {
        home_client(State(s)).await
    }
}

async fn plex_root(state: &AppState) -> Response {
    plex_xml(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><MediaContainer size="1" machineIdentifier="{}" title1="Home" friendlyName="mPlex"><Directory key="/library/sections/1" type="movie" title="Movies" agent="com.plexapp.agents.none" scanner="Plex Movie" /></MediaContainer>"#,
        xml_escape(&state.machine_identifier)
    ))
}

fn plex_xml(body: String) -> Response {
    (
        [(header::CONTENT_TYPE, "application/xml; charset=utf-8")],
        body,
    )
        .into_response()
}

async fn plex_identity(State(state): State<AppState>) -> Response {
    plex_xml(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><MediaContainer size="0" claimed="1" machineIdentifier="{}" version="1.0"/>"#,
        xml_escape(&state.machine_identifier)
    ))
}

async fn plex_sections() -> Response {
    plex_xml(r#"<?xml version="1.0" encoding="UTF-8"?><MediaContainer size="1"><Directory key="1" type="movie" title="Movies" agent="com.plexapp.agents.none" scanner="Plex Movie"/></MediaContainer>"#.into())
}

async fn plex_all(State(s): State<AppState>) -> Response {
    let videos = s.media.iter().map(plex_video).collect::<String>();
    plex_xml(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><MediaContainer size="{}" librarySectionID="1" librarySectionTitle="Movies" librarySectionUUID="mplex">{}</MediaContainer>"#,
        s.media.len(),
        videos
    ))
}

async fn plex_item(State(s): State<AppState>, Path(id): Path<String>) -> Response {
    let Some(media) = find_media(&s, &id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    plex_xml(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><MediaContainer size="1" librarySectionID="1" librarySectionTitle="Movies">{}</MediaContainer>"#,
        plex_video(media)
    ))
}

async fn plex_part(
    State(s): State<AppState>,
    Path((id, _part)): Path<(String, String)>,
) -> Response {
    plex_item(State(s), Path(id)).await
}

fn plex_video(media: &Media) -> String {
    format!(
        r#"<Video ratingKey="{}" key="/library/metadata/{}" type="movie" title="{}" librarySectionID="1" librarySectionTitle="Movies" duration="0"><Media id="{}" duration="0" container="{}" videoResolution="unknown"><Part id="{}" key="/media/{}" duration="0" file="{}" size="{}" container="{}"/></Media></Video>"#,
        xml(&media.id),
        xml(&media.id),
        xml(&media.title),
        xml(&media.id),
        xml(&media.container),
        xml(&media.id),
        xml(&media.id),
        xml_path(&media.path),
        media.size,
        xml(&media.container)
    )
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

fn xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
fn xml_path(path: &PathBuf) -> String {
    xml(&path.to_string_lossy())
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
        ass_to_vtt, clean_relative_path, contains_hidden_component, html, parse_range,
        parse_tv_episode_marker, parse_tv_season_directory, sidebar_nav_with_settings, srt_to_vtt,
    };
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
    fn converts_srt_to_webvtt() {
        let vtt = srt_to_vtt("1\n00:00:01,250 --> 00:00:03,000\nHello\n");
        assert!(vtt.starts_with("WEBVTT\n\n"));
        assert!(vtt.contains("00:00:01.250 --> 00:00:03.000\nHello"));
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
}
