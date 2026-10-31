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
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ConfigLibrary {
    name: String,
    kind: String,
    #[serde(default)]
    folders: Vec<PathBuf>,
}

fn default_bind() -> String {
    "0.0.0.0:8096".to_owned()
}
fn default_token() -> String {
    "development-token".to_owned()
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
        .route("/settings", get(settings_page))
        .route("/settings/libraries", post(create_library))
        .route("/settings/folders", post(add_library_folder))
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
    let mut entries = tokio::fs::read_dir(dir).await?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if entry.file_type().await?.is_file()
            && !is_hidden_path(&path)
            && media_type(&path).is_some()
        {
            paths.push(path);
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
        });
    }
    Ok(toml::from_str(&std::fs::read_to_string(path)?)?)
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

#[derive(Deserialize, Default)]
struct SettingsQuery {
    folder: Option<String>,
    error: Option<String>,
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
                )
            })
            .collect::<Vec<_>>()
    };
    let list = libraries
        .iter()
        .map(|(id, name, kind, folders)| {
            format!(
                "<li><strong>{}</strong> <span class=\"note\">({})</span><ul>{}</ul><details><summary>＋ Add folder</summary><form method=\"post\" action=\"/settings/folders\"><input type=\"hidden\" name=\"library_id\" value=\"{}\"><input name=\"path\" type=\"text\" placeholder=\"/another/folder\" required><button type=\"submit\">Add folder</button></form></details></li>",
                html(name), html(library_kind_label(kind)), folders.iter().map(|path| format!("<li>{}</li>", html(path))).collect::<String>(), id
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
form {{ display: flex; gap: .6rem; margin: 1.5rem 0; }} input, select {{ flex: 1; padding: .7rem; border: 1px solid #4b5563; border-radius: .4rem; color: #fff; background: #1f2937; }} .path-list {{ display: grid; gap: .55rem; flex: 2; }} .path-row {{ display: flex; gap: .5rem; }} .path-row input {{ min-width: 0; }}
button {{ padding: .7rem 1rem; border: 0; border-radius: .4rem; color: #fff; background: #2563eb; cursor: pointer; }} li {{ margin: .65rem 0; overflow-wrap: anywhere; }} .note {{ color: #9ca3af; }}
 .flash-error {{ margin: 1rem 0; padding: .8rem 1rem; border: 1px solid #ef4444; border-radius: .5rem; color: #fecaca; background: #451a1a; }}
details {{ margin-top: .7rem; }} summary {{ display: inline-block; padding: .45rem .7rem; border: 1px solid #3b82f6; border-radius: .4rem; color: #bfdbfe; cursor: pointer; list-style: none; }} summary::-webkit-details-marker {{ display: none; }} details form {{ margin: .7rem 0 0; }}
@media (max-width: 700px) {{ .app-shell {{ display: block; }} .sidebar {{ position: static; height: auto; padding: .7rem; border-right: 0; border-bottom: 1px solid #263244; }} .sidebar-top {{ padding-bottom: .35rem; }} .sidebar-brand {{ padding: .35rem .55rem; }} .sidebar-toggle {{ display: none; }} .sidebar-label {{ display: none; }} .sidebar-nav {{ display: flex; overflow-x: auto; }} .sidebar-link {{ white-space: nowrap; }} }}
</style></head><body><div class="app-shell">{}
<main><h1>Settings</h1>{}
<p class="note">A library is a named collection with one type and one or more monitored folders. Create it with its first folder, then add more folders to that same library below.</p>
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
        html(query.folder.as_deref().unwrap_or("")),
        list
    ))
    .into_response()
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

#[derive(Deserialize, Default)]
struct WebPath {
    path: Option<String>,
    library: Option<usize>,
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
    let libraries = config
        .libraries
        .iter()
        .enumerate()
        .map(|(index, library)| {
            format!(
                "<a class=\"library-card\" href=\"/library?library={}\"><span class=\"library-icon\">{}</span><strong>{}</strong><span class=\"note\">{}</span></a>",
                index,
                if library.kind == "tv" { "📺" } else { "🎬" },
                html(&library.name),
                format!("{} · {} folders", library_kind_label(&library.kind), library.folders.len())
            )
        })
        .collect::<String>();
    let continue_watching = state
        .db
        .lock()
        .ok()
        .and_then(|connection| {
            let mut statement = connection.prepare("SELECT media_key, position_ms FROM watch_progress WHERE user_id = ?1 AND watched = 0 AND position_ms > 0 ORDER BY updated_at DESC LIMIT 12").ok()?;
            let rows = statement.query_map([USER_ID], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))).ok()?;
            Some(rows.filter_map(Result::ok).collect::<Vec<_>>())
        })
        .unwrap_or_default()
        .iter()
        .map(|(path, position)| {
            let path_buf = PathBuf::from(path);
            let title = path_buf.file_stem().and_then(|value| value.to_str()).unwrap_or(path);
            let folder = path_buf.parent().unwrap_or(std::path::Path::new(""));
            format!("<a class=\"continue-item\" href=\"/library?path={}\"><strong>{}</strong><span class=\"note\">Resume at {} seconds</span></a>", url_encode(&folder.to_string_lossy()), html(title), position / 1000)
        })
        .collect::<String>();
    let continue_watching = if continue_watching.is_empty() {
        "<p class=\"note empty-state\">Nothing to resume yet. Start watching something and it will appear here.</p>".to_owned()
    } else {
        continue_watching
    };
    let recent = state
        .media
        .iter()
        .take(12)
        .map(|media| {
            format!(
                "<a class=\"recent-item\" href=\"/library\"><strong>{}</strong><span class=\"note\">Recently added</span></a>",
                html(&media.title)
            )
        })
        .collect::<String>();
    Html(format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>mPlex</title><style>body {{ margin: 0; color: #f9fafb; background: #111827; font-family: system-ui, sans-serif; }} .app-shell {{ display: grid; grid-template-columns: 240px minmax(0, 1fr); min-height: 100vh; }} .sidebar {{ position: sticky; top: 0; height: 100vh; box-sizing: border-box; padding: 1.25rem .9rem; border-right: 1px solid #263244; background: #0d1421; }} .sidebar-brand {{ display: block; padding: .5rem .7rem 1.5rem; color: #f9fafb; font-size: 1.25rem; font-weight: 700; text-decoration: none; }} .sidebar-top {{ display: flex; justify-content: space-between; align-items: center; }} .sidebar-toggle {{ background: none; border: 0; color: #cbd5e1; font-size: 1.3rem; cursor: pointer; }} .sidebar-label {{ padding: 1rem .7rem .4rem; color: #6b7280; font-size: .7rem; text-transform: uppercase; }} .sidebar-nav {{ display: grid; gap: .25rem; }} .sidebar-link {{ display: flex; gap: .7rem; padding: .65rem .7rem; border-radius: .5rem; color: #cbd5e1; text-decoration: none; }} .sidebar-link.active, .sidebar-link:hover {{ background: #1d4ed8; color: #fff; }} .sidebar-icon {{ width: 1.3rem; text-align: center; }} main {{ max-width: 1100px; width: 100%; box-sizing: border-box; margin: auto; padding: 2rem 1rem; }} a {{ color: #93c5fd; text-decoration: none; }} h1 {{ margin-top: 0; }} .section {{ margin: 2rem 0; }} .library-grid, .recent-grid, .continue-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 1rem; }} .library-card, .recent-item, .continue-item {{ display: flex; flex-direction: column; gap: .4rem; padding: 1rem; border: 1px solid #374151; border-radius: .7rem; background: #1f2937; }} .continue-item {{ min-height: 130px; justify-content: flex-end; border-color: #2563eb; background: linear-gradient(135deg, #1e3a8a, #1f2937); font-size: 1.05rem; }} .library-card:hover, .recent-item:hover, .continue-item:hover {{ border-color: #60a5fa; }} .library-icon {{ font-size: 2rem; }} .note {{ color: #9ca3af; }} .empty-state {{ padding: 1.5rem; border: 1px dashed #4b5563; border-radius: .7rem; }} @media (max-width: 700px) {{ .app-shell {{ display: block; }} .sidebar {{ position: static; height: auto; width: 100%; }} .sidebar-nav {{ display: flex; overflow-x: auto; }} .sidebar-link {{ white-space: nowrap; }} .sidebar-label {{ display: none; }} .sidebar-toggle {{ display: none; }} }}</style></head><body><div class="app-shell">{}<main><h1>Home</h1><section class="section"><h2>Continue watching</h2><div class="continue-grid">{}</div></section><section class="section"><h2>Recently added</h2><div class="recent-grid">{}</div></section><section class="section"><h2>Your libraries</h2><div class="library-grid">{}</div></section></main></div><script>const shell=document.querySelector('.app-shell'), toggle=document.querySelector('.sidebar-toggle'); if(localStorage.getItem('mplex-sidebar-collapsed')==='true') shell.classList.add('sidebar-collapsed'); toggle?.addEventListener('click',()=>{{shell.classList.toggle('sidebar-collapsed');localStorage.setItem('mplex-sidebar-collapsed',shell.classList.contains('sidebar-collapsed')?'true':'false')}});</script></body></html>"#,
        sidebar_nav_for(&state, std::path::Path::new(""), None), continue_watching, recent, libraries
    )).into_response()
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
    let directory = match web_path(&s, &library_root, &relative).await {
        Ok(path) if path.is_dir() => path,
        Ok(_) => return StatusCode::NOT_FOUND.into_response(),
        Err(status) => return status.into_response(),
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
        } else if file_type.is_file() && media_type(&path).is_some() {
            files.push((name, child_relative, path));
        }
    }
    directories.sort_by_key(|(name, _)| name.to_ascii_lowercase());
    files.sort_by_key(|(name, _, _)| name.to_ascii_lowercase());

    let mut items = String::new();
    items.push_str(&breadcrumb_nav(&relative, query.library));
    let sidebar = sidebar_nav_for(&s, &relative, query.library);
    let mut folder_cards = String::new();
    let mut empty_folders = String::new();
    let mut has_content = false;
    for (name, child_relative) in directories {
        has_content = true;
        let directory = directory.join(&name);
        let media_count = media_file_count(&directory).await;
        if media_count == 0 {
            empty_folders.push_str(&empty_folder_item(&name, &child_relative, query.library));
        } else {
            folder_cards.push_str(&folder_card(
                &name,
                &child_relative,
                media_count,
                query.library,
            ));
        }
    }
    for (name, child_relative, path) in files {
        has_content = true;
        let Some((_, mime)) = media_type(&path) else {
            continue;
        };
        items.push_str(&web_client_item(
            &name,
            &child_relative.to_string_lossy(),
            mime,
            query.library,
        ));
    }
    items.push_str(&folder_cards);
    if !empty_folders.is_empty() {
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
    .library {{ display: flex; flex-direction: column; align-items: center; gap: 1.25rem; }}
    article, .folder-card, .empty-folders {{ width: min(100%, 900px); box-sizing: border-box; }}
    article {{ overflow: hidden; border: 1px solid #374151; border-radius: .75rem; background: #1f2937; }}
    video {{ display: block; width: 100%; aspect-ratio: 16 / 9; margin: 0 auto; background: #000; }}
    .details {{ padding: .9rem 1rem 1rem; }}
    h2 {{ font-size: 1rem; margin: 0 0 .4rem; overflow-wrap: anywhere; }}
    .breadcrumb {{ display: flex; flex-wrap: wrap; align-items: center; gap: .35rem; width: min(100%, 900px); box-sizing: border-box; color: #9ca3af; }}
    .breadcrumb a {{ padding: .35rem .55rem; border-radius: .35rem; text-decoration: none; }}
    .breadcrumb a:hover {{ background: #24344d; }}
    .breadcrumb-separator {{ color: #6b7280; }}
    .folder-card {{ display: block; overflow: hidden; color: inherit; text-decoration: none; border: 1px solid #374151; border-radius: .75rem; background: #1f2937; transition: border-color .15s, transform .15s; }}
    .folder-card:hover {{ border-color: #60a5fa; transform: translateY(-2px); }}
    .folder-preview {{ display: grid; place-items: center; aspect-ratio: 16 / 6; color: #bfdbfe; font-size: 3rem; background: linear-gradient(135deg, #1e3a8a, #312e81 55%, #111827); }}
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
    }}
  </style>
</head>
<body>
  <div class="app-shell">
    {}
    <main>
    <h1>Library</h1>
    <p class="note">Browse the configured directory. Use ←/→ to seek 5 seconds, Space to play/pause, and F for fullscreen.</p>
    <section class="library">{}</section>
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
    document.querySelectorAll("video").forEach(video => {{
      video.volume = 1;
      video.muted = false;
      const path = video.dataset.progressPath;
      if (!path) return;
      fetch(`/web/progress?path=${{encodeURIComponent(path)}}`, {{ cache: "no-store" }})
        .then(response => response.ok ? response.json() : null)
        .then(progress => {{
          if (!progress || progress.watched || !Number.isFinite(progress.positionMs) || progress.positionMs <= 2) return;
          const resume = () => {{ if (progress.positionMs / 1000 < video.duration - 5) video.currentTime = progress.positionMs / 1000; }};
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
      if (event.key !== "ArrowLeft" && event.key !== "ArrowRight" && event.code !== "Space" && !fullscreen) return;
      const video = selectedVideo || window.document.querySelector("video");
      if (!video) return;
      event.preventDefault();
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
        const playing = [...window.document.querySelectorAll("video")]
          .some(video => !video.paused && !video.ended);
        if (!playing && current && next && current.innerHTML !== next.innerHTML) {{
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
        sidebar,
        items
        )),
    )
        .into_response()
}

fn web_client_item(name: &str, relative: &str, mime: &str, library: Option<usize>) -> String {
    let stream_path = url_encode(relative);
    let library_query = library
        .map(|index| format!("library={index}&"))
        .unwrap_or_default();
    format!(
        r#"<article>
  <video controls preload="metadata" data-progress-path="{}" src="/web/stream?{}path={}" type="{}">
    Your browser does not support HTML video. <a href="/web/stream?{}path={}">Open the stream</a>.
  </video>
  <div class="details">
    <h2>{}</h2>
    <a href="/web/stream?{}path={}">Open stream</a>
  </div>
</article>"#,
        html(relative),
        library_query,
        stream_path,
        mime,
        library
            .map(|index| format!("library={index}&"))
            .unwrap_or_default(),
        url_encode(relative),
        html(name),
        library
            .map(|index| format!("library={index}&"))
            .unwrap_or_default(),
        url_encode(relative)
    )
}

fn sidebar_nav(relative: &std::path::Path) -> String {
    sidebar_nav_with_settings(relative.as_os_str().is_empty() == false, false)
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
    let mut nav = String::from(r#"<nav class="breadcrumb" aria-label="Breadcrumb">"#);
    let prefix = library
        .map(|index| format!("library={index}&"))
        .unwrap_or_default();
    nav.push_str(&format!(r#"<a href="/library?{}">mPlex</a>"#, prefix));
    let mut current = PathBuf::new();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            continue;
        };
        current.push(name);
        let label = name.to_string_lossy();
        nav.push_str(&format!(
            r#"<span class="breadcrumb-separator">/</span><a href="/library?{}path={}">{}</a>"#,
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
    media_count: usize,
    library: Option<usize>,
) -> String {
    let label = if media_count == 1 {
        "1 media file"
    } else {
        "media files"
    };
    format!(
        r#"<a class="folder-card" href="/library?{}path={}">
  <div class="folder-preview" aria-hidden="true">📁</div>
  <div class="folder-info"><h2>{}</h2><span class="note">{} {}</span></div>
</a>"#,
        library
            .map(|index| format!("library={index}&"))
            .unwrap_or_default(),
        url_encode(&relative.to_string_lossy()),
        html(name),
        media_count,
        label
    )
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
        clean_relative_path, contains_hidden_component, html, parse_range, sidebar_nav,
        sidebar_nav_with_settings,
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
        let sidebar = sidebar_nav(std::path::Path::new("shows"));
        assert!(sidebar.contains("href=\"/\""));
        assert!(sidebar.contains("href=\"/library\""));
        assert!(sidebar.contains("class=\"sidebar-link active\""));
        assert!(sidebar.contains("sidebar-toggle"));
        let settings = sidebar_nav_with_settings(false, true);
        assert!(settings.contains("href=\"/settings\""));
        assert!(!settings.contains("<a class=\"sidebar-link active\" href=\"/\""));
    }
}
