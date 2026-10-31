use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, Method, Request, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{any, get, post},
    Json, Router,
};
use clap::Parser;
use serde::Deserialize;
use std::{
    net::SocketAddr,
    path::{Component, PathBuf},
    sync::Arc,
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
    dir: PathBuf,
    #[arg(long, default_value = "development-token")]
    token: String,
    #[arg(long, default_value = "0.0.0.0:8096")]
    bind: SocketAddr,
}

#[derive(Clone)]
struct AppState {
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
    let metadata = tokio::fs::metadata(&args.dir).await?;
    if !metadata.is_dir() {
        return Err(format!("--dir is not a directory: {}", args.dir.display()).into());
    }

    let root = tokio::fs::canonicalize(&args.dir).await?;
    let media = scan_media(&root).await?;
    for item in &media {
        info!(id = %item.id, file = %item.path.display(), size = item.size, "serving media");
    }

    let state = AppState {
        root: Arc::new(root),
        media: Arc::new(media),
        token: Arc::from(args.token),
    };
    let app = Router::new()
        .route("/", get(web_client))
        .route("/web/stream", any(web_stream))
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
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
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

#[derive(Deserialize, Default)]
struct WebPath {
    path: Option<String>,
}

async fn web_client(State(s): State<AppState>, Query(query): Query<WebPath>) -> Response {
    let relative = match clean_relative_path(query.path.as_deref().unwrap_or("")) {
        Some(path) => path,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };
    if contains_hidden_component(&relative) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let directory = match web_path(&s, &relative).await {
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

    let mut items = breadcrumb_nav(&relative);
    let mut folder_cards = String::new();
    let mut empty_folders = String::new();
    let mut has_content = false;
    for (name, child_relative) in directories {
        has_content = true;
        let directory = directory.join(&name);
        let media_count = media_file_count(&directory).await;
        if media_count == 0 {
            empty_folders.push_str(&empty_folder_item(&name, &child_relative));
        } else {
            folder_cards.push_str(&folder_card(&name, &child_relative, media_count));
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
    main {{ max-width: 1100px; margin: 0 auto; padding: 2rem 1rem; }}
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
  </style>
</head>
<body>
  <main>
    <h1><a class="brand" href="/">mPlex</a></h1>
    <p class="note">Browse the configured directory. Use ←/→ to seek 5 seconds, Space to play/pause, and F for fullscreen.</p>
    <section class="library">{}</section>
  </main>
  <script>
    document.querySelectorAll("video").forEach(video => {{
      video.volume = 1;
      video.muted = false;
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
        items
        )),
    )
        .into_response()
}

fn web_client_item(name: &str, relative: &str, mime: &str) -> String {
    let stream_path = url_encode(relative);
    format!(
        r#"<article>
  <video controls preload="metadata" src="/web/stream?path={}" type="{}">
    Your browser does not support HTML video. <a href="/web/stream?path={}">Open the stream</a>.
  </video>
  <div class="details">
    <h2>{}</h2>
    <a href="/web/stream?path={}">Open stream</a>
  </div>
</article>"#,
        stream_path,
        mime,
        url_encode(relative),
        html(name),
        url_encode(relative)
    )
}

fn breadcrumb_nav(relative: &std::path::Path) -> String {
    let mut nav = String::from(r#"<nav class="breadcrumb" aria-label="Breadcrumb">"#);
    nav.push_str(r#"<a href="/">mPlex</a>"#);
    let mut current = PathBuf::new();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            continue;
        };
        current.push(name);
        let label = name.to_string_lossy();
        nav.push_str(&format!(
            r#"<span class="breadcrumb-separator">/</span><a href="/?path={}">{}</a>"#,
            url_encode(&current.to_string_lossy()),
            html(&label)
        ));
    }
    nav.push_str("</nav>");
    nav
}

fn folder_card(name: &str, relative: &std::path::Path, media_count: usize) -> String {
    let label = if media_count == 1 {
        "1 media file"
    } else {
        "media files"
    };
    format!(
        r#"<a class="folder-card" href="/?path={}">
  <div class="folder-preview" aria-hidden="true">📁</div>
  <div class="folder-info"><h2>{}</h2><span class="note">{} {}</span></div>
</a>"#,
        url_encode(&relative.to_string_lossy()),
        html(name),
        media_count,
        label
    )
}

fn empty_folder_item(name: &str, relative: &std::path::Path) -> String {
    format!(
        r#"<div class="empty-folder"><a href="/?path={}">📂 {}</a></div>"#,
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
    let path = match web_path(&s, &relative).await {
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

async fn web_path(state: &AppState, relative: &std::path::Path) -> Result<PathBuf, StatusCode> {
    let path = tokio::fs::canonicalize(state.root.join(relative))
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    if path.starts_with(state.root.as_ref()) {
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

async fn log_not_found(req: Request<Body>) -> StatusCode {
    warn!(method = %req.method(), path = %req.uri().path(), "unsupported route");
    StatusCode::NOT_FOUND
}

fn plex_xml(body: String) -> Response {
    (
        [(header::CONTENT_TYPE, "application/xml; charset=utf-8")],
        body,
    )
        .into_response()
}

async fn plex_identity() -> Response {
    plex_xml(r#"<?xml version="1.0" encoding="UTF-8"?><MediaContainer size="0" claimed="1" machineIdentifier="mplex" version="1.0"/>"#.into())
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
    use super::{clean_relative_path, contains_hidden_component, html, parse_range};
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
}
