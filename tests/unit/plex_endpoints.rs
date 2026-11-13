use super::{
    build_app, migrate_library_model, open_database, plex_external_subtitle_id, plex_part_id,
    plex_remux_audio_mode, sync_config_libraries, AppState, Config, ConfigLibrary, Media, PlexTls,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use base64::Engine as _;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tower::ServiceExt;

#[path = "plex_regressions.rs"]
mod regression_suite;

#[derive(serde::Deserialize)]
struct PlexApiInventory {
    spec_version: String,
    operation_count: usize,
    operations: Vec<PlexApiOperation>,
}

#[derive(serde::Deserialize)]
struct PlexApiOperation {
    method: String,
    path: String,
    operation_id: String,
    summary: String,
    expected_status: u16,
}

fn inventory_request_path(path: &str, fixture: &Fixture) -> String {
    let mut result = String::new();
    let mut remaining = path;
    while let Some(open) = remaining.find('{') {
        result.push_str(&remaining[..open]);
        let after_open = &remaining[open + 1..];
        let Some(close) = after_open.find('}') else {
            result.push_str(&remaining[open..]);
            remaining = "";
            break;
        };
        let name = &after_open[..close];
        let sample = match name {
            "transcodeType" => "video".to_owned(),
            "identifier" => "test".to_owned(),
            "ext" => "srt".to_owned(),
            "filename" => "file.mkv".to_owned(),
            "country" | "region" => "US".to_owned(),
            "task" => "refresh".to_owned(),
            "element" => "thumb".to_owned(),
            "ids" | "metadataId" => "1-1".to_owned(),
            "partId" => fixture.part_id.to_string(),
            "streamId" => fixture.subtitle_id.to_string(),
            "changestamp" => fixture.change_stamp.to_string(),
            _ => "1".to_owned(),
        };
        result.push_str(&sample);
        remaining = &after_open[close + 1..];
    }
    result.push_str(remaining);
    result.replace("start.*", "start.mkv")
}

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(1);

struct Fixture {
    app: Router,
    state: AppState,
    config: Arc<Mutex<Config>>,
    db: Arc<Mutex<rusqlite::Connection>>,
    root: PathBuf,
    part_id: u64,
    subtitle_id: u64,
    change_stamp: u64,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn fixture() -> Fixture {
    let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "mplex-plex-endpoint-tests-{}-{id}",
        std::process::id()
    ));
    let config_dir = root.join("config");
    let media_dir = root.join("media");
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::create_dir_all(media_dir.join(".mplex")).unwrap();
    let media_path = media_dir.join("Demo.mkv");
    std::fs::write(&media_path, b"0123456789abcdef").unwrap();
    std::fs::write(media_dir.join(".mplex/cover.jpg"), b"cover").unwrap();
    std::fs::write(media_dir.join(".mplex/banner.jpg"), b"banner").unwrap();
    std::fs::write(
        media_dir.join("Demo.en.srt"),
        b"1\n00:00:01,000 --> 00:00:02,000\nHello\n",
    )
    .unwrap();

    let config_path = config_dir.join("config.toml");
    let (db, machine_identifier) = open_database(&config_path).unwrap();
    migrate_library_model(&db).unwrap();
    let library = ConfigLibrary {
        name: "Movies".to_owned(),
        kind: "movie".to_owned(),
        folders: vec![media_dir.clone()],
        show_recently_added: true,
        show_continue_watching: true,
        scan_enabled: true,
        scan_interval_value: 5,
        scan_interval_unit: "minutes".to_owned(),
        plex_agent: None,
        plex_scanner: None,
        plex_language: None,
        metadata_agent_provider_group_id: None,
        plex_preferences: None,
    };
    let config = Config {
        bind: "127.0.0.1:0".to_owned(),
        libraries: vec![library],
        metadata_storage: "sidecar".to_owned(),
        tmdb_bearer_token: None,
        subtitle_language_primary: "en".to_owned(),
        subtitle_language_secondary: "vi".to_owned(),
        bilingual_subtitles_enabled: true,
        plex_account_preferences: BTreeMap::new(),
    };
    sync_config_libraries(&db, &config).unwrap();
    let mut media = Media {
        id: "1-1".to_owned(),
        title: "Demo".to_owned(),
        path: media_path.clone(),
        size: 16,
        container: "mkv".to_owned(),
        mime: "video/x-matroska".to_owned(),
        metadata_revision: 0,
    };
    let part_id = plex_part_id(&media);
    let subtitle_id = plex_external_subtitle_id(&media, &media_dir.join("Demo.en.srt"));
    let change_stamp = super::media_modified_at(&media);
    media.metadata_revision = change_stamp.saturating_add(60);
    let shared_config = Arc::new(Mutex::new(config));
    let shared_db = Arc::new(Mutex::new(db));
    let (plex_notifications, _) = tokio::sync::broadcast::channel(64);
    let state = AppState {
        config: shared_config.clone(),
        subtitle_checkpoints_dir: Arc::new(config_dir.join("translated_subtitles")),
        config_path: Arc::new(config_path),
        db: shared_db.clone(),
        server_name: Arc::new(super::RwLock::new("harbringer".to_owned())),
        machine_identifier: Arc::from(machine_identifier),
        root: Arc::new(media_dir),
        media: Arc::new(super::RwLock::new(Arc::new(vec![media]))),
        inventory_scan_lock: Arc::new(tokio::sync::Mutex::new(())),
        plex_notifications,
        media_info: Arc::new(Mutex::new(HashMap::new())),
        plex_stream_selections: Arc::new(Mutex::new(HashMap::new())),
        plex_play_queues: Arc::new(Mutex::new(HashMap::new())),
        next_plex_play_queue_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        plex_playback_states: Arc::new(Mutex::new(HashMap::new())),
        plex_remux_sessions: Arc::new(Mutex::new(HashMap::new())),
        plex_scrobbled_sessions: Arc::new(Mutex::new(HashSet::new())),
        plex_webhook_delivery_lock: Arc::new(tokio::sync::Mutex::new(())),

        plex_tls: PlexTls::load(root.join("config/certs")).unwrap(),
    };
    Fixture {
        app: build_app(state.clone()),
        state,
        config: shared_config,
        db: shared_db,
        root,
        part_id,
        subtitle_id,
        change_stamp,
    }
}

fn tv_fixture() -> Fixture {
    let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "mplex-plex-tv-endpoint-tests-{}-{id}",
        std::process::id()
    ));
    let config_dir = root.join("config");
    let media_dir = root.join("media");
    std::fs::create_dir_all(&config_dir).unwrap();
    let relative_paths = [
        "Example Show/Season 01/Example.Show.S01E01.Pilot.mkv",
        "Example Show/Season 01/Example.Show.S01E02.Next.mkv",
    ];
    let media = relative_paths
        .iter()
        .enumerate()
        .map(|(index, relative)| {
            let path = media_dir.join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, b"0123456789abcdef").unwrap();
            Media {
                id: format!("tv-{index}"),
                title: format!("Episode {index}"),
                path,
                size: 16,
                container: "mkv".to_owned(),
                mime: "video/x-matroska".to_owned(),
                metadata_revision: 0,
            }
        })
        .collect::<Vec<_>>();
    let config_path = config_dir.join("config.toml");
    let (db, machine_identifier) = open_database(&config_path).unwrap();
    migrate_library_model(&db).unwrap();
    let config = Config {
        bind: "127.0.0.1:0".to_owned(),
        libraries: vec![ConfigLibrary {
            name: "Shows".to_owned(),
            kind: "tv".to_owned(),
            folders: vec![media_dir.clone()],
            show_recently_added: true,
            show_continue_watching: true,
            scan_enabled: true,
            scan_interval_value: 5,
            scan_interval_unit: "minutes".to_owned(),
            plex_agent: None,
            plex_scanner: None,
            plex_language: None,
            metadata_agent_provider_group_id: None,
            plex_preferences: None,
        }],
        metadata_storage: "sidecar".to_owned(),
        tmdb_bearer_token: None,
        subtitle_language_primary: "en".to_owned(),
        subtitle_language_secondary: "vi".to_owned(),
        bilingual_subtitles_enabled: true,
        plex_account_preferences: BTreeMap::new(),
    };
    sync_config_libraries(&db, &config).unwrap();
    let part_id = plex_part_id(&media[0]);
    let change_stamp = super::media_modified_at(&media[0]);
    let shared_config = Arc::new(Mutex::new(config));
    let shared_db = Arc::new(Mutex::new(db));
    let (plex_notifications, _) = tokio::sync::broadcast::channel(64);
    let state = AppState {
        config: shared_config.clone(),
        subtitle_checkpoints_dir: Arc::new(config_dir.join("translated_subtitles")),
        config_path: Arc::new(config_path),
        db: shared_db.clone(),
        server_name: Arc::new(super::RwLock::new("harbringer".to_owned())),
        machine_identifier: Arc::from(machine_identifier),
        root: Arc::new(media_dir),
        media: Arc::new(super::RwLock::new(Arc::new(media))),
        inventory_scan_lock: Arc::new(tokio::sync::Mutex::new(())),
        plex_notifications,
        media_info: Arc::new(Mutex::new(HashMap::new())),
        plex_stream_selections: Arc::new(Mutex::new(HashMap::new())),
        plex_play_queues: Arc::new(Mutex::new(HashMap::new())),
        next_plex_play_queue_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        plex_playback_states: Arc::new(Mutex::new(HashMap::new())),
        plex_remux_sessions: Arc::new(Mutex::new(HashMap::new())),
        plex_scrobbled_sessions: Arc::new(Mutex::new(HashSet::new())),
        plex_webhook_delivery_lock: Arc::new(tokio::sync::Mutex::new(())),

        plex_tls: PlexTls::load(root.join("config/certs")).unwrap(),
    };
    Fixture {
        app: build_app(state.clone()),
        state,
        config: shared_config,
        db: shared_db,
        root,
        part_id,
        subtitle_id: 0,
        change_stamp,
    }
}

async fn send(app: &Router, method: Method, uri: &str, body: Body) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn send_form(
    app: &Router,
    method: Method,
    uri: &str,
    body: &str,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn send_async_form(
    app: &Router,
    method: Method,
    uri: &str,
    body: &str,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header("X-Mplex-Async", "1")
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn settings_server_name_is_saved_and_advertised_to_plex_clients() {
    let fixture = fixture();
    let saved = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/settings/server-name")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("server_name=Living+Room+Plex"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        saved.headers()[header::LOCATION],
        "/settings?server_name_saved=true"
    );

    let identity = send(&fixture.app, Method::GET, "/identity", Body::empty()).await;
    assert_eq!(identity.status(), StatusCode::OK);
    let identity_json: serde_json::Value =
        serde_json::from_slice(&to_bytes(identity.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        identity_json["MediaContainer"]["friendlyName"],
        "Living Room Plex"
    );
}

#[tokio::test]
async fn settings_indicates_saved_tmdb_token_without_disclosing_it() {
    let fixture = fixture();
    let initial = send(&fixture.app, Method::GET, "/settings", Body::empty()).await;
    let initial_html = String::from_utf8(
        to_bytes(initial.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(initial_html.contains("No TMDB token is saved."));

    let secret = "tmdb-test-token-must-not-render";
    fixture.config.lock().unwrap().tmdb_bearer_token = Some(secret.to_owned());
    let saved = send(&fixture.app, Method::GET, "/settings", Body::empty()).await;
    let saved_html = String::from_utf8(
        to_bytes(saved.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(saved_html.contains("TMDB token is saved."));
    assert!(saved_html.contains("Leave the field blank to keep it"));
    assert!(!saved_html.contains(secret));
}

#[tokio::test]
async fn settings_logs_tab_displays_recent_events() {
    let mut log_updates = super::log_broadcast().subscribe();
    let fixture = fixture();
    super::push_log_line("event=http_request method=GET path=/settings".to_owned());
    super::push_log_line("event=tmdb \x1b[31msearch\x1b[0m finished".to_owned());
    let mut streamed_lines = Vec::new();
    while let Ok((_, line)) = log_updates.try_recv() {
        streamed_lines.push(line);
    }
    assert!(streamed_lines.contains(&"event=http_request method=GET path=/settings".to_owned()));
    assert!(streamed_lines.contains(&"event=tmdb search finished".to_owned()));
    let response = send(
        &fixture.app,
        Method::GET,
        "/settings?tab=logs",
        Body::empty(),
    )
    .await;
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("<h2 id=\"logs-heading\">Logs</h2>"));
    assert!(body.contains("id=\"settings-logs-search\" type=\"search\""));
    assert!(body.contains("id=\"settings-logs-reverse\" type=\"checkbox\""));
    assert!(body.contains("id=\"settings-logs-output\""));
    assert!(body.contains("Reverse filter (show non-matching)"));
    assert!(body.contains("id=\"settings-logs-live\""));
    assert!(body.contains("id=\"settings-logs-follow\""));
    assert!(body.contains("/settings/logs/socket"));
    assert!(!body.contains(">Refresh</a>"));
    assert!(body.contains("<div class=\"settings-stack\" style=\"display:none\">"));
    assert!(body.contains("event=http_request method=GET path=/settings"));
    assert!(body.contains("event=tmdb search finished"));
    assert!(!body.contains('\u{1b}'));
    let (_, log_snapshot) = super::recent_log_snapshot();
    assert!(log_snapshot.contains(&"event=http_request method=GET path=/settings".to_owned()));
    assert!(log_snapshot.contains(&"event=tmdb search finished".to_owned()));
    assert!(body.contains("class=\"active\" href=\"/settings?tab=logs\""));
}

#[tokio::test]
async fn settings_can_rename_a_library() {
    let fixture = fixture();
    let response = send_form(
        &fixture.app,
        Method::POST,
        "/settings/libraries/0/rename",
        "name=My+Films",
    )
    .await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert!(response.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .contains("library_renamed=true"));
    assert_eq!(fixture.config.lock().unwrap().libraries[0].name, "My Films");
    let saved_config = std::fs::read_to_string(fixture.root.join("config/config.toml")).unwrap();
    assert!(saved_config.contains("My Films"));

    let response = send(
        &fixture.app,
        Method::GET,
        "/settings?library_renamed=true",
        Body::empty(),
    )
    .await;
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("Library name saved."));
    assert!(body.contains("value=\"My Films\" maxlength=\"64\""));
    assert!(body.contains("action=\"/settings/libraries/0/save\""));
    let settings_form_start = body.find("<form class=\"library-settings\"").unwrap();
    let name_row = body.find("class=\"library-rename\"").unwrap();
    let folders_start = body.find("<section class=\"library-folders\"").unwrap();
    assert!(settings_form_start < name_row && name_row < folders_start);
    assert!(
        body.contains("name=\"name\" type=\"text\" form=\"library-settings-0\" value=\"My Films\"")
    );
    assert!(body.contains("flex-wrap:nowrap"));
    assert!(body
        .contains("form=\"library-settings-0\" style=\"margin-left:auto\">Save changes</button>"));
}

#[tokio::test]
async fn library_rename_rejects_blank_names_and_unknown_libraries() {
    let fixture = fixture();
    for (path, body) in [
        ("/settings/libraries/0/rename", "name=%20%20"),
        ("/settings/libraries/9/rename", "name=Unknown"),
    ] {
        let response = send_form(&fixture.app, Method::POST, path, body).await;
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
    }

    let mut duplicate = fixture.config.lock().unwrap().libraries[0].clone();
    duplicate.name = "Archive".to_owned();
    fixture.config.lock().unwrap().libraries.push(duplicate);
    let response = send_form(
        &fixture.app,
        Method::POST,
        "/settings/libraries/0/rename",
        "name=archive",
    )
    .await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(fixture.config.lock().unwrap().libraries[0].name, "Movies");
}

#[tokio::test]
async fn plex_artwork_serves_database_stored_cover() {
    let fixture = fixture();
    fixture.config.lock().unwrap().metadata_storage = "database".to_owned();
    let artwork_path = fixture.root.join("saved-cover.jpg");
    std::fs::write(&artwork_path, b"poster bytes").unwrap();
    fixture
        .db
        .lock()
        .unwrap()
        .execute(
            "INSERT OR REPLACE INTO artwork (media_key, kind, local_path, downloaded_at) VALUES (?1, ?2, ?3, strftime('%s', 'now'))",
            (
                "1-1",
                "cover",
                artwork_path.to_string_lossy().as_ref(),
            ),
        )
        .unwrap();

    let response = send(
        &fixture.app,
        Method::GET,
        "/library/metadata/1-1/thumb",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"poster bytes");
}

#[tokio::test]
async fn settings_saves_and_validates_per_library_scan_schedule() {
    let fixture = fixture();
    let page = send(&fixture.app, Method::GET, "/settings", Body::empty()).await;
    let page = to_bytes(page.into_body(), usize::MAX).await.unwrap();
    let page = String::from_utf8(page.to_vec()).unwrap();
    assert!(page.contains("<div class=\"settings-stack\">"));
    assert!(page.contains("<div class=\"section-content\">"));
    assert!(!page.contains("class=\"settings-stack\" style=\"<div"));
    assert!(page.contains("name=\"scan_interval_value\" min=\"1\" value=\"5\""));
    assert!(page.contains("<option value=\"minutes\" selected>minutes</option>"));
    let library_start = page
        .find("<details class=\"library-card\" data-library-id=\"0\">")
        .unwrap();
    let library_end = page[library_start..].find("</div></details>").unwrap() + library_start;
    let library_markup = &page[library_start..library_end];
    let settings_form_start = library_markup
        .find("<form class=\"library-settings\"")
        .unwrap();
    let scan_settings = library_markup
        .find("<div class=\"library-scan-settings\"")
        .unwrap();
    let scan_actions = library_markup
        .find("class=\"library-scan-actions\"")
        .unwrap();
    let add_folder = library_markup
        .find("<summary style=\"min-height:2.65rem;padding:.65rem .95rem;font-size:.87rem\">Add folder</summary>")
        .unwrap();
    let scan = library_markup.find(">Scan metadata</button>").unwrap();
    let force_scan = library_markup.find(">Force rescan</button>").unwrap();
    let folders_section = library_markup
        .find("<section class=\"library-folders\"")
        .unwrap();
    let folders_heading = library_markup
        .find("<h4 style=\"margin:0\">Folders</h4>")
        .unwrap();
    let folder_list = library_markup.find("<ul>").unwrap();
    let delete = library_markup.find("class=\"library-delete\"").unwrap();
    let save = library_markup
        .find("form=\"library-settings-0\" style=\"margin-left:auto\">Save changes</button>")
        .unwrap();
    assert!(settings_form_start < scan_settings);
    assert!(scan_settings < scan_actions && scan_actions < scan && scan < force_scan);
    assert!(force_scan < folders_section);
    assert!(library_markup.contains("class=\"library-scan-actions\" style=\"display:flex;flex:0 0 auto;align-items:center;gap:.4rem;white-space:nowrap\""));
    assert!(
        folders_section < folders_heading
            && folders_heading < add_folder
            && add_folder < folder_list
    );
    assert!(folder_list < delete && delete < save);
    assert_eq!(library_markup.matches(">Save changes</button>").count(), 1);
    assert!(!library_markup.contains("Save scan schedule"));
    assert!(!library_markup.contains("Save Home sections"));
    assert!(library_markup.contains("justify-content:space-between"));
    assert!(library_markup.contains("justify-content:flex-start"));
    assert!(library_markup.contains("form=\"library-settings-0\" formmethod=\"post\" formaction=\"/settings/libraries/0/scan\" formnovalidate"));
    assert!(page.contains(".library-add-folder[open] > summary::before { content: \"+\"; }"));
    assert!(library_markup.contains("action=\"/settings/folders/remove\""));
    assert!(library_markup.contains("name=\"folder_index\""));
    assert!(library_markup.contains("Remove folder</button>"));
    assert!(library_markup.contains(
        "onclick=\"this.form.reset();this.closest('details').open=false\">Cancel</button>"
    ));
    assert!(library_markup.contains("onsubmit=\"return confirm('Remove this folder from the library? Media files will not be deleted.')\""));

    assert!(library_markup.contains("data-settings-delete=\"folder\""));
    assert!(library_markup.contains("data-settings-delete=\"library\""));
    assert!(page.contains("X-Mplex-Async"));
    assert!(page.contains("currentList.replaceWith(nextList)"));
    assert!(page.contains("window.scrollTo(0, scrollTop)"));

    let saved = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/settings/libraries/scan-settings")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(
                    "library_id=0&scan_enabled=true&scan_interval_value=2&scan_interval_unit=seconds",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::SEE_OTHER);
    {
        let config = fixture.config.lock().unwrap();
        let library = &config.libraries[0];
        assert!(library.scan_enabled);
        assert_eq!(library.scan_interval_value, 2);
        assert_eq!(library.scan_interval_unit, "seconds");
        assert_eq!(
            super::library_scan_interval(library),
            std::time::Duration::from_secs(2)
        );
    }

    let disabled = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/settings/libraries/scan-settings")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(
                    "library_id=0&scan_interval_value=1&scan_interval_unit=hours",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(disabled.status(), StatusCode::SEE_OTHER);
    {
        let config = fixture.config.lock().unwrap();
        assert!(!config.libraries[0].scan_enabled);
        assert_eq!(config.libraries[0].scan_interval_unit, "hours");
    }

    let invalid = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/settings/libraries/scan-settings")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(
                    "library_id=0&scan_enabled=true&scan_interval_value=0&scan_interval_unit=minutes",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::SEE_OTHER);
    let config = fixture.config.lock().unwrap();
    assert!(!config.libraries[0].scan_enabled);
    assert_eq!(config.libraries[0].scan_interval_value, 1);
    assert_eq!(config.libraries[0].scan_interval_unit, "hours");
}

#[tokio::test]
async fn removing_a_library_folder_keeps_media_files_and_rejects_stale_paths() {
    let fixture = fixture();
    let folder_path = fixture.root.join("media").to_string_lossy().into_owned();
    let media_file = fixture.root.join("media/Demo.mkv");

    let stale = send_form(
        &fixture.app,
        Method::POST,
        "/settings/folders/remove",
        "library_id=0&folder_index=0&path=%2Fstale",
    )
    .await;
    assert_eq!(stale.status(), StatusCode::SEE_OTHER);
    assert_eq!(fixture.config.lock().unwrap().libraries[0].folders.len(), 1);

    let response = send_form(
        &fixture.app,
        Method::POST,
        "/settings/folders/remove",
        &format!("library_id=0&folder_index=0&path={folder_path}"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert!(fixture.config.lock().unwrap().libraries[0]
        .folders
        .is_empty());
    assert!(media_file.exists());

    let connection = fixture.db.lock().unwrap();
    let folder_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM library_folders WHERE path = ?1",
            [&folder_path],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(folder_count, 0);
    let group_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM library_groups WHERE name = 'Movies'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(group_count, 1);
}

#[tokio::test]
async fn removing_a_library_folder_uses_async_responses() {
    let fixture = fixture();
    let folder_path = fixture.root.join("media").to_string_lossy().into_owned();
    let media_file = fixture.root.join("media/Demo.mkv");

    let stale = send_async_form(
        &fixture.app,
        Method::POST,
        "/settings/folders/remove",
        "library_id=0&folder_index=0&path=%2Fstale",
    )
    .await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    assert_eq!(fixture.config.lock().unwrap().libraries[0].folders.len(), 1);

    let removed = send_async_form(
        &fixture.app,
        Method::POST,
        "/settings/folders/remove",
        &format!("library_id=0&folder_index=0&path={folder_path}"),
    )
    .await;
    assert_eq!(removed.status(), StatusCode::NO_CONTENT);
    assert!(fixture.config.lock().unwrap().libraries[0]
        .folders
        .is_empty());
    assert!(media_file.exists());
}

#[tokio::test]
async fn library_settings_save_updates_name_schedule_and_home_sections_together() {
    let fixture = fixture();
    let original_name = fixture.config.lock().unwrap().libraries[0].name.clone();
    let invalid = send_form(
        &fixture.app,
        Method::POST,
        "/settings/libraries/0/save",
        "name=Ignored&scan_interval_value=0&scan_interval_unit=seconds",
    )
    .await;
    assert_eq!(invalid.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        fixture.config.lock().unwrap().libraries[0].name,
        original_name
    );

    let response = send_form(
        &fixture.app,
        Method::POST,
        "/settings/libraries/0/save",
        "name=Series&scan_enabled=true&scan_interval_value=2&scan_interval_unit=seconds&show_recently_added=true&show_continue_watching=true",
    )
    .await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let location = response.headers()[header::LOCATION].to_str().unwrap();
    assert!(location.contains("library_saved=true"));
    assert!(location.contains("library_open=0"));
    {
        let config = fixture.config.lock().unwrap();
        let library = &config.libraries[0];
        assert_eq!(library.name, "Series");
        assert!(library.scan_enabled);
        assert_eq!(library.scan_interval_value, 2);
        assert_eq!(library.scan_interval_unit, "seconds");
        assert!(library.show_recently_added);
        assert!(library.show_continue_watching);
    }
    let connection = fixture.db.lock().unwrap();
    assert!(connection
        .query_row(
            "SELECT id FROM library_groups WHERE name = ?1",
            ["Series"],
            |row| { row.get::<_, i64>(0) }
        )
        .is_ok());
}

#[tokio::test]
async fn manual_scan_updates_web_and_plex_inventory_without_changing_existing_ids() {
    let fixture = fixture();
    let media_dir = fixture.root.join("media");
    std::fs::write(media_dir.join("Added.mp4"), b"new media").unwrap();
    std::fs::write(media_dir.join("notes.txt"), b"not a video").unwrap();

    let scanned = send(&fixture.app, Method::POST, "/settings/scan", Body::empty()).await;
    assert_eq!(scanned.status(), StatusCode::SEE_OTHER);
    let location = scanned.headers()[header::LOCATION].to_str().unwrap();
    assert!(location.contains("inventory_scan=2"));
    std::fs::write(media_dir.join("PlexAdded.mp4"), b"new Plex media").unwrap();
    let refreshed = send(
        &fixture.app,
        Method::GET,
        "/library/sections/1/refresh",
        Body::empty(),
    )
    .await;
    assert_eq!(refreshed.status(), StatusCode::OK);

    let web = send(&fixture.app, Method::GET, "/?library=0", Body::empty()).await;
    let web = to_bytes(web.into_body(), usize::MAX).await.unwrap();
    let web = String::from_utf8(web.to_vec()).unwrap();
    assert!(web.contains("Added"));

    let plex = send(
        &fixture.app,
        Method::GET,
        "/library/sections/1/all",
        Body::empty(),
    )
    .await;
    assert_eq!(plex.status(), StatusCode::OK);
    let plex: serde_json::Value =
        serde_json::from_slice(&to_bytes(plex.into_body(), usize::MAX).await.unwrap()).unwrap();
    let items = plex["MediaContainer"]["Metadata"].as_array().unwrap();
    assert_eq!(items.len(), 3);
    assert!(items.iter().any(|item| item["title"] == "Added"));
    assert!(items.iter().any(|item| item["title"] == "PlexAdded"));
    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            "/library/metadata/1048577",
            Body::empty(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let canonical_root = tokio::fs::canonicalize(&media_dir).await.unwrap();
    let mut restarted_media = super::scan_media(&canonical_root).await.unwrap();
    {
        let connection = fixture.db.lock().unwrap();
        super::assign_media_ids(
            &connection,
            1,
            &canonical_root,
            &media_dir,
            &mut restarted_media,
        )
        .unwrap();
    }
    assert_eq!(
        restarted_media
            .iter()
            .find(|media| media.path.ends_with("Demo.mkv"))
            .unwrap()
            .id,
        "1-1"
    );
    assert_eq!(
        restarted_media
            .iter()
            .find(|media| media.path.ends_with("Added.mp4"))
            .unwrap()
            .id,
        "1-2"
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::POST,
            "/playQueues?type=video&uri=%2Flibrary%2Fmetadata%2F1048577",
            Body::empty(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::POST,
            "/playQueues?type=video&uri=%2Flibrary%2Fmetadata%2F1048578",
            Body::empty(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let second_media_dir = fixture.root.join("second-media");
    std::fs::create_dir_all(&second_media_dir).unwrap();
    std::fs::write(
        second_media_dir.join("SectionTwo.mkv"),
        b"section two media",
    )
    .unwrap();
    fixture
        .config
        .lock()
        .unwrap()
        .libraries
        .push(super::ConfigLibrary {
            name: "Other videos".to_owned(),
            kind: "other".to_owned(),
            folders: vec![second_media_dir],
            show_recently_added: true,
            show_continue_watching: true,
            scan_enabled: true,
            scan_interval_value: 5,
            scan_interval_unit: "minutes".to_owned(),
            plex_agent: None,
            plex_scanner: None,
            plex_language: None,
            metadata_agent_provider_group_id: None,
            plex_preferences: None,
        });
    {
        let config = fixture.config.lock().unwrap();
        let connection = fixture.db.lock().unwrap();
        super::sync_config_libraries(&connection, &config).unwrap();
    }
    let second_refresh = send(
        &fixture.app,
        Method::GET,
        "/library/sections/2/refresh",
        Body::empty(),
    )
    .await;
    assert_eq!(second_refresh.status(), StatusCode::OK);
    let second_listing = send(
        &fixture.app,
        Method::GET,
        "/library/sections/2/all",
        Body::empty(),
    )
    .await;
    let second_listing: serde_json::Value = serde_json::from_slice(
        &to_bytes(second_listing.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let second_items = second_listing["MediaContainer"]["Metadata"]
        .as_array()
        .unwrap();
    assert_eq!(second_items.len(), 1);
    assert_eq!(second_items[0]["title"], "SectionTwo");
}

#[tokio::test]
async fn scans_keep_existing_supported_media_omitted_by_scan() {
    let fixture = fixture();
    let omitted_dir = fixture.root.join("media/.scanner-hidden");
    std::fs::create_dir_all(&omitted_dir).unwrap();
    let omitted_path = omitted_dir.join("Demo.mkv");
    std::fs::write(&omitted_path, b"video").unwrap();
    let mut media = fixture.state.media.write().unwrap();
    let mut items = media.as_ref().clone();
    let mut stale_item = items[0].clone();
    stale_item.id = "1-2".to_owned();
    stale_item.title = "Demo hidden".to_owned();
    stale_item.path = omitted_path.clone();
    items.push(stale_item);
    *media = Arc::new(items);
    drop(media);

    let mut notifications = fixture.state.plex_notifications.subscribe();
    let scan = send(&fixture.app, Method::POST, "/settings/scan", Body::empty()).await;
    assert_eq!(scan.status(), StatusCode::SEE_OTHER);
    assert!(omitted_path.exists());
    assert!(fixture
        .state
        .media
        .read()
        .unwrap()
        .iter()
        .any(|media| media.id == "1-2" && media.path == omitted_path));
    assert!(notifications.try_recv().is_err());
}

#[tokio::test]
async fn stale_episode_request_rescans_without_hiding_its_library() {
    let fixture = tv_fixture();
    {
        let mut media = fixture.state.media.write().unwrap();
        let mut updated = media.as_ref().clone();
        updated[0].id = "1-1".to_owned();
        updated[1].id = "1-2".to_owned();
        *media = Arc::new(updated);
    }
    let initial_scan = send(&fixture.app, Method::POST, "/settings/scan", Body::empty()).await;
    assert_eq!(initial_scan.status(), StatusCode::SEE_OTHER);
    let mut notifications = fixture.state.plex_notifications.subscribe();

    let removed_episode = super::media_snapshot(&fixture.state)[0].clone();
    let rating_key = super::plex_rating_key(&removed_episode);
    std::fs::remove_file(&removed_episode.path).unwrap();

    let scan = send(&fixture.app, Method::POST, "/settings/scan", Body::empty()).await;
    assert_eq!(scan.status(), StatusCode::SEE_OTHER);
    let notification: serde_json::Value =
        serde_json::from_str(&notifications.try_recv().unwrap()).unwrap();
    assert_eq!(notification["NotificationContainer"]["type"], "timeline");
    assert_eq!(
        notification["NotificationContainer"]["TimelineEntry"][0]["sectionID"],
        "1"
    );
    assert_eq!(
        notification["NotificationContainer"]["TimelineEntry"][0]["itemID"],
        rating_key.to_string()
    );
    assert_eq!(
        notification["NotificationContainer"]["TimelineEntry"][0]["state"],
        9
    );

    let stale_item = send(
        &fixture.app,
        Method::GET,
        &format!("/library/metadata/{rating_key}"),
        Body::empty(),
    )
    .await;
    assert_eq!(stale_item.status(), StatusCode::OK);
    let stale_item: serde_json::Value =
        serde_json::from_slice(&to_bytes(stale_item.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(stale_item["MediaContainer"]["size"], 0);

    let sections = send(
        &fixture.app,
        Method::GET,
        "/library/sections",
        Body::empty(),
    )
    .await;
    assert_eq!(sections.status(), StatusCode::OK);
    let shows = send(
        &fixture.app,
        Method::GET,
        "/library/sections/1/all?type=2",
        Body::empty(),
    )
    .await;
    assert_eq!(shows.status(), StatusCode::OK);
    let shows: serde_json::Value =
        serde_json::from_slice(&to_bytes(shows.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(shows["MediaContainer"]["totalSize"], 1);
}

#[tokio::test]
async fn notification_websocket_does_not_replay_historical_media_as_deleted() {
    let fixture = tv_fixture();
    let stale_path = fixture.root.join("media/.ignored/old-episode.mkv");
    std::fs::create_dir_all(stale_path.parent().unwrap()).unwrap();
    std::fs::write(&stale_path, b"video").unwrap();
    let canonical_path = std::fs::canonicalize(&stale_path).unwrap();
    fixture
        .db
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO media_identity (library_section, canonical_path, media_key) VALUES (?1, ?2, ?3)",
            rusqlite::params![1, canonical_path.to_string_lossy(), "1-999"],
        )
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = fixture.app.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let websocket_key = base64::engine::general_purpose::STANDARD.encode([0_u8; 16]);
    let request = format!(
        "GET /:/websocket/notifications HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: {websocket_key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut response = Vec::new();
    let mut byte = [0_u8; 1];
    while !response.ends_with(b"\r\n\r\n") {
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            stream.read_exact(&mut byte),
        )
        .await
        .unwrap()
        .unwrap();
        response.push(byte[0]);
    }
    assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 101"));

    let mut frame = [0_u8; 2];
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(250),
        stream.read(&mut frame)
    )
    .await
    .is_err());
    server.abort();
}

#[tokio::test]
async fn manual_scan_adds_new_tv_episode_to_plex_inventory() {
    let fixture = tv_fixture();
    let season = fixture.root.join("media/Example Show/Season 01");
    std::fs::write(season.join("Example.Show.S01E03.Added.mkv"), b"new episode").unwrap();

    let scanned = send(&fixture.app, Method::POST, "/settings/scan", Body::empty()).await;
    assert_eq!(scanned.status(), StatusCode::SEE_OTHER);
    assert!(scanned.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .contains("inventory_scan=3"));

    let response = send(
        &fixture.app,
        Method::GET,
        "/library/sections/1/all",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let json: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let shows = json["MediaContainer"]["Metadata"].as_array().unwrap();
    assert_eq!(shows.len(), 1);
    assert_eq!(shows[0]["leafCount"], 3);
}

#[tokio::test]
async fn duplicate_tv_episode_files_are_versions_of_one_plex_item() {
    let fixture = tv_fixture();
    let duplicate_path = fixture
        .root
        .join("media/Example Show/Season 01/Example.Show.S01E02.Next-copy.mkv");
    std::fs::write(&duplicate_path, b"alternate-version").unwrap();

    let scan = send(&fixture.app, Method::POST, "/settings/scan", Body::empty()).await;
    assert_eq!(scan.status(), StatusCode::SEE_OTHER);

    let show_response = send(
        &fixture.app,
        Method::GET,
        "/library/sections/1/all",
        Body::empty(),
    )
    .await;
    let show_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(show_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(show_json["MediaContainer"]["Metadata"][0]["leafCount"], 2);

    let response = send(
        &fixture.app,
        Method::GET,
        "/library/all?type=4",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let json: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let items = json["MediaContainer"]["Metadata"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    let episode = items
        .iter()
        .find(|item| item["index"].as_u64() == Some(2))
        .unwrap();
    let media = episode["Media"].as_array().unwrap();
    assert_eq!(media.len(), 2);
    assert_ne!(media[0]["id"], media[1]["id"]);
    let part_ids = media
        .iter()
        .map(|source| source["Part"][0]["id"].as_u64().unwrap())
        .collect::<Vec<_>>();
    assert_ne!(part_ids[0], part_ids[1]);

    let original_path = fixture
        .root
        .join("media/Example Show/Season 01/Example.Show.S01E02.Next.mkv")
        .to_string_lossy()
        .into_owned();
    let original_path_copy = duplicate_path.to_string_lossy().into_owned();
    let mut listed_paths = Vec::new();
    for source in media {
        let part = &source["Part"][0];
        let path = part["file"].as_str().unwrap();
        listed_paths.push(path.to_owned());
        let expected_bytes = if path == original_path.as_str() {
            b"0123456789abcdef".as_slice()
        } else if path == original_path_copy.as_str() {
            b"alternate-version".as_slice()
        } else {
            panic!("unexpected episode file path: {path}");
        };
        let response = send(
            &fixture.app,
            Method::GET,
            part["key"].as_str().unwrap(),
            Body::empty(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX).await.unwrap(),
            expected_bytes
        );
    }
    assert!(listed_paths.contains(&original_path));
    assert!(listed_paths.contains(&original_path_copy));

    let rating_key = episode["ratingKey"].as_str().unwrap();
    let direct = send(
        &fixture.app,
        Method::GET,
        &format!("/library/metadata/{rating_key}"),
        Body::empty(),
    )
    .await;
    assert_eq!(direct.status(), StatusCode::OK);
    let direct: serde_json::Value =
        serde_json::from_slice(&to_bytes(direct.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        direct["MediaContainer"]["Metadata"][0]["Media"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    let queue = send(
        &fixture.app,
        Method::POST,
        &format!("/playQueues?type=video&uri=%2Flibrary%2Fmetadata%2F{rating_key}"),
        Body::empty(),
    )
    .await;
    assert_eq!(queue.status(), StatusCode::OK);
    let queue: serde_json::Value =
        serde_json::from_slice(&to_bytes(queue.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        queue["MediaContainer"]["Metadata"][0]["Media"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    let decision = send(
        &fixture.app,
        Method::GET,
        &format!("/video/:/transcode/universal/decision?ratingKey={rating_key}&directPlay=1"),
        Body::empty(),
    )
    .await;
    assert_eq!(decision.status(), StatusCode::OK);
    let decision: serde_json::Value =
        serde_json::from_slice(&to_bytes(decision.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        decision["MediaContainer"]["Metadata"][0]["Media"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn other_libraries_skip_tmdb_metadata_matching() {
    let fixture = fixture();
    fixture.config.lock().unwrap().libraries[0].kind = "other".to_owned();

    let scan = send(
        &fixture.app,
        Method::POST,
        "/settings/libraries/0/scan",
        Body::empty(),
    )
    .await;
    assert_eq!(scan.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        scan.headers()[header::LOCATION],
        "/settings?metadata_matched=0"
    );

    let manual_match = send_form(
        &fixture.app,
        Method::POST,
        "/settings/media/scan",
        "library=0&path=Demo.mkv&title=Demo",
    )
    .await;
    assert_eq!(manual_match.status(), StatusCode::SEE_OTHER);
    assert!(manual_match.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .contains("TMDB"));
}

#[tokio::test]
async fn new_tv_episode_inherits_database_show_match_automatically() {
    let fixture = tv_fixture();
    fixture.config.lock().unwrap().metadata_storage = "database".to_owned();
    {
        let mut inventory = fixture.state.media.write().unwrap();
        let mut media = inventory.as_ref().clone();
        for (index, item) in media.iter_mut().enumerate() {
            item.id = format!("1-{}", index + 1);
        }
        *inventory = Arc::new(media);
    }
    super::scan_library_inventory(&fixture.state, 0)
        .await
        .unwrap();

    let initial = super::media_snapshot(&fixture.state);
    let first_episode = initial
        .iter()
        .find(|media| media.path.ends_with("Example.Show.S01E01.Pilot.mkv"))
        .unwrap();
    let show_metadata = serde_json::json!({
        "id": 4242,
        "name": "Example Show",
        "overview": "Matched show"
    });
    let first_key = first_episode.id.clone();
    assert!(super::write_database_metadata(
        &fixture.state,
        &first_key,
        &show_metadata
    ));

    let new_path = fixture
        .root
        .join("media/Example Show/Season 01/Example.Show.S01E03.Added.mkv");
    std::fs::write(&new_path, b"new episode").unwrap();
    super::scan_library_inventory(&fixture.state, 0)
        .await
        .unwrap();

    let new_episode = super::media_snapshot(&fixture.state)
        .iter()
        .find(|media| media.path == new_path)
        .unwrap()
        .clone();
    let new_metadata = super::read_database_metadata(&fixture.state, &new_episode.id).unwrap();
    assert_eq!(new_metadata["id"], 4242);
    assert_eq!(new_metadata["mplex_season"], 1);
    assert_eq!(new_metadata["mplex_episode"], 3);
    assert_eq!(new_metadata["mplex_episode_title"], "Added");
}

#[tokio::test]
async fn files_view_lists_folders_and_supported_or_unsupported_files() {
    let fixture = fixture();
    let archive = fixture.root.join("media/Archive");
    std::fs::create_dir_all(&archive).unwrap();
    std::fs::write(archive.join("readme.txt"), b"text file").unwrap();

    let root = send(
        &fixture.app,
        Method::GET,
        "/?library=0&view=files",
        Body::empty(),
    )
    .await;
    assert_eq!(root.status(), StatusCode::OK);
    let root = to_bytes(root.into_body(), usize::MAX).await.unwrap();
    let root = String::from_utf8(root.to_vec()).unwrap();
    assert!(root.contains("Demo.mkv"));
    assert!(!root.contains("readme.txt"));
    assert!(root.contains("Archive"));
    assert!(root.contains("view=files"));
    assert!(!root.contains("<div class=\"media-card"));

    let nested = send(
        &fixture.app,
        Method::GET,
        "/?library=0&path=Archive&view=files",
        Body::empty(),
    )
    .await;
    assert_eq!(nested.status(), StatusCode::OK);
    let nested = to_bytes(nested.into_body(), usize::MAX).await.unwrap();
    let nested = String::from_utf8(nested.to_vec()).unwrap();
    assert!(nested.contains("readme.txt"));
    assert!(nested.contains("view=files&path=Archive"));
}

#[tokio::test]
async fn web_and_plex_clients_share_playback_progress() {
    let fixture = fixture();
    let saved = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/web/progress")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"path":"Demo.mkv","position_ms":2500,"duration_ms":10000,"watched":false}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::NO_CONTENT);

    let metadata = send(
        &fixture.app,
        Method::GET,
        "/library/metadata/1-1",
        Body::empty(),
    )
    .await;
    let metadata: serde_json::Value =
        serde_json::from_slice(&to_bytes(metadata.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        metadata["MediaContainer"]["Metadata"][0]["viewOffset"],
        2500
    );

    let timeline = send(
        &fixture.app,
        Method::GET,
        "/:/timeline?ratingKey=1-1&time=4000&duration=10000&state=playing",
        Body::empty(),
    )
    .await;
    assert_eq!(timeline.status(), StatusCode::OK);
    assert_eq!(
        fixture
            .state
            .plex_playback_states
            .lock()
            .unwrap()
            .get("unknown:1-1")
            .map(String::as_str),
        Some("playing")
    );
    let scrobble_timeline = send(
        &fixture.app,
        Method::GET,
        "/:/timeline?ratingKey=1-1&time=9000&duration=10000&state=playing",
        Body::empty(),
    )
    .await;
    assert_eq!(scrobble_timeline.status(), StatusCode::OK);
    assert!(fixture
        .state
        .plex_scrobbled_sessions
        .lock()
        .unwrap()
        .contains("unknown:1-1"));
    let duplicate_scrobble_timeline = send(
        &fixture.app,
        Method::GET,
        "/:/timeline?ratingKey=1-1&time=9500&duration=10000&state=playing",
        Body::empty(),
    )
    .await;
    assert_eq!(duplicate_scrobble_timeline.status(), StatusCode::OK);
    assert_eq!(
        fixture.state.plex_scrobbled_sessions.lock().unwrap().len(),
        1
    );

    let timeline_body = to_bytes(timeline.into_body(), usize::MAX).await.unwrap();
    let timeline_json: serde_json::Value = serde_json::from_slice(&timeline_body).unwrap();
    assert_eq!(timeline_json["MediaContainer"]["size"], 0);
    let progress = send(
        &fixture.app,
        Method::GET,
        "/web/progress?path=Demo.mkv",
        Body::empty(),
    )
    .await;
    let progress: serde_json::Value =
        serde_json::from_slice(&to_bytes(progress.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(progress["positionMs"], 9500);
}

#[tokio::test]
async fn plex_continue_watching_hub_tracks_progress_and_remove_action() {
    let fixture = fixture();
    let empty = send(
        &fixture.app,
        Method::GET,
        "/hubs/continueWatching",
        Body::empty(),
    )
    .await;
    assert_eq!(empty.status(), StatusCode::OK);
    let empty: serde_json::Value =
        serde_json::from_slice(&to_bytes(empty.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        empty["MediaContainer"]["Hub"][0]["Metadata"],
        serde_json::json!([])
    );

    let timeline = send(
        &fixture.app,
        Method::GET,
        "/:/timeline?ratingKey=1-1&time=4000&duration=10000",
        Body::empty(),
    )
    .await;
    assert_eq!(timeline.status(), StatusCode::OK);

    let hub = send(
        &fixture.app,
        Method::GET,
        "/hubs/continueWatching",
        Body::empty(),
    )
    .await;
    assert_eq!(hub.status(), StatusCode::OK);
    let hub: serde_json::Value =
        serde_json::from_slice(&to_bytes(hub.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(hub["MediaContainer"]["Hub"][0]["size"], 1);
    assert_eq!(
        hub["MediaContainer"]["Hub"][0]["Metadata"][0]["ratingKey"],
        "1048577"
    );
    assert_eq!(
        hub["MediaContainer"]["Hub"][0]["Metadata"][0]["viewOffset"],
        4000
    );
    assert_eq!(
        hub["MediaContainer"]["Hub"][0]["hubIdentifier"],
        "home.continue"
    );
    assert_eq!(
        hub["MediaContainer"]["Hub"][0]["context"],
        "hub.home.continue"
    );

    let section_hubs = send(&fixture.app, Method::GET, "/hubs/sections/1", Body::empty()).await;
    assert_eq!(section_hubs.status(), StatusCode::OK);
    let section_hubs: serde_json::Value = serde_json::from_slice(
        &to_bytes(section_hubs.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        section_hubs["MediaContainer"]["Hub"][0]["title"],
        "Continue Watching"
    );
    let section_shelves = section_hubs["MediaContainer"]["Hub"].as_array().unwrap();
    assert!(section_shelves
        .iter()
        .any(|hub| { hub["key"] == "/hubs/items?identifier=movie.recentlyadded.1" }));
    assert!(section_shelves
        .iter()
        .any(|hub| hub["title"] == "Recommended in Movies"));

    let recommended = send(
        &fixture.app,
        Method::GET,
        "/hubs/items?identifier=movie.recommended.1",
        Body::empty(),
    )
    .await;
    assert_eq!(recommended.status(), StatusCode::OK);
    let recommended: serde_json::Value =
        serde_json::from_slice(&to_bytes(recommended.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert!(recommended["MediaContainer"]["size"].as_u64().unwrap() > 0);
    assert_eq!(
        recommended["MediaContainer"]["Metadata"][0]["type"],
        "movie"
    );

    let providers = send(&fixture.app, Method::GET, "/media/providers", Body::empty()).await;
    let providers: serde_json::Value =
        serde_json::from_slice(&to_bytes(providers.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let directories = providers
        .pointer("/MediaContainer/MediaProvider/0/Feature/0/Directory")
        .unwrap()
        .as_array()
        .unwrap();
    let movie_directory = directories
        .iter()
        .find(|directory| directory["type"] == "movie")
        .unwrap();
    let pivots = movie_directory["Pivot"].as_array().unwrap();
    assert_eq!(pivots[0]["id"], "recommended");
    assert!(!pivots.iter().any(|pivot| pivot["id"] == "continueWatching"));
    assert!(!pivots.iter().any(|pivot| pivot["id"] == "recentlyAdded"));
    let continue_hub = section_hubs["MediaContainer"]["Hub"]
        .as_array()
        .unwrap()
        .iter()
        .find(|hub| hub["title"] == "Continue Watching")
        .unwrap();
    assert_eq!(
        continue_hub["key"],
        "/hubs/sections/1/continueWatching/items"
    );
    assert_eq!(continue_hub["Metadata"][0]["ratingKey"], "1048577");

    let section_continue_items = send(
        &fixture.app,
        Method::GET,
        "/hubs/sections/1/continueWatching/items?X-Plex-Container-Start=0&X-Plex-Container-Size=7",
        Body::empty(),
    )
    .await;
    assert_eq!(section_continue_items.status(), StatusCode::OK);
    let section_continue_items: serde_json::Value = serde_json::from_slice(
        &to_bytes(section_continue_items.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(section_continue_items["MediaContainer"]["size"], 1);
    assert_eq!(section_continue_items["MediaContainer"]["totalSize"], 1);
    assert_eq!(section_continue_items["MediaContainer"]["offset"], 0);
    assert_eq!(
        section_continue_items["MediaContainer"]["Metadata"][0]["ratingKey"],
        "1048577"
    );

    let section_continue = send(
        &fixture.app,
        Method::GET,
        "/hubs/sections/1/continueWatching",
        Body::empty(),
    )
    .await;
    assert_eq!(section_continue.status(), StatusCode::OK);

    let removed = send(
        &fixture.app,
        Method::PUT,
        "/actions/removeFromContinueWatching?ratingKey=1-1",
        Body::empty(),
    )
    .await;
    assert_eq!(removed.status(), StatusCode::OK);
    let hub = send(
        &fixture.app,
        Method::GET,
        "/hubs/continueWatching",
        Body::empty(),
    )
    .await;
    let hub: serde_json::Value =
        serde_json::from_slice(&to_bytes(hub.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        hub["MediaContainer"]["Hub"][0]["Metadata"],
        serde_json::json!([])
    );

    let missing_key = send(
        &fixture.app,
        Method::PUT,
        "/actions/removeFromContinueWatching",
        Body::empty(),
    )
    .await;
    assert_eq!(missing_key.status(), StatusCode::BAD_REQUEST);
    let missing_media = send(
        &fixture.app,
        Method::PUT,
        "/actions/removeFromContinueWatching?ratingKey=missing",
        Body::empty(),
    )
    .await;
    assert_eq!(missing_media.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn watch_card_actions_change_and_hide_continue_progress() {
    let fixture = fixture();
    let saved = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/web/progress")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"path":"Demo.mkv","position_ms":2500,"duration_ms":10000,"watched":false}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::NO_CONTENT);

    let remove = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/web/watch-action")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"path":"Demo.mkv","library":0,"action":"remove_from_continue"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(remove.status(), StatusCode::NO_CONTENT);

    let progress = send(
        &fixture.app,
        Method::GET,
        "/web/progress?path=Demo.mkv",
        Body::empty(),
    )
    .await;
    let progress: serde_json::Value =
        serde_json::from_slice(&to_bytes(progress.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(progress["positionMs"], 2500);
    let home = send(&fixture.app, Method::GET, "/", Body::empty()).await;
    let home = String::from_utf8(
        to_bytes(home.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let continue_cards = home
        .split("class=\"continue-grid\">")
        .nth(1)
        .unwrap()
        .split("</div>")
        .next()
        .unwrap();
    assert!(!continue_cards.contains("data-media-path=\"Demo.mkv\""));

    let unwatched = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/web/watch-action")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"path":"Demo.mkv","library":0,"action":"unwatched"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unwatched.status(), StatusCode::NO_CONTENT);
    let progress = send(
        &fixture.app,
        Method::GET,
        "/web/progress?path=Demo.mkv",
        Body::empty(),
    )
    .await;
    let progress: serde_json::Value =
        serde_json::from_slice(&to_bytes(progress.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(progress["positionMs"], 0);

    let watched = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/web/watch-action")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"path":"Demo.mkv","library":0,"action":"watched"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watched.status(), StatusCode::NO_CONTENT);
    let progress = send(
        &fixture.app,
        Method::GET,
        "/web/progress?path=Demo.mkv",
        Body::empty(),
    )
    .await;
    let progress: serde_json::Value =
        serde_json::from_slice(&to_bytes(progress.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(progress["positionMs"], 0);
    assert_eq!(progress["watched"], true);
}

#[tokio::test]
async fn marking_a_tv_episode_watched_shows_the_next_episode_in_continue_watching() {
    let fixture = tv_fixture();
    let watched = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/web/watch-action")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"path":"Example Show/Season 01/Example.Show.S01E01.Pilot.mkv","library":0,"action":"watched"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watched.status(), StatusCode::NO_CONTENT);

    let home = send(&fixture.app, Method::GET, "/", Body::empty()).await;
    let home = String::from_utf8(
        to_bytes(home.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let continue_cards = home
        .split("class=\"continue-grid\">")
        .nth(1)
        .unwrap()
        .split("</div>")
        .next()
        .unwrap();
    assert!(continue_cards.contains("Example.Show.S01E02.Next.mkv"));
    assert!(!continue_cards.contains("Example.Show.S01E01.Pilot.mkv"));
}

#[tokio::test]
async fn continue_watching_shows_only_the_first_unwatched_episode_per_show() {
    let fixture = tv_fixture();
    for (path, position_ms) in [
        (
            "Example Show/Season 01/Example.Show.S01E01.Pilot.mkv",
            2_000,
        ),
        ("Example Show/Season 01/Example.Show.S01E02.Next.mkv", 1_000),
    ] {
        let response = fixture
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/web/progress")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "path": path,
                            "position_ms": position_ms,
                            "duration_ms": 10_000,
                            "watched": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    let home = send(&fixture.app, Method::GET, "/", Body::empty()).await;
    let home = String::from_utf8(
        to_bytes(home.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let continue_cards = home
        .split("class=\"continue-grid\">")
        .nth(1)
        .unwrap()
        .split("</section>")
        .next()
        .unwrap();

    assert!(continue_cards.contains("Example.Show.S01E01.Pilot.mkv"));
    assert!(!continue_cards.contains("Example.Show.S01E02.Next.mkv"));
    assert_eq!(continue_cards.matches("class=\"media-card ").count(), 1);

    let plex_hub = send(
        &fixture.app,
        Method::GET,
        "/hubs/continueWatching",
        Body::empty(),
    )
    .await;
    assert_eq!(plex_hub.status(), StatusCode::OK);
    let plex_hub: serde_json::Value =
        serde_json::from_slice(&to_bytes(plex_hub.into_body(), usize::MAX).await.unwrap()).unwrap();
    let episodes = plex_hub["MediaContainer"]["Hub"][0]["Metadata"]
        .as_array()
        .unwrap();
    assert_eq!(episodes.len(), 1);
    assert!(episodes[0]["Media"][0]["Part"][0]["file"]
        .as_str()
        .unwrap()
        .contains("S01E01.Pilot.mkv"));

    let section_hubs = send(&fixture.app, Method::GET, "/hubs/sections/1", Body::empty()).await;
    let section_hubs: serde_json::Value = serde_json::from_slice(
        &to_bytes(section_hubs.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let hubs = section_hubs["MediaContainer"]["Hub"].as_array().unwrap();
    assert_eq!(hubs[0]["title"], "Continue Watching");
    assert!(hubs
        .iter()
        .any(|hub| { hub["key"] == "/hubs/items?identifier=show.recentlyadded.1" }));
    assert!(hubs
        .iter()
        .any(|hub| hub["title"] == "Recommended in Shows"));

    let recommended = send(
        &fixture.app,
        Method::GET,
        "/hubs/items?identifier=show.recommended.1",
        Body::empty(),
    )
    .await;
    assert_eq!(recommended.status(), StatusCode::OK);
    let recommended: serde_json::Value =
        serde_json::from_slice(&to_bytes(recommended.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert!(recommended["MediaContainer"]["size"].as_u64().unwrap() > 0);
    assert_eq!(recommended["MediaContainer"]["Metadata"][0]["type"], "show");

    let show_rating_key = recommended["MediaContainer"]["Metadata"][0]["ratingKey"]
        .as_str()
        .unwrap();
    let play_queue = send(
        &fixture.app,
        Method::POST,
        &format!(
            "/playQueues?type=video&uri=%2Flibrary%2Fmetadata%2F{show_rating_key}&key=%2Flibrary%2Fmetadata%2F{show_rating_key}"
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(play_queue.status(), StatusCode::OK);
    let play_queue: serde_json::Value =
        serde_json::from_slice(&to_bytes(play_queue.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(
        play_queue["MediaContainer"]["Metadata"][0]["type"],
        "episode"
    );
    assert_ne!(
        play_queue["MediaContainer"]["Metadata"][0]["ratingKey"],
        show_rating_key
    );

    let providers = send(&fixture.app, Method::GET, "/media/providers", Body::empty()).await;
    let providers: serde_json::Value =
        serde_json::from_slice(&to_bytes(providers.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let directories = providers
        .pointer("/MediaContainer/MediaProvider/0/Feature/0/Directory")
        .unwrap()
        .as_array()
        .unwrap();
    let tv_directory = directories
        .iter()
        .find(|directory| directory["type"] == "show")
        .unwrap();
    let pivots = tv_directory["Pivot"].as_array().unwrap();
    assert_eq!(pivots[0]["id"], "recommended");
    assert!(!pivots.iter().any(|pivot| pivot["id"] == "continueWatching"));
    assert!(!pivots.iter().any(|pivot| pivot["id"] == "recentlyAdded"));
}

#[tokio::test]
async fn plex_episode_metadata_includes_guid_ancestry_and_watch_state() {
    let fixture = tv_fixture();
    let listing = send(
        &fixture.app,
        Method::GET,
        "/library/all?type=4",
        Body::empty(),
    )
    .await;
    let listing: serde_json::Value =
        serde_json::from_slice(&to_bytes(listing.into_body(), usize::MAX).await.unwrap()).unwrap();
    let episode = &listing["MediaContainer"]["Metadata"][0];
    let rating_key = episode["ratingKey"].as_str().unwrap();
    let show_key = episode["grandparentRatingKey"].as_str().unwrap();
    let season_key = episode["parentRatingKey"].as_str().unwrap();
    assert_eq!(episode["type"], "episode");
    assert!(episode["guid"]
        .as_str()
        .unwrap()
        .starts_with("com.plexapp.plugins.library://episode/"));
    assert!(episode["parentGuid"]
        .as_str()
        .unwrap()
        .starts_with("com.plexapp.plugins.library://season/"));
    assert!(episode["grandparentGuid"].is_string());

    let show = send(
        &fixture.app,
        Method::GET,
        &format!("/library/metadata/{show_key}"),
        Body::empty(),
    )
    .await;
    let show: serde_json::Value =
        serde_json::from_slice(&to_bytes(show.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        show["MediaContainer"]["Metadata"][0]["guid"],
        episode["grandparentGuid"]
    );

    let season = send(
        &fixture.app,
        Method::GET,
        &format!("/library/metadata/{season_key}"),
        Body::empty(),
    )
    .await;
    let season: serde_json::Value =
        serde_json::from_slice(&to_bytes(season.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        season["MediaContainer"]["Metadata"][0]["guid"],
        episode["parentGuid"]
    );
    let webhook_metadata =
        super::plex_webhook_metadata(&fixture.state, rating_key, 2500, Some(10000))
            .await
            .unwrap()
            .1;
    assert_eq!(webhook_metadata["type"], "episode");
    assert_eq!(webhook_metadata["librarySectionType"], "show");
    assert_eq!(webhook_metadata["parentGuid"], episode["parentGuid"]);
    assert_eq!(
        webhook_metadata["grandparentGuid"],
        episode["grandparentGuid"]
    );
    assert_eq!(webhook_metadata["viewOffset"], 2500);
    assert!(webhook_metadata.get("Media").is_none());

    let watched = send(
        &fixture.app,
        Method::GET,
        &format!("/:/scrobble?ratingKey={rating_key}"),
        Body::empty(),
    )
    .await;
    assert_eq!(watched.status(), StatusCode::OK);
    let metadata = send(
        &fixture.app,
        Method::GET,
        &format!("/library/metadata/{rating_key}"),
        Body::empty(),
    )
    .await;
    let metadata: serde_json::Value =
        serde_json::from_slice(&to_bytes(metadata.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(metadata["MediaContainer"]["Metadata"][0]["viewCount"], 1);

    let unwatched = send(
        &fixture.app,
        Method::GET,
        &format!("/:/unscrobble?ratingKey={rating_key}"),
        Body::empty(),
    )
    .await;
    assert_eq!(unwatched.status(), StatusCode::OK);
    let metadata = send(
        &fixture.app,
        Method::GET,
        &format!("/library/metadata/{rating_key}"),
        Body::empty(),
    )
    .await;
    let metadata: serde_json::Value =
        serde_json::from_slice(&to_bytes(metadata.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(metadata["MediaContainer"]["Metadata"][0]["viewCount"], 0);
    assert_eq!(metadata["MediaContainer"]["Metadata"][0]["lastViewedAt"], 0);
}

#[tokio::test]
async fn marking_a_continue_watching_episode_unwatched_keeps_it_at_zero_progress() {
    let fixture = tv_fixture();
    let path = "Example Show/Season 01/Example.Show.S01E01.Pilot.mkv";
    let saved = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/web/progress")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"path":"{path}","position_ms":2500,"duration_ms":10000,"watched":false}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::NO_CONTENT);

    let unwatched = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/web/watch-action")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"path":"{path}","library":0,"action":"unwatched"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unwatched.status(), StatusCode::NO_CONTENT);

    let progress = send(
        &fixture.app,
        Method::GET,
        &format!("/web/progress?path={}", path.replace(' ', "%20")),
        Body::empty(),
    )
    .await;
    let progress: serde_json::Value =
        serde_json::from_slice(&to_bytes(progress.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(progress["positionMs"], 0);
    assert_eq!(progress["watched"], false);

    let home = send(&fixture.app, Method::GET, "/", Body::empty()).await;
    let home = String::from_utf8(
        to_bytes(home.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let continue_cards = home
        .split("class=\"continue-grid\">")
        .nth(1)
        .unwrap()
        .split("</div>")
        .next()
        .unwrap();
    assert!(continue_cards.contains("Example.Show.S01E01.Pilot.mkv"));
    assert!(home.contains("aria-valuenow=\"0\""));
}

#[tokio::test]
async fn web_and_plex_clients_share_subtitle_selection() {
    let fixture = fixture();
    let update = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/web/subtitle-selection")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"path":"Demo.mkv","library":0,"stream_id":{}}}"#,
                    fixture.subtitle_id
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update.status(), StatusCode::NO_CONTENT);

    let metadata = send(
        &fixture.app,
        Method::GET,
        "/library/metadata/1-1",
        Body::empty(),
    )
    .await;
    let metadata: serde_json::Value =
        serde_json::from_slice(&to_bytes(metadata.into_body(), usize::MAX).await.unwrap()).unwrap();
    let streams = metadata["MediaContainer"]["Metadata"][0]["Media"][0]["Part"][0]["Stream"]
        .as_array()
        .unwrap();
    assert!(streams
        .iter()
        .any(|stream| { stream["id"] == fixture.subtitle_id && stream["selected"] == true }));

    let web_player = send(
        &fixture.app,
        Method::GET,
        "/?library=0&play=Demo.mkv",
        Body::empty(),
    )
    .await;
    assert_eq!(web_player.status(), StatusCode::OK);
    let web_player = String::from_utf8(
        to_bytes(web_player.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(web_player.contains(&format!(
        "data-stream-id=\"{}\" data-bilingual=\"false\" selected",
        fixture.subtitle_id
    )));
    assert!(web_player.contains("Translate while playing"));
    assert!(web_player.contains("Translate full episode"));

    let plex_selection = send(
        &fixture.app,
        Method::PUT,
        &format!(
            "/library/parts/{}?subtitleStreamID={}",
            fixture.part_id, fixture.subtitle_id
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(plex_selection.status(), StatusCode::OK);
    let web_selection = send(
        &fixture.app,
        Method::GET,
        "/web/subtitle-selection?library=0&path=Demo.mkv",
        Body::empty(),
    )
    .await;
    let web_selection: serde_json::Value = serde_json::from_slice(
        &to_bytes(web_selection.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(web_selection["streamId"], fixture.subtitle_id);
}

#[tokio::test]
async fn built_in_player_lists_embedded_text_subtitle_tracks() {
    let fixture = fixture();
    let media = fixture.state.media.read().unwrap()[0].clone();
    fixture.state.media_info.lock().unwrap().insert(media.id.clone(), serde_json::json!({
        "media":{"track":[
            {"@type":"Video","StreamOrder":"0","Format":"AVC"},
            {"@type":"Audio","StreamOrder":"1","Format":"AAC","Language":"eng"},
            {"@type":"Text","StreamOrder":"2","Format":"UTF-8","Language":"eng","Title":"English"}
        ]}
    }));
    let response = send(
        &fixture.app,
        Method::GET,
        "/?library=0&play=Demo.mkv",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let page = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(page.contains("Embedded: English"));
    assert!(page.contains("stream=3"));
}

#[tokio::test]
async fn translation_settings_do_not_require_credentials() {
    let fixture = fixture();
    let response = send(
        &fixture.app,
        Method::GET,
        "/settings",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let page = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(page.contains("No API key is required"));
    assert!(page.contains("Enable bilingual subtitles"));
    assert!(page.contains("checked"));
    assert!(page.contains("id=\"subtitle-settings\""));
    assert!(!page.contains("href=\"/settings/translation\""));
    assert!(!page.contains("google_translate_api_key"));
}

#[tokio::test]
async fn bilingual_subtitles_can_be_disabled_globally() {
    let fixture = fixture();
    std::fs::write(
        fixture.root.join("media/Demo.en-vi.srt"),
        "1\n00:00:01,000 --> 00:00:02,000\nHello\nXin chào\n",
    )
    .unwrap();
    let response = send_form(
        &fixture.app,
        Method::POST,
        "/settings",
        "language_primary=en&language_secondary=vi",
    )
    .await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert!(!fixture
        .state
        .config
        .lock()
        .unwrap()
        .bilingual_subtitles_enabled);

    let metadata = send(&fixture.app, Method::GET, "/library/metadata/1-1", Body::empty()).await;
    let metadata: serde_json::Value = serde_json::from_slice(
        &to_bytes(metadata.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    let streams = metadata["MediaContainer"]["Metadata"][0]["Media"][0]["Part"][0]["Stream"]
        .as_array()
        .unwrap();
    assert!(!streams.iter().any(|stream| stream["extendedDisplayTitle"] == "Bilingual VTT External"));
    assert!(streams.iter().any(|stream| stream["displayTitle"] == "English"));
    assert!(!streams.iter().any(|stream| stream["displayTitle"] == "English + Vietnamese"));

    let media = fixture.state.media.read().unwrap()[0].clone();
    let web_tracks = super::subtitle_tracks(
        &fixture.state,
        &media,
        std::path::Path::new("Demo.mkv"),
        Some(0),
    )
    .await;
    assert!(!web_tracks.iter().any(|track| track.bilingual));
}

#[tokio::test]
async fn bilingual_sidecar_is_recognized_and_reused_without_translation() {
    let fixture = fixture();
    let media = fixture.state.media.read().unwrap()[0].clone();
    let sidecar_path = fixture.root.join("media/Demo.en-vi.srt");
    std::fs::write(
        &sidecar_path,
        "1\n00:00:01,000 --> 00:00:02,000\nHello\nXin chào\n",
    )
    .unwrap();

    let translated = super::translate_subtitle_cached(
        &fixture.state,
        &media,
        "ignored source contents",
        "en",
        "srt",
        "en",
        "vi",
    )
    .await
    .unwrap();
    assert!(translated.contains("Hello\nXin chào"));

    let metadata = send(
        &fixture.app,
        Method::GET,
        "/library/metadata/1-1",
        Body::empty(),
    )
    .await;
    let metadata: serde_json::Value =
        serde_json::from_slice(&to_bytes(metadata.into_body(), usize::MAX).await.unwrap()).unwrap();
    let streams = metadata["MediaContainer"]["Metadata"][0]["Media"][0]["Part"][0]["Stream"]
        .as_array()
        .unwrap();
    assert!(streams.iter().any(|stream| {
        stream["displayTitle"] == "English + Vietnamese"
            && stream["extendedDisplayTitle"] == "Bilingual SRT External"
            && stream["title"] == "Bilingual SRT External"
            && stream["external"] == true
    }));
    let generated_id = super::plex_translated_subtitle_id(&media, "en", "vi");
    assert!(!streams.iter().any(|stream| stream["id"] == generated_id));

    let player = send(
        &fixture.app,
        Method::GET,
        "/?library=0&play=Demo.mkv",
        Body::empty(),
    )
    .await;
    let page = String::from_utf8(
        to_bytes(player.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(page.contains("data-bilingual=\"true\""));
    assert!(page.contains("Bilingual SRT External"));
}

#[tokio::test]
async fn completed_bilingual_track_is_written_as_a_named_srt_sidecar() {
    let fixture = fixture();
    let media = fixture.state.media.read().unwrap()[0].clone();
    let sidecar = super::bilingual_subtitle_path(&media, "en", "vi", "srt");
    super::save_bilingual_sidecar(
        &sidecar,
        "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nHello\nXin chào\n\n",
        "srt",
    )
    .await
    .unwrap();
    assert_eq!(sidecar.file_name().unwrap(), "Demo.en-vi.srt");
    let contents = std::fs::read_to_string(sidecar).unwrap();
    assert!(contents.starts_with("1\n00:00:01,000 --> 00:00:02,000"));
    assert!(contents.contains("Hello\nXin chào"));
}

#[tokio::test]
async fn bilingual_sidecar_keeps_vtt_or_ass_source_format() {
    let fixture = fixture();
    let media = fixture.state.media.read().unwrap()[0].clone();
    let vtt = super::bilingual_subtitle_path(&media, "en", "vi", "vtt");
    super::save_bilingual_sidecar(
        &vtt,
        "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nHello\nXin chào\n\n",
        "vtt",
    )
    .await
    .unwrap();
    assert_eq!(vtt.extension().unwrap(), "vtt");
    assert!(std::fs::read_to_string(vtt).unwrap().starts_with("WEBVTT"));

    let ass = super::bilingual_subtitle_path(&media, "en", "vi", "ass");
    super::save_bilingual_sidecar(
        &ass,
        "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nHello\nXin chào\n\n",
        "ass",
    )
    .await
    .unwrap();
    assert_eq!(ass.extension().unwrap(), "ass");
    let contents = std::fs::read_to_string(ass).unwrap();
    assert!(contents.contains("[Events]"));
    assert!(contents.contains("Hello\\NXin chào"));
}

#[tokio::test]
async fn individual_language_sidecars_are_merged_and_marked_in_extended_title() {
    let fixture = fixture();
    let media = fixture.state.media.read().unwrap()[0].clone();
    std::fs::write(
        fixture.root.join("media/Demo.vi.srt"),
        "1\n00:00:01,000 --> 00:00:02,000\nXin chào\n",
    )
    .unwrap();
    let merged = super::ensure_merged_bilingual_sidecar(&media, "en", "vi")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(merged.file_name().unwrap(), "Demo.en-vi.merge.srt");
    let contents = std::fs::read_to_string(&merged).unwrap();
    assert!(contents.contains("Hello\nXin chào"));
    let (display, extended) = super::external_subtitle_titles(&media.path, &merged);
    assert_eq!(display, "English + Vietnamese");
    assert_eq!(extended, "Bilingual SRT External · Merged");
    let metadata = send(&fixture.app, Method::GET, "/library/metadata/1-1", Body::empty()).await;
    let metadata: serde_json::Value = serde_json::from_slice(
        &to_bytes(metadata.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    let streams = metadata["MediaContainer"]["Metadata"][0]["Media"][0]["Part"][0]["Stream"]
        .as_array()
        .unwrap();
    assert!(streams.iter().any(|stream| {
        stream["displayTitle"] == "English + Vietnamese"
            && stream["extendedDisplayTitle"] == "Bilingual SRT External · Merged"
    }));
}

#[tokio::test]
async fn plex_translated_stream_returns_the_staged_part_without_waiting_for_completion() {
    let fixture = fixture();
    let media = fixture.state.media.read().unwrap()[0].clone();
    let checkpoint = super::bilingual_subtitle_path(&media, "en", "vi", "vtt");
    let checkpoint = std::path::PathBuf::from(format!("{}.part", checkpoint.to_string_lossy()));
    std::fs::write(
        &checkpoint,
        "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nHello\nXin chào\n\n",
    )
    .unwrap();
    let final_path = super::bilingual_subtitle_path(&media, "en", "vi", "vtt");
    let (resume_job, wait_for_resume) = tokio::sync::oneshot::channel::<()>();
    let contents = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        super::staged_subtitle_preview(
            &final_path,
            &checkpoint,
            &media.id,
            final_path.to_string_lossy().into_owned(),
            false,
            move |_cancelled| async move { let _ = wait_for_resume.await; },
        ),
    )
    .await
    .expect("subtitle request waited for the stalled background translation");
    resume_job.send(()).unwrap();
    assert!(contents.contains("Hello\nXin chào"));
}

#[test]
fn stopping_playback_cancels_stream_translation_but_preserves_explicit_preparation() {
    let media_id = format!("test-{}", NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed));
    let streaming = super::start_translation_job(
        format!("{media_id}.stream"),
        media_id.clone(),
        false,
    )
    .unwrap();
    let prepared = super::start_translation_job(
        format!("{media_id}.prepared"),
        media_id.clone(),
        true,
    )
    .unwrap();
    super::cancel_streaming_translation_jobs(&media_id);
    assert!(streaming.load(std::sync::atomic::Ordering::Acquire));
    assert!(!prepared.load(std::sync::atomic::Ordering::Acquire));
    super::finish_translation_job(&format!("{media_id}.stream"));
    super::finish_translation_job(&format!("{media_id}.prepared"));
}

#[tokio::test]
async fn plex_stopped_timeline_cancels_streaming_subtitle_generation() {
    let fixture = fixture();
    let media = fixture.state.media.read().unwrap()[0].clone();
    let job_key = format!("{}.stream", media.path.display());
    let cancellation = super::start_translation_job(job_key.clone(), media.id.clone(), false)
        .unwrap();
    let response = send(
        &fixture.app,
        Method::GET,
        "/:/timeline?ratingKey=1-1&time=2000&duration=10000&state=stopped",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(cancellation.load(std::sync::atomic::Ordering::Acquire));
    super::finish_translation_job(&job_key);
}

#[tokio::test]
async fn embedded_text_subtitle_can_be_selected_as_translation_source() {
    let fixture = fixture();
    let media = fixture.state.media.read().unwrap()[0].clone();
    fixture.state.media_info.lock().unwrap().insert(
        media.id.clone(),
        serde_json::json!({
            "media":{"track":[{"@type":"Text","StreamOrder":"2","Format":"UTF-8","Language":"eng"}]}
        }),
    );
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/web/translated-subtitle")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "path":"Demo.mkv", "library":0, "stream_id":3,
                        "first_language":"en", "second_language":"vi"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let message = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(message.contains("Embedded subtitles could not be read"));
}

#[tokio::test]
async fn plex_metadata_and_selection_expose_configured_bilingual_track() {
    let fixture = fixture();
    let media = fixture.state.media.read().unwrap()[0].clone();
    let translated_id = super::plex_translated_subtitle_id(&media, "en", "vi");
    let metadata = send(
        &fixture.app,
        Method::GET,
        "/library/metadata/1-1",
        Body::empty(),
    )
    .await;
    let metadata: serde_json::Value =
        serde_json::from_slice(&to_bytes(metadata.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(
        metadata["MediaContainer"]["Metadata"][0]["Media"][0]["Part"][0]["Stream"]
            .as_array()
            .unwrap()
            .iter()
            .any(|stream| stream["id"] == translated_id && stream["external"] == true)
    );
    let selection = send(
        &fixture.app,
        Method::PUT,
        &format!(
            "/library/parts/{}?subtitleStreamID={translated_id}",
            fixture.part_id
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(selection.status(), StatusCode::OK);
}

#[tokio::test]
async fn library_media_view_starts_with_flat_poster_cards_and_progress() {
    let fixture = fixture();
    let response = send(&fixture.app, Method::GET, "/?library=0", Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains("Poster for Demo"));
    assert!(body.contains("/?library=0&amp;path=&amp;play=Demo.mkv"));
    assert!(body.contains("class=\"media-card-progress-row\""));
    assert!(body.contains("class=\"media-card-percent\""));
    assert!(body.contains("margin: .3rem 0 0"));
    assert!(body.contains("padding: .55rem 0 0"));
    assert!(body.contains("radial-gradient(ellipse at 75% -20%"));
    assert!(body.contains("data-watch-action=\"unwatched\""));
    assert!(body.contains("data-watch-action=\"watched\""));
    assert!(!body.contains("data-watch-action=\"remove_from_continue\""));
    assert!(body.contains("watchAction: action"));
    assert!(body.contains(">0%</span>"));
    assert!(!body.contains("<video class=\"folder-preview-video\""));
    assert!(!body.contains("<span class=\"folder-preview-icon\""));
}

#[tokio::test]
async fn tv_season_navigation_renders_episode_poster_cards() {
    let fixture = tv_fixture();
    let unindexed_episode = fixture
        .root
        .join("media/Example Show/Season 01/Example.Show.S01E03.New.mkv");
    std::fs::write(&unindexed_episode, b"0123456789abcdef").unwrap();
    let season_info = fixture
        .root
        .join("media/Example Show/Season 01/.mplex/data.json");
    std::fs::create_dir_all(season_info.parent().unwrap()).unwrap();
    std::fs::write(
        &season_info,
        r#"{"title":"Season One","description":"A season description"}"#,
    )
    .unwrap();

    let response = send(
        &fixture.app,
        Method::GET,
        "/?library=0&path=Example%20Show%2FSeason%2001",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();

    assert!(body.contains("class=\"media-card media-card-tv\""));
    assert!(body.contains("A season description"));
    let panel = body.find("class=\"metadata-panel\"").unwrap();
    let items = body.find("class=\"library\"").unwrap();
    assert!(panel < items);

    assert!(body.contains("Poster for Pilot"));
    assert!(body.contains("class=\"media-card-title\" title=\"Pilot\""));
    assert!(body.contains("class=\"media-card-subtitle\">S1 · E1</span>"));
    assert!(body.contains("class=\"media-card-details\">Example Show</span>"));
    assert!(body.contains(
        "data-watch-scope=\"item\" data-watch-key=\"Example Show/Season 01/Example.Show.S01E01.Pilot.mkv\""
    ));
    assert!(body.contains(
        "href=\"/?library=0&amp;path=Example%20Show%2FSeason%2001&amp;episode=Example%20Show%2FSeason%2001%2FExample.Show.S01E01.Pilot.mkv\""
    ));
    assert!(body.contains(
        "class=\"media-card-placeholder\" role=\"img\" aria-label=\"Poster unavailable for New\">No poster available"
    ));
    assert!(!body.contains("tv-episode-card"));
}

#[tokio::test]
async fn flat_tv_files_still_navigate_as_show_season_and_episode() {
    let fixture = tv_fixture();
    let file_names = [
        "04 Episode 3 - Tales of the Gold Monkey 1982  -Türkçe Altyazılı-D3A1oEjHlyM.mkv",
        "05 Episode 4 - Tales of the Gold Monkey 1982  -Türkçe Altyazılı-m-pfIkToDtE.mkv",
    ];
    {
        let mut media = fixture.state.media.write().unwrap();
        let mut flat_media = media.as_ref().clone();
        for (item, file_name) in flat_media.iter_mut().zip(file_names) {
            let flat_path = fixture.root.join("media/Example Show").join(file_name);
            std::fs::write(&flat_path, b"0123456789abcdef").unwrap();
            item.path = flat_path;
        }
        *media = Arc::new(flat_media);
    }
    assert!(super::media_snapshot(&fixture.state).iter().all(|media| {
        media.path.parent() == Some(fixture.root.join("media/Example Show").as_path())
    }));
    assert!(super::media_snapshot(&fixture.state).iter().all(|media| {
        super::tv_season_number(&fixture.root.join("media"), &media.path) == Some(1)
    }));

    let show_response = send(
        &fixture.app,
        Method::GET,
        "/?library=0&path=Example%20Show",
        Body::empty(),
    )
    .await;
    assert_eq!(show_response.status(), StatusCode::OK);
    let show_body = String::from_utf8(
        to_bytes(show_response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(show_body.contains("Season 01"));
    assert!(show_body.contains("path=Example%20Show&amp;season=1"));

    let season_response = send(
        &fixture.app,
        Method::GET,
        "/?library=0&path=Example%20Show&season=1",
        Body::empty(),
    )
    .await;
    assert_eq!(season_response.status(), StatusCode::OK);
    let season_body = String::from_utf8(
        to_bytes(season_response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(season_body.contains("Tales of the Gold Monkey 1982"));
    assert!(season_body.contains("S1 · E3"));
    assert!(season_body.contains("S1 · E4"));

    let episode_path = format!("Example Show/{}", file_names[0]);
    let episode_url = format!(
        "/?library=0&path=Example%20Show&season=1&episode={}",
        super::url_encode(&episode_path)
    );
    let episode_response = send(&fixture.app, Method::GET, &episode_url, Body::empty()).await;
    assert_eq!(episode_response.status(), StatusCode::OK);
    let episode_body = String::from_utf8(
        to_bytes(episode_response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(episode_body.contains("<video"));
    assert!(episode_body.contains("Tales of the Gold Monkey"));
}

#[tokio::test]
async fn show_sidecar_stores_show_metadata_without_a_representative_episode_path() {
    let fixture = tv_fixture();
    let media = super::media_snapshot(&fixture.state)[0].clone();
    let folder = fixture.root.join("media");
    let show_directory = folder.join("Example Show");
    let metadata = serde_json::json!({
        "id": 42,
        "overview": "Show overview",
        "first_air_date": "1982-09-22",
        "number_of_seasons": 1,
        "number_of_episodes": 22
    });
    super::store_metadata_sidecar(
        &folder,
        &show_directory,
        "Tales of the Gold Monkey",
        &media,
        &metadata,
    )
    .await
    .unwrap();

    let sidecar = std::fs::read(show_directory.join(".mplex/data.json")).unwrap();
    let sidecar: serde_json::Value = serde_json::from_slice(&sidecar).unwrap();
    assert_eq!(sidecar["title"], "Tales of the Gold Monkey");
    assert_eq!(sidecar["metadata_json"]["number_of_episodes"], 22);
    assert!(sidecar.get("media_path").is_none());
    assert!(sidecar.get("season").is_none());
    assert!(sidecar.get("episode").is_none());
    assert!(sidecar.get("technical").is_none());
}

#[tokio::test]
async fn tv_show_cards_display_the_season_count() {
    let fixture = tv_fixture();
    let response = send(&fixture.app, Method::GET, "/?library=0", Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();

    assert!(body.contains(
        "class=\"media-card-title\" title=\"Example Show\" style=\"height:auto;margin:0;white-space:nowrap;text-overflow:ellipsis\">Example Show</span>"
    ));

    assert!(body.contains("class=\"media-card-subtitle\">1 season</span>"));
    assert!(body.contains(
        "data-refresh-metadata data-library=\"0\" data-path=\"Example Show\">Refresh metadata</button>"
    ));
    assert!(!body.contains("class=\"metadata-panel\""));
    assert!(body.contains(
        "data-fix-match data-library=\"0\" data-path=\"Example Show\">Fix Match</button>"
    ));
    assert!(body.contains("id=\"tv-fix-match-dialog\""));
    assert!(body.contains("aria-label=\"Search TV shows\""));
    assert!(body.contains("aria-label=\"Close Fix Match dialog\" title=\"Close\""));
    assert!(body.contains("/web/tv-match/search"));
}

#[tokio::test]
async fn tv_show_view_places_show_description_above_the_library_items() {
    let fixture = tv_fixture();
    let show_info = fixture.root.join("media/Example Show/.mplex/data.json");
    std::fs::create_dir_all(show_info.parent().unwrap()).unwrap();
    std::fs::write(
        &show_info,
        r#"{"title":"Example Show","description":"A show description","metadata_json":{"id":42,"overview":"A show description"}}"#,
    )
    .unwrap();

    let response = send(
        &fixture.app,
        Method::GET,
        "/?library=0&path=Example%20Show",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let panel = body.find("class=\"metadata-panel\"").unwrap();
    let items = body.find("class=\"library\"").unwrap();
    assert!(panel < items);
    assert!(body.contains("A show description"));
}

#[tokio::test]
async fn tv_episode_view_uses_episode_description_in_the_shared_card_location() {
    let fixture = tv_fixture();
    let show_info = fixture.root.join("media/Example Show/.mplex/data.json");
    std::fs::create_dir_all(show_info.parent().unwrap()).unwrap();
    std::fs::write(
        &show_info,
        r#"{"title":"Example Show","description":"A show description","metadata_json":{"id":42,"overview":"A show description"}}"#,
    )
    .unwrap();
    let season_info = fixture
        .root
        .join("media/Example Show/Season 01/.mplex/data.json");
    std::fs::create_dir_all(season_info.parent().unwrap()).unwrap();
    std::fs::write(
        &season_info,
        r#"{"title":"Season One","description":"A season description","metadata_json":{"overview":"A season description","episodes":[{"episode_number":1,"name":"Pilot","overview":"An episode description","air_date":"2025-01-02","runtime":42}]}}"#,
    )
    .unwrap();

    let response = send(
        &fixture.app,
        Method::GET,
        "/?library=0&path=Example%20Show%2FSeason%2001&episode=Example%20Show%2FSeason%2001%2FExample.Show.S01E01.Pilot.mkv",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let panel = body.find("class=\"metadata-panel\"").unwrap();
    let items = body.find("class=\"library\"").unwrap();
    assert!(panel < items);
    assert!(body.contains("An episode description"));
    assert!(!body.contains("A season description"));
    assert!(!body.contains("A show description"));
}

#[tokio::test]
async fn movie_fix_match_accepts_movie_paths_without_calling_tmdb() {
    let fixture = fixture();
    let response = send(
        &fixture.app,
        Method::GET,
        "/web/tv-match/search?library=0&path=Demo.mkv&query=Demo",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("Add a TMDB bearer token"));
}

#[tokio::test]
async fn refresh_metadata_requires_and_preserves_existing_match() {
    let fixture = fixture();
    super::scan_library_inventory(&fixture.state, 0)
        .await
        .unwrap();
    let media = super::media_snapshot(&fixture.state)
        .iter()
        .find(|media| media.path.ends_with("Demo.mkv"))
        .unwrap()
        .clone();
    let refresh_url = "/settings/libraries/0/scan?path=Demo.mkv&refresh=true";
    let response = send(&fixture.app, Method::POST, refresh_url, Body::empty()).await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert!(response.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .contains("Refresh%20requires%20an%20existing%20metadata%20match"));
    let media_key = super::database_media_key(1, &media.id);
    assert!(super::read_database_metadata(&fixture.state, &media_key).is_none());

    assert!(super::write_database_metadata(
        &fixture.state,
        &media_key,
        &serde_json::json!({"id": 4242, "title": "Selected movie"})
    ));
    let response = send(&fixture.app, Method::POST, refresh_url, Body::empty()).await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        super::read_database_metadata(&fixture.state, &media_key).unwrap()["id"],
        4242
    );
}

#[tokio::test]
async fn tv_fix_match_rejects_invalid_show_paths_without_calling_tmdb() {
    let fixture = tv_fixture();
    let response = send(
        &fixture.app,
        Method::GET,
        "/web/tv-match/search?library=0&path=..%2Foutside&query=Example",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("Invalid TV show path"));
}

#[tokio::test]
async fn tv_season_without_description_has_no_empty_metadata_panel() {
    let fixture = tv_fixture();
    let response = send(
        &fixture.app,
        Method::GET,
        "/?library=0&path=Example%20Show%2FSeason%2001",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();

    assert!(!body.contains("class=\"metadata-panel\""));
}

#[tokio::test]
async fn database_metadata_renders_in_the_selected_movie_view() {
    let fixture = fixture();
    fixture.config.lock().unwrap().metadata_storage = "database".to_owned();
    let media_path = fixture.root.join("media/Demo.mkv");
    let metadata = serde_json::json!({
        "title": "Database Film",
        "overview": "Stored film description",
        "release_date": "2024-03-14"
    })
    .to_string();
    fixture
        .db
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO media (media_key, path, title, metadata_json) VALUES (?1, ?2, ?3, ?4)",
            [
                "1-1-1",
                media_path.to_str().unwrap(),
                "Demo",
                metadata.as_str(),
            ],
        )
        .unwrap();

    let response = send(
        &fixture.app,
        Method::GET,
        "/?library=0&play=Demo.mkv",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains("<h2>Database Film</h2>"));
    assert!(body.contains("Stored film description"));
    assert!(body.contains("2024-03-14"));
}

#[test]
fn media_cards_use_the_selected_media_poster_and_path() {
    let movie = super::Media {
        id: "1-42".to_owned(),
        title: "Demo Movie".to_owned(),
        path: PathBuf::from("Films/Demo Movie.mkv"),
        size: 0,
        container: "mkv".to_owned(),
        mime: "video/x-matroska".to_owned(),
        metadata_revision: 0,
    };
    let movie_metadata = serde_json::json!({
        "title": "Demo Movie",
        "release_date": "2024-05-03",
        "mplex_view_offset": 40,
        "mplex_duration": 100
    });
    let movie_config = super::ConfigLibrary {
        name: "Films".to_owned(),
        kind: "movie".to_owned(),
        folders: vec![PathBuf::from("Films")],
        show_recently_added: true,
        show_continue_watching: true,
        scan_enabled: true,
        scan_interval_value: 5,
        scan_interval_unit: "minutes".to_owned(),
        plex_agent: None,
        plex_scanner: None,
        plex_language: None,
        metadata_agent_provider_group_id: None,
        plex_preferences: None,
    };
    let movie_card = super::media_card(
        &movie,
        &movie_config,
        Some(&movie_metadata),
        Path::new("Films"),
        std::path::Path::new("Films"),
        std::path::Path::new("Films/Demo Movie.mkv"),
        0,
        false,
    );
    assert!(!movie_card.contains("Remove from Continue Watching"));
    assert!(movie_card.contains(
        "data-refresh-metadata data-library=\"0\" data-path=\"Films\">Refresh metadata</button>"
    ));
    assert!(movie_card
        .contains("data-fix-match data-library=\"0\" data-path=\"Films\">Fix Match</button>"));
    let continue_card = super::media_card(
        &movie,
        &movie_config,
        Some(&movie_metadata),
        Path::new("Films"),
        Path::new("Films"),
        Path::new("Films/Demo Movie.mkv"),
        0,
        true,
    );
    assert!(continue_card.contains("Remove from Continue Watching"));
    assert!(movie_card.contains("/library/metadata/1-42/thumb"));
    assert!(movie_card
        .contains("style=\"align-self:start;background:rgba(31,41,55,.3);padding:.7rem\""));
    assert!(movie_card.contains(
        "style=\"box-sizing:border-box;height:4rem;padding-top:0;gap:0;grid-template-rows:1rem 1rem 1rem 1rem\""
    ));
    assert!(movie_card.contains("grid-row:4;height:1rem"));
    assert!(movie_card.contains("path=Films&amp;play=Films%2FDemo%20Movie.mkv"));
    assert!(movie_card.contains("Poster for Demo Movie"));
    assert!(movie_card.contains(
        "class=\"media-card-title\" title=\"Demo Movie\" style=\"height:auto;margin:0;white-space:nowrap;text-overflow:ellipsis\">Demo Movie</span>"
    ));
    assert!(movie_card.contains("</span><span class=\"media-card-subtitle\">2024</span>"));
    assert!(movie_card.contains("aria-valuenow=\"40\""));
    assert!(!movie_card.contains("% watched"));
    assert!(!movie_card.contains("Continue watching</span>"));
    assert!(!movie_card.contains("folder-preview-video"));

    let episode = super::Media {
        id: "1-43".to_owned(),
        title: "Example.Show.S02E04.mkv".to_owned(),
        path: PathBuf::from("Shows/Example Show/Season 02/Example.Show.S02E04.mkv"),
        size: 0,
        container: "mkv".to_owned(),
        mime: "video/x-matroska".to_owned(),
        metadata_revision: 0,
    };
    let tv_config = super::ConfigLibrary {
        name: "Shows".to_owned(),
        kind: "tv".to_owned(),
        folders: vec![PathBuf::from("Shows")],
        show_recently_added: true,
        show_continue_watching: true,
        scan_enabled: true,
        scan_interval_value: 5,
        scan_interval_unit: "minutes".to_owned(),
        plex_agent: None,
        plex_scanner: None,
        plex_language: None,
        metadata_agent_provider_group_id: None,
        plex_preferences: None,
    };
    let tv_metadata = serde_json::json!({"name":"Example Show"});
    let tv_card = super::media_card(
        &episode,
        &tv_config,
        Some(&tv_metadata),
        Path::new("Shows"),
        Path::new("Example Show/Season 02"),
        Path::new("Example Show/Season 02/Example.Show.S02E04.mkv"),
        1,
        false,
    );
    assert!(tv_card.contains(
        "class=\"media-card-title\" title=\"Example Show\" style=\"height:auto;margin:0;white-space:nowrap;text-overflow:ellipsis\">Example Show</span>"
    ));
    assert!(tv_card.contains("class=\"media-card-details\">S2 · E4</span>"));
    assert!(tv_card.contains("class=\"media-card media-card-tv\""));
    assert!(tv_card.contains("class=\"media-card-progress\""));

    assert!(tv_card.contains("background:rgba(31,41,55,.3)"));
    assert!(tv_card.contains(
        "style=\"box-sizing:border-box;height:4rem;padding-top:0;gap:0;grid-template-rows:1rem 1rem 1rem 1rem\""
    ));
    assert!(tv_card.contains("grid-row:4;height:1rem"));

    let episode_title = serde_json::json!({
        "name":"Example Show",
        "mplex_season":2,
        "mplex_episode":4,
        "mplex_episode_title":"The Lost Episode"
    });
    let titled_tv_card = super::media_card(
        &episode,
        &tv_config,
        Some(&episode_title),
        Path::new("Shows"),
        Path::new("Example Show/Season 02"),
        Path::new("Example Show/Season 02/Example.Show.S02E04.mkv"),
        1,
        false,
    );
    assert!(titled_tv_card.contains("class=\"media-card-subtitle\">The Lost Episode</span>"));
}

#[tokio::test]
async fn deleting_a_library_removes_its_settings_but_keeps_media_files() {
    let fixture = fixture();
    let media_file = fixture.root.join("media/Demo.mkv");
    assert!(media_file.exists());

    let response = send(
        &fixture.app,
        Method::POST,
        "/settings/libraries/0/delete",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert!(media_file.exists());

    let sections = send(
        &fixture.app,
        Method::GET,
        "/library/sections",
        Body::empty(),
    )
    .await;
    let sections_json: serde_json::Value =
        serde_json::from_slice(&to_bytes(sections.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(sections_json["MediaContainer"]["size"], 0);
}

#[tokio::test]
async fn deleting_a_library_uses_an_async_response() {
    let fixture = fixture();
    let media_file = fixture.root.join("media/Demo.mkv");

    let missing = send_async_form(
        &fixture.app,
        Method::POST,
        "/settings/libraries/9/delete",
        "",
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(fixture.config.lock().unwrap().libraries.len(), 1);

    let deleted = send_async_form(
        &fixture.app,
        Method::POST,
        "/settings/libraries/0/delete",
        "",
    )
    .await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    assert!(fixture.config.lock().unwrap().libraries.is_empty());
    assert!(media_file.exists());
}

#[tokio::test]
async fn plex_post_sections_creates_a_library_from_query_parameters() {
    let fixture = fixture();
    let media_root = fixture.root.join("morem");
    std::fs::create_dir_all(&media_root).unwrap();
    let mut query = form_urlencoded::Serializer::new(String::new());
    query
        .append_pair("name", "Other Videos")
        .append_pair("type", "movie")
        .append_pair("agent", "tv.plex.agents.none")
        .append_pair("scanner", "Plex Video Files")
        .append_pair("language", "en-US")
        .append_pair("metadataAgentProviderGroupId", "4")
        .append_pair("location", media_root.to_str().unwrap())
        .append_pair("prefs[hidden]", "1")
        .append_pair("prefs[collectionMode]", "0");
    let uri = format!("/library/sections?{}", query.finish());
    let request = Request::builder()
        .method(Method::POST)
        .uri(&uri)
        .header(header::ACCEPT, "application/xml")
        .body(Body::empty())
        .unwrap();

    let response = fixture.app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/xml");
    let xml = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(xml.starts_with("<?xml version=\"1.0\""));
    assert!(xml.contains("<MediaContainer size=\"1\">"));
    assert!(xml.contains("title=\"Other Videos\""));
    assert!(xml.contains("agent=\"tv.plex.agents.none\""));
    assert!(xml.contains("scanner=\"Plex Video Files\""));
    assert!(xml.contains("metadataAgentProviderGroupId=\"4\""));

    let sections = send(
        &fixture.app,
        Method::GET,
        "/library/sections",
        Body::empty(),
    )
    .await;
    let sections: serde_json::Value =
        serde_json::from_slice(&to_bytes(sections.into_body(), usize::MAX).await.unwrap()).unwrap();
    let directory = sections["MediaContainer"]["Directory"]
        .as_array()
        .unwrap()
        .iter()
        .find(|directory| directory["title"] == "Other Videos")
        .unwrap();
    assert_eq!(directory["key"], "2");
    assert_eq!(directory["type"], "movie");
    assert_eq!(directory["agent"], "tv.plex.agents.none");
    assert_eq!(directory["scanner"], "Plex Video Files");
    assert_eq!(directory["language"], "en-US");
    assert_eq!(directory["metadataAgentProviderGroupId"], 4);
    assert_eq!(
        directory["Location"][0]["path"],
        tokio::fs::canonicalize(&media_root)
            .await
            .unwrap()
            .to_str()
            .unwrap()
    );

    let duplicate = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(&uri)
                .header(header::ACCEPT, "application/xml")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate.status(), StatusCode::OK);
    assert_eq!(
        String::from_utf8(
            to_bytes(duplicate.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec()
        )
        .unwrap(),
        xml
    );

    let config: super::Config =
        toml::from_str(&std::fs::read_to_string(fixture.state.config_path.as_ref()).unwrap())
            .unwrap();
    let created = &config.libraries[1];
    assert_eq!(created.plex_agent.as_deref(), Some("tv.plex.agents.none"));
    assert_eq!(created.plex_scanner.as_deref(), Some("Plex Video Files"));
    assert_eq!(created.plex_language.as_deref(), Some("en-US"));
    assert_eq!(created.metadata_agent_provider_group_id, Some(4));
    let preferences = created.plex_preferences.as_ref().unwrap();
    assert_eq!(preferences.get("hidden").map(String::as_str), Some("1"));
    assert_eq!(
        preferences.get("collectionMode").map(String::as_str),
        Some("0")
    );

    let invalid = send(
        &fixture.app,
        Method::POST,
        "/library/sections?name=Invalid&type=movie&agent=tv.plex.agents.none&scanner=Plex%20Video%20Files&location=%2Fdoes-not-exist",
        Body::empty(),
    )
    .await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert_eq!(fixture.config.lock().unwrap().libraries.len(), 2);
}

#[tokio::test]
async fn official_post_sections_all_accepts_numeric_media_types() {
    let fixture = fixture();
    let mut movie_query = form_urlencoded::Serializer::new(String::new());
    movie_query
        .append_pair("name", "Documented Movies")
        .append_pair("type", "1")
        .append_pair("agent", "tv.plex.agents.none")
        .append_pair("language", "en-US")
        .append_pair("locations", fixture.root.join("media").to_str().unwrap());
    let movie_uri = format!("/library/sections/all?{}", movie_query.finish());
    let movie_response = send(&fixture.app, Method::POST, &movie_uri, Body::empty()).await;
    assert_eq!(movie_response.status(), StatusCode::OK);
    let movie: serde_json::Value = serde_json::from_slice(
        &to_bytes(movie_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(movie["MediaContainer"]["Directory"][0]["type"], "movie");
    assert_eq!(
        movie["MediaContainer"]["Directory"][0]["scanner"],
        "Plex Video Files"
    );

    let mut tv_query = form_urlencoded::Serializer::new(String::new());
    tv_query
        .append_pair("name", "Documented Shows")
        .append_pair("type", "2")
        .append_pair("agent", "tv.plex.agents.series")
        .append_pair("language", "en-US")
        .append_pair("locations", fixture.root.join("media").to_str().unwrap());
    let tv_uri = format!("/library/sections/all?{}", tv_query.finish());
    let tv_response = send(&fixture.app, Method::POST, &tv_uri, Body::empty()).await;
    assert_eq!(tv_response.status(), StatusCode::OK);
    let tv: serde_json::Value =
        serde_json::from_slice(&to_bytes(tv_response.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(tv["MediaContainer"]["Directory"][0]["type"], "show");
    assert_eq!(
        tv["MediaContainer"]["Directory"][0]["scanner"],
        "Plex TV Series"
    );
}

#[tokio::test]
async fn plex_delete_section_removes_library_and_keeps_media_files() {
    let fixture = fixture();
    let create_uri = format!(
        "/library/sections?name=Delete%20Me&type=movie&agent=tv.plex.agents.none&location={}",
        fixture.root.join("media").display()
    );
    let created = send(&fixture.app, Method::POST, &create_uri, Body::empty()).await;
    assert_eq!(created.status(), StatusCode::OK);

    let response = send(
        &fixture.app,
        Method::DELETE,
        "/library/sections/2",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(fixture.config.lock().unwrap().libraries.len(), 1);
    assert_eq!(fixture.config.lock().unwrap().libraries[0].name, "Movies");
    assert!(fixture.root.join("media/Demo.mkv").exists());
    let connection = fixture.db.lock().unwrap();
    let library_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM library_groups WHERE name = 'Delete Me'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(library_count, 0);
    drop(connection);

    for section in ["0", "99"] {
        let response = send(
            &fixture.app,
            Method::DELETE,
            &format!("/library/sections/{section}"),
            Body::empty(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
async fn library_creation_keeps_settings_open_and_publishes_stable_scan_metadata() {
    let fixture = fixture();
    let media_root = fixture.root.join("media");
    let form = format!(
        "name=TV&kind=tv&path%5B%5D={}",
        media_root.to_string_lossy()
    );
    let response = send_form(&fixture.app, Method::POST, "/settings/libraries", &form).await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let location = response.headers()[header::LOCATION].to_str().unwrap();
    assert!(location.contains("add_library=true"));
    assert!(location.contains("library_open=1"));

    let settings = send(&fixture.app, Method::GET, location, Body::empty()).await;
    let settings_body = to_bytes(settings.into_body(), usize::MAX).await.unwrap();
    let settings_body = String::from_utf8(settings_body.to_vec()).unwrap();
    assert!(settings_body.contains("Library created and added to Plex discovery."));
    assert!(settings_body.contains("id=\"folder-picker\""));
    assert!(settings_body.contains("params.get('add_library')==='true'"));
    assert!(settings_body.contains("mplex-folder-selected"));
    assert!(!settings_body.contains("<strong>Scan all libraries</strong>"));
    let browser = send(&fixture.app, Method::GET, "/settings/browse", Body::empty()).await;
    let browser_body = to_bytes(browser.into_body(), usize::MAX).await.unwrap();
    let browser_body = String::from_utf8(browser_body.to_vec()).unwrap();
    assert!(browser_body.contains("window.parent.postMessage"));
    assert!(browser_body.contains("mplex-folder-selected"));

    let providers = send(&fixture.app, Method::GET, "/media/providers", Body::empty()).await;
    let providers: serde_json::Value =
        serde_json::from_slice(&to_bytes(providers.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let sections_feature = providers["MediaContainer"]["MediaProvider"][0]["Feature"]
        .as_array()
        .unwrap()
        .iter()
        .find(|feature| feature["key"] == "/library/sections")
        .unwrap();
    let tv = sections_feature["Directory"]
        .as_array()
        .unwrap()
        .iter()
        .find(|directory| directory["title"] == "TV")
        .unwrap();
    let tv_uuid = tv["uuid"].as_str().unwrap().to_owned();
    assert_eq!(tv["scannedAt"], 0);
    let updated_at_before_scan = providers["MediaContainer"]["updatedAt"].as_u64().unwrap();

    assert_eq!(
        crate::scan_library_inventory(&fixture.state, 1)
            .await
            .unwrap(),
        1
    );
    let providers = send(&fixture.app, Method::GET, "/media/providers", Body::empty()).await;
    let providers: serde_json::Value =
        serde_json::from_slice(&to_bytes(providers.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let sections_feature = providers["MediaContainer"]["MediaProvider"][0]["Feature"]
        .as_array()
        .unwrap()
        .iter()
        .find(|feature| feature["key"] == "/library/sections")
        .unwrap();
    let tv = sections_feature["Directory"]
        .as_array()
        .unwrap()
        .iter()
        .find(|directory| directory["title"] == "TV")
        .unwrap();
    assert!(tv["scannedAt"].as_u64().unwrap() > 0);
    assert_eq!(tv["uuid"], tv_uuid);
    let updated_at_after_scan = providers["MediaContainer"]["updatedAt"].as_u64().unwrap();
    assert!(
        updated_at_after_scan > updated_at_before_scan,
        "updatedAt did not advance: before={updated_at_before_scan}, after={updated_at_after_scan}"
    );

    let response = send(
        &fixture.app,
        Method::POST,
        "/settings/libraries/0/delete",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let providers = send(&fixture.app, Method::GET, "/media/providers", Body::empty()).await;
    let providers: serde_json::Value =
        serde_json::from_slice(&to_bytes(providers.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let sections_feature = providers["MediaContainer"]["MediaProvider"][0]["Feature"]
        .as_array()
        .unwrap()
        .iter()
        .find(|feature| feature["key"] == "/library/sections")
        .unwrap();
    let directories = sections_feature["Directory"].as_array().unwrap();
    assert_eq!(directories.len(), 2);
    let tv = directories
        .iter()
        .find(|directory| directory["title"] == "TV")
        .unwrap();
    assert_eq!(tv["key"], "/library/sections/1");
    assert_eq!(tv["uuid"], tv_uuid);
    assert!(providers["MediaContainer"]["updatedAt"].as_u64().unwrap() > updated_at_after_scan);
}

#[test]
fn old_config_filename_migrates_to_the_new_name() {
    let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let config_dir = std::env::temp_dir().join(format!(
        "mplex-config-migration-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&config_dir).unwrap();
    let old_path = config_dir.join("mplex.toml");
    std::fs::write(&old_path, "bind = \"127.0.0.1:32400\"\n").unwrap();

    let new_path = super::config_path_in_dir(&config_dir).unwrap();
    assert_eq!(new_path.file_name().unwrap(), "config.toml");
    assert!(!old_path.exists());
    assert!(new_path.exists());
    std::fs::remove_dir_all(config_dir).unwrap();
}

#[tokio::test]
async fn plex_web_dash_start_reports_transcoding_unavailable() {
    let fixture = fixture();
    let response = send(
        &fixture.app,
        Method::GET,
        "/video/:/transcode/universal/start.mpd?protocol=dash&path=%2Flibrary%2Fmetadata%2F1048577",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/plain; charset=utf-8"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(String::from_utf8(body.to_vec())
        .unwrap()
        .contains("DASH transcoding is not supported."));
}

#[test]
fn plex_web_dash_start_defaults_missing_audio_flag_to_aac_conversion() {
    let query = std::collections::BTreeMap::from([
        ("directPlay".to_owned(), "0".to_owned()),
        ("directStream".to_owned(), "1".to_owned()),
    ]);
    assert_eq!(plex_remux_audio_mode("dash", &query), Some(true));
    assert_eq!(plex_remux_audio_mode("hls", &query), None);
}

#[tokio::test]
async fn plex_hls_start_does_not_claim_success_for_invalid_media() {
    let fixture = fixture();
    let response = send(
        &fixture.app,
        Method::GET,
        "/video/:/transcode/universal/start.m3u8?protocol=hls&directPlay=0&directStream=1&directStreamAudio=1&path=%2Flibrary%2Fmetadata%2F1048577",
        Body::empty(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/plain; charset=utf-8"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(String::from_utf8(body.to_vec())
        .unwrap()
        .contains("HLS playback is not supported."));
    assert!(fixture.state.plex_remux_sessions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn plex_android_audio_conversion_decision_selects_aac() {
    let fixture = fixture();
    fixture.state.media_info.lock().unwrap().insert(
        "1-1".to_owned(),
        serde_json::json!({
            "media": {
                "track": [
                    {
                        "@type": "Video",
                        "ID": "1",
                        "Format": "HEVC",
                        "Default": "Yes"
                    },
                    {
                        "@type": "Audio",
                        "ID": "2",
                        "Format": "E-AC-3",
                        "Channels": "6",
                        "SamplingRate": "48000",
                        "Default": "Yes"
                    }
                ]
            }
        }),
    );

    let response = send(
        &fixture.app,
        Method::GET,
        "/video/:/transcode/universal/decision?ratingKey=1-1&protocol=hls&directPlay=0&directStream=1&directStreamAudio=0&X-Plex-Platform=Android&X-Plex-Product=Plex%20for%20Android%20(Mobile)",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let decision: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(decision["MediaContainer"]["generalDecisionCode"], 1001);
    assert_eq!(decision["MediaContainer"]["transcodeDecisionCode"], 1001);
    assert_eq!(
        decision["MediaContainer"]["Metadata"][0]["Media"][0]["container"],
        "mp4"
    );
    let part = &decision["MediaContainer"]["Metadata"][0]["Media"][0]["Part"][0];
    assert_eq!(part["decision"], "transcode");
    assert_eq!(part["Stream"][0]["codec"], "hevc");
    assert_eq!(part["Stream"][0]["decision"], "copy");
    assert_eq!(part["Stream"][1]["codec"], "aac");
    assert_eq!(part["Stream"][1]["decision"], "transcode");
    let mkv_response = send(
        &fixture.app,
        Method::GET,
        "/video/:/transcode/universal/decision?ratingKey=1-1&protocol=hls&directPlay=0&directStream=1&directStreamAudio=0&X-Plex-Client-Profile-Extra=add-transcode-target%28type%3DvideoProfile%26context%3Dall%26protocol%3Dhls%26container%3Dmkv%26videoCodec%3Dh264%2Chevc%26audioCodec%3Daac%2Cac3%29",
        Body::empty(),
    )
    .await;
    assert_eq!(mkv_response.status(), StatusCode::OK);
    let mkv_decision: serde_json::Value = serde_json::from_slice(
        &to_bytes(mkv_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let mkv_media = &mkv_decision["MediaContainer"]["Metadata"][0]["Media"][0];
    assert_eq!(mkv_media["container"], "mkv");
    assert_eq!(mkv_media["Part"][0]["container"], "mkv");
}

#[tokio::test]
async fn plex_web_dash_video_conversion_decision_selects_h264_and_aac() {
    let fixture = fixture();
    fixture.state.media_info.lock().unwrap().insert(
        "1-1".to_owned(),
        serde_json::json!({
            "media": {
                "track": [
                    {
                        "@type": "Video",
                        "ID": "1",
                        "Format": "HEVC",
                        "Default": "Yes"
                    },
                    {
                        "@type": "Audio",
                        "ID": "2",
                        "Format": "E-AC-3",
                        "Channels": "6",
                        "SamplingRate": "48000",
                        "Default": "Yes"
                    }
                ]
            }
        }),
    );

    let response = send(
        &fixture.app,
        Method::GET,
        "/video/:/transcode/universal/decision?ratingKey=1-1&protocol=dash&directPlay=0&directStream=0&directStreamAudio=0",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let decision: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(decision["MediaContainer"]["generalDecisionCode"], 1001);
    assert_eq!(decision["MediaContainer"]["transcodeDecisionCode"], 1001);
    assert_eq!(decision["MediaContainer"]["directPlayDecisionCode"], 3000);
    let part = &decision["MediaContainer"]["Metadata"][0]["Media"][0]["Part"][0];
    assert_eq!(part["decision"], "transcode");
    assert_eq!(part["Stream"][0]["codec"], "h264");
    assert_eq!(part["Stream"][0]["decision"], "transcode");
    assert_eq!(part["Stream"][1]["codec"], "aac");
    assert_eq!(part["Stream"][1]["decision"], "transcode");
}

#[tokio::test]
async fn plex_playback_endpoints_cover_success_and_failure_paths() {
    let fixture = fixture();

    let providers = send(&fixture.app, Method::GET, "/media/providers", Body::empty()).await;
    assert_eq!(providers.status(), StatusCode::OK);
    let providers_json: serde_json::Value =
        serde_json::from_slice(&to_bytes(providers.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(providers_json["MediaContainer"]["transcoderVideo"], true);

    let root = send(
        &fixture.app,
        Method::GET,
        "/?X-Plex-Token=test",
        Body::empty(),
    )
    .await;
    assert_eq!(root.status(), StatusCode::OK);
    let root_json: serde_json::Value =
        serde_json::from_slice(&to_bytes(root.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(root_json["MediaContainer"]["transcoderVideo"], true);

    let preflight = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::OPTIONS)
                .uri("/playQueues")
                .header(header::ORIGIN, "file://")
                .header("access-control-request-method", "POST")
                .header(
                    "access-control-request-headers",
                    "content-type,x-plex-token",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(preflight.status(), StatusCode::OK);
    assert_eq!(
        preflight.headers()["access-control-allow-origin"],
        "file://"
    );
    assert_eq!(
        preflight.headers()["access-control-allow-methods"],
        "GET, POST, PUT, DELETE, HEAD, OPTIONS"
    );

    let queue_from_file_origin = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/playQueues?type=video&uri=%2Flibrary%2Fmetadata%2F1-1")
                .header(header::ORIGIN, "file://")
                .header(header::ACCEPT, "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(queue_from_file_origin.status(), StatusCode::OK);
    assert_eq!(
        queue_from_file_origin.headers()["access-control-allow-origin"],
        "file://"
    );

    let item = send(
        &fixture.app,
        Method::GET,
        "/library/metadata/1-1",
        Body::empty(),
    )
    .await;
    assert_eq!(item.status(), StatusCode::OK);
    let item_json: serde_json::Value =
        serde_json::from_slice(&to_bytes(item.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(item_json["MediaContainer"]["directPlayDecisionCode"], 1000);
    assert_eq!(item_json["MediaContainer"]["generalDecisionCode"], 1000);
    assert!(
        item_json["MediaContainer"]["Metadata"][0]["updatedAt"]
            .as_u64()
            .unwrap()
            > fixture.change_stamp
    );
    assert_eq!(
        item_json["MediaContainer"]["Metadata"][0]["Media"][0]["decision"],
        "directplay"
    );
    assert_eq!(
        item_json["MediaContainer"]["Metadata"][0]["Media"][0]["Part"][0]["decision"],
        "directplay"
    );
    let rating_key = item_json["MediaContainer"]["Metadata"][0]["ratingKey"]
        .as_str()
        .unwrap();
    assert_eq!(item_json["MediaContainer"]["Metadata"][0]["type"], "movie");
    assert!(
        item_json["MediaContainer"]["Metadata"][0]["Media"][0]["Part"][0]["key"]
            .as_str()
            .unwrap()
            .starts_with("/library/parts/")
    );

    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            "/library/metadata/missing",
            Body::empty()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );

    let queue_uri = format!(
        "/playQueues?type=video&uri=%2Flibrary%2Fmetadata%2F1-1&key=%2Flibrary%2Fmetadata%2F1-1"
    );
    let queue = send(&fixture.app, Method::POST, &queue_uri, Body::empty()).await;
    assert_eq!(queue.status(), StatusCode::OK);
    let queue_json: serde_json::Value =
        serde_json::from_slice(&to_bytes(queue.into_body(), usize::MAX).await.unwrap()).unwrap();
    let queue_id = queue_json["MediaContainer"]["playQueueID"]
        .as_u64()
        .unwrap();
    assert_eq!(
        queue_json["MediaContainer"]["playQueueSelectedMetadataItemID"],
        rating_key
    );
    assert_eq!(
        queue_json["MediaContainer"]["Metadata"][0]["ratingKey"],
        rating_key
    );
    assert_eq!(
        queue_json["MediaContainer"]["Metadata"][0]["Media"][0]["decision"],
        "directplay"
    );
    assert_eq!(
        queue_json["MediaContainer"]["Metadata"][0]["Media"][0]["Part"][0]["decision"],
        "directplay"
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            &format!("/playQueues/{queue_id}"),
            Body::empty()
        )
        .await
        .status(),
        StatusCode::OK
    );

    assert_eq!(
        send(&fixture.app, Method::GET, "/playQueues/999", Body::empty())
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(&fixture.app, Method::POST, "/playQueues", Body::empty())
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::POST,
            "/playQueues?type=audio&uri=%2Flibrary%2Fmetadata%2F1-1",
            Body::empty()
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::POST,
            "/playQueues?type=video&uri=%2Flibrary%2Fmetadata%2F404",
            Body::empty()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );

    let decision = send(
        &fixture.app,
        Method::GET,
        &format!("/video/:/transcode/universal/decision?ratingKey=1-1&directPlay=1"),
        Body::empty(),
    )
    .await;
    assert_eq!(decision.status(), StatusCode::OK);
    let decision_json: serde_json::Value =
        serde_json::from_slice(&to_bytes(decision.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        decision_json["MediaContainer"]["directPlayDecisionCode"],
        1000
    );
    assert_eq!(
        decision_json["MediaContainer"]["Metadata"][0]["ratingKey"],
        rating_key
    );

    let conversion_decision = send(
        &fixture.app,
        Method::GET,
        "/video/:/transcode/universal/decision?ratingKey=1-1&protocol=dash&directPlay=0&directStream=1&subtitles=auto",
        Body::empty(),
    )
    .await;
    assert_eq!(conversion_decision.status(), StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            "/video/:/transcode/universal/decision",
            Body::empty()
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            "/video/:/transcode/universal/decision?ratingKey=missing",
            Body::empty()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );

    let part = send(
        &fixture.app,
        Method::GET,
        &format!("/library/metadata/1-1/media/{}", fixture.part_id),
        Body::empty(),
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    let part_file = send(
        &fixture.app,
        Method::GET,
        &format!(
            "/library/parts/{}/{}/file.mkv",
            fixture.part_id, fixture.change_stamp
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(part_file.status(), StatusCode::OK);
    assert_eq!(part_file.headers()[header::ACCEPT_RANGES], "bytes");
    let download = send(
        &fixture.app,
        Method::GET,
        &format!(
            "/library/parts/{}/{}/file.mkv?download=1",
            fixture.part_id, fixture.change_stamp
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(download.status(), StatusCode::OK);
    assert_eq!(
        download.headers()[header::CONTENT_DISPOSITION],
        "attachment; filename=\"Demo.mkv\"; filename*=UTF-8''Demo.mkv"
    );
    let part_range = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/library/parts/{}/{}/file.mkv",
                    fixture.part_id, fixture.change_stamp
                ))
                .header(header::RANGE, "bytes=2-5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(part_range.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(part_range.headers()[header::CONTENT_RANGE], "bytes 2-5/16");
    assert_eq!(
        to_bytes(part_range.into_body(), usize::MAX)
            .await
            .unwrap()
            .as_ref(),
        b"2345"
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            &format!(
                "/library/parts/{}/{}/wrong.mkv",
                fixture.part_id, fixture.change_stamp
            ),
            Body::empty()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            &format!(
                "/library/parts/{}/{}/file.mkv",
                fixture.part_id, fixture.change_stamp
            ),
            Body::empty()
        )
        .await
        .status(),
        StatusCode::OK
    );

    let ranged = send(&fixture.app, Method::GET, "/media/1-1", Body::empty()).await;
    assert_eq!(ranged.status(), StatusCode::OK);
    let range_request = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/media/1-1")
                .header(header::RANGE, "bytes=2-5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(range_request.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        range_request.headers()[header::CONTENT_RANGE],
        "bytes 2-5/16"
    );
    assert_eq!(
        to_bytes(range_request.into_body(), usize::MAX)
            .await
            .unwrap()
            .as_ref(),
        b"2345"
    );
    let invalid_range = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/media/1-1")
                .header(header::RANGE, "bytes=99-")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid_range.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        send(&fixture.app, Method::GET, "/media/missing", Body::empty())
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::PUT,
            &format!("/library/parts/{}", fixture.part_id),
            Body::empty()
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::PUT,
            &format!("/library/parts/{}?audioStreamID=999", fixture.part_id),
            Body::empty()
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            &format!("/library/streams/{}.srt?format=vtt", fixture.subtitle_id),
            Body::empty()
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            &format!(
                "/library/streams/{}.srt.srt?format=vtt",
                fixture.subtitle_id
            ),
            Body::empty()
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            "/library/streams/0.unknown",
            Body::empty()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    let thumb = send(
        &fixture.app,
        Method::GET,
        "/library/metadata/1-1/thumb",
        Body::empty(),
    )
    .await;
    assert_eq!(thumb.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(thumb.into_body(), usize::MAX)
            .await
            .unwrap()
            .as_ref(),
        b"cover"
    );
    let photo = send(
        &fixture.app,
        Method::GET,
        "/photo/:/transcode?url=%2Flibrary%2Fmetadata%2F1-1%2Fthumb",
        Body::empty(),
    )
    .await;
    assert_eq!(photo.status(), StatusCode::OK);

    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            "/:/timeline?ratingKey=1-1&time=500&duration=1000",
            Body::empty()
        )
        .await
        .status(),
        StatusCode::OK
    );
    let progress_item = send(
        &fixture.app,
        Method::GET,
        "/library/metadata/1-1",
        Body::empty(),
    )
    .await;
    let progress_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(progress_item.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        progress_json["MediaContainer"]["Metadata"][0]["viewOffset"],
        500
    );
    assert_eq!(
        send(&fixture.app, Method::GET, "/:/timeline", Body::empty())
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            "/:/scrobble?ratingKey=1-1",
            Body::empty()
        )
        .await
        .status(),
        StatusCode::OK
    );
    let watched_item = send(
        &fixture.app,
        Method::GET,
        "/library/metadata/1-1",
        Body::empty(),
    )
    .await;
    let watched_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(watched_item.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        watched_json["MediaContainer"]["Metadata"][0]["viewCount"],
        1
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            "/:/unscrobble?ratingKey=1-1",
            Body::empty()
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        send(&fixture.app, Method::GET, "/:/prefs", Body::empty())
            .await
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn myplex_claim_and_reachability_require_a_claimed_account() {
    let fixture = fixture();
    let claim = send(&fixture.app, Method::POST, "/myplex/claim", Body::empty()).await;
    assert_eq!(claim.status(), StatusCode::UNAUTHORIZED);

    let reachability = send(
        &fixture.app,
        Method::PUT,
        "/myplex/refreshReachability",
        Body::empty(),
    )
    .await;
    assert_eq!(reachability.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn plex_account_preferences_can_be_updated_and_read_back() {
    let fixture = fixture();
    let update = send(
        &fixture.app,
        Method::PUT,
        "/accounts/1?defaultAudioLanguage=ab&autoSelectAudio=1&defaultSubtitleLanguage=ab&subtitleMode=2",
        Body::empty(),
    )
    .await;
    assert_eq!(update.status(), StatusCode::OK);
    assert_eq!(
        fixture
            .config
            .lock()
            .unwrap()
            .plex_account_preferences
            .get("defaultAudioLanguage")
            .map(String::as_str),
        Some("ab")
    );

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/accounts/1")
                .header(header::ACCEPT, "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let account: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let account = &account["MediaContainer"]["Account"][0];
    assert_eq!(account["defaultAudioLanguage"], "ab");
    assert_eq!(account["defaultSubtitleLanguage"], "ab");
    assert_eq!(account["subtitleMode"], 2);

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/accounts/1")
                .header(header::ACCEPT, "text/plain, */*; q=0.01")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/xml; charset=utf-8"
    );

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let account_xml = String::from_utf8(body.to_vec()).unwrap();
    assert!(account_xml.contains("autoSelectAudio=\"1\""));
    assert!(account_xml.contains("defaultAudioLanguage=\"ab\""));
}

#[tokio::test]
async fn every_registered_plex_route_has_success_and_failure_cases() {
    let fixture = fixture();
    let queue = send(
        &fixture.app,
        Method::POST,
        "/playQueues?type=video&uri=%2Flibrary%2Fmetadata%2F1-1",
        Body::empty(),
    )
    .await;
    assert_eq!(queue.status(), StatusCode::OK);

    let cases = [
        (Method::GET, "/?X-Plex-Product=Plex%20Test"),
        (Method::GET, "/identity"),
        (Method::GET, "/myplex/account"),
        (Method::GET, "/accounts"),
        (Method::GET, "/devices"),
        (Method::GET, "/accounts/1"),
        (Method::GET, "/activities"),
        (Method::GET, "/status/sessions"),
        (Method::GET, "/status/sessions/history/all"),
        (Method::GET, "/servers"),
        (Method::GET, "/media/providers"),
        (Method::GET, "/services/browse?includeFiles=0"),
        (Method::GET, "/security/resources"),
        (Method::GET, "/hubs"),
        (Method::GET, "/hubs/promoted"),
        (Method::GET, "/hubs/search?query=Demo"),
        (Method::GET, "/hubs/continueWatching"),
        (
            Method::PUT,
            "/actions/removeFromContinueWatching?ratingKey=1-1",
        ),
        (Method::GET, "/hubs/sections/1"),
        (
            Method::GET,
            "/hubs/sections/1/continueWatching/items?X-Plex-Container-Size=7",
        ),

        (
            Method::GET,
            "/hubs/items?identifier=movie.recommended.1",
        ),

        (Method::GET, "/hubs/metadata/1-1/related"),
        (Method::GET, "/library/sections"),
        (Method::GET, "/library/sections/prefs?type=1"),
        (
            Method::POST,
            &format!(
                "/library/sections?name=Route%20Library&type=movie&agent=tv.plex.agents.none&scanner=Plex%20Video%20Files&location={}",
                fixture.root.join("media").display()
            ),
        ),

        (Method::GET, "/system/scanners/1?action=update"),
        (Method::GET, "/library/sections/1"),
        (Method::GET, "/library/sections/1/refresh"),
        (Method::GET, "/library/sections/1/collections"),
        (Method::GET, "/library/sections/1/prefs"),
        (Method::PUT, "/library/sections/1/prefs?hidden=1"),
        (Method::GET, "/library/sections/1/sorts"),
        (Method::GET, "/library/sections/1/filters"),
        (Method::GET, "/library/sections/1/firstCharacter"),
        (Method::GET, "/library/sections/1/firstCharacter/D"),
        (Method::GET, "/library/sections/1/firstCharacters"),
        (Method::GET, "/library/search?query=Demo"),
        (Method::GET, "/library/all"),
        (Method::GET, "/library/sections/1/all"),
        (Method::GET, "/library/metadata/1-1"),
        (Method::GET, "/library/metadata/1-1/children"),
        (Method::GET, "/library/metadata/1-1/related"),
        (Method::GET, "/library/metadata/1-1/extras"),
        (
            Method::GET,
            &format!("/library/metadata/1-1/media/{}", fixture.part_id),
        ),
        (Method::PUT, &format!("/library/parts/{}", fixture.part_id)),
        (
            Method::GET,
            &format!(
                "/library/parts/{}/{}/file.mkv",
                fixture.part_id, fixture.change_stamp
            ),
        ),
        (
            Method::GET,
            &format!("/library/streams/{}.srt", fixture.subtitle_id),
        ),
        (
            Method::POST,
            "/playQueues?type=video&uri=%2Flibrary%2Fmetadata%2F1-1",
        ),
        (Method::GET, "/playQueues/1"),
        (Method::GET, "/playlists"),
        (
            Method::POST,
            "/playlists?title=Route%20Test&playlistType=video&smart=0",
        ),
        (Method::GET, "/library/collections"),
        (
            Method::POST,
            "/library/collections?title=Route%20Collection&sectionId=1&smart=0",
        ),
        (Method::GET, "/library/metadata/1-1/thumb"),
        (Method::GET, "/library/metadata/1-1/art"),
        (Method::GET, "/media/1-1"),
        (Method::HEAD, "/media/1-1"),
        (
            Method::GET,
            "/video/:/transcode/universal/decision?ratingKey=1-1",
        ),
        (
            Method::GET,
            "/photo/:/transcode?url=%2Flibrary%2Fmetadata%2F1-1%2Fthumb",
        ),
        (Method::GET, "/:/timeline?ratingKey=1-1&time=100"),
        (Method::GET, "/:/scrobble?ratingKey=1-1"),
        (Method::GET, "/:/unscrobble?ratingKey=1-1"),
        (Method::GET, "/:/prefs"),
    ];

    for (method, uri) in cases {
        let response = send(&fixture.app, method.clone(), uri, Body::empty()).await;
        let expected_status = if method == Method::POST && uri.starts_with("/library/sections?") {
            StatusCode::OK
        } else if method == Method::POST
            && (uri.starts_with("/playlists?") || uri.starts_with("/library/collections?"))
        {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        };
        assert_eq!(
            response.status(),
            expected_status,
            "{method} {uri} did not dispatch successfully"
        );
        if uri.starts_with("/video/:/") || uri.starts_with("/photo/:/") || uri.starts_with("/:/") {
            continue;
        }
        let (failure_uri, failure_status) = if uri == "/library/sections/1" {
            ("/library/sections/999", StatusCode::NOT_FOUND)
        } else if uri.starts_with("/media/") {
            ("/media/no-such-item", StatusCode::NOT_FOUND)
        } else {
            (uri, StatusCode::METHOD_NOT_ALLOWED)
        };
        assert_eq!(
            send(&fixture.app, Method::DELETE, failure_uri, Body::empty())
                .await
                .status(),
            failure_status,
            "DELETE {failure_uri} did not produce its expected failure"
        );
    }

    // Axum's in-process oneshot service has no Hyper OnUpgrade extension,
    // so it can verify dispatch and the expected upgrade rejection only.
    let websocket = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/:/websockets/notifications")
                .header(header::CONNECTION, "Upgrade")
                .header(header::UPGRADE, "websocket")
                .header("sec-websocket-version", "13")
                .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(websocket.status(), StatusCode::UPGRADE_REQUIRED);
}

#[tokio::test]
async fn plex_section_prefs_returns_library_settings_for_movies_and_tv() {
    for (fixture, expected_size, expected_tv_setting) in
        [(fixture(), 18, false), (tv_fixture(), 22, true)]
    {
        let response = send(
            &fixture.app,
            Method::GET,
            "/library/sections/1/prefs",
            Body::empty(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let settings = body["MediaContainer"]["Setting"].as_array().unwrap();
        assert_eq!(body["MediaContainer"]["size"], expected_size);
        assert_eq!(settings.len(), expected_size);
        assert_eq!(
            settings
                .iter()
                .any(|setting| setting["id"] == "episodeSort"),
            expected_tv_setting
        );
        assert!(settings.iter().any(|setting| {
            setting["id"] == "collectionMode"
                && setting["enumValues"]
                    .as_str()
                    .unwrap()
                    .contains("2:Show both")
        }));
        assert!(settings.iter().all(|setting| {
            setting["value"].is_string()
                && setting["default"].is_string()
                && setting["hidden"] == false
                && setting["advanced"] == false
                && setting["group"] == ""
        }));
    }
}

#[tokio::test]
async fn plex_global_section_prefs_validates_type_and_agent_prefs_route_is_available() {
    let fixture = fixture();
    let response = send(
        &fixture.app,
        Method::GET,
        "/library/sections/prefs?type=1&agent=tv.plex.agents.none",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["MediaContainer"]["size"], 18);

    let missing_type = send(
        &fixture.app,
        Method::GET,
        "/library/sections/prefs",
        Body::empty(),
    )
    .await;
    assert_eq!(missing_type.status(), StatusCode::BAD_REQUEST);

    let agent_prefs = send(
        &fixture.app,
        Method::GET,
        "/:/plugins/tv.plex.agents.none/prefs",
        Body::empty(),
    )
    .await;
    assert_eq!(agent_prefs.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(agent_prefs.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(body["MediaContainer"]["size"], 0);

    let unknown_agent = send(
        &fixture.app,
        Method::GET,
        "/:/plugins/unknown.agent/prefs",
        Body::empty(),
    )
    .await;
    assert_eq!(unknown_agent.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn plex_section_prefs_put_persists_preferences_and_rejects_empty_updates() {
    let fixture = fixture();
    let response = send(
        &fixture.app,
        Method::PUT,
        "/library/sections/1/prefs?hidden=1&collectionMode=0",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    {
        let config = fixture.config.lock().unwrap();
        let preferences = config.libraries[0].plex_preferences.as_ref().unwrap();
        assert_eq!(preferences.get("hidden").map(String::as_str), Some("1"));
        assert_eq!(
            preferences.get("collectionMode").map(String::as_str),
            Some("0")
        );
    }

    let missing_preferences = send(
        &fixture.app,
        Method::PUT,
        "/library/sections/1/prefs",
        Body::empty(),
    )
    .await;
    assert_eq!(missing_preferences.status(), StatusCode::BAD_REQUEST);

    let missing_section = send(
        &fixture.app,
        Method::PUT,
        "/library/sections/99/prefs?hidden=1",
        Body::empty(),
    )
    .await;
    assert_eq!(missing_section.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn plex_services_browse_lists_directories_and_optional_files() {
    let fixture = fixture();
    let root_response = send(
        &fixture.app,
        Method::GET,
        "/services/browse?includeFiles=0",
        Body::empty(),
    )
    .await;
    assert_eq!(root_response.status(), StatusCode::OK);

    let parent = fixture.root.join("browse");
    let child = parent.join("Child Folder");
    std::fs::create_dir_all(&child).unwrap();
    std::fs::write(parent.join("sample.mkv"), b"sample").unwrap();
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(parent.to_string_lossy().as_bytes());

    let directories = send(
        &fixture.app,
        Method::GET,
        &format!("/services/browse/{encoded}?includeFiles=0"),
        Body::empty(),
    )
    .await;
    assert_eq!(directories.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(directories.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains("<Path"));
    assert!(body.contains(&child.to_string_lossy().to_string()));
    assert!(!body.contains("<File"));

    let files = send(
        &fixture.app,
        Method::GET,
        &format!("/services/browse/{encoded}?includeFiles=1"),
        Body::empty(),
    )
    .await;
    assert_eq!(files.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(files.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains("<File"));

    let invalid_path = send(
        &fixture.app,
        Method::GET,
        "/services/browse/not-base64",
        Body::empty(),
    )
    .await;
    assert_eq!(invalid_path.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn plex_system_agents_returns_movie_languages_as_xml() {
    let fixture = fixture();
    let mut bodies = Vec::new();
    for uri in [
        "/system/agents?mediaType=1&",
        "/system/agents?mediaType=2&action=update&X-Plex-Token=test",
    ] {
        let response = send(&fixture.app, Method::GET, uri, Body::empty()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/xml");
        bodies.push(
            String::from_utf8(
                to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap(),
        );
    }
    assert_eq!(bodies[0], bodies[1]);
    let body = &bodies[0];
    assert!(
        body.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?><MediaContainer size=\"3\">")
    );
    assert_eq!(body.matches("<Agent ").count(), 3);
    assert_eq!(body.matches("<Language ").count(), 135);
    assert_eq!(body.matches("default=\"1\"").count(), 3);
    assert!(body.contains("identifier=\"tv.plex.agents.movie\" name=\"Plex Movie\""));
    assert!(body.contains("code=\"en-US\" title=\"English\" default=\"1\""));
    assert!(body.contains("code=\"nb-NO\" title=\"Norwegian Bokm&#229;l\""));

    assert_eq!(
        send(&fixture.app, Method::GET, "/system/agents", Body::empty())
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            "/system/agents?mediaType=3",
            Body::empty(),
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn plex_section_put_adds_locations_and_preserves_existing_folders() {
    let fixture = fixture();
    let existing = fixture.root.join("media");
    let added = fixture.root.join("more-media");
    let duplicate = fixture.root.join("more-media-2");
    std::fs::create_dir_all(&added).unwrap();
    std::fs::create_dir_all(&duplicate).unwrap();
    let added = tokio::fs::canonicalize(added).await.unwrap();
    let duplicate = tokio::fs::canonicalize(duplicate).await.unwrap();

    let uri = format!(
        "/library/sections/1?agent=tv.plex.agents.movie&locations={}&locations={}&locations={}",
        existing.display(),
        added.display(),
        duplicate.display()
    );
    let response = send(&fixture.app, Method::PUT, &uri, Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK);

    {
        let config = fixture.config.lock().unwrap();
        assert_eq!(
            config.libraries[0].folders,
            vec![existing.clone(), added.clone(), duplicate.clone()]
        );
    }

    let sections = send(
        &fixture.app,
        Method::GET,
        "/library/sections",
        Body::empty(),
    )
    .await;
    let sections: serde_json::Value =
        serde_json::from_slice(&to_bytes(sections.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        sections["MediaContainer"]["Directory"][0]["Location"]
            .as_array()
            .unwrap()
            .len(),
        3
    );

    let missing_agent = send(
        &fixture.app,
        Method::PUT,
        &format!("/library/sections/1?locations={}", added.display()),
        Body::empty(),
    )
    .await;
    assert_eq!(missing_agent.status(), StatusCode::BAD_REQUEST);

    let invalid_location = send(
        &fixture.app,
        Method::PUT,
        "/library/sections/1?agent=tv.plex.agents.movie&locations=%2Fdoes-not-exist",
        Body::empty(),
    )
    .await;
    assert_eq!(invalid_location.status(), StatusCode::BAD_REQUEST);

    let unknown_section = send(
        &fixture.app,
        Method::PUT,
        &format!(
            "/library/sections/99?agent=tv.plex.agents.movie&locations={}",
            added.display()
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(unknown_section.status(), StatusCode::NOT_FOUND);

    let tv_fixture = tv_fixture();
    let tv_folder = tv_fixture.root.join("more-tv");
    std::fs::create_dir_all(&tv_folder).unwrap();
    let tv_folder = tokio::fs::canonicalize(tv_folder).await.unwrap();
    let response = send(
        &tv_fixture.app,
        Method::PUT,
        &format!(
            "/library/sections/1?agent=tv.plex.agents.series&locations={}",
            tv_folder.display()
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        tv_fixture.config.lock().unwrap().libraries[0].folders.len(),
        2
    );
}

#[tokio::test]
async fn plex_section_put_updates_optional_section_fields_without_locations() {
    let fixture = fixture();
    let response = send(
        &fixture.app,
        Method::PUT,
        "/library/sections/1?agent=tv.plex.agents.movie&name=Renamed&scanner=Plex%20Video%20Files&language=fr&metadataAgentProviderGroupId=77&prefs%5Bhidden%5D=2",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    {
        let config = fixture.config.lock().unwrap();
        let library = &config.libraries[0];
        assert_eq!(library.name, "Renamed");
        assert_eq!(library.plex_agent.as_deref(), Some("tv.plex.agents.movie"));
        assert_eq!(library.plex_scanner.as_deref(), Some("Plex Video Files"));
        assert_eq!(library.plex_language.as_deref(), Some("fr"));
        assert_eq!(library.metadata_agent_provider_group_id, Some(77));
        assert_eq!(
            library.plex_preferences.as_ref().unwrap().get("hidden"),
            Some(&"2".to_owned())
        );
        assert_eq!(library.folders.len(), 1);
    }

    let connection = fixture.db.lock().unwrap();
    let renamed_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM library_groups WHERE name = 'Renamed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(renamed_count, 1);
    drop(connection);

    let preferences = send(
        &fixture.app,
        Method::GET,
        "/library/sections/1/prefs",
        Body::empty(),
    )
    .await;
    let preferences: serde_json::Value =
        serde_json::from_slice(&to_bytes(preferences.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let hidden = preferences["MediaContainer"]["Setting"]
        .as_array()
        .unwrap()
        .iter()
        .find(|setting| setting["id"] == "hidden")
        .unwrap();
    assert_eq!(hidden["value"], "2");

    let xml_response = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/library/sections/1?agent=tv.plex.agents.movie")
                .header(header::ACCEPT, "application/xml")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(xml_response.status(), StatusCode::OK);
    assert_eq!(
        xml_response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/xml"
    );
    let xml = String::from_utf8(
        to_bytes(xml_response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(xml.contains("<Directory"));
    assert!(xml.contains("title=\"Renamed\""));
}

#[tokio::test]
async fn plex_desktop_bootstrap_endpoints_return_compatible_containers() {
    let fixture = fixture();

    let sections = send(
        &fixture.app,
        Method::GET,
        "/library/sections",
        Body::empty(),
    )
    .await;
    assert_eq!(sections.status(), StatusCode::OK);
    let sections: serde_json::Value =
        serde_json::from_slice(&to_bytes(sections.into_body(), usize::MAX).await.unwrap()).unwrap();
    let container = &sections["MediaContainer"];
    assert_eq!(container["size"], 1);
    assert_eq!(container["allowSync"], false);
    assert_eq!(container["title1"], "Plex Library");
    let directory = &container["Directory"][0];
    assert_eq!(directory["allowSync"], true);
    assert_eq!(directory["filters"], true);
    assert_eq!(directory["key"], "1");
    assert_eq!(directory["type"], "movie");
    assert_eq!(directory["metadataAgentProviderGroupId"], 1);
    assert!(directory["createdAt"].as_u64().unwrap() > 0);
    assert_eq!(
        directory["contentChangedAt"],
        super::library_catalog_updated_at(&fixture.state)
    );
    assert_eq!(directory["content"], true);
    assert_eq!(directory["directory"], true);
    assert_eq!(directory["hidden"], 0);
    assert_eq!(directory["Location"][0]["id"], 1);
    assert_eq!(
        directory["Location"][0]["path"],
        fixture.root.join("media").to_str().unwrap()
    );

    let tv_fixture = tv_fixture();
    let sections = send(
        &tv_fixture.app,
        Method::GET,
        "/library/sections",
        Body::empty(),
    )
    .await;
    let sections: serde_json::Value =
        serde_json::from_slice(&to_bytes(sections.into_body(), usize::MAX).await.unwrap()).unwrap();
    let directory = &sections["MediaContainer"]["Directory"][0];
    assert_eq!(directory["type"], "show");
    assert_eq!(directory["metadataAgentProviderGroupId"], 2);
    assert_eq!(
        directory["Location"][0]["path"],
        tv_fixture.root.join("media").to_str().unwrap()
    );

    let movie_scanners = send(
        &fixture.app,
        Method::GET,
        "/system/scanners/1?action=update",
        Body::empty(),
    )
    .await;
    assert_eq!(movie_scanners.status(), StatusCode::OK);
    assert_eq!(
        movie_scanners.headers()[header::CONTENT_TYPE],
        "application/xml"
    );
    let movie_scanners = String::from_utf8(
        to_bytes(movie_scanners.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(
        movie_scanners,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><MediaContainer><Scanner name=\"Plex Movie\" /><Scanner name=\"Plex Video Files\" /></MediaContainer>"
    );

    let tv_scanners = send(
        &tv_fixture.app,
        Method::GET,
        "/system/scanners/1?action=update",
        Body::empty(),
    )
    .await;
    let tv_scanners = String::from_utf8(
        to_bytes(tv_scanners.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(tv_scanners.contains("<Scanner name=\"Plex TV Series\" />"));
    assert!(tv_scanners.contains("<Scanner name=\"Plex Series Scanner\" />"));
    assert_eq!(
        send(
            &fixture.app,
            Method::GET,
            "/system/scanners/99?action=update",
            Body::empty(),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );

    let devices = send(&fixture.app, Method::GET, "/devices", Body::empty()).await;
    assert_eq!(devices.status(), StatusCode::OK);
    let devices: serde_json::Value =
        serde_json::from_slice(&to_bytes(devices.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(devices["MediaContainer"]["size"], 0);
    assert_eq!(devices["MediaContainer"]["Device"], serde_json::json!([]));

    let history = send(
        &fixture.app,
        Method::GET,
        "/status/sessions/history/all",
        Body::empty(),
    )
    .await;
    assert_eq!(history.status(), StatusCode::OK);
    let history: serde_json::Value =
        serde_json::from_slice(&to_bytes(history.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(history["MediaContainer"]["size"], 0);
    assert_eq!(history["MediaContainer"]["totalSize"], 0);
    assert_eq!(history["MediaContainer"]["offset"], 0);
    assert_eq!(history["MediaContainer"]["Metadata"], serde_json::json!([]));

    let accounts = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/accounts")
                .header(header::ACCEPT, "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accounts.status(), StatusCode::OK);
    let accounts: serde_json::Value =
        serde_json::from_slice(&to_bytes(accounts.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(accounts["MediaContainer"]["Account"][0]["id"], 1);
}

#[tokio::test]
async fn static_playlists_support_crud_and_item_management() {
    let fixture = fixture();
    let created = send(
        &fixture.app,
        Method::POST,
        "/playlists?title=Favorites&playlistType=video&smart=0",
        Body::empty(),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let created: serde_json::Value =
        serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap()).unwrap();
    let playlist_id = created["MediaContainer"]["Metadata"][0]["ratingKey"]
        .as_str()
        .unwrap();
    assert_eq!(
        created["MediaContainer"]["Metadata"][0]["playlistType"],
        "video"
    );
    assert_eq!(created["MediaContainer"]["Metadata"][0]["smart"], false);

    let listed = send(
        &fixture.app,
        Method::GET,
        "/playlists?playlistType=video",
        Body::empty(),
    )
    .await;
    assert_eq!(listed.status(), StatusCode::OK);
    let listed: serde_json::Value =
        serde_json::from_slice(&to_bytes(listed.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(listed["MediaContainer"]["size"], 1);

    let updated = send(
        &fixture.app,
        Method::PUT,
        &format!("/playlists/{playlist_id}?title=Renamed"),
        Body::empty(),
    )
    .await;
    assert_eq!(updated.status(), StatusCode::OK);

    let added = send(
        &fixture.app,
        Method::PUT,
        &format!(
            "/playlists/{playlist_id}/items?uri=server%3A%2F%2Fmplex-test%2Fcom.plexapp.plugins.library%2Flibrary%2Fmetadata%2F1-1"
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(added.status(), StatusCode::OK);

    let items = send(
        &fixture.app,
        Method::GET,
        &format!("/playlists/{playlist_id}/items"),
        Body::empty(),
    )
    .await;
    assert_eq!(items.status(), StatusCode::OK);
    let items: serde_json::Value =
        serde_json::from_slice(&to_bytes(items.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(items["MediaContainer"]["size"], 1);
    assert_eq!(items["MediaContainer"]["Metadata"][0]["title"], "Demo");
    let item_id = items["MediaContainer"]["Metadata"][0]["playlistItemID"]
        .as_i64()
        .unwrap();

    let moved = send(
        &fixture.app,
        Method::PUT,
        &format!("/playlists/{playlist_id}/items/{item_id}/move?after=-1"),
        Body::empty(),
    )
    .await;
    assert_eq!(moved.status(), StatusCode::NO_CONTENT);

    let removed = send(
        &fixture.app,
        Method::DELETE,
        &format!("/playlists/{playlist_id}/items/{item_id}"),
        Body::empty(),
    )
    .await;
    assert_eq!(removed.status(), StatusCode::NO_CONTENT);
    let cleared = send(
        &fixture.app,
        Method::DELETE,
        &format!("/playlists/{playlist_id}/items"),
        Body::empty(),
    )
    .await;
    assert_eq!(cleared.status(), StatusCode::NO_CONTENT);

    let deleted = send(
        &fixture.app,
        Method::DELETE,
        &format!("/playlists/{playlist_id}"),
        Body::empty(),
    )
    .await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    let missing = send(
        &fixture.app,
        Method::GET,
        &format!("/playlists/{playlist_id}"),
        Body::empty(),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    let invalid_smart = send(
        &fixture.app,
        Method::POST,
        "/playlists?title=Smart&playlistType=video&smart=1",
        Body::empty(),
    )
    .await;
    assert_eq!(invalid_smart.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn static_collections_support_listing_and_item_management() {
    let fixture = fixture();
    let created = send(
        &fixture.app,
        Method::POST,
        "/library/collections?title=Favorites&sectionId=1&smart=0",
        Body::empty(),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let created: serde_json::Value =
        serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap()).unwrap();
    let collection_id = created["MediaContainer"]["Metadata"][0]["ratingKey"]
        .as_str()
        .unwrap();

    let listed = send(
        &fixture.app,
        Method::GET,
        "/library/collections?sectionId=1",
        Body::empty(),
    )
    .await;
    assert_eq!(listed.status(), StatusCode::OK);
    let listed: serde_json::Value =
        serde_json::from_slice(&to_bytes(listed.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(listed["MediaContainer"]["size"], 1);

    let section_collections = send(
        &fixture.app,
        Method::GET,
        "/library/sections/1/collections",
        Body::empty(),
    )
    .await;
    assert_eq!(section_collections.status(), StatusCode::OK);
    let section_collections: serde_json::Value = serde_json::from_slice(
        &to_bytes(section_collections.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        section_collections["MediaContainer"]["Metadata"][0]["title"],
        "Favorites"
    );

    let added = send(
        &fixture.app,
        Method::PUT,
        &format!(
            "/library/collections/{collection_id}/items?uri=server%3A%2F%2Fmplex-test%2Fcom.plexapp.plugins.library%2Flibrary%2Fmetadata%2F1-1"
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(added.status(), StatusCode::OK);

    let duplicate = send(
        &fixture.app,
        Method::PUT,
        &format!(
            "/library/collections/{collection_id}/items?uri=server%3A%2F%2Fmplex-test%2Fcom.plexapp.plugins.library%2Flibrary%2Fmetadata%2F1-1"
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(duplicate.status(), StatusCode::OK);

    let items = send(
        &fixture.app,
        Method::GET,
        &format!("/library/collections/{collection_id}/items"),
        Body::empty(),
    )
    .await;
    assert_eq!(items.status(), StatusCode::OK);
    let items: serde_json::Value =
        serde_json::from_slice(&to_bytes(items.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(items["MediaContainer"]["size"], 1);
    assert_eq!(items["MediaContainer"]["Metadata"][0]["title"], "Demo");
    let media_rating_key = items["MediaContainer"]["Metadata"][0]["ratingKey"]
        .as_str()
        .unwrap();

    let removed = send(
        &fixture.app,
        Method::PUT,
        &format!("/library/collections/{collection_id}/items/{media_rating_key}"),
        Body::empty(),
    )
    .await;
    assert_eq!(removed.status(), StatusCode::OK);

    let missing_item = send(
        &fixture.app,
        Method::PUT,
        &format!("/library/collections/{collection_id}/items/{media_rating_key}"),
        Body::empty(),
    )
    .await;
    assert_eq!(missing_item.status(), StatusCode::NOT_FOUND);
    let items = send(
        &fixture.app,
        Method::GET,
        &format!("/library/collections/{collection_id}/items"),
        Body::empty(),
    )
    .await;
    let items: serde_json::Value =
        serde_json::from_slice(&to_bytes(items.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(items["MediaContainer"]["Metadata"], serde_json::json!([]));

    let smart = send(
        &fixture.app,
        Method::POST,
        "/library/collections?title=Smart&sectionId=1&smart=1",
        Body::empty(),
    )
    .await;
    assert_eq!(smart.status(), StatusCode::BAD_REQUEST);
    let missing_section = send(
        &fixture.app,
        Method::POST,
        "/library/collections?title=Missing&sectionId=2",
        Body::empty(),
    )
    .await;
    assert_eq!(missing_section.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn plex_library_all_lists_filtered_and_paged_media() {
    let fixture = fixture();

    let response = send(&fixture.app, Method::GET, "/library/all", Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let response: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(response["MediaContainer"]["size"], 1);
    assert_eq!(response["MediaContainer"]["totalSize"], 1);
    assert_eq!(response["MediaContainer"]["Metadata"][0]["type"], "movie");
    assert_eq!(response["MediaContainer"]["Metadata"][0]["title"], "Demo");

    let filtered = send(
        &fixture.app,
        Method::GET,
        "/library/all?type=1&title==Demo",
        Body::empty(),
    )
    .await;
    let filtered: serde_json::Value =
        serde_json::from_slice(&to_bytes(filtered.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(filtered["MediaContainer"]["size"], 1);

    let episodes = send(
        &fixture.app,
        Method::GET,
        "/library/all?type=4",
        Body::empty(),
    )
    .await;
    let episodes: serde_json::Value =
        serde_json::from_slice(&to_bytes(episodes.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(episodes["MediaContainer"]["size"], 0);

    let page = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/library/all")
                .header("X-Plex-Container-Start", "1")
                .header("X-Plex-Container-Size", "1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let page: serde_json::Value =
        serde_json::from_slice(&to_bytes(page.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(page["MediaContainer"]["size"], 0);
    assert_eq!(page["MediaContainer"]["offset"], 1);
    assert_eq!(page["MediaContainer"]["totalSize"], 1);

    let tv_fixture = tv_fixture();
    for (uri, expected_type, expected_count) in [
        ("/library/all", "show", 1),
        ("/library/all?type=3", "season", 1),
        ("/library/all?type=4", "episode", 2),
    ] {
        let response = send(&tv_fixture.app, Method::GET, uri, Body::empty()).await;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let response: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let items = response["MediaContainer"]["Metadata"].as_array().unwrap();
        assert_eq!(items.len(), expected_count, "{uri}");
        assert_eq!(items[0]["type"], expected_type, "{uri}");
    }
}

#[tokio::test]
async fn every_official_pms_openapi_operation_has_a_real_router_test() {
    let inventory: PlexApiInventory =
        serde_json::from_str(include_str!("../plex_pms_api_inventory.json"))
            .expect("checked-in PMS API inventory must be valid JSON");
    assert_eq!(inventory.spec_version, "1.2.3");
    assert_eq!(inventory.operations.len(), inventory.operation_count);
    assert_eq!(inventory.operation_count, 261);

    let fixture = fixture();
    let delete_probe = format!(
        "/library/sections?name=Delete%20Probe&type=movie&agent=tv.plex.agents.none&location={}",
        fixture.root.join("media").display()
    );
    let response = send(&fixture.app, Method::POST, &delete_probe, Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK);
    // `expected_status` is the checked-in baseline, including expected
    // 404/405 results for documented operations not implemented yet.
    // When an operation is implemented, update its status alongside
    // request/response assertions rather than masking the old gap.
    let initial_queue = send(
        &fixture.app,
        Method::POST,
        "/playQueues?type=video&uri=%2Flibrary%2Fmetadata%2F1-1&key=%2Flibrary%2Fmetadata%2F1-1",
        Body::empty(),
    )
    .await;
    assert_eq!(initial_queue.status(), StatusCode::OK);
    for operation in &inventory.operations {
        let method = Method::from_bytes(operation.method.as_bytes())
            .unwrap_or_else(|_| panic!("invalid HTTP method: {}", operation.method));
        let mut uri = inventory_request_path(&operation.path, &fixture);
        match operation.operation_id.as_str() {
            "playQueuePostSlash" => uri.push_str(
                "?type=video&uri=%2Flibrary%2Fmetadata%2F1-1&key=%2Flibrary%2Fmetadata%2F1-1",
            ),
            "transcodeStart" => uri.push_str("?path=%2Flibrary%2Fmetadata%2F1-1"),
            "transcodeDecision" => uri.push_str("?ratingKey=1-1"),
            "imageTranscode" => uri.push_str("?url=%2Flibrary%2Fmetadata%2F1-1%2Fthumb"),
            "libraryPostSection" => uri.push_str(&format!(
                "?name=OpenAPI%20Section&type=1&agent=tv.plex.agents.none&language=en-US&locations={}",
                fixture.root.join("media").display()
            )),
            "libraryDeleteSection" => uri = "/library/sections/2".to_owned(),
            "libraryGetSectionsPrefs" => {
                uri.push_str("?type=1&agent=tv.plex.agents.none")
            },
            "librarySectionPutPrefs" => uri.push_str("?hidden=0"),

            "librarySectionPutSection" => uri.push_str(&format!(
                "?agent=tv.plex.agents.movie&locations={}",
                fixture.root.join("media").display()
            )),

            _ => {}
        }
        let request = Request::builder()
            .method(method.clone())
            .uri(&uri)
            .header(header::ACCEPT, "application/json")
            .header("X-Plex-Product", "mplex-openapi-tests")
            .header("X-Plex-Client-Identifier", "mplex-openapi-tests")
            .body(Body::empty())
            .unwrap_or_else(|error| {
                panic!("could not build {} {} test request: {error}", method, uri)
            });
        let response = fixture
            .app
            .clone()
            .oneshot(request)
            .await
            .unwrap_or_else(|error| panic!("router failed for {} {}: {error}", method, uri));
        let status = response.status();
        eprintln!(
            "PMS_OPERATION_STATUS\t{}\t{}\t{}\t{}\t{}",
            operation.operation_id,
            operation.method,
            uri,
            status.as_u16(),
            operation.summary
        );
        assert_eq!(
            status.as_u16(),
            operation.expected_status,
            "{} {} ({}) status changed",
            method,
            uri,
            operation.operation_id
        );
    }
}

#[tokio::test]
async fn plex_route_errors_are_explicit_for_unknown_resources_and_bad_requests() {
    let fixture = fixture();
    let failures = [
        (Method::GET, "/accounts/2", StatusCode::NOT_FOUND),
        (
            Method::GET,
            "/hubs/sections/1/not-a-hub",
            StatusCode::NOT_FOUND,
        ),
        (Method::GET, "/library/sections/99", StatusCode::NOT_FOUND),
        (
            Method::GET,
            "/library/sections/99/refresh",
            StatusCode::NOT_FOUND,
        ),
        (
            Method::GET,
            "/library/sections/0/refresh",
            StatusCode::NOT_FOUND,
        ),
        (
            Method::GET,
            "/library/sections/99/collections",
            StatusCode::NOT_FOUND,
        ),
        (
            Method::GET,
            "/library/sections/99/prefs",
            StatusCode::NOT_FOUND,
        ),
        (
            Method::GET,
            "/library/sections/99/sorts",
            StatusCode::NOT_FOUND,
        ),
        (
            Method::GET,
            "/library/sections/99/filters",
            StatusCode::NOT_FOUND,
        ),
        (
            Method::GET,
            "/library/sections/99/firstCharacter",
            StatusCode::NOT_FOUND,
        ),
        (
            Method::GET,
            "/library/sections/99/firstCharacter/A",
            StatusCode::NOT_FOUND,
        ),
        (
            Method::GET,
            "/library/sections/99/firstCharacters",
            StatusCode::NOT_FOUND,
        ),
        (Method::GET, "/library/search", StatusCode::BAD_REQUEST),
        (Method::GET, "/hubs/search", StatusCode::BAD_REQUEST),
        (Method::GET, "/hubs/items", StatusCode::BAD_REQUEST),
        (
            Method::GET,
            "/hubs/items?identifier=movie.recommended.99",
            StatusCode::NOT_FOUND,
        ),
        (
            Method::GET,
            "/library/metadata/missing/related",
            StatusCode::BAD_REQUEST,
        ),
        (
            Method::GET,
            "/library/metadata/missing/extras",
            StatusCode::NOT_FOUND,
        ),
        (
            Method::GET,
            "/library/metadata/missing/media/1",
            StatusCode::NOT_FOUND,
        ),
        (Method::PUT, "/library/parts/missing", StatusCode::NOT_FOUND),
        (Method::GET, "/playQueues/999", StatusCode::NOT_FOUND),
        (
            Method::GET,
            "/playQueues/not-a-number",
            StatusCode::BAD_REQUEST,
        ),
        (
            Method::GET,
            "/library/metadata/missing/thumb",
            StatusCode::NOT_FOUND,
        ),
        (
            Method::GET,
            "/library/metadata/missing/art",
            StatusCode::NOT_FOUND,
        ),
        (Method::GET, "/media/missing", StatusCode::NOT_FOUND),
        (Method::GET, "/photo/:/transcode", StatusCode::BAD_REQUEST),
        (
            Method::GET,
            "/video/:/transcode/universal/decision",
            StatusCode::BAD_REQUEST,
        ),
        (Method::GET, "/:/timeline", StatusCode::BAD_REQUEST),
        (Method::GET, "/:/scrobble", StatusCode::BAD_REQUEST),
        (Method::GET, "/:/unscrobble", StatusCode::BAD_REQUEST),
        (Method::POST, "/identity", StatusCode::METHOD_NOT_ALLOWED),
    ];

    for (method, uri, expected) in failures {
        let response = send(&fixture.app, method.clone(), uri, Body::empty()).await;
        assert_eq!(
            response.status(),
            expected,
            "{method} {uri} returned an unexpected failure response"
        );
    }
}
