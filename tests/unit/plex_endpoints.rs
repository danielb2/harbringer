use super::{
    build_app, migrate_library_model, open_database, plex_external_subtitle_id, plex_part_id,
    sync_config_libraries, AppState, Config, ConfigLibrary, Media, PlexTls,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tower::ServiceExt;

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
    };
    let config = Config {
        bind: "127.0.0.1:0".to_owned(),
        libraries: vec![library],
        metadata_storage: "sidecar".to_owned(),
        tmdb_bearer_token: None,
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
    let state = AppState {
        config: shared_config.clone(),
        config_path: Arc::new(config_path),
        db: shared_db.clone(),
        server_name: Arc::new(super::RwLock::new("harbringer".to_owned())),
        machine_identifier: Arc::from(machine_identifier),
        root: Arc::new(media_dir),
        media: Arc::new(super::RwLock::new(Arc::new(vec![media]))),
        inventory_scan_lock: Arc::new(tokio::sync::Mutex::new(())),
        media_info: Arc::new(Mutex::new(HashMap::new())),
        plex_stream_selections: Arc::new(Mutex::new(HashMap::new())),
        plex_play_queues: Arc::new(Mutex::new(HashMap::new())),
        next_plex_play_queue_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        plex_tls: PlexTls::load(root.join("config/certs")).unwrap(),
    };
    Fixture {
        app: build_app(state),
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
        }],
        metadata_storage: "sidecar".to_owned(),
        tmdb_bearer_token: None,
    };
    sync_config_libraries(&db, &config).unwrap();
    let part_id = plex_part_id(&media[0]);
    let change_stamp = super::media_modified_at(&media[0]);
    let shared_config = Arc::new(Mutex::new(config));
    let shared_db = Arc::new(Mutex::new(db));
    let state = AppState {
        config: shared_config.clone(),
        config_path: Arc::new(config_path),
        db: shared_db.clone(),
        server_name: Arc::new(super::RwLock::new("harbringer".to_owned())),
        machine_identifier: Arc::from(machine_identifier),
        root: Arc::new(media_dir),
        media: Arc::new(super::RwLock::new(Arc::new(media))),
        inventory_scan_lock: Arc::new(tokio::sync::Mutex::new(())),
        media_info: Arc::new(Mutex::new(HashMap::new())),
        plex_stream_selections: Arc::new(Mutex::new(HashMap::new())),
        plex_play_queues: Arc::new(Mutex::new(HashMap::new())),
        next_plex_play_queue_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        plex_tls: PlexTls::load(root.join("config/certs")).unwrap(),
    };
    Fixture {
        app: build_app(state),
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
async fn settings_saves_and_validates_per_library_scan_schedule() {
    let fixture = fixture();
    let page = send(&fixture.app, Method::GET, "/settings", Body::empty()).await;
    let page = to_bytes(page.into_body(), usize::MAX).await.unwrap();
    let page = String::from_utf8(page.to_vec()).unwrap();
    assert!(page.contains("name=\"scan_interval_value\" min=\"1\" value=\"5\""));
    assert!(page.contains("<option value=\"minutes\" selected>minutes</option>"));

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
async fn manual_scan_updates_web_and_plex_inventory_without_changing_existing_ids() {
    let fixture = fixture();
    let media_dir = fixture.root.join("media");
    std::fs::write(media_dir.join("Added.mp4"), b"new media").unwrap();
    std::fs::write(media_dir.join("notes.txt"), b"not a video").unwrap();

    let scanned = send(&fixture.app, Method::POST, "/settings/scan", Body::empty()).await;
    assert_eq!(scanned.status(), StatusCode::SEE_OTHER);
    let location = scanned.headers()[header::LOCATION].to_str().unwrap();
    assert!(location.contains("inventory_scan=2"));

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
    assert_eq!(items.len(), 2);
    assert!(items.iter().any(|item| item["title"] == "Added"));
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
        "/:/timeline?ratingKey=1-1&time=4000&duration=10000",
        Body::empty(),
    )
    .await;
    assert_eq!(timeline.status(), StatusCode::OK);

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
    assert_eq!(progress["positionMs"], 4000);
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
        "data-stream-id=\"{}\" selected",
        fixture.subtitle_id
    )));

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
        .join("media/Example Show/Season 01/.mplex/info.toml");
    std::fs::create_dir_all(season_info.parent().unwrap()).unwrap();
    std::fs::write(
        &season_info,
        "title = \"Season One\"\ndescription = \"A season description\"\n",
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
        "class=\"media-card-placeholder\" role=\"img\" aria-label=\"No poster for New\""
    ));
    assert!(!body.contains("tv-episode-card"));
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
    assert!(!body.contains("class=\"metadata-panel\""));
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
        (Method::GET, "/hubs/metadata/1-1/related"),
        (Method::GET, "/hubs/sections/1/recommended"),
        (Method::GET, "/library/sections"),
        (Method::GET, "/library/sections/1"),
        (Method::GET, "/library/sections/1/collections"),
        (Method::GET, "/library/sections/1/prefs"),
        (Method::PUT, "/library/sections/1/prefs"),
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
        let expected_status = if method == Method::POST
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
        let (failure_uri, failure_status) = if uri.starts_with("/media/") {
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
async fn plex_desktop_bootstrap_endpoints_return_compatible_containers() {
    let fixture = fixture();

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

    let accounts = send(&fixture.app, Method::GET, "/accounts", Body::empty()).await;
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
    assert_eq!(inventory.operation_count, 260);

    let fixture = fixture();
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
