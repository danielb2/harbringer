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
        media: Arc::new(vec![media]),
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
        media: Arc::new(media),
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
    assert!(body.contains("data-watch-action=\"remove_from_continue\""));
    assert!(body.contains("watchAction: action"));
    assert!(body.contains(">0%</span>"));
    assert!(!body.contains("<video class=\"folder-preview-video\""));
    assert!(!body.contains("<span class=\"folder-preview-icon\""));
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
    };
    let movie_card = super::media_card(
        &movie,
        &movie_config,
        Some(&movie_metadata),
        Path::new("Films"),
        std::path::Path::new("Films"),
        std::path::Path::new("Films/Demo Movie.mkv"),
        0,
    );
    assert!(movie_card.contains("/library/metadata/1-42/thumb"));
    assert!(movie_card.contains("path=Films&amp;play=Films%2FDemo%20Movie.mkv"));
    assert!(movie_card.contains("Poster for Demo Movie"));
    assert!(
        movie_card.contains("class=\"media-card-title\" title=\"Demo Movie\">Demo Movie</span>")
    );
    assert!(movie_card.contains("class=\"media-card-subtitle\">2024</span>"));
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
    );
    assert!(
        tv_card.contains("class=\"media-card-title\" title=\"Example Show\">Example Show</span>")
    );
    assert!(tv_card.contains("class=\"media-card-details\">S2 · E4</span>"));
    assert!(tv_card.contains("class=\"media-card media-card-tv\""));
    assert!(tv_card.contains("class=\"media-card-progress\""));

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
    let numeric_key = rating_key.parse::<u64>().unwrap();
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
        numeric_key
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
        &format!("/video/:/transcode/universal/decision?ratingKey=1-1"),
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
    let passthrough = send(
        &fixture.app,
        Method::GET,
        "/video/:/transcode/universal/start.m3u8?path=%2Flibrary%2Fmetadata%2F1-1",
        Body::empty(),
    )
    .await;
    assert_eq!(passthrough.status(), StatusCode::OK);
    assert_eq!(
        passthrough.headers()[header::CONTENT_TYPE],
        "video/x-matroska"
    );
    assert_eq!(
        to_bytes(passthrough.into_body(), usize::MAX).await.unwrap(),
        &b"0123456789abcdef"[..]
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
        (Method::GET, "/accounts/1"),
        (Method::GET, "/activities"),
        (Method::GET, "/status/sessions"),
        (Method::GET, "/servers"),
        (Method::GET, "/media/providers"),
        (Method::GET, "/security/resources"),
        (Method::GET, "/hubs"),
        (Method::GET, "/hubs/promoted"),
        (Method::GET, "/hubs/search?query=Demo"),
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
        assert_eq!(
            response.status(),
            StatusCode::OK,
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
