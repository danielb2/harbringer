use super::{send, to_bytes, tv_fixture, Body, Fixture, Method, StatusCode};

async fn play_recommended_show(fixture: &Fixture) -> (String, String) {
    let response = send(
        &fixture.app,
        Method::GET,
        "/hubs/items?identifier=show.recommended.1",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let recommended: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let show = &recommended["MediaContainer"]["Metadata"][0];
    assert_eq!(show["type"], "show");
    let show_rating_key = show["ratingKey"].as_str().unwrap().to_owned();

    let response = send(
        &fixture.app,
        Method::POST,
        &format!(
            "/playQueues?type=video&uri=%2Flibrary%2Fmetadata%2F{show_rating_key}&key=%2Flibrary%2Fmetadata%2F{show_rating_key}"
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let queue: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let episode = &queue["MediaContainer"]["Metadata"][0];
    assert_eq!(episode["type"], "episode");
    let episode_rating_key = episode["ratingKey"].as_str().unwrap().to_owned();
    (show_rating_key, episode_rating_key)
}

#[tokio::test]
async fn tv_section_keeps_continue_watching_and_recommended_hubs_together() {
    let fixture = tv_fixture();
    let response = send(&fixture.app, Method::GET, "/hubs/sections/1", Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let section: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let hubs = section["MediaContainer"]["Hub"].as_array().unwrap();
    let continue_hub = hubs
        .iter()
        .find(|hub| hub["title"] == "Continue Watching")
        .unwrap();
    assert_eq!(
        continue_hub["key"],
        "/hubs/sections/1/continueWatching/items"
    );
    assert!(hubs
        .iter()
        .any(|hub| hub["title"] == "Recommended in Shows"));
}

#[tokio::test]
async fn recommended_show_play_uses_a_real_episode_rating_key() {
    let fixture = tv_fixture();
    let (show_rating_key, episode_rating_key) = play_recommended_show(&fixture).await;
    assert_ne!(show_rating_key, episode_rating_key);
}

#[tokio::test]
async fn episode_timeline_is_available_from_continue_watching_items_route() {
    let fixture = tv_fixture();
    let (_, episode_rating_key) = play_recommended_show(&fixture).await;
    let response = send(
        &fixture.app,
        Method::GET,
        &format!(
            "/:/timeline?ratingKey={episode_rating_key}&time=4000&duration=10000&state=playing"
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = send(
        &fixture.app,
        Method::GET,
        "/hubs/sections/1/continueWatching/items?X-Plex-Container-Start=0&X-Plex-Container-Size=7",
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let items: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let container = &items["MediaContainer"];
    assert_eq!(container["size"], 1);
    assert_eq!(container["totalSize"], 1);
    assert_eq!(container["offset"], 0);
    assert!(container.get("Hub").is_none());
    assert_eq!(container["Metadata"][0]["ratingKey"], episode_rating_key);
}
