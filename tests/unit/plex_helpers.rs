use super::{
    apply_plex_technical_metadata, ass_to_vtt, clean_relative_path, contains_hidden_component,
    external_subtitle_content, external_subtitle_language_token, extract_xml_token, html,
    next_tv_episode_paths, ordered_continue_paths, parse_range, parse_sidecar_metadata,
    parse_tv_episode_marker, parse_tv_season_directory, plex_children_json, plex_codec,
    plex_connectivity_url, plex_device_announce_query, plex_device_announce_url,
    plex_direct_play_decision_response, plex_empty_extras_response, plex_external_subtitle_id,
    plex_genre_tokens, plex_hub_categories, plex_language, plex_library_children_container,
    plex_library_media_container, plex_library_provider_features, plex_mark_direct_play,
    plex_media_id, plex_paged_children, plex_part_id, plex_photo_source, plex_play_queue_response,
    plex_play_queue_source_uri, plex_presence_url, plex_publish_urls, plex_query_media_id,
    plex_queue_item_without_playback_decision, plex_ranked_tv_shows, plex_rating_key,
    plex_rating_key_from_source, plex_request_token, plex_safe_numeric_id, plex_search_hubs,
    plex_search_rank, plex_search_result_entries, plex_search_type_matches, plex_stream_id,
    plex_stream_id_and_format, plex_track_label, plex_tv_episode_parents, plex_tv_season_groups,
    plex_tv_season_json, plex_tv_show_groups, plex_tv_show_json, plex_user_avatar_source,
    plex_video_json, plex_video_resolution, render_database_info_panel,
    render_season_description_panel, render_toml_details, render_tv_show_metadata_details,
    sidebar_nav_with_library_links, sidebar_nav_with_settings, srt_to_vtt, technical_duration_ms,
    technical_tracks, tv_metadata_with_season_details, tv_show_title, vtt_to_srt, ConfigLibrary,
    HeaderMap, HeaderValue, Media, PathBuf, PlexStreamSelection, TvEpisodeInfo, Uri,
};

#[test]
fn plex_request_token_reads_header_or_query_parameter() {
    let headers = HeaderMap::new();
    let uri = "/myplex/claim?X-Plex-Token=query-token"
        .parse::<Uri>()
        .unwrap();
    assert_eq!(plex_request_token(&headers, &uri), Some("query-token"));

    let mut headers = HeaderMap::new();
    headers.insert("X-Plex-Token", HeaderValue::from_static("header-token"));
    assert_eq!(plex_request_token(&headers, &uri), Some("header-token"));
}
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
fn plex_external_stream_urls_accept_client_appended_extensions() {
    assert_eq!(plex_stream_id_and_format("123.srt"), Some((123, "srt")));
    assert_eq!(plex_stream_id_and_format("123.srt.srt"), Some((123, "srt")));
    assert_eq!(plex_stream_id_and_format("123.ass.srt"), Some((123, "srt")));
    assert_eq!(plex_stream_id_and_format("123.srt.exe"), None);
}
#[test]
fn plex_section_hubs_use_recently_added_instead_of_recommended() {
    assert_eq!(
        plex_hub_categories(false, Some("recentlyAdded")),
        vec!["recentlyAdded"]
    );
    assert_eq!(plex_hub_categories(false, None), vec!["recentlyAdded"]);
    assert_eq!(plex_hub_categories(true, None), vec!["recentlyAdded"]);
    assert!(plex_hub_categories(true, Some("recommended")).is_empty());
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
fn plex_user_avatar_sources_accept_only_user_avatar_paths() {
    assert_eq!(
        plex_user_avatar_source("/users/96a9727d29e511ca/avatar"),
        Some("96a9727d29e511ca".to_owned())
    );
    assert_eq!(plex_user_avatar_source("/users/../avatar"), None);
    assert_eq!(plex_user_avatar_source("/users/id/avatar/extra"), None);
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
        metadata_revision: 4_000_000_000,
    };
    let library = ConfigLibrary {
        name: "Movies".to_owned(),
        kind: "movie".to_owned(),
        folders: Vec::new(),
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
    let info = serde_json::json!({"media":{"track":[
        {"@type":"General", "Duration":"120.5", "BitRate":"1800000"},
        {"@type":"Video", "StreamOrder":"0", "Format":"AVC", "Format_Profile":"High@L4.0", "Format_Level":"4.0", "BitRate":"1600000", "DisplayAspectRatio":"1.778", "Width":"1280", "Height":"720"},
        {"@type":"Audio", "StreamOrder":"1", "Format":"AC-3", "Channels":"6", "Language":"eng"},
        {"@type":"Text", "StreamOrder":"2", "Format":"UTF-8", "Language":"rum"}
    ]}});
    let subtitle = PathBuf::from("/media/Film.rum.srt");
    let mut item = plex_video_json(&media, 1, &library, None);
    apply_plex_technical_metadata(&mut item, &media, &info, &[subtitle], None);

    assert_eq!(item["duration"], 120_500);
    assert!(item.get("id").is_none());
    assert_eq!(plex_rating_key(&media), (1 << 20) | 7);
    assert!(plex_rating_key(&media) <= i32::MAX as u64);
    assert_eq!(item["ratingKey"], plex_rating_key(&media).to_string());
    assert_eq!(
        item["key"],
        format!("/library/metadata/{}", plex_rating_key(&media))
    );
    assert_eq!(item["librarySectionKey"], "/library/sections/1");
    assert_eq!(item["Media"][0]["videoResolution"], "720");
    assert_eq!(item["Media"][0]["videoCodec"], "h264");
    assert_eq!(item["Media"][0]["bitrate"], 1800);
    assert_eq!(item["Media"][0]["videoProfile"], "high");
    assert_eq!(item["Media"][0]["videoLevel"], 40);
    assert_eq!(item["Media"][0]["aspectRatio"], 1.778);
    assert_eq!(item["Media"][0]["audioCodec"], "ac3");
    assert_eq!(item["Media"][0]["audioChannels"], 6);
    assert_eq!(item["Media"][0]["id"], plex_media_id(&media));
    assert_ne!(item["Media"][0]["id"], item["Media"][0]["Part"][0]["id"]);
    assert!(plex_media_id(&media) <= (1_u64 << 53) - 1);
    assert_eq!(item["Media"][0]["Part"][0]["id"], plex_part_id(&media));
    assert_eq!(item["Media"][0]["Part"][0]["exists"], true);
    assert_eq!(item["Media"][0]["Part"][0]["accessible"], true);
    assert_eq!(item["Media"][0]["Part"][0]["videoProfile"], "high");
    assert!(plex_part_id(&media) <= (1_u64 << 53) - 1);
    assert!(item["Media"][0]["Part"][0]["key"]
        .as_str()
        .unwrap()
        .ends_with("/file.mkv"));
    let streams = item["Media"][0]["Part"][0]["Stream"].as_array().unwrap();
    assert_eq!(streams.len(), 4);
    assert_eq!(streams[0]["profile"], "high");
    assert_eq!(streams[0]["level"], 40);
    assert_eq!(streams[0]["languageCode"], "und");
    assert_eq!(streams[1]["type"], 2);
    assert_eq!(streams[1]["channels"], 6);
    assert_eq!(streams[1]["selected"], true);
    assert_eq!(streams[2]["language"], "Romanian");
    assert_eq!(streams[2]["languageCode"], "ron");
    assert_eq!(streams[2]["languageTag"], "ro");
    assert_eq!(streams[3]["displayTitle"], "Romanian (SRT External)");
    assert_eq!(streams[3]["language"], "Romanian");
    assert_eq!(streams[3]["languageCode"], "ron");
    assert_eq!(streams[3]["languageTag"], "ro");
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
    assert_eq!(decision_container["transcodeDecisionCode"], 1000);
    assert_eq!(
        decision_container["transcodeDecisionText"],
        "Direct play OK."
    );
    assert_eq!(
        decision_container["Metadata"][0]["Media"][0]["Part"][0]["decision"],
        "directplay"
    );
}

#[test]
fn plex_media_infers_average_bitrate_when_media_info_omits_it() {
    let media = Media {
        id: "1-8".to_owned(),
        title: "Film".to_owned(),
        path: PathBuf::from("/media/Film.mkv"),
        size: 9_000_000,
        container: "mkv".to_owned(),
        mime: "video/x-matroska".to_owned(),
        metadata_revision: 0,
    };
    let library = ConfigLibrary {
        name: "Movies".to_owned(),
        kind: "movie".to_owned(),
        folders: Vec::new(),
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
    let info = serde_json::json!({"media":{"track":[
        {"@type":"General", "Duration":"120.5"},
        {"@type":"Video", "StreamOrder":"0", "Format":"AVC", "Width":"1280", "Height":"720"},
        {"@type":"Audio", "StreamOrder":"1", "Format":"AAC", "Channels":"2"}
    ]}});
    let mut item = plex_video_json(&media, 1, &library, None);

    apply_plex_technical_metadata(&mut item, &media, &info, &[], None);

    assert_eq!(item["Media"][0]["bitrate"], 598);
    assert_eq!(item["Media"][0]["Part"][0]["bitrate"], 598);
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
        metadata_revision: 0,
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
    let json_info = serde_json::json!({
        "title": "Portable match",
        "metadata_json": {"id": 42, "title": "Portable match"}
    })
    .to_string();
    let metadata = parse_sidecar_metadata(&json_info).unwrap();
    assert_eq!(metadata["id"], 42);
    assert_eq!(metadata["title"], "Portable match");

    assert_eq!(parse_sidecar_metadata("metadata_json = 'not JSON'"), None);
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
fn metadata_search_title_removes_years_and_release_tags() {
    assert_eq!(
        super::metadata_search_title("The.Matrix.1999.1080p.BluRay.x264-GROUP"),
        "The Matrix"
    );
    assert_eq!(
        super::metadata_search_title("Stranger_Things_(2016)"),
        "Stranger Things"
    );
}

#[test]
fn metadata_matching_prefers_exact_title_and_year() {
    let results = vec![
        serde_json::json!({"id": 11, "title": "The Thing", "release_date": "2011-06-01"}),
        serde_json::json!({"id": 22, "title": "The Thing", "release_date": "1982-06-25"}),
        serde_json::json!({"id": 33, "title": "Another Film", "release_date": "1982-01-01"}),
    ];
    let (selected, score) = super::best_metadata_result(&results, "The Thing (1982)", "movie")
        .expect("a confident title match should be found");
    assert_eq!(selected["id"], 22);
    assert!(score > 100);
    assert!(super::best_metadata_result(&results, "Unknown Film", "movie").is_none());
}

#[test]
fn tv_metadata_matching_replaces_stale_episode_fields() {
    let root = std::path::PathBuf::from("/library");
    let media = root.join("Example Show/Season 01/Example.Show.S01E03.New.mkv");
    let metadata = serde_json::json!({
        "id": 7,
        "mplex_season": 9,
        "mplex_episode": 99,
        "mplex_episode_end": 100,
        "mplex_episode_title": "Stale title"
    });
    let enriched = super::tv_metadata_for_media(&metadata, &root, &media, "tv");
    assert_eq!(enriched["mplex_season"], 1);
    assert_eq!(enriched["mplex_episode"], 3);
    assert_eq!(enriched["mplex_episode_title"], "New");
}

#[test]
fn missing_poster_markup_uses_a_readable_fallback() {
    let card = super::media_poster_card(
        None,
        "/web?library=0",
        "Missing Artwork",
        None,
        None,
        0.0,
        std::path::Path::new("Missing Show/Episode.mkv"),
        0,
        true,
        true,
        "show",
        false,
    );
    assert!(card.contains("No poster available"));
    assert!(card.contains("Poster unavailable for Missing Artwork"));
    assert!(!card.contains("<img"));
    assert!(super::media_card_action_script().contains("image.replaceWith(placeholder)"));
}

#[test]
fn database_metadata_renders_movie_and_episode_information() {
    let movie = serde_json::json!({
        "title": "A <Film>",
        "overview": "A & good story",
        "release_date": "2024-03-14",
        "vote_average": 8.2
    });
    let movie_panel = render_database_info_panel(&movie, None);
    assert!(movie_panel.contains("A &lt;Film&gt;"));
    assert!(movie_panel.contains("A &amp; good story"));
    assert!(movie_panel.contains("2024-03-14"));
    assert!(movie_panel.contains("8.2"));

    let episode = serde_json::json!({
        "name": "Example Show",
        "overview": "Series summary",
        "mplex_episode_title": "Pilot",
        "mplex_episode_metadata": {
            "name": "Pilot",
            "overview": "Episode summary",
            "air_date": "2025-01-02",
            "runtime": 42
        }
    });
    let episode_info = TvEpisodeInfo {
        season: 1,
        episode: 1,
        episode_end: None,
        episode_title: Some("Pilot".to_owned()),
    };
    let episode_panel = render_database_info_panel(&episode, Some(&episode_info));
    assert!(episode_panel.contains("<h2>Pilot</h2>"));
    assert!(episode_panel.contains("Episode description"));
    assert!(episode_panel.contains("Episode summary"));
    assert!(episode_panel.contains("season</strong><span>1"));
    assert!(!episode_panel.contains("Series summary"));

    let season_fallback = serde_json::json!({
        "name": "Example Show",
        "mplex_show_description": "Show summary",
        "mplex_season_description": "Season summary",
        "mplex_episode_metadata": {"name": "Pilot", "overview": ""}
    });
    let season_panel = render_database_info_panel(&season_fallback, Some(&episode_info));
    assert!(season_panel.contains("Season description"));
    assert!(season_panel.contains("Season summary"));
    assert!(!season_panel.contains("Show summary"));

    let show_fallback = serde_json::json!({
        "name": "Example Show",
        "mplex_show_description": "Show summary",
        "mplex_episode_title": "Pilot",
        "mplex_episode_metadata": {"name": "Pilot", "overview": ""}
    });
    let show_panel = render_database_info_panel(&show_fallback, Some(&episode_info));
    assert!(show_panel.contains("Show description"));
    assert!(show_panel.contains("Show summary"));
    let show_view_metadata = serde_json::json!({
        "name": "Example Show",
        "overview": "Show summary",
        "mplex_season_description": "Season summary",
        "mplex_episode_title": "Pilot"
    });
    let show_view_panel = render_database_info_panel(&show_view_metadata, None);
    assert!(show_view_panel.contains("<h2>Example Show</h2>"));
    assert!(show_view_panel.contains("Show summary"));
    assert!(!show_view_panel.contains("Season summary"));
    assert!(!show_view_panel.contains("<h2>Pilot</h2>"));

    let show_metadata = serde_json::json!({"overview": "Show summary"});
    let empty_season = serde_json::json!({"name": "Season One", "overview": ""});
    let season_panel = render_season_description_panel(&empty_season, 1, Some(&show_metadata));
    assert!(season_panel.contains("Show description"));
    assert!(season_panel.contains("Show summary"));
}

#[test]
fn tv_season_metadata_persists_episode_details_as_json() {
    let show = serde_json::json!({"id": 42, "overview": "Show summary"});
    let season = serde_json::json!({
        "overview": "Season summary",
        "episodes": [
            {"episode_number": 1, "name": "Pilot", "overview": "Pilot summary"},
            {"episode_number": 2, "name": "Next", "overview": "Next summary"}
        ]
    });
    let stored = tv_metadata_with_season_details(&show, &season, Some(2));
    assert_eq!(
        stored["mplex_season_description"],
        serde_json::json!("Season summary")
    );
    assert_eq!(stored["mplex_season_metadata_fetched"], true);
    assert_eq!(stored["mplex_episode_metadata"]["name"], "Next");
    assert_eq!(stored["mplex_episode_metadata"]["overview"], "Next summary");
    assert!(stored["mplex_episode_metadata"].is_object());
}

#[test]
fn overview_details_hide_media_path_and_episode_numbers() {
    let details = "title = \"ThunderCats\"\nmedia_path = \"ThunderCats/season.01/ThunderCats.s01e01.Exodus.mkv\"\nseason = 1\nepisode = 1\nepisode_title = \"Exodus\"\nnext_episode = \"S01E02\"\n\nruntime = 42"
        .parse::<toml::Value>()
        .unwrap();
    let table = details.as_table().unwrap();
    let overview = render_toml_details(table, true);
    assert!(overview.contains("ThunderCats"));
    assert!(overview.contains("42"));
    assert!(overview.contains("next episode</strong><span>S01E02"));
    assert!(!overview.contains("media path"));
    assert!(!overview.contains("season</strong>"));
    assert!(!overview.contains("<strong>episode</strong>"));
    assert!(!overview.contains("Exodus"));
    assert!(!overview.contains("ThunderCats/season.01/ThunderCats.s01e01.Exodus.mkv"));

    let season_view = render_toml_details(table, false);
    assert!(season_view.contains("season</strong><span>1"));
    assert!(season_view.contains("episode</strong><span>1"));
}

#[test]
fn tv_show_details_render_genres_and_content_rating() {
    let metadata = serde_json::json!({
        "genres": [{"name": "Adventure"}, {"name": "Fantasy"}],
        "content_ratings": {"results": [
            {"iso_3166_1": "US", "rating": "TV-PG"}
        ]}
    });
    let rendered = render_tv_show_metadata_details(&metadata);
    assert!(rendered.contains("Adventure, Fantasy"));
    assert!(rendered.contains("TV-PG"));
}

#[test]
fn continue_watching_picks_the_first_unwatched_episode_for_a_show() {
    let root = PathBuf::from("/library/tv");
    let first_path = "Example Show/Season 01/Example.Show.S01E01.Pilot.mkv";
    let second_path = "Example Show/Season 01/Example.Show.S01E02.Next.mkv";
    let media = [first_path, second_path]
        .into_iter()
        .enumerate()
        .map(|(index, relative)| Media {
            id: format!("tv-{index}"),
            title: format!("Episode {index}"),
            path: root.join(relative),
            size: 1,
            container: "mkv".to_owned(),
            mime: "video/x-matroska".to_owned(),
            metadata_revision: 0,
        })
        .collect::<Vec<_>>();
    let library = ConfigLibrary {
        name: "Shows".to_owned(),
        kind: "tv".to_owned(),
        folders: vec![root],
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
    let watched = std::collections::HashSet::from([first_path.to_owned()]);
    let hidden = std::collections::HashSet::new();
    let activity = std::collections::HashMap::from([
        (first_path.to_owned(), 100),
        (second_path.to_owned(), 200),
    ]);
    assert_eq!(
        next_tv_episode_paths(&[library.clone()], &media, &watched, &hidden, &activity),
        vec![(second_path.to_owned(), 200)]
    );

    let watched = std::collections::HashSet::new();
    assert_eq!(
        next_tv_episode_paths(&[library.clone()], &media, &watched, &hidden, &activity),
        vec![(first_path.to_owned(), 200)]
    );

    let hidden = std::collections::HashSet::from([first_path.to_owned(), second_path.to_owned()]);
    assert!(next_tv_episode_paths(&[library], &media, &watched, &hidden, &activity).is_empty());
}

#[test]
fn continue_watching_orders_by_activity_and_deduplicates_paths() {
    assert_eq!(
        ordered_continue_paths(vec![
            ("Later path".to_owned(), 200),
            ("Newest".to_owned(), 300),
            ("Earlier path".to_owned(), 200),
            ("Newest".to_owned(), 100),
        ]),
        vec![
            "Newest".to_owned(),
            "Earlier path".to_owned(),
            "Later path".to_owned(),
        ]
    );
}
#[test]
fn extracts_plex_server_token() {
    assert_eq!(
        extract_xml_token("<user><authentication-token>server-token</authentication-token></user>"),
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
fn sidebar_lists_libraries_directly_without_generic_library_item() {
    let library_links = r#"<a class="sidebar-link sidebar-library active" data-spa-link href="/?library=0"><span class="sidebar-icon">▤</span>Movies</a>"#;
    let sidebar = sidebar_nav_with_library_links(false, false, library_links);
    assert!(sidebar.contains("href=\"/\""));
    assert!(sidebar.contains("href=\"/?library=0\""));
    assert!(!sidebar.contains("href=\"/library\""));
    assert!(!sidebar.contains(">Library</a>"));
    let home = sidebar_nav_with_library_links(true, false, library_links);
    assert!(home.contains("<a class=\"sidebar-link active\" data-spa-link href=\"/\""));
    assert!(sidebar.contains("sidebar-toggle"));
    let settings = sidebar_nav_with_settings(true);
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
            metadata_revision: 0,
        },
        Media {
            id: "3-2".to_owned(),
            title: "Example Show s01e02 Arrival.mkv".to_owned(),
            path: root.join("Example Show/Season 01/Example.Show.s01e02.Arrival.mkv"),
            size: 100,
            container: "mkv".to_owned(),
            mime: "video/x-matroska".to_owned(),
            metadata_revision: 0,
        },
    ];
    let roots = vec![root.clone()];
    assert_eq!(tv_show_title(&root, &media[0].path), "Example Show");
    let mut shows = plex_tv_show_groups(3, &roots, media.iter());
    assert_eq!(shows.len(), 1);
    let show = shows.pop().unwrap();
    assert_eq!(show.title, "Example Show");
    assert_eq!(show.episodes.len(), 2);
    let ranked_shows = plex_ranked_tv_shows(&[&media[1], &media[0]], std::slice::from_ref(&show));
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
        scan_enabled: true,
        scan_interval_value: 5,
        scan_interval_unit: "minutes".to_owned(),
        plex_agent: None,
        plex_scanner: None,
        plex_language: None,
        metadata_agent_provider_group_id: None,
        plex_preferences: None,
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
    let show_guid = show_item["guid"].as_str().unwrap().to_owned();
    assert_eq!(
        show_guid,
        format!("com.plexapp.plugins.library://show/{}", show.rating_key)
    );
    assert!(show_item.get("Media").is_none());

    let season_item = plex_tv_season_json(&show, 1, 3, &library, &metadata, 2);
    assert_eq!(season_item["type"], "season");
    assert_eq!(season_item["parentRatingKey"], show.rating_key);
    assert_eq!(season_item["childCount"], 2);
    assert_eq!(
        season_item["guid"],
        format!(
            "com.plexapp.plugins.library://season/{}-season-1",
            show.rating_key
        )
    );
    assert_eq!(season_item["parentGuid"], show_guid);
    let seasons_container =
        plex_library_children_container(&library, 3, "tv-uuid", 1, 0, 1, "season");
    let episodes_container =
        plex_library_children_container(&library, 3, "tv-uuid", 2, 0, 2, "episode");
    assert_eq!(seasons_container["viewGroup"], "season");
    assert_eq!(episodes_container["viewGroup"], "episode");
    let mut episode_item = plex_video_json(&media[0], 3, &library, None);
    plex_tv_episode_parents(&mut episode_item, &show, 1, 1, &show_guid);
    assert_eq!(episode_item["type"], "episode");
    assert_eq!(
        episode_item["guid"],
        format!(
            "com.plexapp.plugins.library://episode/{}",
            plex_rating_key(&media[0])
        )
    );
    assert_eq!(
        episode_item["parentGuid"],
        format!(
            "com.plexapp.plugins.library://season/{}-season-1",
            show.rating_key
        )
    );
    assert_eq!(episode_item["grandparentGuid"], show_guid);
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
    assert_eq!(
        episode_item["grandparentThumb"],
        format!("/library/metadata/{}/thumb", plex_rating_key(&media[0]))
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
    assert_eq!(
        super::plex_episode_search_rank("Example Show", "Pilot", "Pilot.mkv", Some(0)),
        Some(0)
    );
    assert_eq!(
        super::plex_episode_search_rank("Pilot", "Pilot", "Pilot.mkv", None),
        Some(0)
    );
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
fn plex_provider_advertises_the_universal_playqueue_feature() {
    let features = plex_library_provider_features(Vec::new());
    let playqueue = features
        .iter()
        .find(|feature| feature["type"] == "playqueue")
        .expect("playqueue feature is required for Plex Play controls");
    assert_eq!(playqueue["key"], "/playQueues");
    assert_eq!(playqueue["flavor"], "universal");
}

#[test]
fn plex_provider_advertises_manage_feature_for_library_editing() {
    let features = plex_library_provider_features(Vec::new());
    assert!(features.iter().any(|feature| feature["type"] == "manage"));
}

#[test]
fn plex_provider_advertises_timeline_scrobble_actions() {
    let features = plex_library_provider_features(Vec::new());
    let timeline = features
        .iter()
        .find(|feature| feature["type"] == "timeline")
        .expect("timeline feature is required to expose watch-state actions");
    assert_eq!(timeline["key"], "/:/timeline");
    assert_eq!(timeline["scrobbleKey"], "/:/scrobble");
    assert_eq!(timeline["unscrobbleKey"], "/:/unscrobble");

    let playlist = features
        .iter()
        .find(|feature| feature["type"] == "playlist")
        .expect("implemented static playlists must be advertised");
    assert_eq!(playlist["key"], "/playlists");
    assert_eq!(playlist["flavor"], "universal");

    let collections = features
        .iter()
        .find(|feature| feature["type"] == "collection")
        .expect("implemented static collections must be advertised");
    assert_eq!(collections["key"], "/library/collections");

    let universal_search = features
        .iter()
        .find(|feature| feature["type"] == "universalsearch")
        .expect("the implemented universal search route must be advertised");
    assert_eq!(universal_search["key"], "/library/search");

    let continue_watching = features
        .iter()
        .find(|feature| feature["type"] == "continuewatching")
        .expect("the implemented Continue Watching hub must be advertised");
    assert_eq!(continue_watching["key"], "/hubs/continueWatching");

    let actions = features
        .iter()
        .find(|feature| feature["type"] == "actions")
        .expect("implemented item actions must be advertised");
    assert_eq!(actions["key"], "/actions");
    assert_eq!(
        actions["Action"][0]["key"],
        "/actions/removeFromContinueWatching"
    );
}

#[test]
fn plex_extras_for_known_media_return_an_empty_json_container() {
    let library = ConfigLibrary {
        name: "Movies".to_owned(),
        kind: "movie".to_owned(),
        folders: Vec::new(),
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
    let mut item = serde_json::json!({
        "ratingKey": "142",
        "playQueueItemID": 73,
        "Media": [{
            "id": 51,
            "protocol": "http",
            "decision": "directplay",
            "selected": true,
            "Part": [{
                "protocol": "http",
                "decision": "directplay",
                "selected": true,
                "Stream": [
                    {"streamType": 1, "selected": true, "decision": "directplay"},
                    {"streamType": 2, "selected": true, "decision": "directplay"},
                    {"streamType": 3, "selected": false, "decision": "directplay"},
                    {"streamType": 3, "selected": true, "decision": "directplay"}
                ]
            }]
        }]
    });
    plex_queue_item_without_playback_decision(&mut item);
    let source_uri = plex_play_queue_source_uri("library-id", "142");
    let queue = plex_play_queue_response(9, &source_uri, vec![item], 0, false);
    assert_eq!(queue["MediaContainer"]["playQueueID"], 9);
    assert_eq!(queue["MediaContainer"]["playQueueSelectedItemID"], 73);
    assert_eq!(
        queue["MediaContainer"]["playQueueSelectedMetadataItemID"],
        "142"
    );
    assert_eq!(
        queue["MediaContainer"]["playQueueSourceURI"],
        "library://library-id/item/%2Flibrary%2Fmetadata%2F142"
    );
    assert_eq!(
        queue["MediaContainer"]["identifier"],
        "com.plexapp.plugins.library"
    );
    assert_eq!(
        queue["MediaContainer"]["mediaTagPrefix"],
        "/system/bundle/media/flags/"
    );
    assert!(queue["MediaContainer"]["mediaTagVersion"]
        .as_u64()
        .is_some());
    assert_eq!(queue["MediaContainer"]["playQueueTotalCount"], 1);

    let media = &queue["MediaContainer"]["Metadata"][0]["Media"][0];
    assert_eq!(media["protocol"], serde_json::Value::Null);
    assert_eq!(media["decision"], serde_json::Value::Null);
    assert_eq!(media["selected"], serde_json::Value::Null);
    let part = &media["Part"][0];
    assert_eq!(part["protocol"], serde_json::Value::Null);
    assert_eq!(part["decision"], serde_json::Value::Null);
    assert_eq!(part["selected"], serde_json::Value::Null);
    assert_eq!(part["Stream"][0]["selected"], serde_json::Value::Null);
    assert_eq!(part["Stream"][1]["selected"], true);
    assert_eq!(part["Stream"][2]["selected"], serde_json::Value::Null);
    assert_eq!(part["Stream"][3]["selected"], true);
    assert_eq!(part["Stream"][0]["decision"], serde_json::Value::Null);
    assert!(queue["MediaContainer"]
        .get("directPlayDecisionCode")
        .is_none());
    assert!(queue["MediaContainer"].get("generalDecisionCode").is_none());
}
