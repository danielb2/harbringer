
```
|              |         o
|---.,---.,---.|---.,---..,---.,---.,---.,---.
|   |,---||    |   ||    ||   ||   ||---'|
`   '`---^`    `---'`    ``   '`---|`---'`
                               `---'`
```

# Harbringer

`harbringer` is a Rust project to build a fully functional Plex Media Server replacement. The project is past MVP: the target is compatibility with the documented PMS API and real Plex clients, including full playback behavior. Jellyfin-compatible endpoints are out of scope. The built-in web client and Settings pages are Harbringer's own control surface, separate from the Plex protocol.

Direct file serving must remain independent of metadata availability and external services. Full Plex replacement requires the API behavior, playback negotiation, media support, discovery, account/session handling, and client-visible features Plex clients rely on. Media inspection, remuxing, and transcoding must use portable in-process Rust libraries rather than installed command-line tools. The same backend should support seekable media sources supplied by applications such as Weaver and Scryer, including on-demand playback while an NZB is still being acquired.

The highest-priority compatibility gate is an enabled Play button in the official Plex clients, followed by playback, seeking, and resume. The current repository is an early implementation: the Play button is still unverified in the client, and the complete Plex API coverage and client matrix are tracked in [`ROADMAP.md`](ROADMAP.md).

See [`ROADMAP.md`](ROADMAP.md) for the replacement checklist and gap log.

The Settings page’s radial background gradient is also used on built-in web-client pages. Media banners remain visible over the background.


## Run

Install Rust, then run Harbringer:

```sh
cargo run
```

With `just` installed, the same server can be started more conveniently:

```sh
just dev
```

The default configuration is `./config/config.toml`; no config argument is required. The `dev` target installs `cargo-watch` if needed, then watches the Rust source, Cargo manifests, and TOML config, recompiling and restarting harbringer after changes. Existing `./config/mplex.toml` files are automatically renamed on startup when `config.toml` is absent. Use `cargo run -- --config /path/to/config` only when explicitly overriding the configuration directory.

On Unix systems, send `SIGHUP` to the running Harbringer process to gracefully close active connections and re-exec the same binary with its original arguments. This reloads the configuration and media inventory and keeps the same process ID. Build the updated binary before sending `SIGHUP` if you want it to load newly compiled code; `SIGHUP` does not compile source files.

The current local dev executable is 52.1 MB, while `cargo build --release` produces a 16.6 MB executable. The dev profile includes debug information; use the release profile when comparing executable size.

At startup, harbringer prints an ASCII banner, the actual bound port, and a local URL for the built-in web client. TCP ports must be between `0` and `65535`; the default is Plex's standard `32400`.

harbringer uses `./config/config.toml` as the portable source of truth for server settings and named libraries. It creates `./config/state.sqlite` beside that file for server identity, discovered media, users, and watch progress. Matched metadata has one selected storage source: the SQLite database or `.mplex/info.toml` files beside media. Sidecar mode lets a fresh mplex install reuse existing matches without a database backup or another TMDB lookup. Artwork is stored with sidecar metadata. The built-in web client reads selected movie and episode details from the configured metadata source. TMDB matching runs only for movie and TV libraries; `other` libraries are skipped. Back up the `config` directory for server identity and settings; keep media-folder `.mplex` directories portable with the media when using sidecar mode.

The scanner indexes visible `.mp4`, `.m4v`, `.mkv`, and `.webm` files from every configured library folder. Plex and the built-in Media view share this inventory. Use **Scan all libraries** in Settings to refresh it now, or leave automatic scanning enabled per library. Automatic scanning is enabled by default and runs every five minutes. Dotfiles and dot-directories stay hidden. The scanner keeps IDs for unchanged paths across scans and restarts.

When a scan removes media, mplex sends a Plex timeline deletion notification to connected clients. It also sends notifications for previously removed item IDs when a client reconnects. This helps Plex clients clear stale library state. Requests for known removed metadata items also trigger a scan and receive an empty Plex metadata container.

Open `http://<server>:32400/` for the built-in web client. Each library has a **Media | Files** switch. Media shows poster cards. Files shows real folders and every visible file, including unsupported formats. Files browsing keeps its view while you move through folders. The page checks its current view every five seconds and waits until playback pauses before replacing a changed listing. Players support five-second left/right seeking, Space for play/pause, and F for fullscreen. Matching `.srt`, `.ass`, and `.vtt` files beside a video are available through the browser's native subtitle selector; SRT and ASS are converted to WebVTT when requested. Sidecar mode writes matched metadata and artwork to `.mplex/cover.jpg`, `.mplex/banner.jpg`, and `.mplex/info.toml`. Browser playback still depends on the browser's supported containers and codecs; mplex does not convert browser media.

mplex advertises `transcoderVideo` and serves a DASH MPD for supported H.264 Matroska media with optional AAC audio. HLS and other video codecs are not supported. Plex Desktop playback works; Plex Web DASH playback remains under validation. See [`ROADMAP.md`](ROADMAP.md).

TV show overviews show the series cover beside its description and include available genres and content ratings. They hide the representative episode's file path and season/episode numbers.

The web client uses the homepage as its only browsing page. Its collapsible rail lists Home, each library, and Settings. TV libraries group shows by series, then open seasons and episode cards. Home includes Continue Watching, Recently Added, and library cards. Card actions mark items watched or unwatched. **Remove from Continue Watching** appears only on cards in that section. Settings lets you create and delete libraries, add or remove folders, cancel an unfinished folder addition, and set each library's automatic scan state and interval in seconds, minutes, or hours. Each library card saves its name, scan schedule, and Home sections together. Scan actions sit beside scan settings, and folder actions sit beside Folders. Removing a folder detaches it from the library but does not delete its files. Library and folder removal update Settings in the background without a page reload or loss of scroll position. New folders and media files enter the shared inventory during a manual or automatic filesystem scan. Plex clients can also scan a library's files. **Scan metadata** remains a separate TMDB matching action. The folder picker browses server directories. Web playback offers Resume and Play from beginning actions and stores progress while playing or paused.
The Settings folder picker opens in a modal and returns the selected server path to the active field without leaving the page. The add-library panel and the affected library card stay open after a create or folder update.

## Connect Plezy

- Plex: use the server URL `http://<server>:32400`. Link Harbringer to Plex.tv from Settings when account discovery is needed.

Plex library recommendations and recently-added shelves are available through `/hubs/sections/{id}/recommended` and `/hubs/sections/{id}/recentlyAdded`, as well as the parent section hub listing.
`/devices` and `/status/sessions/history/all` return Plex-shaped empty containers; client-device registration and playback-history storage are not yet implemented. `/accounts` was already available and identifies the single local account.
Home and Library share a poster-card renderer. Cards use 70%-transparent backgrounds, equal padding on all edges, and a fixed-height four-row info area. Movie titles stay on one ellipsized line, with the year directly below.
`GET /library/all` aggregates configured movie and TV libraries, returns top-level movies and shows by default, supports type and title filters plus Plex container pagination, and emits the standard metadata container.
TV show, season, and episode responses include stable Plex-compatible GUIDs and parent links. Plex watch-state changes update the `viewCount` field returned for each episode; marking an episode unwatched also clears `lastViewedAt`.
Provider discovery advertises timeline/scrobble actions, universal search, static video playlists and collections, and the Continue Watching hub with its remove action. The hub lists in-progress items and uses saved Plex progress. Static playlists support create, list, rename, delete, add, list, remove, and reorder. Collections support create/list and item add/list/remove. Smart playlists, collection reordering/composite artwork, and playlist upload are not implemented. Plex Desktop menu visibility and behavior still require client verification.
Plex part downloads accept `download=1` and return an attachment `Content-Disposition` with safe ASCII and UTF-8 filenames while preserving byte-range support.
Plex discovery through `/media/providers` and `/library/sections` now uses stable per-library UUIDs and persisted `scannedAt`/`updatedAt` values. Timestamps reflect scans and library changes, not the request time. Plex Web's current request trace includes `/media/providers`; real-client visibility still needs validation.

The Plex API returns JSON from its implemented endpoints and exposes one section per configured library, filtering inventory by section. Library listings support Plex's `X-Plex-Container-Start` and `X-Plex-Container-Size` pagination headers and title/date/rating sorting. Section sort discovery returns supported title sorting; filter discovery returns an empty list until Harbringer exposes filterable metadata. Section preferences return an empty `Setting` list because Harbringer has no per-library preferences; `/:/prefs` returns the server-level preferences Harbringer supports. The `/activities` and `/status/sessions` endpoints return empty containers, and `/accounts/1` identifies the single local account and uses the linked Plex account name/avatar when available. Home and library recommendation hubs show the highest TMDB-rated items when matches exist, with recently modified media as the fallback, and a separate recently added row when ratings are available. Plex metadata responses read matches from the selected source, either SQLite or `.mplex/info.toml` sidecars, and include summary, title, release date/year, rating, genres when present, and TMDB GUID; unmatched items include filename-based title, title sort, year when present, and file timestamps. Plex library, hub, and item responses include native library-container identifiers and section metadata. TV sections list one show item per series; show children list seasons, and season children list playable episode items with parent and episode numbering. Child containers identify their contents with `viewGroup=season` for shows and `viewGroup=episode` for seasons; episodes remain playable leaf items with direct metadata keys and media parts. TV episode ancestry includes explicit parent types and metadata-item keys for its parent season and show. TV recommendation and recently added hubs also group episodes into one show item, with total and viewed episode counts. Related hubs rank same-library items by genre match, rating, and recency; items without managed extras return an empty extras list. TV show and season metadata also support Plex's includeChildren=1 option; TV /children endpoints return 20 items by default and support container pagination. Library rows, recommendation hubs, and detailed item responses use the pure-Rust `vuio-media-info` parser to report duration, video dimensions/resolution/codec, audio codec/channel layout and track language (using named track titles or the configured default for a single untagged audio stream), embedded text tracks, and matching external subtitle tracks. Matching `.srt`, `.ass`, and `.vtt` sidecars are advertised as Plex external subtitle streams with their original extensions in the stream key; `/library/streams/{id}.{ext}` serves the original format and converts to a requested format when Plex supplies `format`. Part and external subtitle stream IDs stay within JavaScript's safe integer range so Plex Web can use the IDs without rounding them. Plex audio/subtitle stream-selection requests are validated and reflected in item metadata for the running server process. Plex playback timeline and scrobble requests persist watch progress in SQLite, and item metadata reports `viewOffset`, `viewCount`, `lastViewedAt`, and known duration so Plex clients can offer Resume. The documented `/video/:/transcode/universal/decision` endpoint reports a successful direct-play decision for the media's existing ranged file URL; Harbringer does not transcode. Plex API routes echo browser origins, answer CORS preflight requests, and expose Plex response headers to hosted clients such as app.plex.tv; the Settings and web-client routes are excluded. Request and response logs use the existing `tracing` subscriber with a server-local timestamp to whole seconds and numeric UTC offset and severity first, followed by readable `key=value` fields; terminal colors remain enabled; request entries include the remote peer IP and response entries include status and latency. The Plex notification WebSocket sends a ping heartbeat every 10 seconds, responds to client pings, and logs close details; Harbringer does not yet publish library-change events. The custom HTTP/HTTPS listener enables Hyper's WebSocket upgrade handoff so upgraded notification connections can remain open. The section collections endpoint currently returns an empty collection list because Harbringer does not yet store user-created collections. Media entries advertise Plex thumbnail and background-art routes backed by `.mplex/cover.jpg` and `.mplex/banner.jpg` when sidecar mode is selected; image lookup follows the metadata group for movies and shows, with season artwork as the episode thumbnail fallback. Missing artwork returns 404 unless a cached frame already exists; automatic 10% frame extraction is disabled because a portable in-process decoder is not integrated. Plex's `/photo/:/transcode` image URL is supported as a passthrough to the matching artwork (the requested dimensions are not applied). Provider discovery advertises library metadata, recommendation hubs, the search hub, and the universal PlayQueue feature used by Plex clients to expose Play controls. Plex search matches movie and show titles, and episode titles or their parent show titles, through `/library/search` and `/hubs/search`, including partial and minor misspelled title matches. TV episode metadata includes the show poster and background artwork routes. `/library/search` returns Plezy-compatible `SearchResult` entries with each item nested under `Metadata`, alongside grouped hubs; section first-character browsing is available through `/library/sections/{id}/firstCharacter`. Playlists, collection creation, and video transcoding are not implemented. Settings can link a Plex.tv account, claim Harbringer as a Plex Media Server, request a trusted `plex.direct` certificate, and publish LAN addresses for Plex client discovery. The root and provider descriptors report Plex certificate support only when the issued `plex.direct` certificate is loaded. The device announce follows Rex's captured PMS request: `PUT https://servers.plex.tv/devices/{machineIdentifier}`, server token in the query, and LAN HTTP URIs with HTTPS enabled after `servers.xml` registration. Harbringer also keeps Rex's Plex pubsub event stream open. Live verification confirmed Plex.tv reports `presence=true`, lists the LAN HTTPS connection as `local=true`, and that URL serves `/identity` over trusted TLS. Full Plex client playback remains to be checked.

## Checks

Run the Rust unit and protocol helper tests with:

```sh
cargo test
```

Run one behavior-focused test by name, for example `cargo test manual_scan_updates`. `tests/unit/plex_endpoints.rs` exercises production-router requests with temporary libraries. `tests/unit/plex_helpers.rs` checks focused response and transformation helpers.


The test suite is driven by the checked-in Plex PMS OpenAPI 1.2.3 inventory: all 260 documented method/path operations are sent through the production router, and their baseline HTTP statuses are asserted. The current probe returns 32 responses with status 200, 9 with 400, 21 with 405, and 198 with 404; non-2xx results expose documented operations that remain unsupported or need valid request fixtures. Focused playback, hub, action, playlist, and collection tests cover progress, media delivery, subtitles, artwork, and item management. The WebSocket route still needs a live-upgrade check because Axum's in-process `oneshot` service cannot provide Hyper's upgrade extension. These tests measure current behavior; they do not claim full PMS compatibility. See [`ROADMAP.md`](ROADMAP.md) for the remaining work.

## Limitations

Harbringer's Plex API coverage is incomplete. Media IDs remain stable for unchanged paths across scans and restarts; renames, moves, and library reordering can still change them. Plex Desktop playback works; Plex Web DASH playback remains under validation. Plex.tv registration, local TLS publication, playback decisions, and ranged delivery have protocol-level checks. DASH remuxing is a limited H.264/MKV prototype; other codecs, HLS, and other client playback remain open. See [`ROADMAP.md`](ROADMAP.md) for the remaining work.

## Testing checklist

- [ ] can play in plex web
- [ ] can play in plex desktop
- [ ] can play in plex mobile
- [ ] can play in plezy

## References

- https://developer.plex.tv/pms/
- https://support.plex.tv/articles/naming-and-organizing-your-tv-show-files/
- https://github.com/edde746/plezy - a plex client
- https://github.com/braedonsaunders/ether/ - a plex server for non-stored content
- https://github.com/debridmediamanager/rex - like ether but in rust
