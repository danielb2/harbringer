# mplex: Plex Media Server replacement roadmap

The product goal is a full replacement for Plex Media Server that keeps working with Plex clients. Plex compatibility is not complete when a server merely answers a few familiar paths: responses, playback negotiation, streaming, account/discovery behavior, and client-visible controls must work end to end. The public PMS OpenAPI specification at <https://developer.plex.tv/pms/> is the compatibility inventory. JSON is the response format for the Plex API.

An item is complete only after its behavior is implemented and checked against a real client or a reproducible protocol-level test. A route returning HTTP 200, an empty placeholder, or a successful playback decision does not by itself prove the feature works.

## P0: visible Play button and working playback

- [x] Show an enabled Play button for playable library items in the official Plex web and macOS clients.
- [ ] Press Play and start the actual media file through the advertised Plex part URL.
- [ ] Verify pause, resume, seeking, playback completion, and saved Resume position in the official clients.
- [ ] Run the same playback checks in Plezy and record each client's requests and responses.
- [x] Serve the media part with HTTP range support; an existing media part returned `206 Partial Content` and 1,024 bytes during development verification.
- [x] Return a direct-play decision from `/video/:/transcode/universal/decision` for an item path.
- [ ] Complete and verify DASH playback when Plex Web requests `protocol=dash`. A prototype remuxes H.264 Matroska video with optional AAC audio to fMP4 and serves MPD/init/fragment URLs; other video codecs and containers are rejected, other audio is omitted, and real Plex client playback remains unverified.
- [x] Confirm that the official client receives and displays the Play control after refreshing Harbringer's item metadata.

The decision endpoint and ranged file route are necessary pieces, not proof that the Play button issue is resolved. Do not move this gate to complete based only on server-side curl checks.

## Compatibility completion gates

### 1. PMS API coverage inventory

- [x] Check in a path-and-method inventory of all 260 operations in the official PMS OpenAPI 1.2.3 specification.
- [x] Exercise all 260 documented operations through the production router and assert the current expected HTTP status, including operations not implemented by Harbringer.
- [x] Keep focused playback success and failure tests for queue creation/retrieval, decision, part delivery, stream selection, subtitles, timeline, and scrobble.
- [ ] Replace expected 404/405 outcomes one operation at a time with Plex-compatible behavior and response assertions; update the inventory status only with the implementation and regression test.
- [ ] For every documented operation, record its request parameters, authentication requirements, response schema, current implementation, and client validation status.
- [ ] Implement every operation needed for full PMS replacement with its documented behavior; do not count empty response placeholders as implemented features.
- [ ] Make every advertised server capability consistent with actual behavior. Unsupported calls must not be silently represented as successful functionality.
- [ ] Add regression coverage for each implemented API operation and each client-discovered undocumented call that is required for compatibility.

### 2. Discovery, account, and connections

- [x] Persist a stable server identity and server configuration.
- [x] Link a Plex.tv account, claim/register the server, and publish local connections.
- [x] Advertise LAN connections and load a Plex-trusted `plex.direct` certificate when available.
- [ ] Validate local discovery, secure connection, reconnection, and server selection in official Plex clients on macOS and web.
- [ ] Validate remote connection behavior through Plex.tv, including failure and recovery cases.

### 3. Library and metadata behavior

- [x] Expose configured libraries as Plex sections and return media metadata with technical tracks and matched metadata.
- [x] Publish stable per-library UUIDs and persisted scan/catalog timestamps through provider and section discovery.
- [x] Skip TMDB matching for libraries with kind `other`.
- [x] Refresh the shared Plex/web inventory with a Settings scan-all action and per-library automatic scans, enabled by default every five minutes.
- [x] Reconcile the affected Plex library section when a client requests a stale media metadata key; preserve the library and remaining items after episode removal.
- [x] Provide a built-in Media/Files switch; Files lists visible directories and all visible files, including unsupported formats.
- [x] Support current title/date/rating sorts, basic recommendations, Plex artwork URLs, external subtitles, and section pagination.
- [x] Implement Plex title search and confirm Plezy returns local-library results.
- [ ] Recheck in Plezy that show-title searches also display matching episodes and the show cover after the latest response changes.
- [x] Support persistent static universal video playlists with CRUD and item add/list/remove/reorder; smart generators and uploads remain unsupported.
- [x] Expose a Plex Continue Watching hub from saved in-progress items and implement its remove action.
- [x] Support static video collections with persistent create/list and item add/list/remove; smart collections, reordering, and composite artwork remain unsupported.
- [ ] Complete and validate all client-used hubs, filters, sorting options, collection artwork/reordering, and library navigation; validate playlist and collection behavior in Plex clients.
- [x] Persist media IDs by canonical file path so additions, rescans, and restarts do not change IDs for unchanged files. Moves, renames, and library reordering still need handling.
- [ ] Complete artwork behavior, including missing-art fallback frames and requested image sizes/formats, using portable in-process Rust libraries.
- [ ] Verify movie and TV show/season/episode hierarchies and metadata in each official client.

### 4. Playback negotiation and media delivery

- [ ] Complete the P0 Play-button and playback gate before expanding to lower-priority endpoints.
- [ ] Inventory and implement decision, direct-play, direct-stream, subtitle, session, and playback-control operations from the PMS specification.
- [ ] Support the media formats and client profiles required for replacement parity, with cross-platform Rust libraries; do not depend on installed `ffmpeg` or `ffprobe` commands.
- [ ] Implement transcoding, remuxing, and subtitle conversion as required by those profiles, or make an explicit product decision if a feature cannot be supported.
- [ ] Verify multiple simultaneous clients, bandwidth/network modes, alternate audio and subtitle selection, seeking, and interruption recovery.

### 5. Users, sessions, events, and administration

- [ ] Implement persistent Plex users, permissions, server preferences, and account responses with documented semantics.
- [x] Persist watch progress for the current local user from Plex timeline/scrobble and browser playback requests.
- [ ] Return actual active playback sessions and activities rather than empty containers.
- [x] Publish timeline deletion events for media removed by a library scan over Plex notification connections.
- [ ] Publish other library and playback events over Plex notification connections, and validate reconnect behavior.
- [ ] Audit and implement server management, library management, device, diagnostics, and other client-used operations in the official API inventory.

### 6. Release validation

- [ ] Maintain a client matrix for Plex Web, Plex for macOS, Plezy, and the other supported official Plex clients, with browse, detail, Play, seek, resume, subtitles, audio selection, artwork, and connection checks.
- [ ] Run the protocol/API suite and client smoke checks on every release candidate.
- [ ] Publish a compatibility report that lists verified behavior and remaining gaps; do not describe the product as a Plex replacement while a P0 gate is open.

## Product scope and follow-on architecture

- [x] Keep the server protocol surface Plex-only; the built-in web client and Settings pages are Harbringer's own control surface, not another media-server adapter.
- [ ] Define a seekable media-source interface for local files and future Weaver/Scryer sources.
- [ ] Add an embeddable Rust library API after the server compatibility gates are met.

## Gap log

### 2026-11-12

- Added bilingual subtitles to the built-in player and Plex stream metadata for external and supported embedded text subtitles. Translation progress checkpoints each completed language per cue, resumes after rate limits, and writes completed pairs beside the video as `<video>.<first>-<second>.srt`; matching sidecars are recognized and served without translation requests.
- Embedded extraction is limited to text tracks in Matroska files; image-based tracks are not translatable. Generated sidecars require write access to the media directory. `Plex.log`/`output.log` showed Plex Desktop selecting the bilingual stream but Google Translate returning HTTP 429 before this change; live client playback after the fix remains to be verified.
- Checks: `cargo test` passed all 170 tests, `cargo build` passed, and `git diff --check` passed.

### 2026-11-05

- Added Docker Compose and `docker run` examples to the README with persistent configuration and a read-only media mount by default; noted that users can remove `:ro` for disposable test media. Validation: `just --list` passed; the examples were not run.


### 2026-11-03

- Settings uses one Save changes action for a library's name, scan schedule, and Home visibility. Scan actions stay beside scan settings, and Add folder stays beside Folders to keep related controls compact. Folder additions can be canceled, and existing folders can be detached without deleting media. Library and folder removal now run in the background and preserve scroll position; library deletion refreshes only the library list and sidebar. This avoids the full-page reload that previously snapped Settings to the top. The modal server-folder browser and open-panel state remain in place.
- Sidecar metadata writes retain an existing `show_id` value instead of clearing it, so a metadata update does not discard the stored identifier.
- Plex Web request traces show `/media/providers` requests after library changes. The live provider and section responses include the TV library. Provider entries previously used the request time for both timestamps, and section IDs determined UUIDs. They now report persisted scan/catalog timestamps and use the database library identity for UUIDs, so deleting an earlier section does not change a surviving library's UUID. Protocol tests cover these fields and reordering; real-client visibility remains unverified.
- Plex Web's file-scan request used `GET /library/sections/2/refresh` and returned 404 because no section-refresh route existed. The server now supports the observed GET and documented POST through the shared inventory scanner. Tests verify the section-2 mapping and invalid-section response.
- Validation: `cargo test` passed all 91 tests; `cargo build`, `cargo fmt --check`, and `git diff --check` passed. The new route was not manually verified with a live Plex client.

### 2026-11-02

- Built-in web Continue Watching shows one first-unwatched, unhidden episode per active TV show and sorts movie/show cards by latest watch activity. Plex API first-unwatched episode selection remains unchanged.
- Provider discovery now advertises universal search, static video playlists and collections, and the Continue Watching hub with its remove action. Playlist, collection, and progress-action behavior have protocol-level tests; smart generators, collection reordering/artwork, and playlist uploads remain unsupported, and Plex Desktop visibility remains unverified.
- Filesystem scans now replace the shared inventory used by Plex and the web client. Settings can scan all libraries, and each library can use an enabled/disabled interval in seconds, minutes, or hours. New IDs persist by canonical file path; focused tests cover new movie/TV discovery and stable existing keys.
- The built-in web client now offers raw Files browsing beside poster-based Media views. Its card menu shows “Remove from Continue Watching” only for cards in that section.
- A saved Plex Web trace requested `protocol=dash` and `/video/:/transcode/universal/start.mpd` for Matroska media. The old handler returned source bytes for a manifest; Plex reported `s4001 (Manifest)`, though its link to browser instability was not confirmed. The handler now has a limited H.264/AAC-to-fMP4 remux path. The user confirmed Plex Desktop playback works again with `transcoderVideo=true`. Plex Web still reports `s4032 (Manifest)`. Logs showed 124,333 keyframe fragments, then 4,469 timed fragments. The MPD uses a one-based `SegmentTemplate` under the MPEG-DASH Full profile, but Plex still requests no init or media segments. Shaka reports `Empty XML source`; the response now sets `Content-Length` and logs the manifest byte count. Client validation is pending.

### 2026-11-01

- Plezy search now returns local-library results after `/library/search` was changed to its expected `SearchResult[].Metadata` shape. Latest follow-up adds episodes to parent-show searches and emits `grandparentThumb`/`grandparentArt`; verify those additions in Plezy after its latest connection refresh.
- Plex Desktop playback works on the current restored-capability source. Plex Web still returns `s4032 (Manifest)` for Millennium Actress. Shaka reports `Empty XML source`; the server's MPD requests return HTTP 200, but no init or media requests follow. The current code uses a one-based `SegmentTemplate`, maps segment 1 to the first fragment, keeps truthful `startWithSAP=0`, and sets explicit manifest length. Verify the latest response body in Plex Web.
- Live item metadata exposed part and external subtitle stream IDs above JavaScript's exact integer limit (2^53−1). These IDs now use deterministic safe-range numbers; on the reloaded `just dev` server, the revised part URL returned HTTP 206 and an advertised external subtitle URL returned HTTP 200 as WebVTT. The official client still needs a metadata refresh and Play-button check.
- Replaced the sparse “drop-in” checklist with an operation-by-operation PMS API inventory, implementation gates, and an official-client compatibility matrix. JSON remains the Plex API response format.
- Plex Desktop requested `GET /devices` and `GET /status/sessions/history/all`. Both now return Plex-shaped empty containers instead of 404; `/accounts` was already implemented. Device registration and playback-history persistence remain open compatibility gaps.
- Home card spacing remained incorrect after the earlier alignment change. The shared renderer now applies 70%-transparent backgrounds, equal padding on all edges, and the same fixed four-row info area to every card. Movie titles stay on one line, and the year follows the title directly. User visual validation confirms equal top/bottom spacing and fixed sizing for movie and TV cards.
- Implemented `GET /library/all` with movie, show, season, and episode type selection, title filtering, sorting, and container pagination. The inventory now expects HTTP 200.
- Episode responses include `guid`, `parentGuid`, `grandparentGuid`, and watch state. Unscrobbling clears `lastViewedAt`; a regression test covers the zeroed state. Provider discovery advertises timeline/scrobble/unscrobble keys, but Plex Desktop still omits the watch action after reload. `output.log` confirms Plex Desktop requests `/media/providers` after reload, so stale provider discovery is not the cause.
- Rex's `docs/findings.md` maps Plex Web menu entries to provider features: `manage`, `playlist`, `actions`, `timeline`, and `subscribe`, plus server flags such as `allowMediaDeletion`, `allowSync`, and `backgroundProcessing`. Harbringer now advertises content/search/metadata/promoted/collection/playlist/playqueue/timeline/continuewatching/actions/universalsearch. Sharing, deletion, sync, and background processing remain disabled. The Rex findings are Web-specific, not proof of Plex Desktop behavior. Harbringer already advertises the `download` protocol; part downloads return attachment headers for `download=1`, but managed sync/download is unsupported. Watchlist, Share, and More Ways to Watch remain unexplained by the reference map.
- Full Plex Desktop context-menu parity remains open; do not advertise menu actions without implementing their routes and behavior.

### 2026-10-31

- Completed the Ether-like Plex account handoff after PIN sign-in: claim-token exchange, server-token persistence, local connection publication, and claimed-state reporting are now implemented; GDM discovery, public registration, and real Plezy validation remain outstanding.
- Plex claim/publication failures now persist their reason and display it in Settings for diagnosis.
- Plex publication now verifies harbringer is returned by Plex’s account resource list; Settings uses a compact checkmark for the successful state.
- Plex PIN polling now uses the server identity and capability header instead of registering the old `harbringer` app identity.
- Plex sign-in now uses the hosted Auth App consent flow with a separate harbringer authentication identity; Settings exposes connect and disconnect as buttons.
- Plex’s hosted consent URL now includes the full device context expected by the Auth App.
- Plex connection now redirects directly to the hosted Auth App and completes through a harbringer callback instead of showing an intermediate code screen.
- Plex PIN creation and polling now match Ether’s strong-PIN Auth App flow.
- Plex server registration now sends the server product/version/platform metadata shown in Plex Authorized Devices.
- Plex registration now includes the server device field used by Ether in Authorized Devices metadata.
- Plex PIN authorization now uses the same stable client identifier as the server registration to avoid creating a separate auth-only client record.
- Plex authentication now uses the documented Ed25519 JWK and signed device-JWT flow instead of the legacy claim PIN path.
- Plex publication now advertises all detected non-loopback IPv4 LAN addresses to reduce unavailable-server failures caused by VPN or multi-interface routing.
- Added Plex GDM LAN discovery responses and announcements; broader official-client validation remains outstanding.
- Settings now supports reconnecting an already connected Plex account without first disconnecting.
- Added Plex `thumb` and `art` routes for media entries with cached sidecar artwork; broader artwork coverage and client validation remain outstanding.
- Implemented the TV browsing hierarchy: show overview, season cards with compact episode-count badges, season artwork/descriptions, episode poster cards with per-episode watch controls, and episode detail views with descriptions.
- TV show overviews now show the series cover beside its description, display available genres and content ratings, and omit representative episode paths and episode numbers; official-client parity still needs validation.
- Added Plex-style TV naming support for `Season 01`, `season.01`, `Specials`, `s01e01`, and multi-episode markers such as `s02e18-e19`.
- Added TMDB season artwork and season metadata caching under each season’s `.mplex` directory.
- Added movie/show/TV metadata matching, sidecar metadata storage, artwork download, per-media rematching, force rescans, and ffprobe technical metadata caching.
- Selected web-client movie and episode views read descriptions and details from either SQLite metadata or sidecar metadata.
- Added web-client watch progress, resume behavior, subtitle selection, and playback controls; Plex account/user authentication and official-client compatibility remain unverified.
- Added shared media-card actions to mark movies or TV shows watched or unwatched and persist the status.
- Home’s Continue Watching shelf shows the next unwatched TV episode after an episode is marked watched, unless an episode in the series is already in progress or the next episode is hidden.
- Added a collapsible shared explorer/sidebar, Home dashboard, Media/Files library views, and filesystem polling for the built-in web client.
- Added Plex-aware root negotiation: Plex requests receive XML while ordinary browser requests receive the web client.
- Shared the Settings page’s radial background gradient with web-client pages, while retaining media banners.
- Added Settings-based library registration, scan-all, per-library automatic scans, and startup scanning of all saved library directories.
- Reworked persisted library configuration into named, typed libraries with multiple folders; existing directory records migrate into a Default Library.
- Replaced `--dir` with `config.toml` configuration and made SQLite the rebuildable cache/state layer beside the TOML file.
- Added a SQLite state database under the configured library root, with persistent server identity, initial media records, users, watch-progress, and artwork tables.
- Plex `/` and `/identity` now return the persisted machine identifier, and startup records the current local media inventory.
- Confirmed the current server is not yet discoverable through Plex.tv: it lacks persistent identity, PIN authentication, claim exchange, and resource publication.
- Added the Plex.tv PIN linking flow in Settings and persist the returned account token; the server deliberately remains unclaimed until the claim exchange and resource publication steps are implemented.
- Confirmed Plex.tv authentication/discovery must precede real Plezy and official-client validation; direct URL/token connections remain only a development shortcut.
- Media IDs now persist across additions, rescans, and restarts for unchanged paths. Stable identity across moves, renames, and library reordering remains outstanding.
- Recorded metadata matching, artwork, presentation, and per-user watch progress as required product behavior.
- Checks run: `cargo test`, `cargo fmt`, `just --fmt --check`, and `git diff --check`.
