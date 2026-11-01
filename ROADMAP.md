# mplex: Plex Media Server replacement roadmap

The product goal is a full replacement for Plex Media Server that keeps working with Plex clients. Plex compatibility is not complete when a server merely answers a few familiar paths: responses, playback negotiation, streaming, account/discovery behavior, and client-visible controls must work end to end. The public PMS OpenAPI specification at <https://developer.plex.tv/pms/> is the compatibility inventory. JSON is the response format for the Plex API.

An item is complete only after its behavior is implemented and checked against a real client or a reproducible protocol-level test. A route returning HTTP 200, an empty placeholder, or a successful playback decision does not by itself prove the feature works.

## P0: visible Play button and working playback

- [ ] Show an enabled Play button for playable library items in the official Plex web and macOS clients.
- [ ] Press Play and start the actual media file through the advertised Plex part URL.
- [ ] Verify pause, resume, seeking, playback completion, and saved Resume position in the official clients.
- [ ] Run the same playback checks in Plezy and record each client's requests and responses.
- [x] Serve the media part with HTTP range support; an existing media part returned `206 Partial Content` and 1,024 bytes during development verification.
- [x] Return a direct-play decision from `/video/:/transcode/universal/decision` for an item path.
- [ ] Confirm that the official client receives and displays the Play control after refreshing Harbringer's item metadata. Keep this open until observed in the client.

The decision endpoint and ranged file route are necessary pieces, not proof that the Play button issue is resolved. Do not move this gate to complete based only on server-side curl checks.

## Compatibility completion gates

### 1. PMS API coverage inventory

- [ ] Build a path-and-method coverage matrix from the current official PMS OpenAPI specification.
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
- [x] Support current title/date/rating sorts, basic recommendations, Plex artwork URLs, external subtitles, and section pagination.
- [x] Implement Plex title search and confirm Plezy returns local-library results.
- [ ] Recheck in Plezy that show-title searches also display matching episodes and the show cover after the latest response changes.
- [ ] Complete and validate all client-used hubs, filters, sorting options, collections, playlists, and library navigation.
- [ ] Replace scan-order IDs with stable media and part IDs; handle moves, renames, removals, rescans, and library updates reliably.
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
- [ ] Publish library and playback events over Plex notification connections, and validate reconnect behavior.
- [ ] Audit and implement server management, library management, device, diagnostics, and other client-used operations in the official API inventory.

### 6. Release validation

- [ ] Maintain a client matrix for Plex Web, Plex for macOS, Plezy, and the other supported official Plex clients, with browse, detail, Play, seek, resume, subtitles, audio selection, artwork, and connection checks.
- [ ] Run the protocol/API suite and client smoke checks on every release candidate.
- [ ] Publish a compatibility report that lists verified behavior and remaining gaps; do not describe the product as a Plex replacement while a P0 gate is open.

## Shared media engine and additional adapters

- [ ] Keep Plex and Jellyfin protocol adapters separate from shared media and file serving.
- [ ] Define a seekable media-source interface for local files and future Weaver/Scryer sources.
- [ ] Add an embeddable Rust library API after the server compatibility gates are met.
- [ ] Add Audiobookshelf-compatible APIs and assess BookOrbit references after the Plex replacement path is on track.

## Gap log

### 2026-11-01

- Plezy search now returns local-library results after `/library/search` was changed to its expected `SearchResult[].Metadata` shape. Latest follow-up adds episodes to parent-show searches and emits `grandparentThumb`/`grandparentArt`; verify those additions in Plezy after its latest connection refresh.
- The direct-play decision endpoint returned JSON HTTP 200 with a successful decision, and its advertised file part returned HTTP 206 for a byte-range request on the existing port 32400 dev server. The official Plex client still showed no Play button; investigation found the missing `playqueue` provider feature, now advertised. Verify the button and playback in Plex Web/macOS after the dev server reloads; the P0 gate remains open until confirmed.
- Live item metadata exposed part and external subtitle stream IDs above JavaScript's exact integer limit (2^53−1). These IDs now use deterministic safe-range numbers; on the reloaded `just dev` server, the revised part URL returned HTTP 206 and an advertised external subtitle URL returned HTTP 200 as WebVTT. The official client still needs a metadata refresh and Play-button check.
- Replaced the sparse “drop-in” checklist with an operation-by-operation PMS API inventory, implementation gates, and an official-client compatibility matrix. JSON remains the Plex API response format.

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
- Implemented the TV browsing hierarchy: show overview, season cards with compact episode-count badges, season artwork/descriptions, selectable episode cards, and one-player episode detail views with episode descriptions.
- Added Plex-style TV naming support for `Season 01`, `season.01`, `Specials`, `s01e01`, and multi-episode markers such as `s02e18-e19`.
- Added TMDB season artwork and season metadata caching under each season’s `.mplex` directory.
- Added movie/show/TV metadata matching, sidecar metadata storage, artwork download, per-media rematching, force rescans, and ffprobe technical metadata caching.
- Added web-client watch progress, resume behavior, subtitle selection, and playback controls; Plex account/user authentication and official-client compatibility remain unverified.
- Added a collapsible shared explorer/sidebar, Home dashboard, Media/Folder library views, and filesystem polling for the built-in web client.
- Added Plex-aware root negotiation: Plex requests receive XML while ordinary browser requests receive the web client.
- Added Settings-based library registration and startup scanning of all saved library directories.
- Reworked persisted library configuration into named, typed libraries with multiple folders; existing directory records migrate into a Default Library.
- Replaced `--dir` with `mplex.toml` configuration and made SQLite the rebuildable cache/state layer beside the TOML file.
- Added a SQLite state database under the configured library root, with persistent server identity, initial media records, users, watch-progress, and artwork tables.
- Plex `/` and `/identity` now return the persisted machine identifier, and startup records the current local media inventory.
- Confirmed the current server is not yet discoverable through Plex.tv: it lacks persistent identity, PIN authentication, claim exchange, and resource publication.
- Added the Plex.tv PIN linking flow in Settings and persist the returned account token; the server deliberately remains unclaimed until the claim exchange and resource publication steps are implemented.
- Confirmed Plex.tv authentication/discovery must precede real Plezy and official-client validation; direct URL/token connections remain only a development shortcut.
- Confirmed the current media model still assigns IDs from scan order; stable identities independent of renames remain outstanding.
- Recorded metadata matching, artwork, presentation, and per-user watch progress as required product behavior.
- Checks run: `cargo test`, `cargo fmt`, `just --fmt --check`, and `git diff --check`.
