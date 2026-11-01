# harbringer replacement checklist

This is the working checklist for making harbringer a drop-in Plex Media Server replacement. Items are checked only when implemented and validated against the relevant client behavior.

## Proven MVP pieces

- [x] Serve local video files with GET, HEAD, byte ranges, correct lengths, and seeking.
- [x] Share one media model between Plex and Jellyfin adapters.
- [x] Expose basic Plex identity, library, metadata, and direct-play routes.
- [x] Expose basic Jellyfin login, library, playback-info, and direct-play routes.
- [x] Scan supported local video files and refresh the browser view after filesystem changes.
- [x] Provide a local browser client for inspecting and directly playing files.
- [x] Return a Plex `MediaContainer` at `/` to Plex clients while retaining the browser UI for normal requests.
- [x] Expose each configured library as a separate Plex section and filter section inventory by library.

## Drop-in Plex replacement

- [x] Persist a stable harbringer server identity and initial server configuration.
- [x] Add Plex.tv account sign-in/PIN authentication in the web UI.
- [x] Exchange a Plex claim token and persist the resulting server account credentials.
- [x] Register/claim harbringer with Plex.tv and publish its local connection in the account resource list.
- [ ] Support the connection details Plex clients need for local and remote discovery.
- [ ] Validate connection and library browsing with an actual Plezy build.
- [ ] Validate connection and playback with official Plex clients.

## Local library

- [x] Create a persistent database and record the currently scanned media files.
- [x] Store user-editable server and library configuration in a portable TOML file.
- [x] Support multiple configured libraries with live filesystem updates and per-library browsing.
- [x] Recursively scan library paths without blocking direct playback.
- [ ] Assign stable media identities independent of sorted scan order.
- [ ] Detect additions, removals, and renames reliably.
- [x] Match local files to movies, shows, seasons, and episodes.
- [x] Scrape and cache metadata for matched media, including TV season and episode metadata.
- [x] Download/cache movie, show, and TV-season artwork and expose it through the web client.

## Users and playback state

- [ ] Persist Plex users and server/account identity.
- [x] Store watch position and watched state in SQLite for the current web user.
- [x] Implement web playback start, progress, stop, and resume behavior.
- [ ] Persist playback sessions without making direct streaming depend on them.

## Plex compatibility depth

- [ ] Add library search, hubs, filtering, and sorting expected by clients.
- [ ] Return accurate duration, codecs, streams, audio, and subtitle information.
- [ ] Support Plex artwork, thumbnails, and background image requests across all client routes.
- [ ] Implement playback negotiation and capability responses.
- [ ] Verify direct play, seeking, resume, and multi-client behavior.

## Future media sources

- [ ] Define a seekable media-source interface separate from local files.
- [ ] Allow Weaver/Scryer-provided sources to drive buffering and seeking.
- [ ] Add an embeddable Rust library API.
- [ ] Keep acquisition, Usenet, repair, and discovery outside the harbringer core.

## Later client compatibility

- [ ] Add Audiobookshelf-compatible APIs so Audiobookshelf clients can browse, play, and track audiobooks served by harbringer.
- [ ] Study BookOrbit’s book-library and metadata-provider implementation as a reference for audiobook matching and cover-art retrieval; evaluate reuse or interoperability before adding a dependency.

## Gap log

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
