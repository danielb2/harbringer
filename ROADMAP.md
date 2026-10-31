# mPlex replacement checklist

This is the working checklist for making mPlex a drop-in Plex Media Server replacement. Items are checked only when implemented and validated against the relevant client behavior.

## Proven MVP pieces

- [x] Serve local video files with GET, HEAD, byte ranges, correct lengths, and seeking.
- [x] Share one media model between Plex and Jellyfin adapters.
- [x] Expose basic Plex identity, library, metadata, and direct-play routes.
- [x] Expose basic Jellyfin login, library, playback-info, and direct-play routes.
- [x] Scan supported local video files and refresh the browser view after filesystem changes.
- [x] Provide a local browser client for inspecting and directly playing files.
- [x] Return a Plex `MediaContainer` at `/` to Plex clients while retaining the browser UI for normal requests.

## Drop-in Plex replacement

- [x] Persist a stable mPlex server identity and initial server configuration.
- [ ] Add Plex.tv PIN authentication in the web UI.
- [ ] Claim/register mPlex with Plex.tv and publish it in the account resource list.
- [ ] Support the connection details Plex clients need for local and remote discovery.
- [ ] Validate connection and library browsing with an actual Plezy build.
- [ ] Validate connection and playback with official Plex clients.

## Local library

- [x] Create a persistent database and record the currently scanned media files.
- [x] Store user-editable server and library configuration in a portable TOML file.
- [ ] Support multiple configured libraries with live rescans and per-library browsing.
- [ ] Recursively scan library paths without blocking direct playback.
- [ ] Assign stable media identities independent of sorted scan order.
- [ ] Detect additions, removals, and renames reliably.
- [ ] Match local files to movies, shows, seasons, and episodes.
- [ ] Scrape and cache metadata for matched media.
- [ ] Download/cache artwork and expose Plex presentation endpoints.

## Users and playback state

- [ ] Persist Plex users and server/account identity.
- [ ] Store per-user watch position and watched state.
- [ ] Implement playback start, progress, stop, and resume behavior.
- [ ] Persist playback sessions without making direct streaming depend on them.

## Plex compatibility depth

- [ ] Add library search, hubs, filtering, and sorting expected by clients.
- [ ] Return accurate duration, codecs, streams, audio, and subtitle information.
- [ ] Support artwork, thumbnails, and background image requests.
- [ ] Implement playback negotiation and capability responses.
- [ ] Verify direct play, seeking, resume, and multi-client behavior.

## Future media sources

- [ ] Define a seekable media-source interface separate from local files.
- [ ] Allow Weaver/Scryer-provided sources to drive buffering and seeking.
- [ ] Add an embeddable Rust library API.
- [ ] Keep acquisition, Usenet, repair, and discovery outside the mPlex core.

## Gap log

### 2026-10-31

- Added Plex-aware root negotiation: Plex requests receive XML while ordinary browser requests receive the web client.
- Added Settings-based library registration and startup scanning of all saved library directories.
- Reworked persisted library configuration into named, typed libraries with multiple folders; existing directory records migrate into a Default Library.
- Replaced `--dir` with `mplex.toml` configuration and made SQLite the rebuildable cache/state layer beside the TOML file.
- Added a SQLite state database under the configured library root, with persistent server identity, initial media records, users, watch-progress, and artwork tables.
- Plex `/` and `/identity` now return the persisted machine identifier, and startup records the current local media inventory.
- Confirmed the current server is not yet discoverable through Plex.tv: it lacks persistent identity, PIN authentication, claim exchange, and resource publication.
- Confirmed the current media model is in-memory and filename-order based; it needs database-backed identities before watch state and metadata can be reliable.
- Recorded metadata matching, artwork, presentation, and per-user watch progress as required product behavior.
- Checks run: `cargo test`, `cargo fmt`, `just --fmt --check`, and `git diff --check`.
