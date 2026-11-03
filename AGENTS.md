# mplex

Project guidance for `mplex`: the target is a 100% drop-in replacement for Plex's official Media Server. This includes documented PMS APIs and matching behavior in official Plex clients, including every client-visible capability and item menu. This is past the MVP stage; treat full compatibility as the goal, not a single-route or single-action milestone.

## Essential rules

- Read relevant code and current Plex request traces before changing behavior.
- Keep the protocol surface Plex-only. Do not add Jellyfin routes or mix adapters.
- Implement Plex behavior against the official PMS documentation, current request traces, and a reference PMS or compatible implementation where available. Validate client-visible behavior, including the full item menus, against Plex clients; do not infer parity from route tests alone.
- Playback is a core requirement: support direct play, direct stream/remux, and transcoding when a Plex client requires it.
- Media inspection, remuxing, decoding, encoding, and transcoding must use portable in-process Rust libraries. Do not depend on installed commands such as `ffmpeg` or `ffprobe`.
- Keep platform support in mind when selecting media libraries and features. Do not make one operating system's hardware decoder mandatory.
- Preserve original-file serving and seeking when a client can play the source directly. Transcoding must be selected from client capabilities and media metadata, not used as a blanket path.
- Keep shared file handling and media logic reusable. Avoid duplicated protocol responses, route behavior, or UI implementations.
- Add or update tests for behavior changes, including success and failure paths. Keep the Plex endpoint inventory and route tests in sync with the official API documentation.
- Do not commit secrets, generated artifacts, or changes unless explicitly asked.
- When asked to commit, use the commit subject as a concise summary; include a detailed body when useful to explain the change, rationale, and validation. The subject is not a substitute for the body.

## Commands

- Build: `cargo build`
- Test: `cargo test`
- Run: `cargo run`
- Restart the running server with `SIGHUP`; do not use `SIGTERM`.

## Structure

- `src/main.rs`: configuration, media/library state, Plex routes, shared media serving, playback, and the built-in web client.
- `src/plex_tls.rs`: Plex discovery, TLS, and HTTP/WebSocket serving.
- `README.md`: user-facing project overview, setup, connection, playback, and current limitations.
- `ROADMAP.md`: full PMS compatibility checklist and client validation status.
- `CHANGELOG.md`: change history and short implementation reflections.

## Documentation and UI rules
- Keep `README.md` user-focused. Put detailed implementation notes, technical compatibility status, and technical reflections in `ROADMAP.md`.

- Update `README.md` and `ROADMAP.md` only when preparing a requested commit. In `ROADMAP.md`, record a brief reflection with what changed, why, and the checks run. Do not update `CHANGELOG.md` unless explicitly requested.
- Dialog close controls must stay fixed at the dialog's top-right corner, have an accessible label and tooltip, and close on Escape.
- Follow DRY principles across UI components, markup, styles, route behavior, data transformations, validation, and media logic. Home and Library presentation should share implementations where sensible.
