# mplex

Project guidance for `mplex`: a Rust implementation of a fully functional Plex Media Server replacement. The goal is compatibility with Plex's documented PMS API and real Plex clients, including Plex Web and Plex Desktop. This is past the MVP stage.

## Essential rules

- Read relevant code and current Plex request traces before changing behavior.
- Keep the protocol surface Plex-only. Do not add Jellyfin routes or mix adapters.
- Implement Plex behavior against the PMS documentation and validate it with route tests and client playback where available.
- Playback is a core requirement: support direct play, direct stream/remux, and transcoding when a Plex client requires it.
- Media inspection, remuxing, decoding, encoding, and transcoding must use portable in-process Rust libraries. Do not depend on installed commands such as `ffmpeg` or `ffprobe`.
- Keep platform support in mind when selecting media libraries and features. Do not make one operating system's hardware decoder mandatory.
- Preserve original-file serving and seeking when a client can play the source directly. Transcoding must be selected from client capabilities and media metadata, not used as a blanket path.
- Keep shared file handling and media logic reusable. Avoid duplicated protocol responses, route behavior, or UI implementations.
- Add or update tests for behavior changes, including success and failure paths. Keep the Plex endpoint inventory and route tests in sync with the official API documentation.
- Do not commit secrets, generated artifacts, or changes unless explicitly asked.

## Commands

- Build: `cargo build`
- Test: `cargo test`
- Run: `cargo run`

## Structure

- `src/main.rs`: configuration, media/library state, Plex routes, shared media serving, playback, and the built-in web client.
- `src/plex_tls.rs`: Plex discovery, TLS, and HTTP/WebSocket serving.
- `README.md`: setup, connection, playback behavior, validation, and current limitations.
- `ROADMAP.md`: full PMS compatibility checklist and client validation status.
- `CHANGELOG.md`: change history and short implementation reflections.

## Documentation and UI rules

- Keep `README.md`, `ROADMAP.md`, and `CHANGELOG.md` in sync with behavior changes. Each change needs a brief reflection recording what changed, why, and the checks run.
- Dialog close controls must stay fixed at the dialog's top-right corner, have an accessible label and tooltip, and close on Escape.
- Follow DRY principles across UI components, markup, styles, route behavior, data transformations, validation, and media logic. Home and Library presentation should share implementations where sensible.
