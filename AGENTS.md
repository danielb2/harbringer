# mplex

Project guidance for the `mplex` repository. `mplex` is a small Rust proof-of-concept media server that exposes one local video through Plex and Jellyfin-compatible APIs.

## Essential rules

- Read relevant code before making changes.
- Keep changes focused and remove unused code.
- Add or update tests for behavior changes.
- Do not commit secrets or generated files.
- Keep Plex and Jellyfin adapters separate from shared file serving.
- Keep the media scope to one configured directory; do not add recursive scanning, databases, transcoding, or user management.

## Commands

- Build: `cargo build`
- Test: `cargo test`
- Run: `cargo run -- --dir /path/to/videos --token development-token --bind 0.0.0.0:8096`

## Structure

- `src/main.rs`: command-line configuration, directory scan, shared media state, adapters, and range-enabled file serving.
- `README.md`: setup, connection, validation, and limitations.
- `CHANGELOG.md`: change history and reflections.

## Doc Routing

| Topic | Document |
| --- | --- |
| Setup and Plezy connection | [README.md](README.md) |
| Changes and validation reflections | [CHANGELOG.md](CHANGELOG.md) |

## Rules

Keep documentation and `CHANGELOG.md` in sync with code changes. For each change, record a short reflection that states what changed, why it changed, and what checks were run.
- Dialog close controls must remain fixed in the dialog's top-right corner, with an accessible label and tooltip.
- Pressing Escape must close an open dialog.
- Follow DRY principles across the project. Anything used in more than one place—UI components, markup, styles, route behavior, data transformations, validation, or media logic—must use a shared reusable implementation whenever sensible. Avoid duplicated implementations that can drift, including but not limited to Home and Library media presentation.
