# Changelog

## Unreleased

- Added an ASCII startup banner showing the bound port and an easy local web-client URL.
- Reflection: the URL is derived from the listener's actual address, including ephemeral ports, and invalid ports remain rejected by normal socket parsing; `cargo fmt -- --check`, `cargo test`, and `just --fmt --check` were run.
- Added a small built-in web client at `/` with browser video players and a `just dev` convenience target.
- Added on-demand web browsing and playback for supported files in subfolders, including safe path validation.
- Added five-second web UI polling so filesystem changes in the current folder appear without restarting the server or interrupting unchanged video players.
- Hidden dotfiles and dot-directories from media scans, web listings, and direct web playback.
- Centered web players and explicitly kept browser audio unmuted when supported by the source format.
- Added a home link, media-containing folder cards with placeholder previews, and a separate list for folders without supported media.
- Added five-second left/right arrow-key seeking to the web player.
- Added Space-bar play/pause toggling to the web player.
- Added F-key fullscreen toggling to the web player.
- Replaced parent-folder navigation with clickable breadcrumbs for the full current path.
- Updated `just dev` to install `cargo-watch` if needed, then watch Rust source and Cargo manifests and automatically rebuild/restart.
- Reflection: folder classification recursively counts supported media only when the current page is requested, keeping startup indexing unchanged; `cargo fmt -- --check`, `cargo test`, and `just --fmt --check` were run.
- Reflection: subfolders are read only when requested by the web client, so startup does not recursively scan or index them; `cargo fmt -- --check`, `cargo test`, and `git diff --check` were run.
- Fixed compilation in media stream responses and range parsing by borrowing the stored MIME type and explicitly parsing range endpoints as `u64`.
- Reflection: the fixes preserve existing streaming behavior and restore compatibility with the current Rust compiler; `cargo fmt` and `cargo test` were run.
- Added a minimal Rust server that scans one directory for supported video files and exposes them through Plex and Jellyfin-compatible routes.
- Added stable filename-sorted media IDs while keeping one shared file-serving backend.
- Reflection: directory scanning removes the single-file startup limit without adding recursion or a media database; range-parser tests remain the available check, while Plezy playback was not run.
