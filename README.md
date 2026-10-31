# mplex

`mplex` is a proof-of-concept Rust media server for local videos in one directory. It exposes the files through small Plex and Jellyfin-compatible APIs.

## Run

Install Rust, then run:

```sh
cargo run -- --dir /path/to/videos --token development-token --bind 0.0.0.0:8096
```

With `just` installed, the same server can be started more conveniently:

```sh
just dev /path/to/videos
```

The `dev` target installs `cargo-watch` if needed, then watches the Rust source and Cargo manifests, recompiling and restarting mPlex after changes.

At startup, mPlex prints an ASCII banner, the actual bound port, and a local URL for the built-in web client. TCP ports must be between `0` and `65535`; the default is `8096`.

Plex and Jellyfin scan only the configured directory, not subdirectories. The built-in web client browses subdirectories on demand and includes `.mp4`, `.m4v`, `.mkv`, and `.webm` files wherever they appear below the configured directory. Dotfiles and dot-directories are hidden. Use files that the Plezy device or browser can play directly; the server does not transcode.

Open `http://<server>:8096/` for the built-in web client. The `mPlex` heading links home. Folders containing supported media files anywhere below them appear as cards with a placeholder preview, while folders without supported media are listed separately. It navigates the configured directory and its subfolders without recursively scanning them at startup, and checks the current folder every five seconds so newly added or removed files appear without a restart. A listing change waits until active playback is paused or ended so it does not interrupt a video. Players are centered, leave audio enabled, support five-second left/right arrow-key seeking, use Space to toggle play/pause, and use F to toggle fullscreen. Browser playback still depends on support for the file's container and audio/video codecs; the web client does not add transcoding.

## Connect Plezy

- Plex: use the server URL `http://<server>:8096` and the configured token where the client supports direct server connections.
- Jellyfin: add the same URL as a Jellyfin server. Use any username and password; the configured token is returned by the proof-of-concept login flow.

The Plex MVP uses direct server API routes. Plezy's cloud account and server discovery flow was not tested because no Plezy checkout was available in this repository. If the target Plezy build has no direct Plex connection option, its existing Plex client needs a small local server-URL/token connection change. Record the exact Plezy commit used for that compatibility test here.

## Checks

Run the focused range tests with:

```sh
cargo test
```

The tests cover full-file serving range calculation, a valid range, a suffix range, an unsatisfiable range, and HTML escaping used by the web client. HTTP and real Plezy playback checks require a built binary, a video file, and a checked-out Plezy client; they were not run in this environment.

## Limitations

This server has one hardcoded Plex/Jellyfin library and assigns stable IDs by sorted top-level filename. The web client browses subfolders on demand, but APIs do not expose recursive media scanning. There is no database, metadata provider, artwork, transcoding, watch history, multi-user support, or plugin system. Stream metadata is intentionally minimal.
