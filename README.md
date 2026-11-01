# mplex

`mplex` is a proposed lightweight Rust media-server engine. It aims to replace the server side of Plex while reusing clients such as Plezy, with Plex and Jellyfin protocol adapters over a shared media-delivery backend.

The core is intended to remain small: direct local file serving should remain independent of metadata availability, transcoding, or external services. The product will nevertheless need metadata scraping and movie/series matching so Plex can present a useful library, plus a small persistent database for per-user viewing progress, Plex/account identity, matched media, and artwork/presentation state. Later, the same backend should support seekable media sources supplied by applications such as Weaver and Scryer, including on-demand playback while an NZB is still being acquired.

The first milestone is deliberately narrow: make one local video appear in Plezy, play directly, and seek successfully through the Plex adapter. Jellyfin follows using the same backend. The current repository is an exploratory MVP and has not yet verified full Plezy compatibility or the future Weaver/Scryer integration.

See [`ROADMAP.md`](ROADMAP.md) for the replacement checklist and gap log.

## Run

Install Rust, then run the current local-directory MVP:

```sh
cargo run
```

With `just` installed, the same server can be started more conveniently:

```sh
just dev
```

The default configuration is `./config/mplex.toml`; no config argument is required. The `dev` target installs `cargo-watch` if needed, then watches the Rust source, Cargo manifests, and TOML config, recompiling and restarting harbringer after changes. Use `cargo run -- --config /path/to/config` only when explicitly overriding the configuration directory.

At startup, harbringer prints an ASCII banner, the actual bound port, and a local URL for the built-in web client. TCP ports must be between `0` and `65535`; the default is Plex's standard `32400`.

harbringer uses `./config/mplex.toml` as the portable source of truth for server settings and named libraries. It creates `./config/state.sqlite` beside that file as a cache for server identity, discovered media, users, watch progress, matched metadata, and artwork. Back up the `config` directory to preserve the user-editable configuration; the SQLite file can be rebuilt from the TOML as the cache layer evolves.

The current MVP scans visible subfolders in configured libraries for `.mp4`, `.m4v`, `.mkv`, and `.webm` files. Its web client can browse the first configured folder on demand, while Plex inventory includes every configured folder. Dotfiles and dot-directories are hidden. Use files that the Plezy device or browser can play directly; the server does not transcode.

Open `http://<server>:32400/` for the built-in web client. The `harbringer` heading links home. Folders containing supported media files anywhere below them appear as cards with a first-media screenshot preview when the browser can decode it, while folders without supported media are listed separately. It navigates the configured directory and its subfolders on demand, and checks the current folder every five seconds so newly added or removed files appear without a restart. A listing change waits until active playback is paused or ended so it does not interrupt a video. Players are centered, leave audio enabled, support five-second left/right arrow-key seeking, use Space to toggle play/pause, and use F to toggle fullscreen. Matching `.srt`, `.ass`, and `.vtt` files beside a video are available through the browser's native subtitle/CC selector; SRT and ASS are converted to WebVTT when requested. With sidecar storage enabled, matched media folders receive `.mplex/cover.jpg`, `.mplex/banner.jpg`, and `.mplex/info.toml`. Browser playback still depends on support for the file's container and audio/video codecs; the web client does not add transcoding.

The web client has an omnipresent collapsible left navigation rail for quickly returning home, opening the library, selecting a configured library submenu, or changing settings. Home is a dashboard with Continue Watching, Recently Added, and library cards; Library is the actual folder browser with Media and Folder views. Media hides empty folders; Folder view lists every non-hidden file in the current folder. Its collapsed state is remembered in the browser and it remains available on the Settings page. Settings can create named libraries classified as Movies, TV Shows, or Other with an initial validated folder, then add multiple additional folders to each library. The folder picker can browse server directories and choose a path without shell copy/paste. Web playback offers Resume and Play from beginning actions, saves while paused or playing, and marks completed files watched. Add a TMDB bearer token in Settings, then use Scan metadata on a library to match filenames and download poster/backdrop artwork. All library folders are included on the next server start. On narrow screens the rail becomes a horizontal navigation bar; breadcrumbs remain available for precise folder navigation.

## Connect Plezy

- Plex: use the server URL `http://<server>:32400` and the configured token where the client supports direct server connections.
- Jellyfin: add the same URL as a Jellyfin server. Use any username and password; the configured token is returned by the proof-of-concept login flow.

The Plex adapter returns JSON from its API endpoints and exposes one section per configured library, filtering inventory by section. Library listings support Plex's `X-Plex-Container-Start` and `X-Plex-Container-Size` pagination headers and title/date/rating sorting. Section sort discovery returns supported title sorting; filter discovery returns an empty list until Harbringer exposes filterable metadata. Home and library recommendation hubs show the highest TMDB-rated items when matches exist, with recently modified media as the fallback, and a separate recently added row when ratings are available. Plex metadata responses read matches from the database or `.mplex/info.toml` sidecars and include summary, title, release date/year, rating, genres when present, and TMDB GUID; unmatched items include filename-based title, title sort, year when present, and file timestamps. The Plex notification WebSocket accepts clients and responds to ping frames, but Harbringer does not yet publish library-change events. The section collections endpoint currently returns an empty collection list because Harbringer does not yet store user-created collections. Media entries advertise Plex thumbnail and background-art routes backed by cached `.mplex/cover.jpg` and `.mplex/banner.jpg` files; image lookup follows the metadata group for movies and shows, with season artwork as the episode thumbnail fallback. Plex's `/photo/:/transcode` image URL is supported as a passthrough to the matching cached artwork (the requested dimensions are not applied). Provider discovery advertises library metadata and recommendation hubs; search, playlists, collection creation, and video transcoding are not implemented. Settings can link a Plex.tv account, claim Harbringer as a Plex Media Server, request a trusted `plex.direct` certificate, and publish LAN addresses for Plex client discovery. The device announce follows Rex's captured PMS request: `PUT https://servers.plex.tv/devices/{machineIdentifier}`, server token in the query, and LAN HTTP URIs with HTTPS enabled after `servers.xml` registration. Harbringer also keeps Rex's Plex pubsub event stream open. Live verification confirmed Plex.tv reports `presence=true`, lists the LAN HTTPS connection as `local=true`, and that URL serves `/identity` over trusted TLS. Full Plex client playback remains to be checked.

## Checks

Run the focused range tests with:

```sh
cargo test
```

The tests cover full-file serving range calculation, a valid range, a suffix range, an unsatisfiable range, path validation, and HTML escaping used by the web client. HTTP and real Plezy playback checks require a built binary, a video file, and a checked-out Plezy client; they were not run in this environment.

## Limitations

Harbringer supports configured libraries and stores watch progress and matched metadata, but its Plex API coverage is still incomplete and API IDs are not yet stable across renames or scan-order changes. Plex.tv registration, the published local TLS connection, and the root/provider handshake have been verified; full official-client browsing and playback still need an end-to-end check. A seekable media-source interface, embeddable library API, and Weaver/Scryer integration are future work.

## Sources

- https://developer.plex.tv/pms/
- https://github.com/edde746/plezy - a plex client
- https://github.com/braedonsaunders/ether/ - a plex server for non-stored content
- https://github.com/debridmediamanager/rex - like ether but in rust
