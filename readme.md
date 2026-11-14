![logo](./logo.avif)

Harbringer is a self-hosted media server for organizing and streaming your personal video library. It scans folders on your computer or NAS, organizes movies and TV shows into libraries, and adds available metadata and artwork. You can browse and play supported media in its built-in web client or connect compatible Plex clients.

Harbringer is an alternative, independent implementation of Plex Media Server. It is not affiliated with Plex, Inc. Harbringer is under active development: its supported media formats, playback options, and client compatibility are still limited. See [Current limitations](#current-limitations) for details.

## What you can do

- Create movie, TV, and other libraries from folders on your server.
- Browse posters and folders in the built-in web client.
- Connect compatible Plex clients to your server.
- Scan libraries manually or on a schedule.
- Match movie and TV titles with TMDB metadata.
- Store matched metadata in the server database or beside your media files as portable sidecars.
- Play supported files directly, with seeking and playback progress.
- Use Plex Desktop for playback of supported H.264 Matroska media, including optional AAC audio.
- Configure optional bilingual subtitles in the collapsible **Subtitle translation** section of Settings. Choose the source subtitle and two output languages, translate while playing, or prepare the full episode; the player identifies the selected source, and progress is staged beside the video as `<video>.en-vi.vtt.part`. A different source starts from its own matching progress. During playback, clients receive the available subtitle snapshot while translation continues in the background; later cues appear when the subtitle is loaded again. Completed bilingual sidecars keep the source subtitle format and are available to Plex clients. Matching individual language sidecars can be merged automatically. Turning bilingual subtitles off hides bilingual tracks from clients without deleting their files.

## Run Harbringer

Install Rust, then run:

```sh
cargo run
```

Harbringer reads its settings from `./config/config.toml` and creates its state database in the same directory. By default, it listens on port `32400`. At startup, it prints the address of the built-in web client.

Open `http://<server>:32400/` in a browser on your network. Use **Settings** to create a library and add a folder on the server. Run **Scan all libraries** to index the files, or leave automatic scanning enabled.

### Reset the web admin password

Stop Harbringer, then remove the admin account and its login sessions from the SQLite state database:

```sh
sqlite3 config/state.sqlite 'DELETE FROM web_sessions; DELETE FROM web_admin;'
```

Start Harbringer and open its web client to create a new admin password. If you use a custom config directory, run the command against `state.sqlite` beside that directory's `config.toml`. For Docker, run it against the `state.sqlite` file in the host directory mounted at `/config`.

### Docker Compose

The published image supports `linux/amd64` and `linux/arm64`. It runs on Linux hosts and in Docker Desktop's Linux-container mode on macOS and Windows.

Save this as `compose.yaml` and start Harbringer with `docker compose up -d`:

```yaml
services:
  harbringer:
    image: ghcr.io/danielb2/harbringer:latest
    ports:
      - "32400:32400"
    volumes:
      - ./config:/config
      - ./media:/media:ro
    restart: unless-stopped
```

Or run it directly without Compose:

```sh
docker run --name harbringer -p 32400:32400 -v "./config:/config" -v "./media:/media:ro" ghcr.io/danielb2/harbringer:latest
```

Open `http://localhost:32400/`. In **Settings**, add `/media` as a library folder.

The media mount is read-only by default while Harbringer is in alpha. For disposable test media, remove `:ro` (`./media:/media`) to let server actions delete files.

Bilingual subtitle generation writes progress and the finished sidecar beside the video. The read-only `:ro` media mount prevents that; use a writable media mount to generate bilingual sidecars. Playback-driven translation stops when playback stops and resumes from staged progress on a later play. Full-episode preparation can be started from that episode in the built-in web player.

Harbringer currently indexes visible `.mp4`, `.m4v`, `.mkv`, and `.webm` files. The **Files** view also shows other visible files in your library folders.

## Connect a Plex app

In the Plex app, connect to your server at:

```text
http://<server>:32400
```

Replace `<server>` with the hostname or IP address of the machine running Harbringer. You can link a Plex.tv account from Settings if the app needs account discovery.

Plex client support is still in progress. Plex Desktop playback works for supported H.264 Matroska media. Playback in Plex Web and other clients is not fully validated.

## Metadata and files

TMDB matching is available for movie and TV libraries. Other libraries are not matched. To keep metadata with your media when moving it, select sidecar metadata storage; Harbringer stores it in `.harbringer` folders beside media files. Back up the `config` directory to keep server settings, identity, and database-backed state.

Removing a folder from a library only detaches it. It does not delete the media files. Removing media from a folder is detected by a later scan.

## Current limitations

Harbringer is under active development and is not yet a complete replacement for Plex Media Server. API coverage and client behavior are incomplete. Playback support is limited: HLS and video codecs other than H.264 are not supported by the current transcoding path, and Plex Web playback remains under validation. Embedded subtitle extraction currently supports text tracks in Matroska files; image-based subtitles cannot be translated. Google Translate can rate-limit requests; completed bilingual cues are staged beside the video so a later attempt can resume.

See [`ROADMAP.md`](ROADMAP.md) for the compatibility status and remaining work.
