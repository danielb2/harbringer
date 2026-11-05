```
|              |         o
|---.,---.,---.|---.,---..,---.,---.,---.,---.
|   |,---||    |   ||    ||   ||   ||---'|
`   '`---^`    `---'`    ``   '`---|`---'`
                               `---'`
```

# Harbringer

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

## Run Harbringer

Install Rust, then run:

```sh
cargo run
```

Harbringer reads its settings from `./config/config.toml` and creates its state database in the same directory. By default, it listens on port `32400`. At startup, it prints the address of the built-in web client.

Open `http://<server>:32400/` in a browser on your network. Use **Settings** to create a library and add a folder on the server. Run **Scan all libraries** to index the files, or leave automatic scanning enabled.

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

Harbringer is under active development and is not yet a complete replacement for Plex Media Server. API coverage and client behavior are incomplete. Playback support is limited: HLS and video codecs other than H.264 are not supported by the current transcoding path, and Plex Web playback remains under validation. Some client features, including device registration and playback history, are not implemented.

See [`ROADMAP.md`](ROADMAP.md) for the compatibility status and remaining work.
