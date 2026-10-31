# mPlex Goal

mPlex is a proposed lightweight media server written in Rust. Its purpose is to replace the server side of Plex while reusing existing clients such as Plezy. It should eventually expose both Plex-compatible and Jellyfin-compatible APIs over a shared backend.

The central idea is to separate media delivery from client protocol compatibility. mPlex owns media presentation, client authentication, playback sessions, and streaming responses. Protocol adapters translate the same internal media objects and operations into the formats Plex and Jellyfin clients expect.

mPlex should be usable in two forms:

- A standalone executable that serves local files.
- An embeddable Rust library that another application can integrate without running a separate media-server process.

A major intended use is integration with Weaver and Scryer. Weaver handles Usenet downloading, decoding, repair, and extraction. Scryer handles media discovery, selection, and acquisition. mPlex would let existing media clients browse and play content made available by those applications.

The longer-term streaming goal is to preview or watch an NZB before the complete download finishes. Playback requests would drive buffering and prioritize the NNTP articles needed for the current position. Seeking would request the relevant data rather than wait for the entire file. This requires a seekable media-source interface and cooperation with Weaver’s download and extraction pipeline. It is a proposed capability, not something already verified in Weaver.

The same content could be played through Plezy using a compatibility API, or inside Scryer through an embedded player. Scryer would need UI and backend integration for the latter. Its existing provider plugin interfaces should not be assumed to support arbitrary streaming endpoints or player extensions.

Later, mPlex should provide the same kind of compatibility layer for Audiobookshelf, allowing Audiobookshelf clients to connect to mPlex and browse, play, and track audiobooks from configured libraries. Audiobooks are a later goal after the Plex and Jellyfin media-server work is established.

Keep the core small. Serving a local file should not require metadata scraping, a database, background scanning, transcoding, or external services. Rich metadata, persistent watch state, subtitles, transcoding, and other features can be optional additions. Avoid rebuilding the operational complexity that motivated replacing Plex.

The product does need a small persistent database as user-facing features arrive. It should store per-user viewing progress, server/account identity needed for Plex sessions, matched media identities, scraped metadata, and references or cached state for downloaded artwork and presentation metadata. Local files should be matched to movies and series so Plex can present a proper library rather than only filenames. Metadata and artwork work may happen asynchronously and must not prevent direct playback when a match is unavailable.

Plex compatibility includes more than returning library metadata. Server discovery, account registration, authentication, secure connections, and playback negotiation need investigation. Do not assume a modified client is necessary: the existing Go project https://github.com/braedonsaunders/ether claims to implement Plex server compatibility, including account registration and official-client playback. Study its implementation and validate those claims before choosing an approach.

The first milestone is deliberately tiny: one hardcoded local video appears in Plezy, plays directly, and supports seeking. Implement Plex first, then add Jellyfin using the same backend. This milestone proves the architecture; the overall goal is a reusable, lightweight media-server engine for both local files and on-demand sources.
