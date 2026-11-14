# harbringer

Project guidance for `harbringer`: the target is a 100% drop-in replacement for Plex's official Media Server. This includes documented PMS APIs and matching behavior in official Plex clients, including every client-visible capability and item menu. This is past the MVP stage; treat full compatibility as the goal, not a single-route or single-action milestone.

## Essential rules
- Never expand scope beyond what the user requested. Make only changes needed to fulfill the request; do not add adjacent features, cleanup, or speculative improvements. If broader work seems necessary, explain why and ask before proceeding.

- Read relevant code and current Plex request traces before changing behavior.
- Keep the protocol surface Plex-only. Do not add Jellyfin routes or mix adapters.
- Before implementing or changing any Plex API behavior, consult the official PMS API documentation at https://developer.plex.tv/pms and verify the operation, parameters, response formats, and status codes. Do not rely on request traces or assumptions in place of the documentation. If the documentation does not specify a detail or is unavailable, state that clearly and use current request traces and a reference PMS or compatible implementation where available; do not present an inference as documented behavior. Validate client-visible behavior, including the full item menus, against Plex clients; do not infer parity from route tests alone.
- For Plex webhooks, also consult the official support article at https://support.plex.tv/articles/115002267687-webhooks/ and inspect current Plex client traces before implementation. Webhook destination management is account-level; verify the account API behavior against traces and a compatible implementation when the PMS API reference does not document it. Do not replace existing Plex webhook destinations with an unrelated local URL list.
- Playback is a core requirement: support direct play, direct stream/remux, and transcoding when a Plex client requires it.
- Media inspection, remuxing, decoding, encoding, and transcoding must use portable in-process Rust libraries. Do not depend on installed commands such as `ffmpeg` or `ffprobe`.
- Keep platform support in mind when selecting media libraries and features. Do not make one operating system's hardware decoder mandatory.
- Preserve original-file serving and seeking when a client can play the source directly. Transcoding must be selected from client capabilities and media metadata, not used as a blanket path.
- Keep shared file handling and media logic reusable. Avoid duplicated protocol responses, route behavior, or UI implementations.
- Add or update tests for behavior changes, including success and failure paths. Keep the Plex endpoint inventory and route tests in sync with the official API documentation.
- Treat `tests/unit/plex_regressions.rs` as an append-only protected suite. Never edit, reformat, rename, or remove an existing test or expectation there unless the user explicitly authorizes it. Add new cases only; ask before changing an existing case.
- Do not commit secrets, generated artifacts, or changes unless explicitly asked.
- When asked to commit, use the commit subject as a concise summary; include a detailed body when useful to explain the change, rationale, and validation. The subject is not a substitute for the body.
- Whenever the project version is bumped, create a Git tag for that version after the version bump commit, using the `v` prefix (for example, `v0.1.1`).
- Before work begins on a task refer to https://developer.plex.tv/pms and check offcial API for a solution rather than guessing or checking logs. also consolt implementations from https://github.com/debridmediamanager/rex, https://github.com/braedonsaunders/ether/, and https://github.com/edde746/plezy

## Commands

- Build: `cargo build`
- Test: `cargo test`
- After every code change, run an appropriate build, test, or lint check and inspect its complete output for errors and warnings; fix new diagnostics instead of overlooking them.
- Before every code handoff, run the full `cargo test` suite. If it cannot run or fails, report that clearly and do not claim verification is complete.
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
