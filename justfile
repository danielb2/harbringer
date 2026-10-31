dev directory token="development-token" bind="0.0.0.0:8096":
    @cargo watch --version >/dev/null 2>&1 || cargo install cargo-watch
    cargo watch --watch src --watch Cargo.toml --watch Cargo.lock -x "run -- --dir '{{ directory }}' --token '{{ token }}' --bind '{{ bind }}'"
