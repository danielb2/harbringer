dev bind="0.0.0.0:32400":
    @mkdir -p config
    @touch config/config.toml
    @cargo watch --version >/dev/null 2>&1 || cargo install cargo-watch
    cargo watch --watch src --watch Cargo.toml --watch Cargo.lock --watch config/config.toml -x "run -- --bind '{{ bind }}'"

test:
    cargo test
