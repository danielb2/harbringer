set shell := ["fish", "-c"]

# list justfile tasks
list:
    @just -l

# starts dev server
dev bind="0.0.0.0:32400":
    @mkdir -p config
    @touch config/config.toml
    @cargo watch --version >/dev/null 2>&1 || cargo install cargo-watch
    cargo watch --watch src --watch Cargo.toml --watch Cargo.lock --watch config/config.toml -x "run -- --bind '{{ bind }}'" | tee output.log

# runs the tests
test:
    cargo test

# build docker image
image:
    docker build -t harbringer:latest .

# build and publish docker image
publish:
    #!/usr/bin/env bash
    set -euo pipefail
    owner="$(gh repo view --json owner --jq .owner.login)"
    username="$(gh api user --jq .login)"
    image="ghcr.io/${owner}/harbringer:latest"
    gh auth token | docker login ghcr.io --username "$username" --password-stdin
    docker build -t "$image" .
    docker push "$image"

# restart local server
restart:
    kill -HUP (pgrep -x harbringer)
