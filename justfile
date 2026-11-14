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
    #!/usr/bin/env bash
    set -euo pipefail
    if ! docker buildx version >/dev/null 2>&1; then
        printf 'Docker Buildx is required. Install it with: brew install docker-buildx\n' >&2
        exit 1
    fi
    build_date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    docker buildx build --load --build-arg "IMAGE_BUILD_DATE=$build_date" -t harbringer:latest .

# build and publish docker image
publish:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! docker buildx version >/dev/null 2>&1; then
        printf 'Docker Buildx is required. Install it with: brew install docker-buildx\n' >&2
        exit 1
    fi
    owner="$(gh repo view --json owner --jq .owner.login)"
    username="$(gh api user --jq .login)"
    image="ghcr.io/${owner}/harbringer:latest"
    gh auth token | docker login ghcr.io --username "$username" --password-stdin
    build_date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    docker buildx build --platform linux/amd64,linux/arm64 --push --build-arg "IMAGE_BUILD_DATE=$build_date" -t "$image" .

# restart local server
restart:
    kill -HUP (pgrep -x harbringer)
