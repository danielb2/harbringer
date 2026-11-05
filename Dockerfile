FROM rust:1-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/harbringer /usr/local/bin/harbringer
RUN mkdir -p /config
VOLUME ["/config", "/media"]
EXPOSE 32400/tcp
ENTRYPOINT ["harbringer", "--config", "/config", "--bind", "0.0.0.0:32400"]
