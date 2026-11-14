FROM --platform=$BUILDPLATFORM rust:1-bookworm AS builder
ARG BUILDARCH
ARG TARGETARCH
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN set -eux; \
    case "$TARGETARCH" in \
        amd64) rust_target=x86_64-unknown-linux-gnu; cross_package=gcc-x86-64-linux-gnu; cross_libc_dev=libc6-dev-amd64-cross; cross_cc=x86_64-linux-gnu-gcc; cross_ar=x86_64-linux-gnu-ar; linker_var=CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER; cc_var=CC_x86_64_unknown_linux_gnu; ar_var=AR_x86_64_unknown_linux_gnu ;; \
        arm64) rust_target=aarch64-unknown-linux-gnu; cross_package=gcc-aarch64-linux-gnu; cross_libc_dev=libc6-dev-arm64-cross; cross_cc=aarch64-linux-gnu-gcc; cross_ar=aarch64-linux-gnu-ar; linker_var=CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER; cc_var=CC_aarch64_unknown_linux_gnu; ar_var=AR_aarch64_unknown_linux_gnu ;; \
        *) echo "Unsupported target architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    rustup target add "$rust_target"; \
    if [ "$BUILDARCH" != "$TARGETARCH" ]; then \
        apt-get update; \
        apt-get install -y --no-install-recommends "$cross_package" "$cross_libc_dev"; \
        rm -rf /var/lib/apt/lists/*; \
        env "$linker_var=$cross_cc" "$cc_var=$cross_cc" "$ar_var=$cross_ar" cargo build --release --target "$rust_target"; \
    else \
        cargo build --release --target "$rust_target"; \
    fi; \
    cp "target/$rust_target/release/harbringer" /build/harbringer

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ARG IMAGE_BUILD_DATE=unknown
ENV IMAGE_BUILD_DATE=${IMAGE_BUILD_DATE}
COPY --from=builder /build/harbringer /usr/local/bin/harbringer
RUN mkdir -p /config
VOLUME ["/config", "/media"]
EXPOSE 32400/tcp
ENTRYPOINT ["harbringer", "--config", "/config", "--bind", "0.0.0.0:32400"]
