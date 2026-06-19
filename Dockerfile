# Multi-stage build for yacron2 (Rust).
#
#   docker build -t yacron2 .
#   docker run --rm -v "$PWD/my-crontab.yaml:/etc/yacron2.d/crontab.yaml" yacron2
#
# The resulting image is a small Debian-slim runtime containing only the
# statically-configured binary plus the OpenSSL/CA bundle needed for SMTP TLS.
# The IANA timezone database is bundled into the binary (via chrono-tz), so no
# system tzdata package is required.

# ---- builder ----------------------------------------------------------------
FROM rust:1-bookworm AS builder

# OpenSSL dev headers for the native-tls SMTP backend.
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Pre-build dependencies against a stub so they cache across source changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Build the real binary.
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ---- runtime ----------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/yacron2 /usr/local/bin/yacron2

# Default config location; mount your crontab YAML here, or override with -c.
VOLUME ["/etc/yacron2.d"]

ENTRYPOINT ["/usr/local/bin/yacron2"]
