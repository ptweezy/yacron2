# A simple example of baking a crontab into a yacron2 image.
#
# The build context must be the repository ROOT (so the Rust sources are
# available):
#
#   docker build -f example/docker/Dockerfile -t yacron2demo .
#   docker run --rm -ti yacron2demo -l DEBUG     # Ctrl-C to stop

# ---- builder ----
FROM rust:1-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ---- runtime ----
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/yacron2 /usr/local/bin/yacron2
COPY example/docker/yacron2tab.yaml /etc/yacron2.d/yacron2tab.yaml
ENTRYPOINT ["yacron2"]
