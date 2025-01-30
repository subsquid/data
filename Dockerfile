FROM rust:1.84-bookworm AS rust
RUN cargo install cargo-chef --locked --version "0.1.70" \
    && rm -rf $CARGO_HOME/registry/


FROM rust AS planner
WORKDIR /app
ADD Cargo.toml .
ADD Cargo.lock .
ADD crates crates
RUN cargo chef prepare --recipe-path recipe.json


FROM rust AS builder
RUN apt-get update && apt-get -y --no-install-recommends install \
    build-essential \
    clang \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
ADD Cargo.toml .
ADD Cargo.lock .
ADD crates crates


FROM builder AS node-builder
RUN cargo build -p sqd-node-example --release