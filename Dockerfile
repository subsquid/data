FROM rust:1.84-bookworm AS builder
RUN apt-get update && apt-get -y install build-essential clang
ADD Cargo.toml .
ADD Cargo.lock .
ADD crates crates
RUN cargo fetch


FROM builder AS node-builder
RUN cargo build -p sqd-node-example --release