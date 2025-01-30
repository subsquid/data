FROM rust:1.84-bookworm AS rust


FROM rust AS builder
RUN apt-get update && apt-get -y --no-install-recommends install \
    clang \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ADD Cargo.toml .
ADD Cargo.lock .
ADD crates crates
RUN cargo fetch


FROM builder AS node-builder
RUN cargo build -p sqd-node-example --release


FROM rust AS node
WORKDIR /app
COPY --from=node-builder /app/target/release/sqd-node-example .
RUN mkdir -p /etc/sqd/node && echo "{}" > /etc/sqd/node/datasets.json
VOLUME /var/sqd/node
ENTRYPOINT ["/app/sqd-node-example", "--db", "/var/sqd/node/db", "--datasets", "/etc/sqd/node/datasets.json"]
EXPOSE 3000