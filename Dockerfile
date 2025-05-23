FROM rust:1.84-bookworm AS rust


FROM rust AS builder
RUN apt-get update && apt-get -y --no-install-recommends install \
    clang \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ADD .cargo .cargo
ADD Cargo.toml .
ADD Cargo.lock .
ADD crates crates


FROM builder AS node-builder
RUN cargo build -p sqd-node-example --release


FROM rust AS node-example
WORKDIR /app
COPY --from=node-builder /app/target/release/sqd-node-example .
RUN mkdir -p /etc/sqd/node && echo "{}" > /etc/sqd/node/datasets.json
VOLUME /var/sqd/node
ENTRYPOINT ["/app/sqd-node-example", "--db", "/var/sqd/node/db", "--datasets", "/etc/sqd/node/datasets.json"]
EXPOSE 3000


FROM builder AS archive-builder
RUN cargo build -p sqd-archive --release


FROM debian:bookworm-slim AS sqd-archive
RUN apt-get update && apt-get install ca-certificates -y
WORKDIR /app
COPY --from=archive-builder /app/target/release/sqd-archive .
ENTRYPOINT ["/app/sqd-archive"]
