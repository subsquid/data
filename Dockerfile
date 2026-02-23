FROM rust:1.89-bookworm AS rust


FROM rust AS builder
RUN apt-get update && apt-get -y --no-install-recommends install \
    clang \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ADD .cargo .cargo
ADD Cargo.toml .
ADD Cargo.lock .
ADD crates crates


FROM builder AS hotblocks-builder
RUN cargo build -p sqd-hotblocks --release


FROM rust AS hotblocks
WORKDIR /app
COPY --from=hotblocks-builder /app/target/release/sqd-hotblocks .
ENTRYPOINT ["/app/sqd-hotblocks"]


FROM builder AS hotblocks-sidecar-builder
RUN cargo build -p sqd-hotblocks-sidecar --release


FROM rust AS hotblocks-sidecar
WORKDIR /app
COPY --from=hotblocks-sidecar-builder /app/target/release/sqd-hotblocks-sidecar .
ENTRYPOINT ["/app/sqd-hotblocks-sidecar"]


FROM builder AS archive-builder
RUN cargo build -p sqd-archive --release


FROM debian:bookworm-slim AS sqd-archive
RUN apt-get update && apt-get install ca-certificates -y
WORKDIR /app
COPY --from=archive-builder /app/target/release/sqd-archive .
ENTRYPOINT ["/app/sqd-archive"]
