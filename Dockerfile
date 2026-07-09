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
RUN cargo build -p sqd-hotblocks -p reclaim-measure --release


FROM rust AS hotblocks
WORKDIR /app
COPY --from=hotblocks-builder /app/target/release/sqd-hotblocks .
# Read-only probe: opens the live db as a RocksDB secondary instance and reports how much
# --startup-disk-reclaim would free. Run it in the pod before turning that flag on.
COPY --from=hotblocks-builder /app/target/release/reclaim-measure .
ENTRYPOINT ["/app/sqd-hotblocks"]


FROM builder AS hotblocks-retain-builder
RUN cargo build -p sqd-hotblocks-retain --release


FROM rust AS hotblocks-retain
WORKDIR /app
COPY --from=hotblocks-retain-builder /app/target/release/sqd-hotblocks-retain .
ENTRYPOINT ["/app/sqd-hotblocks-retain"]


FROM builder AS archive-builder
RUN cargo build -p sqd-archive --release


FROM debian:bookworm-slim AS sqd-archive
RUN apt-get update && apt-get install ca-certificates -y
WORKDIR /app
COPY --from=archive-builder /app/target/release/sqd-archive .
ENTRYPOINT ["/app/sqd-archive"]
