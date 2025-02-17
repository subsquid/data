FROM rust:1.84-bookworm AS rust


FROM rust AS builder
RUN apt-get update && apt-get -y --no-install-recommends install \
    clang \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ADD Cargo.toml .
ADD Cargo.lock .
ADD crates crates


FROM builder AS hotblocks-builder
RUN cargo build -p sqd-hotblocks-example --release


FROM rust AS hotblocks-example
WORKDIR /app
COPY --from=hotblocks-builder /app/target/release/sqd-hotblocks-example .
RUN mkdir -p /etc/sqd/hotblocks && echo "{}" > /etc/sqd/hotblocks/datasets.json
VOLUME /var/sqd/hotblocks
ENTRYPOINT ["/app/sqd-hotblocks-example", "--db", "/var/sqd/hotblocks/db", "--datasets", "/etc/sqd/hotblocks/datasets.json"]
EXPOSE 3000