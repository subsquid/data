# syntax=docker/dockerfile:1
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

# BuildKit cache mounts turn every build into an incremental one: only changed crates
# recompile instead of the whole dependency graph. They require a persistent builder to be
# useful — see .github/workflows/docker.yaml (the `dev-server-cache` buildx builder) and the
# github-runner Ansible role that keeps the builder's cache volume alive across jobs.
#
#   - /usr/local/cargo/{registry,git}: downloaded crate sources; shared across targets/arches.
#   - /app/target: compiled artifacts; scoped per-arch via id=...-$TARGETARCH so amd64 and
#     arm64 don't clobber each other's object files when both are built at once.
#   - sharing=locked: two runners share one builder, so serialize access to keep cargo from
#     corrupting the shared dirs when two builds overlap.
#
# The target/ dir is a cache mount, so its contents are NOT part of the image layer. Each
# builder stage copies the finished binaries out to /out, and the runtime stage picks them up
# from there.


FROM builder AS hotblocks-builder
ARG TARGETARCH
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,id=cargo-target-${TARGETARCH},sharing=locked \
    cargo build -p sqd-hotblocks -p reclaim-measure --release \
    && mkdir -p /out \
    && cp target/release/sqd-hotblocks target/release/reclaim-measure /out/


FROM rust AS hotblocks
WORKDIR /app
COPY --from=hotblocks-builder /out/sqd-hotblocks .
# Read-only probe: opens the live db as a RocksDB secondary instance and reports how much
# --startup-disk-reclaim would free. Run it in the pod before turning that flag on.
COPY --from=hotblocks-builder /out/reclaim-measure .
ENTRYPOINT ["/app/sqd-hotblocks"]


FROM builder AS hotblocks-retain-builder
ARG TARGETARCH
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,id=cargo-target-${TARGETARCH},sharing=locked \
    cargo build -p sqd-hotblocks-retain --release \
    && mkdir -p /out \
    && cp target/release/sqd-hotblocks-retain /out/


FROM rust AS hotblocks-retain
WORKDIR /app
COPY --from=hotblocks-retain-builder /out/sqd-hotblocks-retain .
ENTRYPOINT ["/app/sqd-hotblocks-retain"]


FROM builder AS flush-bench-builder
ARG TARGETARCH
# `cargo bench --no-run` emits target/release/deps/flush_spill-<hash>; the cache mount may
# hold stale hashes, so take the newest non-.d artifact.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,id=cargo-target-${TARGETARCH},sharing=locked \
    cargo bench -p sqd-data --bench flush_spill --no-run \
    && mkdir -p /out \
    && cp "$(ls -t target/release/deps/flush_spill-* | grep -v '\.d$' | head -1)" /out/flush_spill


FROM debian:bookworm-slim AS flush-bench
WORKDIR /app
COPY --from=flush-bench-builder /out/flush_spill .
ENTRYPOINT ["/app/flush_spill"]


FROM builder AS archive-builder
ARG TARGETARCH
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,id=cargo-target-${TARGETARCH},sharing=locked \
    cargo build -p sqd-archive --release \
    && mkdir -p /out \
    && cp target/release/sqd-archive /out/


# keep this stage last: it is the default target of a bare `docker build .`
FROM debian:bookworm-slim AS sqd-archive
RUN apt-get update && apt-get install ca-certificates -y
WORKDIR /app
COPY --from=archive-builder /out/sqd-archive .
ENTRYPOINT ["/app/sqd-archive"]
