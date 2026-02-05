# SQD Data Processing Crates

This repo contains Rust crates for blockchain data processing, indexing, and querying. They are used in various SQD components.

## Crate Overview

### Core Primitives & Types

| Crate | Description |
|-------|-------------|
| [`sqd-primitives`](crates/primitives/) | Basic types, range types, and SID (sequence ID) primitives used throughout the system |
| [`sqd-dataset`](crates/dataset/) | Dataset descriptions, table schemas, and schema options for defining data structures |
| [`sqd-array`](crates/array/) | Arrow array operations including building, slicing, sorting, I/O, and chunking utilities |
| [`sqd-bloom-filter`](crates/bloom-filter/) | Bloom filter implementation for arrow buffers |

### Data Models & Processing

| Crate | Description |
|-------|-------------|
| [`sqd-data`](crates/data/) | Data models for various blockchain protocols. Used in the query engine and parquet writers |
| [`sqd-data-core`](crates/data-core/) | Core data processing utilities: chunk building, table processing, serialization, and sorting algorithms |
| [`sqd-data-client`](crates/data-client/) | HTTP client for fetching data from SQD data sources with streaming support |
| [`sqd-data-source`](crates/data-source/) | Abstractions for standard and mapped data sources |

### Query & Storage

| Crate | Description |
|-------|-------------|
| [`sqd-query`](crates/query/) | Query engine used by the Worker |
| [`sqd-query-example`](crates/query-example/) | Example demonstrating how to use the query engine with Parquet files |
| [`sqd-storage`](crates/storage/) | RocksDB-based storage layer with key-value store and table management |
| [`sqd-polars`](crates/polars/) | Polars DataFrame integration for high-performance data analysis |

### Services & Ingestion

| Crate | Description |
|-------|-------------|
| [`sqd-archive`](crates/archive/) | Archive service for ingesting and storing data to S3 with layout management, progress tracking, and Prometheus metrics |
| [`sqd-bds`](crates/bds/) | Big Data Service (WIP) - Cassandra-based data storage |
| [`sqd-hotblocks`](crates/hotblocks/) | Hotblocks database with portal-like API |

## Building Docker image

The ["docker"](/.github/workflows/docker.yaml) workflow should be triggered manually with the following inputs:
- `target` — either `hotblocks` or `sqd-archive`.
- `tag` — a 8-byte hash of the commit. This will be the published docker tag.
- `platforms` — platforms to build for. Using only `linux/amd64` instead of default values can save a lot of building time.
