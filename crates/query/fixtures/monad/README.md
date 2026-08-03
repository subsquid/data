Blocks 45216-45220 of `s3://monad-mainnet/0000000000/0000000000-0001861119-57983595`,
carrying the chunk's first transaction.

`logs.parquet` is the chunk's own logs table, copied verbatim. It holds no rows, but
the arrow-rs version that wrote it still emitted one row group for the empty table.
Current arrow-rs emits no row group at all, so this file cannot be regenerated -
keep it as is. `blocks.parquet` and `transactions.parquet` are the trimmed block
window, with `_idx` renumbered.
