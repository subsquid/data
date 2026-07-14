//! Integer properties back the `hotblocks_rocksdb_*` collector. They must stay available
//! without `enable_statistics()` because production disables RocksDB statistics by default.

use sqd_storage::db::{DatabaseSettings, CF_CHUNKS, CF_DATASETS, CF_DELETED_TABLES, CF_DIRTY_TABLES, CF_TABLES};

const DB_WIDE_PROPERTIES: &[&str] = &[
    "rocksdb.is-write-stopped",
    "rocksdb.actual-delayed-write-rate",
    "rocksdb.num-running-compactions",
    "rocksdb.num-running-flushes",
    "rocksdb.num-snapshots",
    "rocksdb.oldest-snapshot-time"
];

const PER_CF_PROPERTIES: &[&str] = &[
    "rocksdb.num-files-at-level0",
    "rocksdb.estimate-pending-compaction-bytes",
    "rocksdb.compaction-pending",
    "rocksdb.num-immutable-mem-table",
    "rocksdb.mem-table-flush-pending",
    "rocksdb.cur-size-all-mem-tables",
    "rocksdb.estimate-num-keys",
    "rocksdb.background-errors",
    "rocksdb.live-sst-files-size"
];

const COLUMN_FAMILIES: &[&str] = &[CF_DATASETS, CF_CHUNKS, CF_TABLES, CF_DIRTY_TABLES, CF_DELETED_TABLES];

#[test]
fn collector_properties_are_available_without_statistics() {
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseSettings::default().open(dir.path()).unwrap();

    for property in DB_WIDE_PROPERTIES {
        assert!(
            db.get_int_property(CF_TABLES, property).unwrap().is_some(),
            "DB-wide property {property} returned None"
        );
    }

    for cf in COLUMN_FAMILIES {
        for property in PER_CF_PROPERTIES {
            assert!(
                db.get_int_property(cf, property).unwrap().is_some(),
                "property {property} on column family {cf} returned None"
            );
        }
    }

    assert_eq!(
        db.get_int_property(CF_TABLES, "rocksdb.is-write-stopped").unwrap(),
        Some(0)
    );
    assert_eq!(
        db.get_int_property("UNKNOWN", "rocksdb.is-write-stopped").unwrap(),
        None
    );
    assert_eq!(
        db.get_int_property(CF_TABLES, "rocksdb.unknown-property").unwrap(),
        None
    );
}
