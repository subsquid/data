//! `get_int_property` backs the `hotblocks_rocksdb_*` gauges; it must work without
//! `enable_statistics()` (prod default). Pins the property names -- a typo => silent None.

use sqd_storage::db::{DatabaseSettings, CF_CHUNKS, CF_TABLES};

const DB_WIDE_PROPS: &[&str] = &[
    "rocksdb.is-write-stopped",
    "rocksdb.actual-delayed-write-rate",
    "rocksdb.num-running-compactions",
    "rocksdb.num-running-flushes"
];

const PER_CF_PROPS: &[&str] = &[
    "rocksdb.num-files-at-level0",
    "rocksdb.estimate-pending-compaction-bytes",
    "rocksdb.num-immutable-mem-table",
    "rocksdb.mem-table-flush-pending"
];

#[test]
fn int_properties_available_without_stats() {
    let dir = tempfile::tempdir().unwrap();
    // Default settings => rocksdb stats disabled, mirroring production.
    let db = DatabaseSettings::default().open(dir.path()).unwrap();

    for prop in DB_WIDE_PROPS {
        assert!(
            db.get_int_property(CF_TABLES, prop).unwrap().is_some(),
            "db-wide property {prop} returned None"
        );
    }

    for cf in [CF_CHUNKS, CF_TABLES] {
        for prop in PER_CF_PROPS {
            assert!(
                db.get_int_property(cf, prop).unwrap().is_some(),
                "property {prop} on cf {cf} returned None"
            );
        }
    }

    // A fresh database is not stalled.
    assert_eq!(
        db.get_int_property(CF_TABLES, "rocksdb.is-write-stopped").unwrap(),
        Some(0)
    );

    // An unknown column family yields None rather than erroring.
    assert_eq!(db.get_int_property("NOPE", "rocksdb.is-write-stopped").unwrap(), None);
}
