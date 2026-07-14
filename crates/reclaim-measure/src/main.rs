//! Read-only measurement of reclaimable disk in a hotblocks RocksDB.
//!
//! Opens the live DB as a RocksDB *secondary instance* (no lock, does not disturb the
//! running primary) and reports how much `--startup-disk-reclaim` would file-unlink.
//!
//! Sizes come from `live_files()` metadata -- no SST opens. The rules deciding what is
//! unlinkable live in `sqd_storage::db::reclaim`, shared with the database itself.
//!
//! Usage: reclaim-measure <primary_db_dir> [secondary_scratch_dir]

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use rocksdb::{Options, DB};
use sqd_storage::db::{
    reclaim::{deleted_table_record_kind, reclaim_upper_bound, sst_is_unlinkable, watermark, DeletedTableRecordKind},
    Chunk, TableId, CF_CHUNKS, CF_DATASETS, CF_DELETED_TABLES, CF_DIRTY_TABLES, CF_TABLES
};

/// Scale to the unit that keeps the number readable -- a prod volume reports GiB, a test db
/// would otherwise report `0.0`.
fn human(bytes: u64) -> String {
    const UNITS: [(u64, &str); 3] = [(1 << 30, "GiB"), (1 << 20, "MiB"), (1 << 10, "KiB")];
    for (scale, unit) in UNITS {
        if bytes >= scale {
            return format!("{:.1} {unit}", bytes as f64 / scale as f64);
        }
    }
    format!("{bytes} B")
}

/// First 16 bytes of an SST boundary key = the owning `TableId`.
fn id_of(key: &Option<Vec<u8>>) -> Option<TableId> {
    key.as_ref()
        .filter(|k| k.len() >= 16)
        .and_then(|k| TableId::try_from_key(&k[..16]))
}

#[derive(PartialEq)]
enum Cat {
    Live,
    Dirty,
    Dead
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let primary = args.next().unwrap_or_else(|| "/run/db".to_string());
    let secondary = args.next().unwrap_or_else(|| "/tmp/reclaim-measure-sec".to_string());

    let mut opts = Options::default();
    // RocksDB warns against anything else on a secondary: the primary deletes obsolete
    // files freely, and only an eagerly-opened table cache keeps them readable here.
    opts.set_max_open_files(-1);

    let cfs = [CF_DATASETS, CF_CHUNKS, CF_TABLES, CF_DIRTY_TABLES, CF_DELETED_TABLES];
    eprintln!("[*] opening secondary on {primary} ...");
    let db = DB::open_cf_as_secondary(&opts, &primary, &secondary, cfs)
        .with_context(|| format!("open secondary instance on {primary}"))?;
    let _ = db.try_catch_up_with_primary();

    let cf_chunks = db.cf_handle(CF_CHUNKS).context("no CHUNKS cf")?;
    let cf_dirty = db.cf_handle(CF_DIRTY_TABLES).context("no DIRTY_TABLES cf")?;
    let cf_deleted = db.cf_handle(CF_DELETED_TABLES).context("no DELETED_TABLES cf")?;

    // Live table ids = referenced by a committed chunk.
    eprintln!("[*] scanning CHUNKS ...");
    let mut live: BTreeSet<TableId> = BTreeSet::new();
    let mut chunks = 0u64;
    {
        let mut it = db.raw_iterator_cf(&cf_chunks);
        it.seek_to_first();
        while it.valid() {
            let chunk: Chunk = borsh::from_slice(it.value().context("missing chunk value")?).context("decode chunk")?;
            for id in chunk.tables().values() {
                live.insert(*id);
            }
            chunks += 1;
            it.next();
        }
        it.status()?;
    }
    eprintln!("[*] {chunks} chunks, {} live tables", live.len());

    // Dirty markers = built but not committed (orphans pin the watermark).
    let mut dirty: BTreeSet<TableId> = BTreeSet::new();
    {
        let mut it = db.raw_iterator_cf(&cf_dirty);
        it.seek_to_first();
        while it.valid() {
            if let Some(id) = TableId::try_from_key(it.key().context("dirty key")?) {
                dirty.insert(id);
            }
            it.next();
        }
        it.status()?;
    }

    // Deletion lifecycle records: only DeleteRequested still awaits point tombstones.
    let (mut pending_deletes, mut unstamped_purges, mut retained_purges, mut malformed_deletes) =
        (0u64, 0u64, 0u64, 0u64);
    {
        let mut it = db.raw_iterator_cf(&cf_deleted);
        it.seek_to_first();
        while it.valid() {
            match deleted_table_record_kind(it.value().context("missing deleted-table value")?) {
                DeletedTableRecordKind::DeleteRequested => pending_deletes += 1,
                DeletedTableRecordKind::PurgedUnstamped => unstamped_purges += 1,
                DeletedTableRecordKind::Purged => retained_purges += 1,
                DeletedTableRecordKind::Malformed => malformed_deletes += 1
            }
            it.next();
        }
        it.status()?;
    }

    // The watermark today, and the one a startup orphan purge would leave behind.
    let wm_now = watermark(live.iter().copied(), dirty.iter().copied());
    let bound_now = reclaim_upper_bound(wm_now);
    let bound_after_purge = reclaim_upper_bound(watermark(live.iter().copied(), None));

    let cat = |id: &TableId| -> Cat {
        if live.contains(id) {
            Cat::Live
        } else if dirty.contains(id) {
            Cat::Dirty
        } else {
            Cat::Dead
        }
    };

    eprintln!("[*] reading live SST file metadata ...");
    let files = db.live_files().context("live_files")?;

    let (mut total, mut live_b, mut dirty_b, mut dead_b, mut mixed_b) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let (mut below_now, mut below_purge, mut stuck_in_l0) = (0u64, 0u64, 0u64);
    let mut n_files = 0u64;

    for f in &files {
        if f.column_family_name != CF_TABLES {
            continue;
        }
        let sz = f.size as u64;
        total += sz;
        n_files += 1;

        // A file whose two ends are dead but which spans a live table in between still
        // counts dead, so `dead_b` is an upper bound.
        match (id_of(&f.start_key), id_of(&f.end_key)) {
            (Some(a), Some(b)) => match (cat(&a), cat(&b)) {
                (Cat::Live, Cat::Live) => live_b += sz,
                (Cat::Dirty, Cat::Dirty) => dirty_b += sz,
                (Cat::Dead, Cat::Dead) => dead_b += sz,
                _ => mixed_b += sz
            },
            _ => mixed_b += sz
        }

        let end = f.end_key.as_deref();
        if sst_is_unlinkable(f.level, end, &bound_now) {
            below_now += sz;
        }
        if sst_is_unlinkable(f.level, end, &bound_after_purge) {
            below_purge += sz;
        }
        // Dead bytes the unlink cannot touch, because DeleteFilesInRange skips level 0.
        if f.level == 0 && sst_is_unlinkable(1, end, &bound_after_purge) {
            stuck_in_l0 += sz;
        }
    }

    println!("== reclaim-measure: {primary} ==");
    println!("chunks                  : {chunks}");
    println!("live tables             : {}", live.len());
    println!("dirty markers (orphans) : {}", dirty.len());
    println!("pending deletes         : {pending_deletes}");
    println!("unstamped purges        : {unstamped_purges}");
    println!("retained purge records  : {retained_purges}");
    println!("malformed delete records: {malformed_deletes}");
    println!("TABLES sst files        : {n_files}");
    match wm_now {
        Some(w) => println!("watermark (live+dirty)  : {w}"),
        None => println!("watermark (live+dirty)  : NONE (no live tables)")
    }
    println!();
    println!("CF_TABLES total         : {:>12}", human(total));
    println!("  live  (chunk-ref'd)   : {:>12}", human(live_b));
    println!("  dirty (orphan builds) : {:>12}", human(dirty_b));
    println!("  DEAD  (upper bound)   : {:>12}", human(dead_b));
    println!("  mixed (boundary SSTs) : {:>12}", human(mixed_b));
    println!();
    println!("file-unlinkable at startup (whole SSTs below wm, above L0):");
    println!("  below wm now          : {:>12}", human(below_now));
    println!("  below wm after purge  : {:>12}", human(below_purge));
    println!(
        "  below wm but in L0    : {:>12}  (unlink skips L0; compaction only)",
        human(stuck_in_l0)
    );

    Ok(())
}
