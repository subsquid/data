use std::{collections::BTreeSet, sync::Arc};

use parking_lot::Mutex;

use crate::db::table_id::TableId;

/// Immutable inputs that keep a runtime unlink below every table build concurrent with it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReclaimFence {
    /// Smallest build active when the fence was taken. It remains pinned even if that build
    /// finishes before the later RocksDB metadata snapshot is created.
    pub(crate) active_build_watermark: Option<TableId>,
    /// Unused process-monotonic id. Every build registered after the fence sorts above it.
    pub(crate) boundary: TableId
}

/// Tracks table ids from allocation until their data and dirty marker are durable.
///
/// The mutex is held only for set updates and reclaim-fence creation; table construction and
/// reclaim scans never retain it.
#[derive(Default)]
pub(crate) struct InflightTableRegistry {
    ids: Mutex<BTreeSet<TableId>>
}

impl InflightTableRegistry {
    pub(crate) fn register(self: &Arc<Self>) -> InflightTableGuard {
        let mut ids = self.ids.lock();
        let table_id = TableId::new();
        let inserted = ids.insert(table_id);
        debug_assert!(inserted);
        InflightTableGuard {
            registry: Arc::clone(self),
            table_id
        }
    }

    pub(crate) fn reclaim_fence(&self) -> ReclaimFence {
        let ids = self.ids.lock();
        let active_build_watermark = ids.first().copied();
        let boundary = TableId::new();
        debug_assert!(active_build_watermark.is_none_or(|id| id < boundary));
        ReclaimFence {
            active_build_watermark,
            boundary
        }
    }
}

pub(crate) struct InflightTableGuard {
    registry: Arc<InflightTableRegistry>,
    table_id: TableId
}

impl InflightTableGuard {
    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }
}

impl Drop for InflightTableGuard {
    fn drop(&mut self) {
        let removed = self.registry.ids.lock().remove(&self.table_id);
        debug_assert!(removed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fence_captures_active_builds_and_bounds_later_ones() {
        let registry = Arc::new(InflightTableRegistry::default());
        let first_active = registry.register();
        let second_active = registry.register();

        let fence = registry.reclaim_fence();
        assert_eq!(fence.active_build_watermark, Some(first_active.table_id()));
        assert!(second_active.table_id() < fence.boundary);

        let later = registry.register();
        assert!(fence.boundary < later.table_id());
    }

    #[test]
    fn fence_keeps_a_builder_that_finishes_before_the_metadata_scan() {
        let registry = Arc::new(InflightTableRegistry::default());
        let active = registry.register();
        let active_id = active.table_id();

        let fence = registry.reclaim_fence();
        drop(active);

        assert_eq!(fence.active_build_watermark, Some(active_id));
        assert!(registry.reclaim_fence().active_build_watermark.is_none());
    }
}
