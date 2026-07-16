//! Reproduction: a reorg-canceled tip batch leaves a permanent id gap that bricks restart.
//!
//! A canceled batch never persists its metadata, yet keeps its allocated id, so the next block
//! takes a higher one. The resulting hole in the BatchMetadata column family is never backfilled,
//! and every subsequent start replays a non-contiguous id sequence.

use std::path::Path;

use tempfile::TempDir;
use vprogs_core_test_utils::ResourceIdExt;
use vprogs_core_types::{AccessMetadata, ResourceId, SchedulerTransaction};
use vprogs_scheduling_scheduler::{ExecutionConfig, Scheduler};
use vprogs_scheduling_test_utils::Processor;
use vprogs_state_batch_metadata::BatchMetadata as StoredBatchMetadata;
use vprogs_storage_manager::StorageConfig;
use vprogs_storage_rocksdb_store::RocksDbStore;

/// A single-transaction batch writing to its own resource, so batches never contend.
fn write_tx(tx_id: usize, resource: usize) -> Vec<SchedulerTransaction<usize>> {
    vec![SchedulerTransaction::new(
        0,
        vec![AccessMetadata::write(ResourceId::for_test(resource))],
        tx_id,
    )]
}

/// Opens a scheduler over the store rooted at `path`, replaying whatever is already persisted.
fn open_scheduler(path: &Path) -> Scheduler<RocksDbStore, Processor> {
    let storage: RocksDbStore = RocksDbStore::open(path);
    Scheduler::new(
        ExecutionConfig::default().with_processor(Processor),
        StorageConfig::default().with_store(storage),
    )
}

/// A reorg canceling an in-flight tip batch strands its id, and the node never starts again.
///
/// The gap must be interior: `CanonicalChainManager::new` takes `base` from the first replayed
/// entry, so a leading gap restores fine and only a hole above the base strands the tip.
///
/// The second reorg target must be a fresh block hash. Flipping back onto the canceled block's
/// hash would reuse its retained id through the manager's reverse index and refill the gap.
#[test]
#[ignore = "repro for the reorg-canceled batch id gap: fails at restart until the gap is fixed"]
fn canceled_tip_batch_strands_an_id_and_bricks_restart() {
    // The debug_assert_eq! in CanonicalChainManager::new catches the non-contiguous replay and
    // aborts before the restart path runs, demonstrating a different failure than the one shipped
    // to users. Only a debug-assertions-off build exercises the real defect.
    assert!(
        !cfg!(debug_assertions),
        "run this repro with debug assertions off (cargo test --release), otherwise the \
         debug_assert_eq! in CanonicalChainManager::new masks the restart panic under test"
    );

    let temp_dir = TempDir::new().expect("failed to create temp dir");

    // Persist ids 1 and 3 while a reorg strands id 2.
    {
        let mut scheduler = open_scheduler(temp_dir.path());

        let batch1 = scheduler.schedule(1, write_tx(0, 1));
        batch1.wait_committed_blocking();
        assert_eq!(batch1.checkpoint().index(), 1);

        // The tip batch stays in flight: without waiting, its commit has not run yet.
        let batch2 = scheduler.schedule(2, write_tx(1, 2));

        // The reorg. rollback_to drives cancel_and_rollback, which cancels the tip batch and
        // rolls the canonical chain back, leaving the batch's commit a no-op.
        scheduler.rollback_to(1).expect("rollback should succeed");
        assert!(batch2.canceled(), "the tip batch must be canceled while in flight");
        assert_eq!(batch2.checkpoint().index(), 2, "the canceled batch held id 2");

        // Block 3 is a distinct hash, so the reverse index cannot hand back the retained id 2.
        let batch3 = scheduler.schedule(3, write_tx(2, 3));
        batch3.wait_committed_blocking();
        assert_eq!(batch3.checkpoint().index(), 3, "the next block takes a higher id");

        let store = scheduler.state().storage().store();
        let persisted: Vec<u64> =
            (1..=3).filter(|&id| StoredBatchMetadata::exists(&**store, id)).collect();
        assert_eq!(persisted, vec![1, 3], "the canceled id must leave an interior gap");

        scheduler.shutdown();
    }

    // Restart over the persisted metadata. The replay re-densifies id 3 onto id 2, so the tip has
    // no live entry and the ancestry walk panics: the node cannot start.
    {
        let scheduler = open_scheduler(temp_dir.path());
        scheduler.shutdown();
    }
}
