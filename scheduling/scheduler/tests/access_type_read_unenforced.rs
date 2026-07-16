//! Isolates the host-side half of the `AccessType::Read` gap: a processor that writes a
//! `Read`-declared resource has its write silently discarded, with no error surfaced to anyone.
//!
//! The shared `vprogs_scheduling_test_utils::Processor` gates its writes on
//! `access_type == AccessType::Write`, which is precisely what the real risc0 guest does not do:
//! the real transaction processor increments every resource unconditionally. This file therefore
//! defines its own processor that writes unconditionally, so the scheduler is exercised the way the
//! real guest exercises it. A test using the shared mock would pass vacuously.
//!
//! This proves only that the write is dropped. That the dropped write diverges the settled root
//! from the host store, and wedges the lane, is proven end to end against the real `Vm` and the
//! real guest in `zk/backend/risc0/test-suite/tests/access_type_read_unenforced.rs`.

use tempfile::TempDir;
use vprogs_core_test_utils::ResourceIdExt;
use vprogs_core_types::{AccessMetadata, ResourceId, SchedulerTransaction};
use vprogs_scheduling_scheduler::{ExecutionConfig, Scheduler, TransactionContext};
use vprogs_state_version::StateVersion;
use vprogs_storage_manager::StorageConfig;
use vprogs_storage_rocksdb_store::RocksDbStore;
use vprogs_storage_types::Store;

/// Data the processor writes into every resource it is handed.
const WRITTEN: &[u8] = b"written";

/// A processor that writes every resource it is handed, regardless of the declared access type.
///
/// Mirrors the real risc0 transaction processor, which consults no access type:
/// `Resource::access_type()` has no call sites anywhere in the repository.
#[derive(Clone)]
struct UnconditionalWriter;

impl<S: Store> vprogs_scheduling_scheduler::Processor<S> for UnconditionalWriter {
    fn process_transaction(
        &self,
        ctx: &mut TransactionContext<S, Self>,
    ) -> Result<(), Self::Error> {
        for resource in ctx.resources_mut() {
            resource.data_mut().extend_from_slice(WRITTEN);
        }
        Ok(())
    }

    // This processor never proves, so its image ids only need to be stable.
    fn tx_image_id(&self) -> [u8; 32] {
        [0u8; 32]
    }

    fn batch_image_id(&self) -> [u8; 32] {
        [1u8; 32]
    }

    type Transaction = usize;
    type TransactionArtifact = Vec<u8>;
    type BatchArtifact = Vec<u8>;
    type AggregatorArtifact = Vec<u8>;
    type BatchMetadata = u64;
    type Error = ();
}

/// A write to a `Read`-declared resource must not be silently discarded.
///
/// `AccessHandle::commit_changes` forwards the handle's state to the access only when
/// `access_type == AccessType::Write`, and `ResourceAccess::set_read_state` pins a `Read` access's
/// written state equal to its read state before the transaction runs. The processor's write lands
/// on the handle's private `Arc` and evaporates when the handle drops. No error is returned and no
/// site notices.
///
/// Either enforcement point closes this: reject the transaction, or honor the write. The Write
/// control in the same batch shows the machinery works when the declaration matches.
#[test]
#[ignore = "repro: AccessType::Read is declared by the user and enforced by nobody; the write is \
            silently dropped"]
fn write_to_a_read_declared_resource_must_not_be_silently_dropped() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let storage: RocksDbStore = RocksDbStore::open(temp_dir.path());
    let mut scheduler = Scheduler::new(
        ExecutionConfig::default().with_processor(UnconditionalWriter),
        StorageConfig::default().with_store(storage.clone()),
    );

    let read_declared = ResourceId::for_test(1);
    let write_declared = ResourceId::for_test(2);

    let batch = scheduler.schedule(
        1,
        vec![
            SchedulerTransaction::new(0, vec![AccessMetadata::read(read_declared)], 0),
            SchedulerTransaction::new(1, vec![AccessMetadata::write(write_declared)], 1),
        ],
    );
    batch.wait_committed_blocking();

    // Control: the identically-written Write-declared resource is persisted.
    let control = StateVersion::from_latest_data(&storage, write_declared);
    assert_eq!(control.data().as_slice(), WRITTEN, "Write-declared resource should persist");
    assert_eq!(control.version(), 1, "Write-declared resource should advance to version 1");

    // The processor wrote this resource exactly as it wrote the control, and the transaction
    // returned Ok, yet the host kept the pre-execution state.
    let dropped = StateVersion::from_latest_data(&storage, read_declared);
    assert_eq!(
        dropped.data().as_slice(),
        WRITTEN,
        "the write to the Read-declared resource must not be silently discarded"
    );

    scheduler.shutdown();
}
