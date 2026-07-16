//! Reproduces the divergence between the proven state root and the host store when a transaction
//! declares a resource `AccessType::Read` and the guest writes it anyway.
//!
//! The declaration is deserialized verbatim out of the user-supplied L1 payload by
//! `SchedulerTransaction::extract_payload` with no validation. Nothing downstream checks it against
//! what the guest actually did: `zk/` contains no reference to `access_type` at all, and the output
//! commitment keys purely on `Resource::is_dirty()`. The only three sites that consume the
//! declaration are host-side (`resource_access.rs` once, `access_handle.rs` twice) and all three
//! discard the write rather than reject the transaction.
//!
//! Both tests assert the invariant that must hold, so both fail against unfixed code. Either
//! enforcement point closes them: reject a transaction whose declaration does not cover what it
//! wrote, or honor the write. Un-ignore them as the acceptance criterion for a fix.
//!
//! Both run the real `Vm`, the real risc0 transaction-processor guest, the real batch-processor
//! guest, and the real scheduler. No mock processor is involved. The two mock processors
//! (`scheduling/test-utils/src/processor.rs`, `node/test-utils/src/vm.rs`) do gate on
//! `access_type` and would mask the gap; the real guest increments every resource unconditionally.

use tempfile::TempDir;
use vprogs_core_smt::{EMPTY_HASH, Tree as _};
use vprogs_core_test_utils::ResourceIdExt;
use vprogs_core_types::{AccessMetadata, ResourceId, SchedulerTransaction};
use vprogs_l1_types::{ChainBlockMetadata, L1Transaction};
use vprogs_scheduling_scheduler::{ExecutionConfig, Scheduler};
use vprogs_state_version::StateVersion;
use vprogs_storage_manager::StorageConfig;
use vprogs_storage_rocksdb_store::RocksDbStore;
use vprogs_zk_abi::batch_processor::StateTransition;
use vprogs_zk_backend_risc0_api::Backend;
use vprogs_zk_backend_risc0_test_suite::{batch_processor_elf, transaction_processor_elf};
use vprogs_zk_batch_prover::Backend as _;
use vprogs_zk_vm::{ProvingPipeline, Vm};

/// Resource the attacker declares `Read` and the guest writes anyway.
const TARGET: usize = 1;

/// Builds a scheduler whose processor is the real `Vm` over the real risc0 guests.
fn real_vm_scheduler(storage: &RocksDbStore) -> Scheduler<RocksDbStore, Vm<Backend, RocksDbStore>> {
    let backend = Backend::new(&transaction_processor_elf(), &batch_processor_elf());
    let proving = ProvingPipeline::batch(backend.clone(), storage.clone());
    let vm = Vm::new(backend, proving);

    Scheduler::new(
        ExecutionConfig::default().with_processor(vm),
        StorageConfig::default().with_store(storage.clone()),
    )
}

/// Schedules one transaction against `TARGET` under the given declaration and returns the proven
/// `(prev_root, new_root)` for the resulting batch.
fn settle_one(
    scheduler: &mut Scheduler<RocksDbStore, Vm<Backend, RocksDbStore>>,
    access: AccessMetadata,
    payload: Vec<u8>,
) -> ([u8; 32], [u8; 32]) {
    let mut tx = L1Transaction::default();
    tx.payload = payload;

    let batch = scheduler
        .schedule(ChainBlockMetadata::default(), vec![SchedulerTransaction::new(tx, vec![access])]);

    batch.wait_committed_blocking();
    batch.wait_artifact_published_blocking();

    let journal = Backend::journal_bytes(&batch.artifact());
    match StateTransition::decode(&journal).expect("journal should decode") {
        StateTransition::Success { prev_root, new_root, .. } => (*prev_root, *new_root),
        StateTransition::Error(e) => panic!("expected a successful batch, got error: {e}"),
    }
}

/// The settled root and the host store's root must agree for every batch.
///
/// The guest marks the `Read`-declared resource dirty and journals `Changed(hash(new_data))`; the
/// batch processor folds that hash into `new_root`. Host-side, `AccessHandle::commit_changes`
/// drops the write because the declaration says `Read`, so the SMT commits the pre-execution data.
/// The batch's own proof still succeeds: it is internally consistent. Only the host disagrees, and
/// no site anywhere compares the two.
///
/// This proves the divergence only. It does not prove that the lane wedges.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "repro: AccessType::Read is enforced by nobody; the guest's write diverges the settled \
            root from the host store"]
async fn settled_root_must_match_host_root_when_guest_writes_a_read_declared_resource() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let storage: RocksDbStore = RocksDbStore::open(temp_dir.path());
    let mut scheduler = real_vm_scheduler(&storage);

    let (prev_root, new_root) = settle_one(
        &mut scheduler,
        AccessMetadata::read(ResourceId::for_test(TARGET)),
        vec![1, 2, 3],
    );
    assert_eq!(prev_root, EMPTY_HASH, "prev_root should be empty (no prior state)");

    // The write evaporated host-side: the handle's version bump never reached the state diff, so
    // no version 1 data exists for the resource at all.
    assert_eq!(
        StateVersion::get(&storage, 1, &ResourceId::for_test(TARGET)),
        None,
        "host store holds no version 1 data: the Read-declared write was dropped"
    );

    // The invariant. The proof says the resource changed; the host's own state says it did not.
    assert_eq!(
        new_root,
        storage.root(1),
        "settled new_root must equal the host store's root for the same batch"
    );

    scheduler.shutdown();
}

/// Every batch's `prev_root` must equal the `new_root` settled by the batch before it.
///
/// This is the wedge. Each batch's `prev_root` is proved from an SMT proof the batch prover reads
/// out of the host store, which never recorded the write, so it can no longer equal the divergent
/// batch's settled `new_root`. Every later batch inherits the break: the offending L1 transaction
/// is permanently in the chain, so any replay re-derives the same divergence.
///
/// The trigger is one L1 carrier transaction from any user, with no keys or privileges.
///
/// On branches that carry a batch aggregator, this same break is what fails its
/// `prev_state == prev.new_state` assert. Master has no aggregator, so the break is asserted here
/// at the proof-chain level it already exists at.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "repro: AccessType::Read is enforced by nobody; the divergent batch permanently breaks \
            chain continuity"]
async fn next_batch_must_chain_onto_the_root_settled_by_a_read_declared_write() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let storage: RocksDbStore = RocksDbStore::open(temp_dir.path());
    let mut scheduler = real_vm_scheduler(&storage);

    // Batch 1: the offending transaction.
    let (_, divergent_new_root) = settle_one(
        &mut scheduler,
        AccessMetadata::read(ResourceId::for_test(TARGET)),
        vec![1, 2, 3],
    );

    // Batch 2: any subsequent transaction touching the same resource. Its prev_root is built from
    // the host SMT.
    let (next_prev_root, _) = settle_one(
        &mut scheduler,
        AccessMetadata::write(ResourceId::for_test(TARGET)),
        vec![4, 5, 6],
    );

    // The chain-continuity invariant `proving_e2e.rs` asserts for Write-declared batches.
    assert_eq!(
        next_prev_root, divergent_new_root,
        "the next batch's prev_root must chain onto the settled new_root"
    );

    scheduler.shutdown();
}
