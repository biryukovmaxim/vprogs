//! Reproduces the divergence between the proven state root and the host store when a transaction
//! declares a resource `AccessType::Read` and the guest writes it anyway.
//!
//! The declaration is decoded verbatim out of the user-supplied L1 payload by
//! `AccessMetadata::decode_vec` and is never validated against what the guest actually did.
//! `Resource::access_type()` has no call sites anywhere in the repository, and the output
//! commitment keys purely on `Resource::is_dirty()`. The only three sites that consume the
//! declaration are host-side (`ResourceAccess::set_read_state`, `AccessHandle::commit_changes` and
//! `AccessHandle::rollback_changes`) and all three discard the write rather than reject the
//! transaction.
//!
//! Both tests assert an invariant that must hold, so both fail against unfixed code. Either
//! enforcement point closes them: reject a transaction whose declaration does not cover what it
//! wrote, or honor the write. Their passing is the acceptance criterion for a fix.
//!
//! Both run the real `Vm`, the real risc0 transaction-processor guest, the real batch-processor
//! guest, the real batch prover and the real scheduler, against a real simnet L1 node. No mock
//! processor is involved: the real guest increments every resource unconditionally, which is
//! exactly what the two mock processors (`scheduling/test-utils/src/processor.rs`,
//! `node/test-utils/src/vm.rs`) do not do, since both gate their writes on `access_type`.

use kaspa_consensus_core::network::{NetworkId, NetworkType};
use kaspa_hashes::Hash;
use kaspa_rpc_core::api::rpc::RpcApi;
use tempfile::TempDir;
use vprogs_core_smt::{EMPTY_HASH, Tree as _};
use vprogs_core_test_utils::ResourceIdExt;
use vprogs_core_types::{AccessMetadata, ResourceId};
use vprogs_l1_types::{ChainBlockMetadata, L1Transaction};
use vprogs_node_test_utils::L1Node;
use vprogs_scheduling_scheduler::{ExecutionConfig, Scheduler};
use vprogs_state_version::StateVersion;
use vprogs_storage_manager::StorageConfig;
use vprogs_storage_rocksdb_store::RocksDbStore;
use vprogs_zk_abi::batch_processor::BatchTransition;
use vprogs_zk_backend_risc0_api::{Backend, ProofType, Receipt};
use vprogs_zk_backend_risc0_test_suite::{
    L1TransactionExt, aggregate_batches, batch_aggregator_elf, batch_processor_elf, test_lane_key,
    transaction_processor_elf,
};
use vprogs_zk_batch_prover::{Backend as _, BatchProverConfig};
use vprogs_zk_vm::{ProvingPipeline, Vm};
use zerocopy::FromBytes;

/// Resource the attacker declares `Read` and the guest writes anyway.
const TARGET: usize = 1;

/// The real `Vm` over the real risc0 guests, plus the simnet node its prover reads lane proofs
/// from.
struct Fixture {
    /// Backend wrapping the three real guest ELFs.
    backend: Backend,
    /// Simnet node the batch prover fetches lane proofs from.
    l1: L1Node,
    /// The host store whose root the settled state is compared against.
    storage: RocksDbStore,
    /// Scheduler driving the real `Vm`.
    scheduler: Scheduler<RocksDbStore, Vm<Backend, RocksDbStore>>,
    /// Mined simnet blocks batch metadata is anchored to.
    block_hashes: Vec<Hash>,
    /// Backing directory for `storage`, dropped at end of test.
    _temp_dir: TempDir,
}

impl Fixture {
    /// Builds the fixture and mines `blocks` simnet blocks to anchor batch metadata against.
    async fn new(blocks: usize) -> Self {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let storage: RocksDbStore = RocksDbStore::open(temp_dir.path());

        let backend = Backend::new(
            &transaction_processor_elf(),
            &batch_processor_elf(),
            &batch_aggregator_elf(),
            ProofType::Succinct,
        );

        let l1 = L1Node::new(NetworkId::new(NetworkType::Simnet), None).await;
        let block_hashes = l1.mine_blocks(blocks).await;

        let config = BatchProverConfig {
            lane_key: test_lane_key(),
            covenant_id: Hash::default(),
            deposit_spk_hash: [0u8; 32],
        };

        let vm = Vm::new(
            backend.clone(),
            ProvingPipeline::batch(backend.clone(), storage.clone(), config),
        );
        let scheduler = Scheduler::new(
            ExecutionConfig::default().with_processor(vm),
            StorageConfig::default().with_store(storage.clone()),
        );

        Self { backend, l1, storage, scheduler, block_hashes, _temp_dir: temp_dir }
    }

    /// Builds a `ChainBlockMetadata` from the mined simnet block at `idx`.
    async fn metadata(&self, idx: usize) -> ChainBlockMetadata {
        let block = self
            .l1
            .grpc_client()
            .get_block(self.block_hashes[idx], false)
            .await
            .expect("get_block");
        let h = block.header;

        ChainBlockMetadata {
            hash: h.hash,
            blue_score: h.blue_score,
            daa_score: h.daa_score,
            timestamp: h.timestamp,
            seq_commit: h.accepted_id_merkle_root,
            ..Default::default()
        }
    }

    /// Schedules one L1 carrier transaction against `TARGET` under `access`, anchored to the block
    /// at `block_idx`, and returns the proven per-batch receipt.
    async fn settle_one(
        &mut self,
        access: AccessMetadata,
        ix_data: &[u8],
        block_idx: usize,
    ) -> Receipt {
        let tx = L1Transaction::for_l2_test(&[access], ix_data);
        let metadata = self.metadata(block_idx).await;

        let batch = self.scheduler.schedule(metadata, vec![tx.into_scheduler_tx(0)]);
        batch.wait_committed_blocking();
        batch.wait_artifact_published_blocking();

        (*batch.artifact()).clone()
    }
}

/// Decodes a per-batch receipt's journal into its `(prev_state, new_state)`.
fn state_transition(receipt: &Receipt) -> ([u8; 32], [u8; 32]) {
    let journal = Backend::journal_bytes(receipt);
    let t = BatchTransition::ref_from_bytes(&journal).expect("decode BatchTransition");

    (t.prev_state, t.new_state)
}

/// The settled `new_state` and the host store's root must agree for every batch.
///
/// The guest marks the `Read`-declared resource dirty and journals `Changed(hash(new_data))`; the
/// batch processor folds that hash into `new_state`. Host-side, `AccessHandle::commit_changes`
/// drops the write because the declaration says `Read`, so `data_updated()` is false, the diff is
/// filtered out of `updated_state_diffs()`, and the SMT never hears about it. The batch's own proof
/// still succeeds: it is internally consistent. Only the host disagrees, and no site compares them.
///
/// This proves the divergence only. It does not prove that the lane wedges.
#[tokio::test(flavor = "multi_thread")]
async fn settled_state_must_match_host_root_when_guest_writes_a_read_declared_resource() {
    let mut fx = Fixture::new(1).await;

    let receipt =
        fx.settle_one(AccessMetadata::read(ResourceId::for_test(TARGET)), &[1, 2, 3], 0).await;
    let (prev_state, new_state) = state_transition(&receipt);

    assert_eq!(prev_state, EMPTY_HASH, "prev_state should be empty (no prior state)");

    // The write evaporated host-side: the state diff was filtered out of the SMT update, so no
    // version 1 data exists for the resource at all.
    assert_eq!(
        StateVersion::get(&fx.storage, 1, &ResourceId::for_test(TARGET)),
        None,
        "host store holds no version 1 data: the Read-declared write was dropped"
    );

    // The invariant. The proof says the resource changed; the host's own state says it did not.
    assert_eq!(
        new_state,
        fx.storage.root(1),
        "settled new_state must equal the host store's root for the same batch"
    );

    fx.scheduler.shutdown();
}

/// A bundle must aggregate cleanly after any batch the pipeline produced.
///
/// This is the wedge. The aggregator folds each batch onto its predecessor and asserts
/// `assert_eq!(this.prev_state, prev.new_state, "prev_state")`. Every batch's `prev_state` is
/// proved from an SMT proof the batch prover reads out of the host store, which never recorded the
/// write, so it can no longer equal the divergent batch's settled `new_state`. The bundle is
/// unprovable, and every later bundle inherits the break: the offending L1 transaction is
/// permanently in the chain, so any replay re-derives the same divergence.
///
/// The trigger is one L1 carrier transaction from any user, with no keys or privileges.
///
/// Unlike the aggregator's existing chain tests, which hand-build `BatchTransition` journals, both
/// journals here are produced by the real pipeline: the divergence is not injected.
#[tokio::test(flavor = "multi_thread")]
async fn bundle_must_aggregate_after_a_read_declared_write() {
    let mut fx = Fixture::new(2).await;

    // Batch 1: the offending transaction. Its settled new_state records the guest's write.
    let divergent =
        fx.settle_one(AccessMetadata::read(ResourceId::for_test(TARGET)), &[1, 2, 3], 0).await;

    // Batch 2: any subsequent transaction touching the same resource. Its prev_state is proved
    // from the host SMT.
    let next =
        fx.settle_one(AccessMetadata::write(ResourceId::for_test(TARGET)), &[4, 5, 6], 1).await;

    // Precondition: the two batches do not chain, which is what the aggregator is about to reject.
    let (_, divergent_new_state) = state_transition(&divergent);
    let (next_prev_state, _) = state_transition(&next);
    assert_ne!(
        next_prev_state, divergent_new_state,
        "precondition: the Read-declared write should have broken the state chain"
    );

    // The invariant: the bundle aggregates. Fails inside the aggregator guest on the `prev_state`
    // assert.
    let bundle = aggregate_batches(
        &fx.backend,
        fx.l1.grpc_client(),
        &test_lane_key(),
        fx.block_hashes[1],
        vec![divergent, next],
    )
    .await;
    assert!(!Backend::journal_bytes(&bundle).is_empty(), "bundle receipt should carry a journal");

    fx.scheduler.shutdown();
}
