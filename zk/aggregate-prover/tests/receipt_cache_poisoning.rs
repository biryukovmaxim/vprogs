//! Reproduces the aggregate prover caching a settlement receipt whose committed `new_seq_commit`
//! disagrees with the scheduled final block's.
//!
//! The receipt's `new_seq_commit` is the value the aggregate cache key claims, so a receipt
//! committing a different one is filed under a key that misdescribes it. The only guard is a
//! `debug_assert_eq!`, and it sits after the store write: in a build with debug assertions off the
//! divergent receipt is written to the cache, and every later attempt at the same bundle coordinate
//! is served that same receipt from the cache instead of being re-proved.
//!
//! Both tests must run with debug assertions off. Under the default dev profile the
//! `debug_assert_eq!` fires and aborts the worker, which hides the behaviour under test:
//!
//! ```text
//! RUSTFLAGS="-C debug-assertions=off" cargo test -p vprogs-zk-aggregate-prover \
//!     --test receipt_cache_poisoning -- --ignored --test-threads=1
//! ```

// The backend traits return `impl Future + 'static`, which an `async fn` cannot satisfy: its future
// borrows `&self`.
#![allow(clippy::manual_async_fn)]

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use kaspa_hashes::Hash;
use kaspa_rpc_core::{GetSeqCommitLaneProofResponse, RpcHash};
use tempfile::TempDir;
use vprogs_core_atomics::AsyncQueue;
use vprogs_core_test_utils::ResourceIdExt;
use vprogs_core_types::{AccessMetadata, ResourceId, SchedulerTransaction};
use vprogs_l1_types::ChainBlockMetadata;
use vprogs_scheduling_scheduler::{ExecutionConfig, Scheduler, TransactionContext};
use vprogs_storage_manager::StorageConfig;
use vprogs_storage_rocksdb_store::RocksDbStore;
use vprogs_zk_abi::batch_aggregator::{StateTransition, StateTransitionArgs};
use vprogs_zk_aggregate_prover::{
    AggregateProver, AggregateProverConfig, ScheduledBundle, SettlementArtifact,
};
use vprogs_zk_batch_prover::{LaneProofRequest, LaneProofSource};

/// Transaction-guest image id. These tests prove nothing, so image ids only key cache lookups.
const TX_IMAGE_ID: [u8; 32] = [0u8; 32];
/// Batch-guest image id.
const BATCH_IMAGE_ID: [u8; 32] = [1u8; 32];
/// Aggregator-guest image id, keying a bundle's settlement receipt.
const AGGREGATOR_IMAGE_ID: [u8; 32] = [2u8; 32];

/// The `seq_commit` the scheduled final block carries. The cache key claims this value.
const SCHEDULED_SEQ_COMMIT: [u8; 32] = [0x11u8; 32];
/// The `seq_commit` the proved receipt actually commits. Divergent by construction: this stands in
/// for any cause of a receipt that does not prove the statement its key names.
const DIVERGENT_SEQ_COMMIT: [u8; 32] = [0x22u8; 32];

/// Backend whose aggregator receipt commits [`DIVERGENT_SEQ_COMMIT`] rather than the scheduled
/// block's, and which counts how many times it was asked to prove so a cache hit is visible.
#[derive(Clone)]
struct DivergentBackend {
    /// Incremented on every `prove_aggregator` call.
    proofs: Arc<AtomicUsize>,
}

impl vprogs_zk_transaction_prover::Backend for DivergentBackend {
    fn image_id(&self) -> &[u8; 32] {
        &TX_IMAGE_ID
    }

    fn prove_transaction(
        &self,
        _input_bytes: Vec<u8>,
    ) -> impl Future<Output = Self::Receipt> + Send + 'static {
        async { unreachable!("the repro proves no transaction") }
    }

    type Receipt = Vec<u8>;
}

impl vprogs_zk_batch_prover::Backend for DivergentBackend {
    fn prove_batch(
        &self,
        _inputs: &[u8],
        _receipts: Vec<Self::Receipt>,
    ) -> impl Future<Output = Self::Receipt> + Send + 'static {
        async { unreachable!("the repro publishes per-batch receipts directly") }
    }

    /// The mock receipt is its own journal.
    fn journal_bytes(receipt: &Self::Receipt) -> Vec<u8> {
        receipt.clone()
    }

    fn batch_image_id(&self) -> &[u8; 32] {
        &BATCH_IMAGE_ID
    }
}

impl vprogs_zk_aggregate_prover::Backend for DivergentBackend {
    fn prove_aggregator(
        &self,
        _inputs: &[u8],
        _batch_receipts: Vec<Self::Receipt>,
    ) -> impl Future<Output = Self::Receipt> + Send + 'static {
        self.proofs.fetch_add(1, Ordering::SeqCst);
        async { state_transition_journal(DIVERGENT_SEQ_COMMIT, Hash::default()) }
    }

    fn aggregator_image_id(&self) -> &[u8; 32] {
        &AGGREGATOR_IMAGE_ID
    }
}

/// An aggregator journal committing `new_seq_commit` and `lane_key`. `prev_state` and `new_state`
/// differ so the worker does not classify the bundle as a no-op and return before the cache write.
fn state_transition_journal(new_seq_commit: [u8; 32], lane_key: Hash) -> Vec<u8> {
    let mut buf = Vec::new();
    StateTransition::encode(
        &mut buf,
        StateTransitionArgs {
            prev_state: &[0xa1u8; 32],
            prev_lane_tip: &Hash::default(),
            new_state: &[0xa2u8; 32],
            new_lane_tip: &Hash::default(),
            new_seq_commit: &Hash::from_bytes(new_seq_commit),
            covenant_id: &[0u8; 32],
            tx_image_id: &TX_IMAGE_ID,
            batch_image_id: &BATCH_IMAGE_ID,
            permission_spk_hash: &[0u8; 32],
            deposit_spk_hash: &[0u8; 32],
            lane_key: &lane_key,
        },
    );
    buf
}

/// Lane source returning an empty proof: the mock backend ignores its inputs.
struct StubLaneProofs;

impl LaneProofSource for StubLaneProofs {
    async fn fetch_lane_proof(&self, _req: LaneProofRequest) -> GetSeqCommitLaneProofResponse {
        GetSeqCommitLaneProofResponse {
            smt_proof: Vec::new(),
            lane: None,
            payload_and_ctx_digest: RpcHash::default(),
            parent_seq_commit: RpcHash::default(),
            inactivity_shortcut: RpcHash::default(),
        }
    }
}

/// Processor that executes every transaction as a no-op; the tests publish batch receipts directly.
#[derive(Clone)]
struct StubProcessor;

impl vprogs_scheduling_scheduler::Processor<RocksDbStore> for StubProcessor {
    fn process_transaction(
        &self,
        _ctx: &mut TransactionContext<RocksDbStore, Self>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn tx_image_id(&self) -> [u8; 32] {
        TX_IMAGE_ID
    }

    fn batch_image_id(&self) -> [u8; 32] {
        BATCH_IMAGE_ID
    }

    type Transaction = usize;
    type TransactionArtifact = Vec<u8>;
    type BatchArtifact = Vec<u8>;
    type AggregatorArtifact = Vec<u8>;
    type BatchMetadata = ChainBlockMetadata;
    type Error = ();
}

/// The bundle's final chain block, carrying [`SCHEDULED_SEQ_COMMIT`]: the value the aggregate
/// cache key is built from.
fn final_block() -> ChainBlockMetadata {
    ChainBlockMetadata {
        hash: Hash::from_bytes([7u8; 32]),
        parent_id: 0,
        seq_commit: Hash::from_bytes(SCHEDULED_SEQ_COMMIT),
        ..Default::default()
    }
}

/// Pops the next bundle the worker emits, or `None` once `timeout` elapses.
fn next_bundle(
    queue: &AsyncQueue<ScheduledBundle<SettlementArtifact<Vec<u8>>>>,
    timeout: Duration,
) -> Option<ScheduledBundle<SettlementArtifact<Vec<u8>>>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(bundle) = queue.pop() {
            return Some(bundle);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

/// Tests that a receipt committing a `new_seq_commit` other than the scheduled final block's is
/// rejected before it reaches the cache.
///
/// The worker keys the receipt by the scheduled block's `seq_commit` and writes it to the store
/// before ever comparing the receipt's own committed value against that block. With debug
/// assertions off the comparison is compiled out entirely, so the store keeps a receipt that does
/// not prove the statement its key names, and the covenant will reject the settlement built from it
/// for the lifetime of the store.
#[test]
#[ignore = "repro: G6 -- the divergent receipt is cached because the seq_commit binding is a debug_assert placed after the store write; run with -C debug-assertions=off"]
fn divergent_seq_commit_receipt_is_not_cached() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let storage: RocksDbStore = RocksDbStore::open(temp_dir.path());
    let mut scheduler = Scheduler::new(
        ExecutionConfig::default().with_processor(StubProcessor),
        StorageConfig::default().with_store(storage),
    );

    // A batch with one transaction: the worker composes only non-empty batches, so an empty batch
    // would take the no-op path and never reach the cache write.
    let batch = scheduler.schedule(
        final_block(),
        vec![SchedulerTransaction::new(0, vec![AccessMetadata::write(ResourceId::for_test(1))], 0)],
    );
    batch.wait_processed_blocking();
    batch.publish_artifact(Some(vec![0xbbu8; 8]));

    let receipt_store = scheduler.state().receipt_store();
    let proofs = Arc::new(AtomicUsize::new(0));
    let settlement_queue: AsyncQueue<ScheduledBundle<SettlementArtifact<Vec<u8>>>> =
        AsyncQueue::new();
    let prover = AggregateProver::new(
        DivergentBackend { proofs: Arc::clone(&proofs) },
        receipt_store.clone(),
        AggregateProverConfig {
            lane_key: Hash::default(),
            covenant_id: None,
            lane_source: StubLaneProofs,
            settlement_queue: Some(settlement_queue.clone()),
            settlement: None,
            bundle_size: 1..=usize::MAX,
        },
    );
    prover.submit(&batch);

    let bundle = next_bundle(&settlement_queue, Duration::from_secs(5)).expect("bundle emitted");
    bundle.wait_artifact_published_blocking();
    assert_eq!(proofs.load(Ordering::SeqCst), 1, "the bundle proves once on the miss path");

    // The key the worker wrote under: the bundle's start coordinate plus the scheduled block's
    // seq_commit, which is exactly the value the stored receipt does not commit.
    let agg_key = bundle.agg_key(AGGREGATOR_IMAGE_ID, SCHEDULED_SEQ_COMMIT);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    let cached = rt.block_on(receipt_store.read_agg_receipt(agg_key).resolve());

    prover.shutdown();
    scheduler.shutdown();

    assert!(
        cached.is_none(),
        "a receipt committing a new_seq_commit other than the scheduled final block's must be \
         rejected before write_agg_receipt, not cached under a key claiming that block's value",
    );
}

/// Tests that a retry at the same bundle coordinate does not serve a previously cached divergent
/// receipt.
///
/// This is the consequence the first test's cache write sets up. The covenant rejects the
/// settlement on chain, the settler retries, and the retry forms the same bundle at the same
/// coordinate, so it hits the same key. The worker serves the stored receipt without re-proving and
/// without ever reaching the check, so the retry submits the identical doomed settlement.
#[test]
#[ignore = "repro: G6 -- a retry at the same coordinate is served the cached divergent receipt and never re-proves; run with -C debug-assertions=off"]
fn retry_does_not_serve_the_cached_divergent_receipt() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let storage: RocksDbStore = RocksDbStore::open(temp_dir.path());
    let mut scheduler = Scheduler::new(
        ExecutionConfig::default().with_processor(StubProcessor),
        StorageConfig::default().with_store(storage),
    );

    let batch = scheduler.schedule(
        final_block(),
        vec![SchedulerTransaction::new(0, vec![AccessMetadata::write(ResourceId::for_test(1))], 0)],
    );
    batch.wait_processed_blocking();
    batch.publish_artifact(Some(vec![0xbbu8; 8]));

    let proofs = Arc::new(AtomicUsize::new(0));
    let settlement_queue: AsyncQueue<ScheduledBundle<SettlementArtifact<Vec<u8>>>> =
        AsyncQueue::new();
    let prover = AggregateProver::new(
        DivergentBackend { proofs: Arc::clone(&proofs) },
        scheduler.state().receipt_store(),
        AggregateProverConfig {
            lane_key: Hash::default(),
            covenant_id: None,
            lane_source: StubLaneProofs,
            settlement_queue: Some(settlement_queue.clone()),
            settlement: None,
            bundle_size: 1..=usize::MAX,
        },
    );

    // First pass: proves the divergent receipt and (on the unfixed tree) caches it.
    prover.submit(&batch);
    next_bundle(&settlement_queue, Duration::from_secs(5))
        .expect("first bundle emitted")
        .wait_artifact_published_blocking();

    // Retry: the same batch forms the same bundle at the same coordinate, so it looks up the same
    // key that the first pass wrote.
    prover.submit(&batch);
    next_bundle(&settlement_queue, Duration::from_secs(5))
        .expect("retry bundle emitted")
        .wait_artifact_published_blocking();

    let proved = proofs.load(Ordering::SeqCst);
    prover.shutdown();
    scheduler.shutdown();

    assert_eq!(
        proved, 2,
        "the retry must re-prove the bundle rather than be served the cached receipt whose \
         committed new_seq_commit disagrees with the scheduled final block",
    );
}
