//! Reproduces the proof-receipt cache keys omitting the pins that define the proved statement, at
//! both the per-batch and the aggregate level.
//!
//! Both statements accept host-supplied configuration the key does not name. The batch statement
//! commits `tx_image_id`, `covenant_id`, `deposit_spk_hash`, and `lane_key`, while `BatchKey` holds
//! only the checkpoint index, the block hash, and the batch image id. The aggregate statement
//! commits `lane_key` and the batch image id, while `AggregatorKey` holds only the bundle's start
//! coordinate, the aggregator image id, and the claimed final `seq_commit`. Two provers at the same
//! chain coordinate under different pins therefore share a key, and the second is served the first's
//! receipt: a valid proof of a statement it did not ask for.
//!
//! ```text
//! cargo test -p vprogs-zk-aggregate-prover --test receipt_cache_pin_omission -- --ignored
//! ```
//!
//! Scope: these tests establish the key-domain defect, which holds unconditionally. They do not
//! establish the denial-of-service exploit, which additionally requires a receipt store to survive a
//! lane or configuration change across a restart. The configuration change here is modelled by a
//! second prover over the same live store, not by a real restart.

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
use vprogs_core_codec::Reader;
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
use vprogs_zk_batch_prover::{BatchProver, BatchProverConfig, LaneProofRequest, LaneProofSource};

/// Batch-guest image id. Identical for both configurations: it is the one batch-level pin the key
/// does contain, so holding it fixed isolates the pins the key omits.
const BATCH_IMAGE_ID: [u8; 32] = [1u8; 32];
/// Aggregator-guest image id, identical for both configurations for the same reason.
const AGGREGATOR_IMAGE_ID: [u8; 32] = [2u8; 32];

/// First configuration's lane key.
const LANE_A: [u8; 32] = [0xa0u8; 32];
/// Second configuration's lane key: a different lane, proving a different statement.
const LANE_B: [u8; 32] = [0xb0u8; 32];

/// Backend pinned to one configuration: its receipts commit that configuration's `lane_key`, and it
/// counts proofs so a cache hit is visible.
#[derive(Clone)]
struct PinnedBackend {
    /// Lane key this backend's statements are over.
    lane_key: Hash,
    /// Transaction-guest image id this backend proves against.
    tx_image_id: [u8; 32],
    /// Incremented on every `prove_batch` call.
    batch_proofs: Arc<AtomicUsize>,
    /// Incremented on every `prove_aggregator` call.
    agg_proofs: Arc<AtomicUsize>,
}

impl PinnedBackend {
    /// A backend over `lane_key`, tagging its transaction image id with the same byte so the two
    /// configurations differ in every pin the key omits.
    fn new(lane_key: [u8; 32]) -> Self {
        Self {
            lane_key: Hash::from_bytes(lane_key),
            tx_image_id: lane_key,
            batch_proofs: Arc::new(AtomicUsize::new(0)),
            agg_proofs: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl vprogs_zk_transaction_prover::Backend for PinnedBackend {
    fn image_id(&self) -> &[u8; 32] {
        &self.tx_image_id
    }

    fn prove_transaction(
        &self,
        _input_bytes: Vec<u8>,
    ) -> impl Future<Output = Self::Receipt> + Send + 'static {
        async { unreachable!("the repro proves no transaction") }
    }

    type Receipt = Vec<u8>;
}

impl vprogs_zk_batch_prover::Backend for PinnedBackend {
    fn prove_batch(
        &self,
        _inputs: &[u8],
        _receipts: Vec<Self::Receipt>,
    ) -> impl Future<Output = Self::Receipt> + Send + 'static {
        self.batch_proofs.fetch_add(1, Ordering::SeqCst);
        let lane_key = self.lane_key;
        async move { state_transition_journal(lane_key) }
    }

    /// The mock receipt is its own journal.
    fn journal_bytes(receipt: &Self::Receipt) -> Vec<u8> {
        receipt.clone()
    }

    fn batch_image_id(&self) -> &[u8; 32] {
        &BATCH_IMAGE_ID
    }
}

impl vprogs_zk_aggregate_prover::Backend for PinnedBackend {
    fn prove_aggregator(
        &self,
        _inputs: &[u8],
        _batch_receipts: Vec<Self::Receipt>,
    ) -> impl Future<Output = Self::Receipt> + Send + 'static {
        self.agg_proofs.fetch_add(1, Ordering::SeqCst);
        let lane_key = self.lane_key;
        async move { state_transition_journal(lane_key) }
    }

    fn aggregator_image_id(&self) -> &[u8; 32] {
        &AGGREGATOR_IMAGE_ID
    }
}

/// A journal committing `lane_key`. `prev_state` and `new_state` differ so the aggregate worker
/// does not classify the bundle as a no-op and return before the cache is consulted.
fn state_transition_journal(lane_key: Hash) -> Vec<u8> {
    let mut buf = Vec::new();
    StateTransition::encode(
        &mut buf,
        StateTransitionArgs {
            prev_state: &[0xa1u8; 32],
            prev_lane_tip: &Hash::default(),
            new_state: &[0xa2u8; 32],
            new_lane_tip: &Hash::default(),
            new_seq_commit: &Hash::from_bytes(SEQ_COMMIT),
            covenant_id: &[0u8; 32],
            tx_image_id: &[0u8; 32],
            batch_image_id: &BATCH_IMAGE_ID,
            permission_spk_hash: &[0u8; 32],
            deposit_spk_hash: &[0u8; 32],
            lane_key: &lane_key,
        },
    );
    buf
}

/// Reads the `lane_key` a journal commits.
fn journal_lane_key(journal: &[u8]) -> Hash {
    (&mut &journal[..])
        .array_as::<StateTransition>("state_transition")
        .expect("aggregator journal")
        .lane_key
}

/// The `seq_commit` the scheduled block carries, shared by both configurations: it is part of the
/// aggregate key, so holding it fixed isolates the pins the key omits.
const SEQ_COMMIT: [u8; 32] = [0x11u8; 32];

/// Lane source returning an empty proof: the mock backends ignore their inputs.
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

/// Processor that executes every transaction as a no-op.
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
        [0u8; 32]
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

/// The chain block both configurations are at: identical coordinates are the precondition for the
/// shared key.
fn chain_block() -> ChainBlockMetadata {
    ChainBlockMetadata {
        hash: Hash::from_bytes([7u8; 32]),
        parent_id: 0,
        seq_commit: Hash::from_bytes(SEQ_COMMIT),
        ..Default::default()
    }
}

/// The batch-prover configuration for `lane_key`, differing in every pin the batch key omits.
fn batch_config(lane_key: [u8; 32]) -> BatchProverConfig {
    BatchProverConfig {
        lane_key: Hash::from_bytes(lane_key),
        covenant_id: Hash::from_bytes(lane_key),
        deposit_spk_hash: lane_key,
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

/// Tests that a per-batch receipt proved under one set of [`BatchPins`] is not reused for a batch
/// proved under different pins at the same chain coordinate.
///
/// `BatchPins` (`tx_image_id`, `covenant_id`, `deposit_spk_hash`, `lane_key`) are host-supplied
/// inputs the batch statement commits, and none of them appears in `BatchKey`. The two provers here
/// share a checkpoint index, a block hash, and a batch image id, and differ in every pin, so they
/// look up the same key: the second is served the first's receipt and publishes it as its own
/// batch artifact without proving.
///
/// [`BatchPins`]: vprogs_zk_abi::batch_processor::BatchPins
#[test]
#[ignore = "repro: G6 -- BatchKey omits BatchPins, so a receipt proved under one configuration is served for another at the same chain coordinate"]
fn batch_cache_does_not_reuse_a_receipt_across_pins() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let storage: RocksDbStore = RocksDbStore::open(temp_dir.path());
    let mut scheduler = Scheduler::new(
        ExecutionConfig::default().with_processor(StubProcessor),
        StorageConfig::default().with_store(storage.clone()),
    );

    let batch = scheduler.schedule(chain_block(), vec![]);
    batch.wait_committed_blocking();

    // The first configuration proves and caches the batch receipt.
    let backend_a = PinnedBackend::new(LANE_A);
    let prover_a: BatchProver<RocksDbStore, StubProcessor> =
        BatchProver::new(backend_a.clone(), storage.clone(), batch_config(LANE_A));
    prover_a.submit(&batch);
    wait_for(|| backend_a.batch_proofs.load(Ordering::SeqCst) == 1, "config A proves the batch");
    prover_a.shutdown();

    // The second configuration, at the same chain coordinate, proves a different statement: a
    // different lane, covenant, deposit address, and transaction image.
    let backend_b = PinnedBackend::new(LANE_B);
    let prover_b: BatchProver<RocksDbStore, StubProcessor> =
        BatchProver::new(backend_b.clone(), storage.clone(), batch_config(LANE_B));
    prover_b.submit(&batch);

    // Give the worker room to reach either outcome: a miss (it proves) or a hit (it republishes).
    thread::sleep(Duration::from_secs(1));
    let proved_b = backend_b.batch_proofs.load(Ordering::SeqCst);
    let published = journal_lane_key(&batch.artifact());
    prover_b.shutdown();
    scheduler.shutdown();

    assert_eq!(
        published,
        Hash::from_bytes(LANE_B),
        "the batch receipt published under configuration B must prove B's statement, not be A's \
         receipt served from a key that names none of the pins the statement commits",
    );
    assert_eq!(
        proved_b, 1,
        "configuration B must prove its own batch: its pins differ from A's in every field the \
         batch statement commits and the key omits",
    );
}

/// Tests that an aggregate receipt proved for one lane is not reused for a bundle over a different
/// lane at the same chain coordinate.
///
/// `AggregatorKey` holds the bundle's start coordinate, the aggregator image id, and the claimed
/// final `seq_commit`, and omits the lane key the aggregate statement commits. The two provers here
/// share all three key fields and differ in lane, so the second is served the first's receipt and
/// hands settlement an artifact proving the wrong lane's transition. The lane key is never checked
/// on the hit path, unlike `covenant_id`, which is checked when one is configured.
#[test]
#[ignore = "repro: G6 -- AggregatorKey omits lane_key, so a bundle receipt proved for one lane is served for another at the same coordinate"]
fn aggregate_cache_does_not_reuse_a_receipt_across_lane_keys() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let storage: RocksDbStore = RocksDbStore::open(temp_dir.path());
    let mut scheduler = Scheduler::new(
        ExecutionConfig::default().with_processor(StubProcessor),
        StorageConfig::default().with_store(storage),
    );

    // The aggregate worker composes only non-empty batches, so an empty batch would take the no-op
    // path and never consult the cache.
    let batch = scheduler.schedule(
        chain_block(),
        vec![SchedulerTransaction::new(
            0,
            vec![AccessMetadata::write(ResourceId::for_test(1))],
            0,
        )],
    );
    batch.wait_processed_blocking();
    batch.publish_artifact(Some(vec![0xbbu8; 8]));
    let receipt_store = scheduler.state().receipt_store();

    // The first lane's prover proves and caches the bundle receipt.
    let backend_a = PinnedBackend::new(LANE_A);
    let queue_a: AsyncQueue<ScheduledBundle<SettlementArtifact<Vec<u8>>>> = AsyncQueue::new();
    let prover_a = AggregateProver::new(
        backend_a.clone(),
        receipt_store.clone(),
        AggregateProverConfig {
            lane_key: Hash::from_bytes(LANE_A),
            covenant_id: None,
            lane_source: StubLaneProofs,
            settlement_queue: Some(queue_a.clone()),
            settlement: None,
            bundle_size: 1..=usize::MAX,
        },
    );
    prover_a.submit(&batch);
    next_bundle(&queue_a, Duration::from_secs(5))
        .expect("lane A bundle emitted")
        .wait_artifact_published_blocking();
    prover_a.shutdown();

    // The second lane's prover forms a bundle at the same start coordinate with the same claimed
    // final seq_commit, so it looks up the key lane A's receipt was filed under.
    let backend_b = PinnedBackend::new(LANE_B);
    let queue_b: AsyncQueue<ScheduledBundle<SettlementArtifact<Vec<u8>>>> = AsyncQueue::new();
    let prover_b = AggregateProver::new(
        backend_b.clone(),
        receipt_store,
        AggregateProverConfig {
            lane_key: Hash::from_bytes(LANE_B),
            covenant_id: None,
            lane_source: StubLaneProofs,
            settlement_queue: Some(queue_b.clone()),
            settlement: None,
            bundle_size: 1..=usize::MAX,
        },
    );
    prover_b.submit(&batch);
    let bundle_b = next_bundle(&queue_b, Duration::from_secs(5)).expect("lane B bundle emitted");
    bundle_b.wait_artifact_published_blocking();

    let artifact = bundle_b.artifact().expect("lane B artifact");
    let published = journal_lane_key(&artifact.receipt);
    let proved_b = backend_b.agg_proofs.load(Ordering::SeqCst);
    prover_b.shutdown();
    scheduler.shutdown();

    assert_eq!(
        published,
        Hash::from_bytes(LANE_B),
        "the artifact handed to lane B's settlement must prove lane B's transition, not be lane \
         A's receipt served from a key that omits the lane",
    );
    assert_eq!(
        proved_b, 1,
        "lane B must prove its own bundle: its statement is over a different lane than the cached \
         receipt's",
    );
}

/// Spins until `condition` holds, panicking with `what` if it has not within five seconds.
fn wait_for(condition: impl Fn() -> bool, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        thread::sleep(Duration::from_millis(20));
    }
}
