//! Pins the startup recovery semantics for a lost batch receipt: a committed batch whose
//! metadata is durable but whose cached receipt was lost (a kill between the commit flush and
//! the prove, or an image-id change on restart) cannot be re-proved in place, so the node must
//! roll the executor back to the journal tail before anything runs, letting the re-feed
//! re-execute, re-commit, and re-prove the range so it settles ahead of new work. Without the
//! rollback the committed-gap pass treats the miss as terminal and every later bundle chains
//! from a base the covenant never took.

#![allow(clippy::manual_async_fn)]

use std::{
    future::Future,
    thread,
    time::{Duration, Instant},
};

use kaspa_hashes::Hash;
use kaspa_rpc_core::GetSeqCommitLaneProofResponse;
use tempfile::TempDir;
use tokio::sync::watch;
use vprogs_core_atomics::AsyncQueue;
use vprogs_core_test_utils::ResourceIdExt;
use vprogs_core_types::{AccessMetadata, ResourceId, SchedulerTransaction};
use vprogs_l1_types::{ChainBlockMetadata, SettlementInfo};
use vprogs_scheduling_scheduler::{ExecutionConfig, Scheduler, SchedulerState, TransactionContext};
use vprogs_state_proof_receipt::{AggregatorKey, Prefix};
use vprogs_state_settlement_journal::{JournalEntry, StoreJournal};
use vprogs_storage_manager::StorageConfig;
use vprogs_storage_rocksdb_store::RocksDbStore;
use vprogs_zk_abi::batch_aggregator::{StateTransition, StateTransitionArgs};
use vprogs_zk_aggregate_prover::{
    AggregateProver, AggregateProverConfig, ScheduledBundle, SettlementArtifact,
    rollback_uncoverable_gap,
};
use vprogs_zk_batch_prover::{LaneProofError, LaneProofRequest, LaneProofSource};

/// Transaction-guest image id. This repro proves nothing real, so image ids only key receipt
/// lookups.
const TX_IMAGE_ID: [u8; 32] = [0u8; 32];
/// Batch-guest image id, keying a per-batch receipt in the proof-receipt store.
const BATCH_IMAGE_ID: [u8; 32] = [1u8; 32];
/// Aggregator-guest image id, keying a bundle's settlement receipt.
const AGGREGATOR_IMAGE_ID: [u8; 32] = [2u8; 32];

/// `seq_commit` the synthetic settlement journal derives. Every block's metadata carries it so the
/// worker's journal-vs-metadata check holds.
fn seq_commit() -> Hash {
    Hash::from_bytes([0x33; 32])
}

/// Block-hash helper keyed to the single byte every test block is built from.
fn block_hash(byte: u8) -> Hash {
    Hash::from_bytes([byte; 32])
}

/// Encodes the settlement journal the synthetic aggregator receipt carries: a real (non-no-op)
/// state transition whose `new_seq_commit` matches [`seq_commit`], so the worker publishes an
/// artifact instead of resolving the bundle as a no-op. Fields are encoded in declared order by the
/// journal's own encoder.
fn settlement_journal() -> Vec<u8> {
    let mut buf = Vec::new();
    StateTransition::encode(
        &mut buf,
        StateTransitionArgs {
            prev_state: &[0x00; 32],
            prev_lane_tip: &Hash::default(),
            new_state: &[0x11; 32],
            new_lane_tip: &Hash::default(),
            new_seq_commit: &seq_commit(),
            covenant_id: &[0u8; 32],
            tx_image_id: &TX_IMAGE_ID,
            batch_image_id: &BATCH_IMAGE_ID,
            permission_spk_hash: &[0u8; 32],
            deposit_spk_hash: &[0u8; 32],
            lane_key: &Hash::default(),
        },
    );
    buf
}

/// Backend standing in for all three guests: the aggregator receipt is the settlement journal
/// itself (identity `journal_bytes`), so the worker parses exactly the transition above.
#[derive(Clone)]
struct SyntheticBackend;

impl vprogs_zk_transaction_prover::Backend for SyntheticBackend {
    fn image_id(&self) -> &[u8; 32] {
        &TX_IMAGE_ID
    }

    fn prove_transaction(
        &self,
        _input_bytes: Vec<u8>,
    ) -> impl Future<Output = Self::Receipt> + Send + 'static {
        async { unreachable!("the repro publishes batch receipts directly") }
    }

    type Receipt = Vec<u8>;
}

impl vprogs_zk_batch_prover::Backend for SyntheticBackend {
    fn prove_batch(
        &self,
        _inputs: &[u8],
        _receipts: Vec<Self::Receipt>,
    ) -> impl Future<Output = Self::Receipt> + Send + 'static {
        async { unreachable!("the repro publishes batch receipts directly") }
    }

    fn journal_bytes(receipt: &Self::Receipt) -> Vec<u8> {
        receipt.clone()
    }

    fn batch_image_id(&self) -> &[u8; 32] {
        &BATCH_IMAGE_ID
    }
}

impl vprogs_zk_aggregate_prover::Backend for SyntheticBackend {
    fn prove_aggregator(
        &self,
        _inputs: &[u8],
        _batch_receipts: Vec<Self::Receipt>,
    ) -> impl Future<Output = Self::Receipt> + Send + 'static {
        async { settlement_journal() }
    }

    fn aggregator_image_id(&self) -> &[u8; 32] {
        &AGGREGATOR_IMAGE_ID
    }
}

/// Lane source serving every fetch, so a gap fails only on a missing receipt, never on a fetch.
struct ServeLaneProofs;

impl LaneProofSource for ServeLaneProofs {
    async fn fetch_lane_proof(
        &self,
        _req: LaneProofRequest,
    ) -> Result<GetSeqCommitLaneProofResponse, LaneProofError> {
        Ok(GetSeqCommitLaneProofResponse {
            smt_proof: Vec::new(),
            lane: None,
            payload_and_ctx_digest: Hash::default(),
            parent_seq_commit: Hash::default(),
            inactivity_shortcut: Hash::default(),
        })
    }
}

/// Processor executing every transaction without touching resource bytes.
#[derive(Clone)]
struct PlainProcessor;

impl vprogs_scheduling_scheduler::Processor<RocksDbStore> for PlainProcessor {
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

/// Chain-block metadata for a block carrying the journal's `seq_commit` and an advancing lane tip,
/// so the committed-gap pass composes this batch's cached receipt.
fn block(hash: u8, parent_id: u64) -> ChainBlockMetadata {
    ChainBlockMetadata {
        hash: Hash::from_bytes([hash; 32]),
        parent_id,
        seq_commit: seq_commit(),
        prev_lane_tip: Hash::default(),
        lane_tip: Hash::from_bytes([hash; 32]),
        ..Default::default()
    }
}

/// One lane transaction: enough for the batch to be non-empty, so its bundle composes a receipt
/// and reaches the lane-proof fetch.
fn lane_tx() -> SchedulerTransaction<usize> {
    SchedulerTransaction::new(0, vec![AccessMetadata::write(ResourceId::for_test(1))], 0)
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

/// Commits a one-transaction batch for `meta` and seeds its cached per-batch receipt the way the
/// batch prover would have, returning the batch handle without publishing or submitting it: the
/// caller decides when the pipeline sees this batch as live work.
fn commit_batch_with_receipt(
    scheduler: &mut Scheduler<RocksDbStore, PlainProcessor>,
    meta: ChainBlockMetadata,
) -> vprogs_scheduling_scheduler::ScheduledBatch<RocksDbStore, PlainProcessor> {
    let batch = scheduler.schedule(meta, vec![lane_tx()]);
    batch.wait_committed_blocking();
    batch.write_batch_receipt(settlement_journal()).wait_blocking();
    batch
}

/// Publishes `batch`'s artifact and feeds it to the aggregate prover as live work.
fn submit_live(
    prover: &AggregateProver<RocksDbStore, PlainProcessor>,
    batch: &vprogs_scheduling_scheduler::ScheduledBatch<RocksDbStore, PlainProcessor>,
) {
    batch.publish_artifact(Some(settlement_journal()));
    prover.submit(batch);
}

/// A settlement tip proving through `block`, so the resume pass resolves it against the journal.
fn tip_through(block: u8) -> SettlementInfo {
    SettlementInfo { block_prove_to: block_hash(block), ..Default::default() }
}

/// Waits until `pred` holds, panicking after `timeout`.
fn wait_until(desc: &str, timeout: Duration, pred: impl Fn() -> bool) {
    let deadline = Instant::now() + timeout;
    while !pred() {
        assert!(Instant::now() < deadline, "timed out waiting for {desc}");
        thread::sleep(Duration::from_millis(10));
    }
}

/// Tests that a committed gap batch whose metadata is durable but whose cached receipt was lost
/// (a kill between the commit flush and the prove, or an image-id change on restart) is recovered
/// by the startup rollback: the executor state rolls back to the journal tail, the metadata rows
/// above the tail are reverted so the re-fed blocks reoccupy their checkpoint indexes, and the
/// re-executed range re-proves and settles ahead of new work. Leaving the miss to the
/// committed-gap pass instead strands the range forever: the pass composes cached receipts only,
/// and every later bundle proves from a base the covenant never took, so the settler skips each
/// one and no settlement lands again.
#[test]
fn committed_gap_with_lost_receipt_rolls_back_and_refeeds() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        // The pre-restart half: block 1 settled and journaled (the tail), blocks 2 and 3
        // committed above it. Block 2's receipt never reached disk while its metadata did.
        let storage: RocksDbStore = RocksDbStore::open(temp_dir.path());
        let journal = StoreJournal::new(storage.clone());
        let mut scheduler = Scheduler::new(
            ExecutionConfig::default().with_processor(PlainProcessor),
            StorageConfig::default().with_store(storage.clone()),
        );
        commit_batch_with_receipt(&mut scheduler, block(1, 0));
        let lost = scheduler.schedule(block(2, 1), vec![lane_tx()]);
        lost.wait_committed_blocking();
        commit_batch_with_receipt(&mut scheduler, block(3, 2));
        journal.record(
            1,
            &JournalEntry {
                end_index: 1,
                from_block: block_hash(1),
                block_prove_to: block_hash(1),
                seq_commit: seq_commit(),
            },
        );
        scheduler.shutdown();

        // The restart half runs the startup rollback the proving node wires before any
        // scheduler, bridge, or prover operates on the state.
        let state = SchedulerState::new(StorageConfig::default().with_store(storage.clone()));
        assert_eq!(
            rollback_uncoverable_gap(&state, &journal, &BATCH_IMAGE_ID),
            Some(1),
            "the uncoverable gap (block 2's lost receipt) rolls the executor back to the tail",
        );
        assert_eq!(
            journal.committed_tip().map(|(index, _)| index),
            Some(1),
            "the metadata rows above the tail are reverted with the rollback",
        );
        assert!(journal.batch_metadata(2).is_none(), "block 2's stale row is gone");
        assert!(journal.batch_metadata(3).is_none(), "block 3's stale row is gone");
        assert_eq!(state.last_committed().index(), 1, "last_committed sits at the tail");

        restart_refeed_and_settle(
            &state,
            &journal,
            ("the resume pass drops the settled tail entry", || {
                !journal.entries().iter().any(|(start, _)| *start == 1)
            }),
        );
    }
}

/// Tests the same recovery when a prior restart already consumed the journal: its resume pass
/// deleted the tail entry the on-chain tip covered, so the journal reads empty at this restart
/// and the settled boundary survives only where the worker persisted it (the pruning root stands
/// in before any worker has run). The lost-receipt batch is still found above that floor, the
/// rollback still recovers on this restart, and the worker's startup pass re-records the boundary
/// for the next one.
#[test]
fn committed_gap_with_consumed_journal_recovers() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        // The pre-restart half matches the lost-receipt setup except no journal entry survives:
        // the settled bundle compacted away on the earlier restart.
        let storage: RocksDbStore = RocksDbStore::open(temp_dir.path());
        let journal = StoreJournal::new(storage.clone());
        let mut scheduler = Scheduler::new(
            ExecutionConfig::default().with_processor(PlainProcessor),
            StorageConfig::default().with_store(storage.clone()),
        );
        commit_batch_with_receipt(&mut scheduler, block(1, 0));
        let lost = scheduler.schedule(block(2, 1), vec![lane_tx()]);
        lost.wait_committed_blocking();
        commit_batch_with_receipt(&mut scheduler, block(3, 2));
        scheduler.shutdown();

        assert!(journal.entries().is_empty(), "test setup: the journal reads empty");
        let state = SchedulerState::new(StorageConfig::default().with_store(storage.clone()));
        assert_eq!(
            rollback_uncoverable_gap(&state, &journal, &BATCH_IMAGE_ID),
            Some(1),
            "the floor resolves from the pruning root (no entry, no recorded boundary) and the \
             lost receipt still bounds the gap",
        );
        assert_eq!(
            journal.committed_tip().map(|(index, _)| index),
            Some(1),
            "the metadata rows above the boundary are reverted with the rollback",
        );

        restart_refeed_and_settle(
            &state,
            &journal,
            ("the startup pass records the settled boundary", || {
                journal.settled_boundary() == Some(1)
            }),
        );
    }
}

/// Tests the same recovery when a wedged run kept proving new batches above the gap and
/// journalled bundles chaining across it: the journal tail then sits far above the interior gap
/// and must not be trusted as the walk's boundary, and the entries above the gap must be
/// dropped with the rollback (their bundles chain from state the covenant never took, so the
/// settler would skip them forever). The persisted settled boundary is distrusted while the
/// journal holds an entry at or below it, which is exactly the poisoned shape a boundary
/// recorded from such a tail would leave behind.
#[test]
fn committed_gap_with_entries_chained_across_recovers() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        // The pre-restart half: block 1 settled, block 2 committed without its receipt (the
        // gap), block 3 proved above the gap with a journalled bundle chaining across it, and a
        // settled boundary recorded at that tail (the poisoned marker).
        let storage: RocksDbStore = RocksDbStore::open(temp_dir.path());
        let journal = StoreJournal::new(storage.clone());
        let mut scheduler = Scheduler::new(
            ExecutionConfig::default().with_processor(PlainProcessor),
            StorageConfig::default().with_store(storage.clone()),
        );
        commit_batch_with_receipt(&mut scheduler, block(1, 0));
        let lost = scheduler.schedule(block(2, 1), vec![lane_tx()]);
        lost.wait_committed_blocking();
        commit_batch_with_receipt(&mut scheduler, block(3, 2));
        journal.record(
            3,
            &JournalEntry {
                end_index: 3,
                from_block: block_hash(3),
                block_prove_to: block_hash(3),
                seq_commit: seq_commit(),
            },
        );
        journal.record_settled_boundary(3);
        scheduler.shutdown();

        let state = SchedulerState::new(StorageConfig::default().with_store(storage.clone()));
        assert_eq!(
            rollback_uncoverable_gap(&state, &journal, &BATCH_IMAGE_ID),
            Some(1),
            "the journal tail and its poisoned marker are distrusted; the floor resolves from \
             the pruning root and the interior gap still bounds the rollback",
        );
        assert_eq!(
            journal.committed_tip().map(|(index, _)| index),
            Some(1),
            "the metadata rows above the boundary are reverted with the rollback",
        );
        assert!(
            journal.entries().iter().all(|(_, entry)| entry.end_index <= 1),
            "the entries chaining across the gap are dropped with the rollback",
        );
        assert_eq!(journal.settled_boundary(), Some(3), "the poisoned marker is left as-is");

        restart_refeed_and_settle(
            &state,
            &journal,
            ("the startup pass re-records the settled boundary from the mapped tip", || {
                journal.settled_boundary() == Some(1)
            }),
        );
    }
}

/// Runs the restarted half both recovery tests share: builds the scheduler and worker over the
/// rolled-back state, re-feeds the recovered range as the bridge would (commits and fresh
/// receipts, no submit yet), releases the worker's startup gate with the settled tip and waits
/// for `startup_done`, then feeds the re-fed batches plus genuinely new work as live commands.
/// Asserts the recovered range settles ahead of the new work, the boundary marker is recorded,
/// and the journal ends with one contiguous record per checkpoint above the boundary.
fn restart_refeed_and_settle(
    state: &SchedulerState<RocksDbStore, PlainProcessor>,
    journal: &StoreJournal<RocksDbStore>,
    startup_done: (&str, impl Fn() -> bool),
) {
    // The scheduler and worker start over the rolled-back state, exactly as the node builds
    // them after the recovery call. The worker parks at its startup gate until the bridge
    // (this test, through the settlement watch) publishes the covenant's last settlement.
    let mut scheduler = Scheduler::with_state(
        ExecutionConfig::default().with_processor(PlainProcessor),
        state.clone(),
    );
    let settlement_queue: AsyncQueue<ScheduledBundle<SettlementArtifact<Vec<u8>>>> =
        AsyncQueue::new();
    let (settlement_tx, settlement_rx) = watch::channel::<Option<SettlementInfo>>(None);
    let prover = AggregateProver::new(
        SyntheticBackend,
        state.receipt_store(),
        Some(journal.clone()),
        AggregateProverConfig {
            lane_key: Hash::default(),
            covenant_id: None,
            lane_source: ServeLaneProofs,
            settlement_queue: Some(settlement_queue.clone()),
            settlement: Some(settlement_rx),
            bundle_size: 1..=1,
            exits: None,
        },
    );

    // The bridge re-feeds blocks 2 and 3: they re-execute, re-commit at their reoccupied
    // checkpoint indexes, and re-prove (the fresh receipt standing in for the lost one). No
    // submit yet, so the worker stays parked at its gate.
    let two = commit_batch_with_receipt(&mut scheduler, block(2, 1));
    let three = commit_batch_with_receipt(&mut scheduler, block(3, 2));
    assert_eq!(
        journal.committed_tip().map(|(index, _)| index),
        Some(3),
        "the re-fed range re-commits through its top",
    );
    assert_eq!(state.last_committed().index(), 3, "the scheduler tracks the re-fed tip");

    // The bridge's baseline tip releases the worker's gate: the resume pass acts on the journal
    // and the committed-gap pass composes the re-fed range, whose receipts are fresh again,
    // into one covering bundle.
    settlement_tx.send_replace(Some(tip_through(1)));
    let (desc, pred) = startup_done;
    wait_until(desc, Duration::from_secs(10), pred);
    // The marker lands in the committed-gap pass, just past whatever `startup_done` observed,
    // so wait it out rather than assert it mid-flight.
    wait_until("the settled boundary is recorded", Duration::from_secs(10), || {
        journal.settled_boundary() == Some(1)
    });

    // The re-fed batches then join as live commands, and genuinely new work (block 4)
    // arrives past the gap.
    submit_live(&prover, &two);
    submit_live(&prover, &three);
    let four = commit_batch_with_receipt(&mut scheduler, block(4, 3));
    submit_live(&prover, &four);

    // The covering bundle settles first and block 4's new-work bundle last: the recovered range
    // settles ahead of new work, never behind it. A republication race can re-feed the covering
    // entry once more (the worker's gate consumes the settlement change only when it actually
    // parks; when the tip predates the worker's first gate poll, the first loop iteration
    // re-runs the resume advance pass over the just-recorded gap entry), so the order and the
    // coverage are pinned rather than the exact partitioning.
    let mut spans = Vec::new();
    loop {
        let timeout =
            if spans.len() < 4 { Duration::from_secs(10) } else { Duration::from_millis(500) };
        let Some(bundle) = next_bundle(&settlement_queue, timeout) else { break };
        bundle.wait_artifact_published_blocking();
        assert!(bundle.artifact().is_some(), "every bundle carries a real artifact");
        spans.push(bundle.block_prove_to());
        assert!(
            spans.len() <= 5,
            "at most the covering bundles plus the live work may arrive (got {spans:?})",
        );
    }
    assert!(spans.len() >= 4, "the covering and new-work bundles must all settle");
    assert_eq!(spans.first(), Some(&block_hash(3)), "the covered gap settles first");
    assert_eq!(spans.last(), Some(&block_hash(4)), "the new work settles last");
    assert!(spans.contains(&block_hash(2)), "the re-fed range's own bundle settles");
    assert!(
        spans.iter().all(|hash| [block_hash(2), block_hash(3), block_hash(4)].contains(hash)),
        "no bundle may prove outside the recovered range and the new work (got {spans:?})",
    );

    // The journal ends with one contiguous record per re-fed and new checkpoint above the
    // boundary: the live bundles' records replace the covering bundle's span, leaving no gap.
    let recorded: Vec<(u64, u64)> =
        journal.entries().into_iter().map(|(start, entry)| (start, entry.end_index)).collect();
    assert_eq!(recorded, vec![(2, 2), (3, 3), (4, 4)], "no gap may remain in the journal");

    prover.shutdown();
    scheduler.shutdown();
}

/// Encodes the settlement journal of a bundle that chains from a base the covenant never took:
/// identical to [`settlement_journal`] except its proven `prev_state`, so the resume pass's
/// tip-pins check rejects it.
fn stale_settlement_journal() -> Vec<u8> {
    let mut buf = Vec::new();
    StateTransition::encode(
        &mut buf,
        StateTransitionArgs {
            prev_state: &[0xAA; 32],
            prev_lane_tip: &Hash::default(),
            new_state: &[0x11; 32],
            new_lane_tip: &Hash::default(),
            new_seq_commit: &seq_commit(),
            covenant_id: &[0u8; 32],
            tx_image_id: &TX_IMAGE_ID,
            batch_image_id: &BATCH_IMAGE_ID,
            permission_spk_hash: &[0u8; 32],
            deposit_spk_hash: &[0u8; 32],
            lane_key: &Hash::default(),
        },
    );
    buf
}

/// Tests that a journal entry left by a settlement that died without landing (its proven
/// transition chains from a base the covenant never took) is dropped by the resume pass rather
/// than re-fed forever, and the committed-gap pass re-covers the range as one fresh bundle
/// chaining from the on-chain tip. Re-feeding the dead entry instead wedges the settler: every
/// bundle re-folds from the stale base, the settler skips it as a base mismatch, and the entry
/// never resolves.
#[test]
fn committed_gap_with_dead_settlement_entry_recovers() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        // The pre-restart half: block 1 settled, blocks 2 and 3 proved above it, and one journal
        // entry recorded above the settled tip whose receipt chains from a base the covenant
        // never took (the shape a dead settlement leaves behind: the bundle below it never
        // landed, so it chains from that bundle's end state, not the on-chain tip).
        let storage: RocksDbStore = RocksDbStore::open(temp_dir.path());
        let journal = StoreJournal::new(storage.clone());
        let mut scheduler = Scheduler::new(
            ExecutionConfig::default().with_processor(PlainProcessor),
            StorageConfig::default().with_store(storage.clone()),
        );
        commit_batch_with_receipt(&mut scheduler, block(1, 0));
        commit_batch_with_receipt(&mut scheduler, block(2, 1));
        commit_batch_with_receipt(&mut scheduler, block(3, 2));
        journal.record(
            3,
            &JournalEntry {
                end_index: 3,
                from_block: block_hash(3),
                block_prove_to: block_hash(3),
                seq_commit: seq_commit(),
            },
        );
        scheduler.shutdown();

        // The restart half: every receipt resolves, so the rollback policy has nothing to do;
        // the dead entry's aggregate receipt is stored where the re-feed would reload it.
        let state = SchedulerState::<RocksDbStore, PlainProcessor>::new(
            StorageConfig::default().with_store(storage.clone()),
        );
        assert_eq!(
            rollback_uncoverable_gap(&state, &journal, &BATCH_IMAGE_ID),
            None,
            "every committed batch resolves; nothing to roll back",
        );
        state
            .receipt_store()
            .write_agg_receipt(
                AggregatorKey {
                    prefix: Prefix { checkpoint_index: 3.into() },
                    block_hash: block_hash(3).as_bytes(),
                    image_id: AGGREGATOR_IMAGE_ID,
                    seq_commit: seq_commit().as_bytes(),
                },
                stale_settlement_journal(),
            )
            .wait_blocking();

        let settlement_queue: AsyncQueue<ScheduledBundle<SettlementArtifact<Vec<u8>>>> =
            AsyncQueue::new();
        let (settlement_tx, settlement_rx) = watch::channel::<Option<SettlementInfo>>(None);
        let prover = AggregateProver::new(
            SyntheticBackend,
            state.receipt_store(),
            Some(journal.clone()),
            AggregateProverConfig {
                lane_key: Hash::default(),
                covenant_id: None,
                lane_source: ServeLaneProofs,
                settlement_queue: Some(settlement_queue.clone()),
                settlement: Some(settlement_rx),
                bundle_size: 1..=1,
                exits: None,
            },
        );

        // The bridge's baseline tip proves through block 1 and carries the covenant's on-chain
        // state, which the dead entry's receipt does not chain from.
        settlement_tx.send_replace(Some(tip_through(1)));

        // The dead entry is dropped and the range re-covered as one fresh bundle chaining from
        // the tip: the emitted artifact carries the fresh compose's prev_state, not the stored
        // dead receipt's. A republication re-feed of the fresh entry may follow it.
        let mut prev_states = Vec::new();
        loop {
            let timeout = if prev_states.is_empty() {
                Duration::from_secs(10)
            } else {
                Duration::from_millis(500)
            };
            let Some(bundle) = next_bundle(&settlement_queue, timeout) else { break };
            bundle.wait_artifact_published_blocking();
            let artifact =
                bundle.artifact().expect("the re-covered bundle carries a real artifact");
            assert_eq!(
                bundle.block_prove_to(),
                block_hash(3),
                "the re-covered bundle spans the dead entry's whole range",
            );
            prev_states.push(artifact.prev_state);
            assert!(prev_states.len() <= 2, "at most the compose and one republication re-feed");
        }
        assert_eq!(
            prev_states.first(),
            Some(&[0x00; 32]),
            "the range is re-composed from the tip, not re-fed from the dead receipt",
        );

        let recorded: Vec<(u64, u64)> =
            journal.entries().into_iter().map(|(s, e)| (s, e.end_index)).collect();
        assert_eq!(recorded, vec![(2, 3)], "the re-covered entry replaces the dead one");

        prover.shutdown();
        state.storage().shutdown();
    }
}

/// A lane source serving every fetch except one dead block: the block exists on chain but its
/// lane proof is unobtainable (pruned lane history), the shape the live store served for the
/// era's prove-through block.
struct DeadBlockLaneProofs {
    /// Block whose lane proof always fails.
    dead: Hash,
}

impl LaneProofSource for DeadBlockLaneProofs {
    async fn fetch_lane_proof(
        &self,
        req: LaneProofRequest,
    ) -> Result<GetSeqCommitLaneProofResponse, LaneProofError> {
        if req.block == self.dead {
            Err(LaneProofError("dead block: lane history pruned".into()))
        } else {
            Ok(GetSeqCommitLaneProofResponse {
                smt_proof: Vec::new(),
                lane: None,
                payload_and_ctx_digest: Hash::default(),
                parent_seq_commit: Hash::default(),
                inactivity_shortcut: Hash::default(),
            })
        }
    }
}

/// Tests that a committed-gap range whose final block's lane proof is unobtainable still makes
/// progress: the bundle's end walks down to the previous batch and covers the live prefix,
/// instead of deferring the same dead-ended range on every wake. The still-dead suffix stays
/// for a later pass, matching how live bundling parks on a dead final block.
#[test]
fn committed_gap_walks_end_down_over_a_dead_final_block() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        // The pre-restart half: block 1 settled and journaled, blocks 2 and 3 proved above it
        // with no journal record (the kill preceded it). Block 3's lane proof is dead.
        let storage: RocksDbStore = RocksDbStore::open(temp_dir.path());
        let journal = StoreJournal::new(storage.clone());
        let mut scheduler = Scheduler::new(
            ExecutionConfig::default().with_processor(PlainProcessor),
            StorageConfig::default().with_store(storage.clone()),
        );
        commit_batch_with_receipt(&mut scheduler, block(1, 0));
        commit_batch_with_receipt(&mut scheduler, block(2, 1));
        commit_batch_with_receipt(&mut scheduler, block(3, 2));
        journal.record(
            1,
            &JournalEntry {
                end_index: 1,
                from_block: block_hash(1),
                block_prove_to: block_hash(1),
                seq_commit: seq_commit(),
            },
        );
        scheduler.shutdown();

        let state = SchedulerState::<RocksDbStore, PlainProcessor>::new(
            StorageConfig::default().with_store(storage.clone()),
        );
        let settlement_queue: AsyncQueue<ScheduledBundle<SettlementArtifact<Vec<u8>>>> =
            AsyncQueue::new();
        let (settlement_tx, settlement_rx) = watch::channel::<Option<SettlementInfo>>(None);
        let prover = AggregateProver::new(
            SyntheticBackend,
            state.receipt_store(),
            Some(journal.clone()),
            AggregateProverConfig {
                lane_key: Hash::default(),
                covenant_id: None,
                lane_source: DeadBlockLaneProofs { dead: block_hash(3) },
                settlement_queue: Some(settlement_queue.clone()),
                settlement: Some(settlement_rx),
                bundle_size: 1..=1,
                exits: None,
            },
        );

        settlement_tx.send_replace(Some(tip_through(1)));

        // The pass walks the end down from the dead block 3 to the live block 2 and covers the
        // prefix, recording its entry; no bundle ever proves through the dead block. A
        // republication re-feed of the covered entry may follow the compose.
        let mut proved_to = Vec::new();
        loop {
            let timeout = if proved_to.is_empty() {
                Duration::from_secs(10)
            } else {
                Duration::from_millis(500)
            };
            let Some(bundle) = next_bundle(&settlement_queue, timeout) else { break };
            bundle.wait_artifact_published_blocking();
            assert!(bundle.artifact().is_some(), "the covered prefix carries a real artifact");
            assert_eq!(
                bundle.block_prove_to(),
                block_hash(2),
                "the end walks down over the dead final block to the previous batch",
            );
            proved_to.push(bundle.checkpoint_index());
            assert!(proved_to.len() <= 2, "at most the compose and one republication re-feed");
        }
        assert_eq!(proved_to.first(), Some(&2), "the walk-down covered the live prefix");

        let recorded: Vec<(u64, u64)> =
            journal.entries().into_iter().map(|(s, e)| (s, e.end_index)).collect();
        assert_eq!(
            recorded,
            vec![(2, 2)],
            "the covered prefix is journaled; the dead suffix stays for a later pass",
        );

        prover.shutdown();
        state.storage().shutdown();
    }
}
