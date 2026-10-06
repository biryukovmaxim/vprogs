# CUDA invalid-proof wedge: kill mid-prove leaves an uncoverable committed gap (2026-10-06)

Incident report and fix brief. Status: fixed on branch `fix/uncoverable-gap-rollback`
(rollback + re-feed recovery, prove retry + explicit verification).

## Incident

The live gpu prover (build 055ae28, same aggregate-prover code as 5da27851) panicked mid-prove
at 09:08 UTC:

- risc0 CUDA produced an invalid proof segment: `proving failed: verify segment / verification
  indicates proof is invalid`. This is the known upstream bug risc0/risc0#3760 at risc0-zkvm
  3.0.5; 3.0.6 does not fix it.
- The `.expect("proving failed")` in `zk/backend/risc0/api/src/backend.rs` (prove path) killed
  the proving thread, so the process kept running but proved nothing.
- Batch 1881878 had already committed: `ScheduledBatch::commit` persists `BatchMetadata` (and
  `last_committed`) BEFORE proving starts, so the commit is durable while the receipt never
  existed.
- Settlements froze from that moment (no new aggregate receipts).

On restart the node wedged instead of recovering:

- The startup committed-gap pass logged
  `[ERROR] committed batch 1881878 above the journal tail lacks its metadata or receipt; covering nothing and leaving 1881878..=1929858 uncovered`
  (`reform_committed_gap`, zk/aggregate-prover/src/worker.rs).
- The node then idled permanently: 0 executions, 0 proofs, no settlements. The pass outcome is
  terminal by contract, the scheduler never re-schedules a committed batch (the bridge feeds
  only from the persisted canonical tip), and any new bundle would chain from an executed state
  the covenant never settled, so the settler base-mismatch-skips every later bundle forever.

## Root cause chain

1. risc0/risc0#3760: the CUDA prover path intermittently emits invalid proof segments at
   risc0-zkvm 3.0.5, surfacing as a prove error or as a receipt that fails verification.
2. The backend treated one failed attempt as fatal (`.expect`), killing the proving thread
   instead of retrying.
3. Commit happens before prove, so a kill in that window durably persists `BatchMetadata`
   rows whose per-batch receipts never existed.
4. The committed-gap pass can only compose cached receipts; a receipt miss is terminal, and no
   mechanism re-feeds committed blocks, so the range above the journal tail became permanently
   uncoverable.

## Why re-proving in place is impossible

A 2026-10-02 spike (session that also wrote the original red test) proved the batch-prover
miss-path cannot run from persisted state alone:

- The prover's resource-id access set and its ordered transaction-artifact list derive from the
  caller-supplied transaction list (scheduling/scheduler/src/scheduled_batch.rs:99-101 and
  295-318), not from anything persisted.
- No state space holds an access set (storage/types/src/state_space.rs:2-25);
  `StatePtrLatest` is latest-wins writes-only, and the SMT's `Tree::prove` takes the key list
  as an input (core/smt/src/tree.rs:77), so neither can reconstruct it.
- Only the bridge re-feed plus the scheduler's restore path re-supply transactions
  (scheduling/scheduler/src/scheduler.rs:255-266), and restore is disabled on proving nodes
  (`Vm::supports_restore`, zk/vm/src/vm.rs:134-137): a proving node re-executes replayed
  blocks instead.

Conclusion recorded then and kept here: recovery must roll the executor back and let the re-feed
re-execute and re-prove the range.

## The rollback + re-feed recovery

Two functions and one wiring call:

- `rollback_uncoverable_gap(state, journal, batch_image_id)` in
  zk/aggregate-prover/src/worker.rs: walks the committed range above the journal tail with the
  same metadata/non-empty-receipt heuristic the gap pass applies; the first batch whose
  metadata or cached receipt cannot be resolved bounds the uncoverable range, and the persisted
  executor state is rolled back to the boundary just below it. A coverable range (the normal
  restart) returns without touching anything, so plain restarts keep composing their gap bundle
  from cached receipts as before.
- `rollback_persisted_to(state, target_index)` in scheduling/scheduler/src/scheduler.rs: the
  store-level sibling of `Scheduler::rollback_to` for the window before any scheduler exists.
  It repoints the persisted latest pointers via the stored rollback pointers (the same
  `Write::Rollback` command), lowers `last_committed`, and deletes the `BatchMetadata` rows
  above the target. The deletion matters twice over: `committed_tip()` reads the highest
  metadata key, so stale rows would keep the startup gap pass seeing the uncoverable range, and
  checkpoint ids are allocated as `entries.back() + 1`
  (storage/canonical-chain/src/manager.rs), so dropping the rows makes the re-fed blocks
  reoccupy their original checkpoint indexes. Refuses with `PruningConflict` when the pruning
  root has passed the boundary (KIP-21 lane-purge crossing is an independent blocker for ranges
  older than the pruning horizon; out of scope, and irrelevant for gaps well inside F).
- The seam: runner/src/node.rs `build_proving_node`, between `SchedulerState::new` and
  `ProvingPipeline::aggregate`. The aggregate prover's worker thread spawns at pipeline
  construction and runs its startup passes immediately, so the rollback must land before that;
  the app indexer is attached to the state first so the rollback's resource reverts feed it.
  Exec and journal-free paths are untouched.

After the rollback the canonical tip sits at the boundary, so the bridge (which follows the
scheduler's persisted canonical tip) re-fetches and re-feeds the range: batches re-execute,
re-commit at their reoccupied indexes, and re-prove with fresh receipts. The startup gap pass
then sees a consistent store (nothing committed past the tail), the re-fed range settles as
normal work ahead of genuinely new blocks, and the covenant's on-chain tip remains the chaining
base for the settler.

## Prove-path hardening

`prove_with_retries` in zk/backend/risc0/api/src/backend.rs now backs all three prove paths
(transaction, batch, aggregator): up to three attempts, a fresh `ExecutorEnv` per attempt (an
env is consumed by proving), and an explicit `Receipt::verify` against the matching image id
before any receipt is returned or stored, so an invalid receipt can no longer reach the cache.
A failed attempt logs a warning and retries; after the final attempt the call panics with the
attempt count and the upstream bug, and the restart now recovers via the rollback above instead
of wedging. The prove trait signatures stay infallible.

## The test

zk/aggregate-prover/tests/committed_gap_receipt_miss.rs pins the recovery. History: the
2026-10-02 session parked it as `.disabled` assuming in-place re-prove; it is re-armed here
reworked to drive the rollback + re-feed honestly. The pre-half commits block 1 (journaled
tail), block 2 (metadata durable, receipt lost), and block 3 (with receipt), then shuts down.
The restart half runs `rollback_uncoverable_gap` over a fresh state and asserts the executor
rolled back to the tail with the stale rows gone; the re-fed blocks re-commit through the top
of the range; the covering bundle settles ahead of block 4's new-work bundle; and the journal
ends with one contiguous record per checkpoint above the tail. Demonstrated red (recovery
stubbed off) and green in debug and release.

## Loose ends

- The worker's startup gap pass can still log the miss when it races a batch whose prove has
  not landed yet (the miss-path message now says so); that range is live work the normal
  bundling path covers, so it is benign.
- The mirror-image race (the gap pass composing a range whose re-fed batches are also queued
  live) converges through the settler's supersede path; the window is the worker's two store
  reads against the bridge's first re-fed commit and predates this fix.
- A KIP-21 lane purge that crosses the gap leaves `PruningConflict` and no recovery; the
  reactivation-proof work that unblocks those ranges is separate.
