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
- The worker's startup gate consumes the settlement watch's change flag only when it actually
  parks on it; when the tip predates the worker's first gate poll (easy under test load, near
  impossible in production, where the worker spawns before the bridge), the first main-loop
  iteration treats the baseline republication as an advance and the resume advance pass
  re-feeds the just-recorded covering entry once. The settler's supersede path absorbs the
  duplicate; the recovery tests pin order and coverage rather than exact bundle counts.

## Postscript: the empty-journal boundary (2026-10-06 14:34 UTC)

The first live acceptance run on the wedged gpu store exposed a hole in the shipped recovery.
That store had already been restarted once before the fix existed, and that restart's resume
pass had deleted the journal entry the on-chain tip covered, so the journal read empty at the
fixed node's startup: the recovery's tail defaulted to 0, the walk probed checkpoint 1 (long
pruned below the committed frontier), took the absent metadata for the uncoverable boundary,
and refused the rollback to checkpoint 0 with `PruningConflict`. The settled boundary
(1881877) was known only to the worker's tip scan, which runs after the recovery seam.

The fix makes the walk's floor durable and structural rather than journal-dependent:

- The floor is the highest of the journal tail, the persisted settled boundary, and the
  pruning root. Nothing below the root is recoverable and nothing above it is ever pruned
  (metadata and receipts alike), so a walk from the root only ever stops at a genuine miss,
  however deep the committed range runs above the boundary (the live store's executor kept
  committing new batches above the wedge during startup sync).
- The settled boundary is persisted (StateMetadata key) by the committed-gap pass whenever
  the on-chain tip resolves one, so it survives the entry deletions that empty the journal
  and raises the floor above the root for stores whose receipts miss on an image-id change
  (where a root-floor walk would over-roll below the settled tip and the settler would skip
  everything re-formed from there).
- Metadata holes are skipped rather than counted as misses: a reorg-canceled batch keeps its
  checkpoint id but never commits metadata, so the row set carries interior holes above the
  root, and the first hole would otherwise masquerade as the wedge.

With this, the wedged store recovers on the first restart: the walk from the root finds every
settled batch above it coverable, stops at batch 1881878's missing receipt, and rolls the
executor back to 1881877. The consumed-journal restart (journal already emptied by a prior
resume, no marker yet) is pinned by a second test alongside the original; both run green in
debug and release.

## Postscript 2: the journal tail is not a boundary (2026-10-06 15:26 UTC)

The second deploy found the wedged store in a new shape: the wedged run had not been idle. Its
live path kept proving new batches above the gap (per-batch proving works fine; only the gap
range is unprovable), formed bundles across it, and recorded 31 journal entries spanning
1952639..=1997644, all skipped by the settler as base mismatches. At the next restart the
recovery's floor included the journal tail, so the floor sat above the committed tip, the walk
range was empty, and the recovery returned without acting: the interior gap sat below the
journal's first entry the whole time.

Three corrections:

- The floor no longer includes the journal tail. It is the pruning root raised by the persisted
  settled boundary, and the boundary is trusted only while the journal holds no entry at or
  below it (entries above the settled tip are unsettled by definition; a boundary sitting above
  an entry contradicts it). The read-time guard also de-poisons this store: the deployed build's
  gap pass had recorded the unmapped fallback (the journal tail) as the settled boundary, which
  alone would have kept every later walk empty. The gap pass now records the boundary only when
  the on-chain tip actually maps to a batch.
- A normal restart now walks the whole root-to-tip span (receipts above the root always
  resolve, so the walk returns without rolling anything back); only the cost grows, and it is
  bounded by the pruning window.
- The rollback drops journal entries extending above the target: their bundles chain from
  re-executed state the covenant never took, and the settler would skip them forever. The
  re-feed re-records them from the recovered state.

A third test pins the exact shape: entries journalled above an interior receipt gap, a settled
boundary recorded at that tail, restart, floor from the root, boundary at the gap, stale
entries dropped, recovered range and new work settling in order.

## Postscript 3: the dead-settlement entry and the dead final block (2026-10-07 02:08 UTC)

The stack ran green for 14 h after the second fix (83 settlements overnight), then froze. A
funding race double-spend killed one settlement: two settlements built seconds apart funded
their fees from the same UTXOs, the later one could never land, and the settler correctly
recognized the conflict ("covenant outpoint spent by another settlement"). The freeze itself
came from what the dead settlement left behind.

- The wedge: journal entries whose bundles chain from a base the covenant never took (the dead
  settlement's end state, not the on-chain tip). The settler skips each re-fed bundle as a
  base mismatch, but the skip's resolution cannot delete them: it verifies the CURRENT tip's
  outpoint, which the live continuation holds unspent, correctly. Nothing else ever removed
  the entries, so every wake re-folded from the same stale base and the journal grew an entry
  per skipped bundle.
- The restart failure: the resume pass cannot map the tip's boundary into the entries' span
  (it sits below them) and re-fed the tail unchanged; and the committed-gap re-form, pinned by
  its deferral bound to the era's committed tip, deferred forever on that block's unobtainable
  lane proof (the block exists on chain but its lane history is pruned).

Fixes:

- The resume pass now tells the no-mapping case apart by the first entry's own receipt: an
  entry chains from the tip iff its proven transition starts at the tip's state and lane tip.
  Chaining entries re-feed as before (the normal pending tail); dead ones are dropped, and the
  committed-gap pass re-covers their range as one bundle chaining from the tip. An entry whose
  receipt cannot be reloaded counts as dead, matching the re-feed's own drop.
- The committed-gap bundle's end now walks down over dead final blocks: each failed end
  retries through the previous non-empty batch until one proves, so the pass makes progress
  instead of deferring the same dead-ended range every wake. The still-dead suffix stays for a
  later pass (exactly how live bundling parks on a dead final block), and the journal tail
  advancing past each covered prefix compounds the progress.
- The funding race itself is not separately serialized: the settler's existing fee-rejected
  loop already re-funds from another UTXO when the node names the spent input. The residual
  window is both colliding submissions accepted into the mempool before either confirms, which
  ends as this dead-settlement conflict and is now cleaned by the resume drop above. Two tests
  pin the pair: the dead-chaining entry is dropped and its range re-covered from the tip, and
  the dead final block walks the end down to the live prefix.

## Postscript 4: receipts present but poisoned (2026-10-07 09:04 UTC)

The third deploy ran the new paths, then the gap re-cover's compose hit a deterministic guest
assert twice ("resource hash mismatch": the batch verifier's per-resource check in
zk/abi/src/batch_processor/verifier.rs) and the third attempt's panic killed the proving
thread. Simultaneously the walk-down correctly found no live end: the range carried persisted
metadata and cached receipts from a lineage the live chain no longer matches (its blocks' lane
proofs are unobtainable), so the receipts are present but poison. The presence-only probe
passed, the compose ran, and only the guest assert plus the panic stood between the poisoned
range and a bad settlement. The same wedge class as the original incident, generalized from
receipt-absent to receipt-invalid.

Fixes:

- The committed-gap pass now probes each cached receipt's proven pins host-side: the receipt's
  batch transition must carry the batch metadata's lane tips, and each receipt's entry pins
  must continue the previous receipt's exit pins (the host-side cousin of the guest's
  per-resource hash assert; a receipt that does not decode as a batch transition is left to
  the guest's own check). A contradiction counts as a miss and is recorded as a durable
  unprovable boundary (a StateMetadata key), which the next startup's rollback consumes:
  the walk treats the recorded index as a guaranteed miss, rolls the executor below it,
  drops the stale metadata rows and journal entries, and clears the finding.
- The walk-down's exhaustion path records the same unprovable boundary when the node is
  demonstrably healthy: it probes the lane source for the settlement tip's own block (the
  bridge observed it on the live chain), so a reachable tip with an all-dead range means dead
  blocks, not a stalled node. An unreachable tip keeps the plain deferral. A partial cover
  (the walk-down proved through a lower end) now defers its remainder instead of abandoning
  it, so nothing strands between the covered bundle and new work.
- The prove retries classify deterministic failures: a guest assert skips the retry loop
  entirely and panics immediately naming the class (the host-side probe and the startup
  rollback are the paths that act on it; the panic stays the last resort that keeps an
  invalid receipt from being composed).
- The policy cannot do the receipt decode itself (it has no backend bound, so no
  journal-bytes projection), which is why the finding flows through the durable marker
  instead: the worker (which holds the backend) validates, the startup policy (which holds
  the rollback) acts. Recovery is therefore two-phase on a poisoned store: the run that
  discovers the contradiction records it, the next restart rolls back below it and re-executes
  the range from the live chain.

The poisoned-receipt test pins the whole loop: a well-encoded receipt whose exit lane tip
contradicts its metadata, the worker recording the boundary, the next startup's rollback
below the range, and the re-feed settling ahead of new work. The test fixtures across the
committed-gap suites now write metadata-consistent batch-transition receipts (chained lane
tips), which is what the probe compares.

## Postscript 5: the parked worker thread (2026-10-07 09:49 UTC)

The marker-consuming restart (the fourth deploy) executed correctly: the rollback landed at
2226447, the resume and gap re-form ran, the settler skipped and resolved the superseded
bundles on chain, and the startup drain resolved the republished tip through the lane-tip
fallback. Then the worker thread went silent for 40+ minutes while the rest of the node hummed:
the bridge replayed and synced, the executor re-executed the reverted range and caught up to
the live tip, and the batch prover ran a real GPU burst for the poisoned range. Ready receipts
and newly scheduled batches produced no wake.

Root cause: the worker was not parked, it was blocked. Its first bundle after the recovery
waits on the front batch (a poisoned-range block, cache-missed, so nothing formed until the
GPU burst published it ~10:10), then submits one aggregate prove. The risc0 client call has no
timeout of its own, and that request was lost (the live shape matches a request issued while
the CUDA server was still settling after process start: it is never answered and never errors,
while every later request, including the batch burst, is served). The blocking prove call
holds the worker thread forever; no inbox notification, artifact latch, or watch change can
reach it. The wake primitives were audited and are sound (cancellation-token latches,
notify_one queues), which is why nothing else on the node stalled.

Fix: every prove attempt now runs on the blocking pool under a per-attempt timeout
(`VPROGS_PROVE_TIMEOUT_SECS`, defaulting to 30 minutes, well above real prove durations).
A timed-out attempt is abandoned (its thread lingers on the lost request) and retried like a
failed one, so a lost request costs one timeout instead of the worker; exhaustion after the
third attempt panics into the restart recovery as before. A unit test pins it: a prove call
that never errors and never returns is retried once per attempt and the final panic names the
attempts. The api crate's tokio dependency is host-gated, so the guest (no_std) build is
unchanged.

## Postscript 6: receipts surviving the rollback (2026-10-07 11:25 UTC)

The fifth deploy executed the whole recovery as designed (rollback at 2242446, bridge replay,
re-execution underway), and then the re-cover's compose hit the guest's resource-hash assert
again, this time on a receipt the host probe had passed: the deterministic classifier fired
immediately with no retries, killing the proving thread cleanly. Root cause: the rollback
deleted the metadata rows and journal entries above the target but left the cached receipts.
Receipt keys are checkpoint index + block hash + image id, so a block whose hash repeats
between the reverted lineage and the live re-execution hits the stale key, and the re-cover
reuses a receipt proved under the reverted lineage's per-resource assumptions. The iteration-5
host probe validates lane pins and the state splice, exactly the dimensions it can see; the
per-resource hashes the guest additionally checks were never probed host-side (the seam the
iteration-5 report flagged).

Fix: rollback_persisted_to now invalidates every cached receipt above the target, using the
pruning worker's own checkpoint-granular invalidate_checkpoint (one prefix scan per checkpoint
index; batch, transaction, and aggregate receipts share the index prefix, so all three die
together). The re-covered range then proves fresh from the live-derived batches, which cannot
disagree with themselves. The cost is GPU work per rollback (no cache hits above the
boundary), which is the accepted trade against mirroring the guest verifier host-side.

The deterministic-panic path stays: converting it into a marker-record-and-continue would
mean threading an error through the infallible prove trait across three crates, and with
receipts invalidated at the rollback this recurrence has no path back.
