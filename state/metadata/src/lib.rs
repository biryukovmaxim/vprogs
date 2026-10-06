use vprogs_core_types::{BatchMetadata, Checkpoint};
use vprogs_storage_types::{ReadStore, StateSpace, WriteBatch};

/// Well-known metadata keys.
mod keys {
    /// Key for the root batch (oldest surviving batch; index + metadata stored together).
    pub const ROOT: &[u8] = b"root";
    /// Key for the last committed batch (index + metadata stored together).
    pub const LAST_COMMITTED: &[u8] = b"last_committed";
    /// Key for the settled boundary (highest checkpoint whose bundle has settled; bare index).
    pub const SETTLED_BOUNDARY: &[u8] = b"settled_boundary";
}

/// Provides type-safe operations for the Metadata column family.
///
/// StateMetadata stores system-level metadata such as root tracking and commit progress, allowing
/// crash-fault tolerant operations.
pub struct StateMetadata;

impl StateMetadata {
    /// Returns the root checkpoint (oldest surviving batch), or defaults if no batches have been
    /// committed yet.
    ///
    /// The root is initialized when the first batch commits and advances forward as pruning deletes
    /// older batches. A default (index 0) indicates a fresh database with no commits.
    pub fn root<M: BatchMetadata, S>(store: &S) -> Checkpoint<M>
    where
        S: ReadStore,
    {
        store
            .get(StateSpace::Metadata, keys::ROOT)
            .map(|bytes| borsh::from_slice(&bytes).expect("corrupted store: unrecoverable"))
            .unwrap_or_default()
    }

    /// Sets the root checkpoint (oldest surviving batch).
    pub fn set_root<M: BatchMetadata, W>(wb: &mut W, checkpoint: &Checkpoint<M>)
    where
        W: WriteBatch,
    {
        wb.put(
            StateSpace::Metadata,
            keys::ROOT,
            &borsh::to_vec(checkpoint).expect("failed to serialize Checkpoint"),
        );
    }

    /// Returns the last committed batch, or defaults if no batches have been committed yet.
    pub fn last_committed<M: BatchMetadata, S>(store: &S) -> Checkpoint<M>
    where
        S: ReadStore,
    {
        store
            .get(StateSpace::Metadata, keys::LAST_COMMITTED)
            .map(|bytes| borsh::from_slice(&bytes).expect("corrupted store: unrecoverable"))
            .unwrap_or_default()
    }

    /// Sets the last committed batch.
    pub fn set_last_committed<M: BatchMetadata, W>(wb: &mut W, checkpoint: &Checkpoint<M>)
    where
        W: WriteBatch,
    {
        wb.put(
            StateSpace::Metadata,
            keys::LAST_COMMITTED,
            &borsh::to_vec(checkpoint).expect("failed to serialize Checkpoint"),
        );
    }

    /// Returns the highest checkpoint whose bundle has settled, or `None` when no worker has
    /// observed a settlement.
    ///
    /// Settlement-journal entries compact away as settlements land, so an empty journal cannot
    /// distinguish "nothing ever settled" from "settled and compacted"; this boundary preserves
    /// that knowledge across the deletions.
    pub fn settled_boundary<S>(store: &S) -> Option<u64>
    where
        S: ReadStore,
    {
        store
            .get(StateSpace::Metadata, keys::SETTLED_BOUNDARY)
            .map(|bytes| borsh::from_slice(&bytes).expect("corrupted store: unrecoverable"))
    }

    /// Sets the settled boundary.
    pub fn set_settled_boundary<W>(wb: &mut W, index: u64)
    where
        W: WriteBatch,
    {
        wb.put(
            StateSpace::Metadata,
            keys::SETTLED_BOUNDARY,
            &borsh::to_vec(&index).expect("failed to serialize boundary index"),
        );
    }
}
