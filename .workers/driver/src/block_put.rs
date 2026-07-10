//! `BlockWalPut` — an `ObjectStore` wrapper that holds the WAL SST PUT
//! in-flight (blocked) until it is explicitly released, so a reader can observe
//! the exact flush boundary where a value is in the WAL buffer but its WAL
//! object has NOT yet landed durably.
//!
//! Why the WAL single-PUT path: `TableStore::write_sst` routes WAL SSTs
//! (`SsTableId::Wal`) through `write_sst_in_object_store`, which is a single
//! `put_opts` call (tablestore.rs:382-392, 1117-1130). Compacted SSTs use the
//! multipart/streaming path instead. So blocking `put_opts` for the
//! `.../wal/{id:020}.sst` object stalls exactly the WAL flush's durable PUT.
//!
//! Ordering this exploits (verified against source):
//!   do_flush (wal_buffer.rs:300) -> do_flush_one_wal -> table_store.write_sst
//!   -> put_opts (the PUT).  Only AFTER write_sst returns Ok does do_flush take
//!   the lock, set last_flushed_seq (wal_buffer.rs:326), fire
//!   WalEvent::WalFlushed (335-338); the DbWalObserver listener then calls
//!   oracle.advance_durable_seq(seq) (db.rs:2064-2076). Remote reads cap at
//!   oracle.last_remote_persisted_seq() == last_durable_seq (reader.rs:113,
//!   oracle.rs:103-105). So while this wrapper holds the PUT, the watermark
//!   stays BELOW the flushing batch and Remote MUST exclude the in-flight value.
//!
//! Arming: the gate starts DISARMED (pure pass-through) so the DB open and the
//! buffered writes are never stalled. The driver arms it immediately before it
//! triggers db.flush(), so only the flush's WAL PUT is caught.

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use tokio::sync::Notify;

/// Shared control handle for the WAL-PUT block. The wrapper reads it; the driver
/// arms / observes / releases it.
pub struct PutGate {
    /// When false the wrapper is a pure pass-through (open + buffered writes).
    armed: AtomicBool,
    /// When true a blocked WAL PUT may proceed.
    released: AtomicBool,
    /// Count of WAL PUTs that entered the block (i.e. the fault actually armed).
    entered: AtomicU64,
    /// Wakes a blocked PUT once `released` flips true.
    release_notify: Notify,
}

impl PutGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            armed: AtomicBool::new(false),
            released: AtomicBool::new(false),
            entered: AtomicU64::new(0),
            release_notify: Notify::new(),
        })
    }

    /// Start catching WAL PUTs. Call immediately before triggering the flush.
    pub fn arm(&self) {
        self.armed.store(true, SeqCst);
    }

    /// Let any blocked (and any future) WAL PUT proceed.
    pub fn release(&self) {
        self.released.store(true, SeqCst);
        self.release_notify.notify_waiters();
    }

    /// Number of WAL PUTs that have entered the block so far.
    pub fn entered_count(&self) -> u64 {
        self.entered.load(SeqCst)
    }

    /// Wait (async, polled) until at least one WAL PUT is blocked in-flight, or
    /// `deadline` elapses. Returns true if a PUT was actually caught.
    pub async fn wait_entered(&self, deadline: std::time::Duration) -> bool {
        let start = std::time::Instant::now();
        loop {
            if self.entered_count() >= 1 {
                return true;
            }
            if start.elapsed() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }
}

pub struct BlockWalPut {
    inner: Arc<dyn ObjectStore>,
    gate: Arc<PutGate>,
}

impl BlockWalPut {
    pub fn new(inner: Arc<dyn ObjectStore>, gate: Arc<PutGate>) -> Self {
        Self { inner, gate }
    }

    /// Match any WAL SST object: `.../wal/{id:020}.sst` (paths.rs:75). WAL id
    /// varies per flush, so we match the `wal/` segment + `.sst` suffix rather
    /// than a fixed id.
    fn is_wal_sst(location: &Path) -> bool {
        let s = location.as_ref();
        s.ends_with(".sst") && s.contains("wal/")
    }

    /// Block here (async) until released, if armed and this is a WAL SST PUT.
    async fn maybe_block(&self, location: &Path) {
        if !(self.gate.armed.load(SeqCst) && Self::is_wal_sst(location)) {
            return;
        }
        self.gate.entered.fetch_add(1, SeqCst);
        // Safe wait: create the notified() future BEFORE re-checking the flag so
        // a release() that races cannot be missed.
        loop {
            if self.gate.released.load(SeqCst) {
                return;
            }
            let notified = self.gate.release_notify.notified();
            if self.gate.released.load(SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

impl fmt::Debug for BlockWalPut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockWalPut({:?})", self.inner)
    }
}

impl fmt::Display for BlockWalPut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockWalPut({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for BlockWalPut {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        // Hold the WAL SST PUT in-flight until released. The value is durably
        // absent for the whole block window — Remote MUST exclude it.
        self.maybe_block(location).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        // WAL SSTs never use multipart (only compacted SSTs do); pass through.
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
