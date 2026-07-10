//! `HeadFalseNegative` — an `ObjectStore` wrapper that returns a false-negative
//! HEAD/exists for exactly one WAL id while the object stays durably present.
//!
//! Why override `get_opts` and not `head`: in object_store 0.14 `head` is no
//! longer a trait method you can override — it lives on the `ObjectStoreExt`
//! blanket impl (`impl<T> ObjectStoreExt for T`) which routes every `head(p)`
//! to `self.get_opts(p, GetOptions::new().with_head(true))`. So the ONLY seam
//! to lie at the HEAD layer is `get_opts` with `options.head == true`. We
//! special-case that probe for the one target WAL id (returning NotFound) and
//! pass EVERYTHING else — including real GETs of that same object during
//! replay — straight through. That is exactly the wal-head-contiguity fault:
//! HEAD says "gone", the bytes are still there.
//!
//! The WAL object path is `.../wal/{id:020}.sst` (slatedb `paths.rs:75`); we
//! match on that suffix so the wrapper is agnostic to the root prefix.
//!
//! Baseline does NOT use this wrapper. It is a correct pass-through for the
//! frontier-truncation case an executor drives next episode.
//!
//! TODO(executor): the wal-head-contiguity case must install this on the
//! *verify/reopen* open (not the run open) and sweep which id to lie about via
//! the crashclock timing space; confirm the id→path suffix still matches once
//! the reopen frontier search is exercised end-to-end.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

pub struct HeadFalseNegative {
    inner: Arc<dyn ObjectStore>,
    /// The `wal/{id:020}.sst` suffix whose HEAD probe we falsify.
    target_suffix: String,
    wal_id: u64,
}

impl HeadFalseNegative {
    pub fn new(inner: Arc<dyn ObjectStore>, wal_id: u64) -> Self {
        Self {
            inner,
            target_suffix: format!("wal/{wal_id:020}.sst"),
            wal_id,
        }
    }

    fn is_target(&self, location: &Path) -> bool {
        location.as_ref().ends_with(&self.target_suffix)
    }
}

impl fmt::Debug for HeadFalseNegative {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HeadFalseNegative(wal_id={}, {:?})", self.wal_id, self.inner)
    }
}

impl fmt::Display for HeadFalseNegative {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HeadFalseNegative(wal_id={}, {})", self.wal_id, self.inner)
    }
}

#[async_trait]
impl ObjectStore for HeadFalseNegative {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // Falsify ONLY the HEAD/exists probe (options.head) for the one target
        // WAL id. Real reads of the same object pass through — the bytes are
        // durably present; only existence-by-HEAD lies.
        if options.head && self.is_target(location) {
            return Err(Error::NotFound {
                path: location.to_string(),
                source: "head-false-negative (fault injection)".into(),
            });
        }
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
