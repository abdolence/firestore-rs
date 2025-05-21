use crate::FirestoreConsistencySelector;
use rsb_derive::*;

/// Parameters that define the behavior of a Firestore session or a specific set of operations.
///
/// `FirestoreDbSessionParams` allow for configuring aspects like read consistency
/// (e.g., reading data as of a specific time) and caching behavior for operations
/// performed with a [`FirestoreDb`](crate::FirestoreDb) instance that is associated
/// with these parameters.
///
/// These parameters can be applied to a `FirestoreDb` instance using methods like
/// [`FirestoreDb::with_session_params()`](crate::FirestoreDb::with_session_params) or
/// [`FirestoreDb::clone_with_session_params()`](crate::FirestoreDb::clone_with_session_params).
/// ```
#[derive(Clone, Builder)]
pub struct FirestoreDbSessionParams {
    /// Specifies the consistency guarantee for read operations.
    ///
    /// If `None` (the default), strong consistency is used (i.e., the latest version of data is read).
    /// Can be set to a [`FirestoreConsistencySelector`] to read data at a specific
    /// point in time or within a transaction.
    pub consistency_selector: Option<FirestoreConsistencySelector>,

    /// Defines the caching behavior for this session.
    /// Defaults to [`FirestoreDbSessionCacheMode::None`].
    ///
    /// This field is only effective if the `caching` feature is enabled.
    #[default = "FirestoreDbSessionCacheMode::None"]
    pub cache_mode: FirestoreDbSessionCacheMode,
}

/// Defines the caching mode for Firestore operations within a session.
///
/// This enum is used in [`FirestoreDbSessionParams`] to control how and if
/// caching is utilized for read operations.
#[derive(Clone)]
pub enum FirestoreDbSessionCacheMode {
    /// No caching is performed. All read operations go directly to Firestore.
    None,
    /// Enables read-through caching.
    ///
    /// When a read operation is performed:
    /// 1. The cache is checked first.
    /// 2. If data is found in the cache, it's returned.
    /// 3. If data is not in the cache, it's fetched from Firestore, stored in the cache,
    ///    and then returned.
    ///
    /// This mode is only available if the `caching` feature is enabled.
    #[cfg(feature = "caching")]
    ReadThroughCache(FirestoreSharedCacheBackend),
    /// Reads exclusively from the cache.
    ///
    /// When a read operation is performed:
    /// 1. The cache is checked.
    /// 2. If data is found, it's returned.
    /// 3. If data is not found, the operation will typically result in a "not found"
    ///    status without attempting to fetch from Firestore.
    ///
    /// This mode is only available if the `caching` feature is enabled.
    #[cfg(feature = "caching")]
    ReadCachedOnly(FirestoreSharedCacheBackend),
}

/// A type alias for a thread-safe, shareable Firestore cache backend.
///
/// This is an `Arc` (Atomically Reference Counted) pointer to a trait object
/// that implements [`FirestoreCacheBackend`](crate::FirestoreCacheBackend).
/// It allows multiple parts of the application or different `FirestoreDb` instances
/// to share the same underlying cache storage.
///
/// This type is only available if the `caching` feature is enabled.
#[cfg(feature = "caching")]
pub type FirestoreSharedCacheBackend =
    std::sync::Arc<dyn crate::FirestoreCacheBackend + Send + Sync + 'static>;
