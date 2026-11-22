use rsb_derive::Builder;
use rvstruct::ValueStruct;

/// A name identifying a cache instance.
///
/// It appears in this crate's log and tracing output, which is what makes it worth setting when
/// an application runs more than one cache.
#[derive(Clone, Debug, Eq, PartialEq, Hash, ValueStruct)]
pub struct FirestoreCacheName(String);

/// Options for a [`FirestoreCache`](crate::FirestoreCache).
///
/// Prefer configuring these through the builder - [`FirestoreCache::memory`](crate::FirestoreCache::memory)
/// or [`FirestoreCache::persistent`](crate::FirestoreCache::persistent) - rather than
/// constructing this type directly.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreCacheOptions {
    /// The name of this cache instance.
    pub name: FirestoreCacheName,
    /// Options for the Firestore listener that keeps the cache up to date, such as its retry
    /// delay. When unset, the listener defaults are used. Ignored when
    /// [`sync_mode`](Self::sync_mode) polls.
    pub listener_params: Option<crate::FirestoreListenerParams>,
    /// How the cache keeps its collections current. Listens by default.
    #[default = "FirestoreCacheSyncMode::Listen"]
    pub sync_mode: FirestoreCacheSyncMode,
}

/// How a [`FirestoreCache`](crate::FirestoreCache) keeps its collections current.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreCacheSyncMode {
    /// A Firestore listener pushes every change to the cache as it happens. The default.
    Listen,
    /// The cache reads its collections again on an interval, in one background task for the whole
    /// cache, and opens no listener.
    Poll(FirestoreCachePollParams),
}

/// How a [`FirestoreCache`](crate::FirestoreCache) in [`FirestoreCacheSyncMode::Poll`] reads its
/// collections.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreCachePollParams {
    /// The pause between the end of one round, which reads every cached collection, and the start
    /// of the next. The first round starts one interval after the cache loads. Must not be zero.
    pub interval: std::time::Duration,
}
