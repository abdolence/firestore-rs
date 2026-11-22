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
#[non_exhaustive]
pub struct FirestoreCacheOptions {
    /// The name of this cache instance.
    pub name: FirestoreCacheName,
    /// How the cache keeps its collections current. Listens, with the listener defaults, unless
    /// set.
    #[default = "FirestoreCacheSyncMode::Listen(crate::FirestoreListenerParams::new())"]
    pub sync_mode: FirestoreCacheSyncMode,
}

/// How a [`FirestoreCache`](crate::FirestoreCache) keeps its collections current.
#[derive(Debug, Eq, PartialEq, Clone)]
#[non_exhaustive]
pub enum FirestoreCacheSyncMode {
    /// A Firestore listener with these parameters pushes every change to the cache as it happens.
    /// The default.
    Listen(crate::FirestoreListenerParams),
    /// The cache reads its collections again on an interval, in one background task for the whole
    /// cache, and opens no listener. Only for a cache built with
    /// [`FirestoreCache::memory`](crate::FirestoreCache::memory) or
    /// [`FirestoreCache::persistent`](crate::FirestoreCache::persistent).
    Poll(FirestoreCachePollParams),
}

/// How a [`FirestoreCache`](crate::FirestoreCache) in [`FirestoreCacheSyncMode::Poll`] reads its
/// collections.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
#[non_exhaustive]
pub struct FirestoreCachePollParams {
    /// The pause between the end of one round, which reads every cached collection, and the start
    /// of the next. Must not be zero.
    ///
    /// The first round starts one interval after the cache loads. A collection whose documents the
    /// load kept from an earlier run rather than downloading them is read at once instead.
    pub interval: std::time::Duration,
}
