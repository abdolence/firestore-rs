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
    /// delay. When unset, the listener defaults are used.
    pub listener_params: Option<crate::FirestoreListenerParams>,
}
