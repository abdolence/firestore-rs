use crate::{FirestoreDb, FirestoreListenerTarget};
use std::collections::HashMap;

/// Describes which collections a [`FirestoreCache`](crate::FirestoreCache) holds and how each of
/// them is loaded.
///
/// Collections are keyed by their *absolute* document path (for example
/// `projects/my-project/databases/(default)/documents/users`), which is why building a
/// configuration needs a [`FirestoreDb`] to resolve the project and database.
///
/// You normally do not construct this directly — the cache builder
/// ([`FirestoreCache::memory`](crate::FirestoreCache::memory) /
/// [`FirestoreCache::persistent`](crate::FirestoreCache::persistent)) builds it for you.
#[derive(Clone)]
pub struct FirestoreCacheConfiguration {
    /// The configured collections, keyed by absolute collection path.
    pub collections: HashMap<String, FirestoreCacheCollectionConfiguration>,
    /// Controls whether collections that are not fully preloaded may serve `list`/`query`
    /// requests from the cache. See [`FirestoreCacheIncompleteCollectionPolicy`].
    pub incomplete_collection_policy: FirestoreCacheIncompleteCollectionPolicy,
}

impl FirestoreCacheConfiguration {
    /// Creates an empty configuration.
    #[inline]
    pub fn new() -> Self {
        Self {
            collections: HashMap::new(),
            incomplete_collection_policy: FirestoreCacheIncompleteCollectionPolicy::default(),
        }
    }

    /// Adds a collection to the configuration.
    ///
    /// The `db` reference is used to resolve the collection's absolute path.
    #[inline]
    pub fn add_collection_config(
        self,
        db: &FirestoreDb,
        config: FirestoreCacheCollectionConfiguration,
    ) -> Self {
        self.add_collection_config_at(db.get_documents_path(), config)
    }

    /// Sets the policy for collections that are not fully preloaded.
    #[inline]
    pub fn with_incomplete_collection_policy(
        self,
        incomplete_collection_policy: FirestoreCacheIncompleteCollectionPolicy,
    ) -> Self {
        Self {
            incomplete_collection_policy,
            ..self
        }
    }

    /// Same as [`add_collection_config`](Self::add_collection_config), but takes the resolved
    /// documents path directly so that configurations can be built without a live `FirestoreDb`.
    #[inline]
    pub(crate) fn add_collection_config_at(
        mut self,
        documents_path: &str,
        config: FirestoreCacheCollectionConfiguration,
    ) -> Self {
        let collection_path = config.resolve_collection_path(documents_path);
        self.collections.insert(collection_path, config);
        self
    }

    /// Returns `true` when the cache is configured to hold a *complete* copy of the collection at
    /// `collection_path`, meaning `list` and `query` requests may be served from the cache.
    ///
    /// Only preloaded collections ([`PreloadAllDocs`](FirestoreCacheCollectionLoadMode::PreloadAllDocs)
    /// / [`PreloadAllIfEmpty`](FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty)) qualify. A
    /// [`PreloadNone`](FirestoreCacheCollectionLoadMode::PreloadNone) collection holds an
    /// arbitrary subset of documents — whatever happens to have been read through it so far — so
    /// serving a listing from it would silently return partial results.
    ///
    /// This can be relaxed with
    /// [`FirestoreCacheIncompleteCollectionPolicy::PartialResults`].
    #[inline]
    pub fn is_collection_listable(&self, collection_path: &str) -> bool {
        match self.collections.get(collection_path) {
            Some(config) => match self.incomplete_collection_policy {
                FirestoreCacheIncompleteCollectionPolicy::SkipCache => {
                    config.collection_load_mode.is_preloading()
                }
                FirestoreCacheIncompleteCollectionPolicy::PartialResults => true,
            },
            None => false,
        }
    }
}

impl Default for FirestoreCacheConfiguration {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Controls whether collections that are not fully preloaded may serve `list`/`query` requests
/// from the cache.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum FirestoreCacheIncompleteCollectionPolicy {
    /// The default and recommended behaviour.
    ///
    /// Only preloaded collections may serve `list`/`query` from the cache. For other collections
    /// `read_through_cache` transparently falls back to Firestore, and `read_cached_only` returns
    /// an error rather than a partial result.
    #[default]
    SkipCache,
    /// The pre-0.52 behaviour: serve `list`/`query` from whatever the cache happens to hold.
    ///
    /// **Results may be silently partial.** Only use this if your application can tolerate an
    /// incomplete view of a collection.
    PartialResults,
}

/// Configuration for a single cached collection.
#[derive(Debug, Clone)]
pub struct FirestoreCacheCollectionConfiguration {
    /// The collection ID, for example `users`.
    pub collection_name: String,
    /// An optional parent document path, for caching a sub-collection.
    pub parent: Option<String>,
    /// The Firestore listener target ID used to keep this collection up to date. It must be
    /// unique across every listener in your application, not just within the cache.
    pub listener_target: FirestoreListenerTarget,
    /// How this collection is populated at startup, which also determines whether `list`/`query`
    /// may be served from the cache.
    pub collection_load_mode: FirestoreCacheCollectionLoadMode,
    #[doc(hidden)]
    /// Not implemented and has no effect. Scheduled for removal.
    pub indices: Vec<FirestoreCacheIndexConfiguration>,
}

impl FirestoreCacheCollectionConfiguration {
    /// Creates a configuration for a collection.
    #[inline]
    pub fn new<S>(
        collection_name: S,
        listener_target: FirestoreListenerTarget,
        collection_load_mode: FirestoreCacheCollectionLoadMode,
    ) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            collection_name: collection_name.as_ref().to_string(),
            parent: None,
            listener_target,
            collection_load_mode,
            indices: Vec::new(),
        }
    }

    /// Caches a sub-collection under the given parent document path.
    ///
    /// Use [`FirestoreDb::parent_path`](crate::FirestoreDb::parent_path) to build the parent.
    #[inline]
    pub fn with_parent<S>(self, parent: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            parent: Some(parent.as_ref().to_string()),
            ..self
        }
    }

    #[doc(hidden)]
    #[deprecated(
        since = "0.52.0",
        note = "Cache indices are not implemented and have no effect. This method is scheduled for removal."
    )]
    #[inline]
    pub fn with_index(self, index: FirestoreCacheIndexConfiguration) -> Self {
        let mut indices = self.indices;
        indices.push(index);
        Self { indices, ..self }
    }

    /// Resolves the absolute path of this collection against the given documents path.
    #[inline]
    pub(crate) fn resolve_collection_path(&self, documents_path: &str) -> String {
        match self.parent {
            Some(ref parent) => format!("{}/{}", parent, self.collection_name),
            None => format!("{}/{}", documents_path, self.collection_name),
        }
    }
}

/// Determines how a cached collection is populated when the cache starts.
///
/// This also decides whether `list` and `query` operations may be served from the cache: only
/// preloaded collections hold a complete copy, so only they can answer a listing correctly.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FirestoreCacheCollectionLoadMode {
    /// Download every document in the collection at startup.
    ///
    /// Enables cached `list` and `query`.
    PreloadAllDocs,
    /// Download every document only if the cache is currently empty.
    ///
    /// Enables cached `list` and `query`. Mainly useful for the persistent backend, where it
    /// avoids re-downloading a collection that a previous run already stored. For the in-memory
    /// backend this is equivalent to [`PreloadAllDocs`](Self::PreloadAllDocs), since an in-memory
    /// cache always starts empty.
    PreloadAllIfEmpty,
    /// Do not preload anything; fill the cache lazily as documents are read through it.
    ///
    /// Reads by ID are cached, but `list` and `query` are **not** served from the cache, because
    /// the collection is only ever partially present.
    PreloadNone,
}

impl FirestoreCacheCollectionLoadMode {
    /// Returns `true` if this mode populates the collection completely at startup.
    #[inline]
    pub fn is_preloading(&self) -> bool {
        matches!(self, Self::PreloadAllDocs | Self::PreloadAllIfEmpty)
    }
}

#[doc(hidden)]
/// Not implemented and has no effect. Scheduled for removal.
#[derive(Debug, Clone)]
pub struct FirestoreCacheIndexConfiguration {
    pub fields: Vec<String>,
    pub unique: bool,
}

impl FirestoreCacheIndexConfiguration {
    #[inline]
    pub fn new<I>(fields: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self {
            fields: fields.into_iter().map(|s| s.as_ref().to_string()).collect(),
            unique: false,
        }
    }

    #[inline]
    pub fn unique(self, value: bool) -> Self {
        Self {
            unique: value,
            ..self
        }
    }
}
