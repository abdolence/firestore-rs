use crate::errors::{
    FirestoreError, FirestoreInvalidParametersError, FirestoreInvalidParametersPublicDetails,
};
use crate::{FirestoreDb, FirestoreListenerTarget, FirestoreResult};
use rvstruct::ValueStruct;
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
#[derive(Debug, Clone)]
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

    /// Decides what a Firestore target change means for the collections of this cache.
    ///
    /// A backend's [`on_listen_event`](crate::FirestoreCacheBackend::on_listen_event) should act on
    /// this rather than interpreting the target change itself: it resolves Firestore's "empty
    /// target IDs means all targets" rule, and scopes the result to the documents a target actually
    /// covers.
    #[inline]
    pub fn target_change_action(
        &self,
        target_change: &gcloud_sdk::google::firestore::v1::TargetChange,
    ) -> crate::FirestoreCacheTargetChangeAction {
        crate::cache::cache_target_change_action(self, target_change)
    }

    /// Whether the cache holds this document: its collection is cached, and the collection is not
    /// restricted to a different set of document IDs.
    #[inline]
    pub fn is_document_cached(&self, collection_path: &str, document_id: &str) -> bool {
        match self.collections.get(collection_path) {
            Some(config) => match config.watched_document_ids() {
                Some(document_ids) => document_ids.iter().any(|id| id == document_id),
                None => true,
            },
            None => false,
        }
    }

    /// Returns the first listener target ID at or above `from` that no collection uses.
    ///
    /// Shared by the builder's up-front assignment and by adding a collection at runtime, so the
    /// two cannot drift apart.
    pub(crate) fn allocate_listener_target(
        &self,
        from: u32,
    ) -> FirestoreResult<FirestoreListenerTarget> {
        let used: std::collections::HashSet<u32> = self
            .collections
            .values()
            .map(|config| *config.listener_target.value())
            .collect();

        let mut candidate = from.max(1);
        while used.contains(&candidate) {
            candidate = candidate.checked_add(1).ok_or_else(|| {
                FirestoreError::InvalidParametersError(FirestoreInvalidParametersError::new(
                    FirestoreInvalidParametersPublicDetails::new(
                        "listener_target".into(),
                        "Ran out of listener target IDs while assigning them automatically.".into(),
                    ),
                ))
            })?;
        }

        let target = FirestoreListenerTarget::new(candidate);
        target.validate()?;
        Ok(target)
    }

    /// The highest listener target ID in use, if any.
    pub(crate) fn max_listener_target(&self) -> Option<u32> {
        self.collections
            .values()
            .map(|config| *config.listener_target.value())
            .max()
    }

    /// Returns what a listener target covers, or `None` if the target does not belong to this
    /// cache.
    ///
    /// Used to scope the invalidation when Firestore resets or removes a target. A linear scan is
    /// deliberate: the map is small, and this runs only on target changes, never per document.
    pub(crate) fn target_scope(
        &self,
        target: &FirestoreListenerTarget,
    ) -> Option<FirestoreCacheTargetScope<'_>> {
        self.collections
            .iter()
            .find(|(_, config)| config.listener_target == *target)
            .map(|(collection_path, config)| target_scope_of(collection_path, config))
    }

    /// Returns what every listener target of this cache covers.
    ///
    /// Firestore uses an empty set of target IDs to mean "all targets", which is what this is for.
    pub(crate) fn all_target_scopes(&self) -> Vec<FirestoreCacheTargetScope<'_>> {
        self.collections
            .iter()
            .map(|(collection_path, config)| target_scope_of(collection_path, config))
            .collect()
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
            // A collection watched by document ID holds only those documents, however it is
            // loaded, so it can never answer a listing of the collection completely.
            Some(config) if config.watched_document_ids().is_some() => false,
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

/// Which documents of a collection the cache holds and listens to.
///
/// This is independent of [`FirestoreCacheCollectionLoadMode`]: that decides *when* documents are
/// fetched, this decides *which* ones exist as far as the cache is concerned.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FirestoreCacheCollectionWatch {
    /// Listen to every document in the collection. The default.
    WholeCollection,
    /// Listen only to these document IDs.
    ///
    /// The listener watches exactly these documents rather than the whole collection, so unrelated
    /// changes are never streamed to your process or written into the cache. Such a collection is
    /// never listable: it holds a chosen subset, so answering `list` or `query` from it would
    /// return a partial result that looks complete.
    Documents(Vec<String>),
}

impl Default for FirestoreCacheCollectionWatch {
    #[inline]
    fn default() -> Self {
        Self::WholeCollection
    }
}

/// The scope of the listener target that keeps one cached collection up to date.
#[inline]
pub(crate) fn target_scope_of<'a>(
    collection_path: &'a str,
    config: &'a FirestoreCacheCollectionConfiguration,
) -> FirestoreCacheTargetScope<'a> {
    match config.watched_document_ids() {
        Some(document_ids) => FirestoreCacheTargetScope::Documents {
            collection_path,
            document_ids,
        },
        None => FirestoreCacheTargetScope::Collection(collection_path),
    }
}

/// What a Firestore listener target of this cache covers.
///
/// Every cached target is currently a whole-collection query, so only
/// [`Collection`](Self::Collection) is produced today. Invalidation is written against this rather
/// than against a bare collection path so that a target watching a handful of documents does not
/// end up wiping the entire collection.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum FirestoreCacheTargetScope<'a> {
    /// The target covers every document in the collection at this path.
    Collection(&'a str),
    /// The target covers only these document IDs within the collection at this path.
    Documents {
        collection_path: &'a str,
        document_ids: &'a [String],
    },
}

impl FirestoreCacheTargetScope<'_> {
    /// The collection this scope belongs to.
    #[inline]
    pub(crate) fn collection_path(&self) -> &str {
        match self {
            Self::Collection(collection_path) => collection_path,
            Self::Documents {
                collection_path, ..
            } => collection_path,
        }
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
    /// Which documents of the collection the cache holds and listens to.
    pub collection_watch: FirestoreCacheCollectionWatch,
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
            collection_watch: FirestoreCacheCollectionWatch::WholeCollection,
            indices: Vec::new(),
        }
    }

    /// The listener target that keeps this collection up to date.
    ///
    /// Backends use this in [`load`](crate::FirestoreCacheBackend::load) so that a collection
    /// limited to named documents is listened to as a documents target rather than as a query over
    /// the whole collection.
    #[inline]
    pub fn listener_target_params(
        &self,
        resume_type: Option<crate::FirestoreListenerTargetResumeType>,
    ) -> crate::FirestoreListenerTargetParams {
        crate::cache::target_params_for_collection(self, resume_type)
    }

    /// The point in time a newly attached listener target should be resumed from.
    ///
    /// Deliberately a little behind the local clock: a read time in the server's future is rejected
    /// as invalid, and a client clock can easily run slightly ahead. The cost is a few redundant
    /// document changes, which are idempotent.
    #[inline]
    pub fn listener_read_time() -> crate::FirestoreInstant {
        crate::cache::cache_target_read_time()
    }

    /// Caches and listens to only these documents of the collection, instead of all of them.
    #[inline]
    pub fn with_documents<I>(self, document_ids: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self {
            collection_watch: FirestoreCacheCollectionWatch::Documents(
                document_ids
                    .into_iter()
                    .map(|id| id.as_ref().to_string())
                    .collect(),
            ),
            ..self
        }
    }

    /// The document IDs this collection is limited to, if any.
    #[inline]
    pub fn watched_document_ids(&self) -> Option<&[String]> {
        match &self.collection_watch {
            FirestoreCacheCollectionWatch::WholeCollection => None,
            FirestoreCacheCollectionWatch::Documents(ids) => Some(ids),
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
