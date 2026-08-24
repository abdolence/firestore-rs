use crate::errors::*;
use crate::FirestoreInstant;
use crate::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use moka::future::{Cache, CacheBuilder};

use crate::cache::cache_query_engine::FirestoreCacheQueryEngine;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::*;

#[doc(hidden)]
/// Exposes the underlying `moka` cache type. Not part of the supported API surface: the `moka`
/// version is not covered by this crate's semver contract.
pub type FirestoreMemCache = Cache<String, FirestoreDocument>;

#[doc(hidden)]
/// Exposes the underlying `moka` cache builder. Not part of the supported API surface: the `moka`
/// version is not covered by this crate's semver contract.
pub type FirestoreMemCacheOptions = CacheBuilder<String, FirestoreDocument, FirestoreMemCache>;

/// An in-memory cache backend, backed by the [moka](https://github.com/moka-rs/moka) cache.
///
/// Documents live only for the lifetime of the process; an in-memory cache always starts empty,
/// so preloaded collections are downloaded again on every start. Use
/// [`FirestorePersistentCacheBackend`](crate::FirestorePersistentCacheBackend) if you need the
/// cache to survive restarts.
///
/// Create one through the builder rather than directly:
///
/// ```rust,no_run
/// # use firestore::*;
/// # async fn example(db: &FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let cache = FirestoreCache::memory(db)
///     .preloaded_collection("countries")
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct FirestoreMemoryCacheBackend {
    /// Whether an entry count can be compared against Firestore's.
    ///
    /// False when the cache expires entries on its own, because then a shortfall is this cache
    /// doing its job rather than a divergence from the server.
    count_authoritative: bool,
    /// The per-collection capacity, above which eviction makes the count meaningless.
    max_capacity: u64,
    /// Behind a lock so that collections can be added and removed while the cache runs. Readers
    /// clone the `Arc` out and drop the guard, so no guard is ever held across an await.
    config: std::sync::RwLock<Arc<FirestoreCacheConfiguration>>,
    collection_caches: std::sync::RwLock<Arc<HashMap<String, FirestoreMemCache>>>,
    /// Retained so that a collection added at runtime gets a cache configured the same way as the
    /// ones the backend was built with.
    collection_mem_options: Box<dyn Fn(&str) -> FirestoreMemCacheOptions + Send + Sync>,
    /// Collections whose listener target Firestore has reset or removed, and which are therefore
    /// mid-replay. They must not answer `list`/`query` until the replay completes, because what
    /// they hold in the meantime is a partial view that would look like a complete one.
    suspended_collections: std::sync::RwLock<HashSet<String>>,
}

/// The maximum number of documents kept per collection unless configured otherwise.
const FIRESTORE_MEMORY_CACHE_DEFAULT_MAX_CAPACITY: u64 = 50000;

/// Tuning options for [`FirestoreMemoryCacheBackend`].
///
/// These map onto the underlying cache without exposing its types, so that the backing
/// implementation can change without breaking your code.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FirestoreMemoryCacheOptions {
    /// The maximum number of documents to keep per collection. Defaults to 50 000.
    pub max_capacity: u64,
    /// Evict a document this long after it was written, if set.
    pub time_to_live: Option<std::time::Duration>,
    /// Evict a document this long after it was last read, if set.
    pub time_to_idle: Option<std::time::Duration>,
}

impl FirestoreMemoryCacheOptions {
    /// Creates options with the default capacity and no expiry.
    #[inline]
    pub fn new() -> Self {
        Self {
            max_capacity: FIRESTORE_MEMORY_CACHE_DEFAULT_MAX_CAPACITY,
            time_to_live: None,
            time_to_idle: None,
        }
    }

    /// Sets the maximum number of documents kept per collection.
    #[inline]
    pub fn with_max_capacity(self, max_capacity: u64) -> Self {
        Self {
            max_capacity,
            ..self
        }
    }

    /// Evicts a document this long after it was written.
    #[inline]
    pub fn with_time_to_live(self, time_to_live: std::time::Duration) -> Self {
        Self {
            time_to_live: Some(time_to_live),
            ..self
        }
    }

    /// Evicts a document this long after it was last read.
    #[inline]
    pub fn with_time_to_idle(self, time_to_idle: std::time::Duration) -> Self {
        Self {
            time_to_idle: Some(time_to_idle),
            ..self
        }
    }
}

impl Default for FirestoreMemoryCacheOptions {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl FirestoreMemoryCacheBackend {
    /// Creates a backend with the default maximum capacity of 50 000 documents per collection.
    pub fn new(config: FirestoreCacheConfiguration) -> FirestoreResult<Self> {
        Self::with_max_capacity(config, FIRESTORE_MEMORY_CACHE_DEFAULT_MAX_CAPACITY)
    }

    /// Creates a backend keeping at most `max_capacity` documents per collection.
    pub fn with_max_capacity(
        config: FirestoreCacheConfiguration,
        max_capacity: u64,
    ) -> FirestoreResult<Self> {
        Self::with_options(
            config,
            FirestoreMemoryCacheOptions::new().with_max_capacity(max_capacity),
        )
    }

    /// Creates a backend with the given tuning options.
    pub fn with_options(
        config: FirestoreCacheConfiguration,
        options: FirestoreMemoryCacheOptions,
    ) -> FirestoreResult<Self> {
        let count_authoritative = options.time_to_live.is_none() && options.time_to_idle.is_none();
        let max_capacity = options.max_capacity;

        let backend = Self::with_collection_options(config, move |_| {
            let mut builder = FirestoreMemCache::builder().max_capacity(options.max_capacity);
            if let Some(time_to_live) = options.time_to_live {
                builder = builder.time_to_live(time_to_live);
            }
            if let Some(time_to_idle) = options.time_to_idle {
                builder = builder.time_to_idle(time_to_idle);
            }
            builder
        })?;

        Ok(Self {
            count_authoritative,
            max_capacity,
            ..backend
        })
    }

    #[doc(hidden)]
    /// Configures each collection's cache through the underlying `moka` builder. Not part of the
    /// supported API surface - prefer [`with_options`](Self::with_options).
    pub fn with_collection_options<FN>(
        config: FirestoreCacheConfiguration,
        collection_mem_options: FN,
    ) -> FirestoreResult<Self>
    where
        FN: Fn(&str) -> FirestoreMemCacheOptions + Send + Sync + 'static,
    {
        let collection_caches: HashMap<String, FirestoreMemCache> = config
            .collections
            .keys()
            .map(|collection_path| {
                (
                    collection_path.clone(),
                    collection_mem_options(collection_path.as_str()).build(),
                )
            })
            .collect();

        Ok(Self {
            // An arbitrary moka builder can expire entries however it likes, so counts from a
            // backend built this way are never comparable with Firestore's.
            count_authoritative: false,
            max_capacity: u64::MAX,
            config: std::sync::RwLock::new(Arc::new(config)),
            collection_caches: std::sync::RwLock::new(Arc::new(collection_caches)),
            collection_mem_options: Box::new(collection_mem_options),
            suspended_collections: std::sync::RwLock::new(HashSet::new()),
        })
    }

    /// A snapshot of the collections this backend currently caches.
    pub fn config(&self) -> Arc<FirestoreCacheConfiguration> {
        self.config
            .read()
            .expect("cache configuration lock poisoned")
            .clone()
    }

    /// The cache of one collection. `moka` caches are cheap to clone - they are reference counted
    /// internally - so this hands out a handle rather than borrowing the map.
    fn collection_cache(&self, collection_path: &str) -> Option<FirestoreMemCache> {
        self.collection_caches
            .read()
            .expect("cache collections lock poisoned")
            .get(collection_path)
            .cloned()
    }

    fn all_collection_caches(&self) -> Arc<HashMap<String, FirestoreMemCache>> {
        self.collection_caches
            .read()
            .expect("cache collections lock poisoned")
            .clone()
    }

    /// Stops a collection answering `list`/`query` until Firestore reports it consistent again.
    fn suspend_collection(&self, collection_path: &str) {
        self.suspended_collections
            .write()
            .expect("cache suspended collections lock poisoned")
            .insert(collection_path.to_string());
    }

    /// Whether a collection is mid-replay after Firestore reset or removed its listener target.
    fn is_suspended(&self, collection_path: &str) -> bool {
        self.suspended_collections
            .read()
            .expect("cache suspended collections lock poisoned")
            .contains(collection_path)
    }

    /// Drops everything cached for one collection.
    async fn invalidate_collection(&self, collection_path: &str) {
        if let Some(mem_cache) = self.collection_cache(collection_path) {
            debug!(collection_path, "Invalidating cache for collection.");
            mem_cache.invalidate_all();
            mem_cache.run_pending_tasks().await;
        }
    }

    /// Removes a document from the cache, wherever the listener said it went.
    async fn evict_doc_by_path(&self, document_path: &str) {
        let (collection_path, document_id) = split_document_path(document_path);
        if let Some(mem_cache) = self.collection_cache(collection_path) {
            trace!(
                document_path,
                "Removing document from cache due to listener event.",
            );
            mem_cache.remove(document_id).await;
        }
    }

    /// Preloads one collection into a cache that is not published in the configuration yet.
    ///
    /// Keeping the publish until after the preload is what stops a half-filled collection from
    /// answering a `list` as though it were complete.
    async fn preload_one_collection(
        &self,
        db: &FirestoreDb,
        collection_path: &str,
        config: &FirestoreCacheCollectionConfiguration,
        mem_cache: &FirestoreMemCache,
    ) -> Result<(), FirestoreError> {
        debug!(collection_path, "Preloading collection.");

        // A collection watched by document ID is preloaded by reading exactly those documents,
        // rather than downloading the collection and throwing most of it away.
        if let Some(document_ids) = config.watched_document_ids() {
            let selector = db
                .fluent()
                .select()
                .by_id_in(config.collection_name.as_str());
            let selector = match &config.parent {
                Some(parent) => selector.parent(parent),
                None => selector,
            };

            let mut stream = selector.batch(document_ids.to_vec()).await?;
            while let Some((_, doc)) = stream.next().await {
                if let Some(doc) = doc {
                    let (_, document_id) = split_document_path(&doc.name);
                    mem_cache.insert(document_id.to_string(), doc).await;
                }
            }

            mem_cache.run_pending_tasks().await;
            info!(
                collection_path,
                entry_count = mem_cache.entry_count(),
                "Preloading watched documents has been finished.",
            );
            return Ok(());
        }

        let params = if let Some(parent) = &config.parent {
            db.fluent()
                .select()
                .from(config.collection_name.as_str())
                .parent(parent)
        } else {
            db.fluent().select().from(config.collection_name.as_str())
        };

        let stream = params.stream_query().await?;

        stream
            .for_each_concurrent(1, |doc| {
                let mem_cache = mem_cache.clone();
                async move {
                    let (_, document_id) = split_document_path(&doc.name);
                    mem_cache.insert(document_id.to_string(), doc).await;
                }
            })
            .await;

        mem_cache.run_pending_tasks().await;

        info!(
            collection_path,
            entry_count = mem_cache.entry_count(),
            "Preloading collection has been finished.",
        );

        Ok(())
    }

    async fn preload_collections(&self, db: &FirestoreDb) -> Result<(), FirestoreError> {
        let config_snapshot = self.config();
        for (collection_path, config) in &config_snapshot.collections {
            if !config.collection_load_mode.is_preloading() {
                continue;
            }
            if let Some(mem_cache) = self.collection_cache(collection_path.as_str()) {
                self.preload_one_collection(db, collection_path, config, &mem_cache)
                    .await?;
            }
        }
        Ok(())
    }

    async fn query_cached_docs<'b>(
        &self,
        collection_path: &str,
        query_engine: FirestoreCacheQueryEngine,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        match self.collection_cache(collection_path) {
            Some(mem_cache) => {
                let filtered_results: Vec<FirestoreResult<FirestoreDocument>> = mem_cache
                    .iter()
                    .filter(|(_, doc)| query_engine.matches_doc(doc))
                    .map(|(_, doc)| Ok(doc))
                    .collect();

                let filtered_stream = futures::stream::iter(filtered_results);
                let output_stream = query_engine
                    .process_query_stream(Box::pin(filtered_stream))
                    .await?;

                Ok(output_stream)
            }
            None => Ok(Box::pin(futures::stream::empty())),
        }
    }
}

#[async_trait]
impl FirestoreCacheBackend for FirestoreMemoryCacheBackend {
    async fn load(
        &self,
        _options: &FirestoreCacheOptions,
        db: &FirestoreDb,
    ) -> Result<Vec<FirestoreListenerTargetParams>, FirestoreError> {
        let read_from_time = FirestoreInstant::now();

        self.preload_collections(db).await?;

        Ok(self
            .config()
            .collections
            .values()
            .map(|collection_config| {
                crate::cache::target_params_for_collection(
                    collection_config,
                    Some(FirestoreListenerTargetResumeType::ReadTime(read_from_time)),
                )
            })
            .collect())
    }

    async fn invalidate_all(&self) -> FirestoreResult<()> {
        for (collection_path, mem_cache) in self.all_collection_caches().iter() {
            debug!(collection_path, "Invalidating cache for collection.");
            mem_cache.invalidate_all();
            mem_cache.run_pending_tasks().await;
        }
        Ok(())
    }

    fn cache_configuration(&self) -> Arc<FirestoreCacheConfiguration> {
        self.config()
    }

    async fn add_collection(
        &self,
        _options: &FirestoreCacheOptions,
        db: &FirestoreDb,
        collection_config: FirestoreCacheCollectionConfiguration,
    ) -> FirestoreResult<FirestoreListenerTargetParams> {
        // Captured before reading anything, so that writes landing during the preload are still
        // delivered once the listener target attaches from this point in time.
        let read_from_time = FirestoreInstant::now();
        let collection_path = collection_config.resolve_collection_path(db.get_documents_path());

        let mem_cache = (self.collection_mem_options)(collection_path.as_str()).build();

        if collection_config.collection_load_mode.is_preloading() {
            self.preload_one_collection(db, &collection_path, &collection_config, &mem_cache)
                .await?;
        }

        // Published only now: until this point the collection does not exist as far as reads are
        // concerned, so no listing can see it half filled.
        {
            let mut caches = self
                .collection_caches
                .write()
                .expect("cache collections lock poisoned");
            let mut updated = (**caches).clone();
            updated.insert(collection_path.clone(), mem_cache);
            *caches = Arc::new(updated);
        }

        let target_params = crate::cache::target_params_for_collection(
            &collection_config,
            Some(FirestoreListenerTargetResumeType::ReadTime(read_from_time)),
        );

        {
            let mut config = self
                .config
                .write()
                .expect("cache configuration lock poisoned");
            *config = Arc::new(
                (**config)
                    .clone()
                    .add_collection_config_at(db.get_documents_path(), collection_config),
            );
        }

        Ok(target_params)
    }

    async fn remove_collection(
        &self,
        collection_path: &str,
    ) -> FirestoreResult<Option<FirestoreListenerTarget>> {
        // The configuration goes first, so reads stop using the collection immediately and any
        // listener event arriving before the target is dropped is ignored.
        let removed = {
            let mut config = self
                .config
                .write()
                .expect("cache configuration lock poisoned");
            let mut updated = (**config).clone();
            let removed = updated.collections.remove(collection_path);
            *config = Arc::new(updated);
            removed
        };

        let Some(removed) = removed else {
            return Ok(None);
        };

        let mem_cache = {
            let mut caches = self
                .collection_caches
                .write()
                .expect("cache collections lock poisoned");
            let mut updated = (**caches).clone();
            let mem_cache = updated.remove(collection_path);
            *caches = Arc::new(updated);
            mem_cache
        };

        if let Some(mem_cache) = mem_cache {
            debug!(
                collection_path,
                "Dropping the cache of a removed collection."
            );
            mem_cache.invalidate_all();
            mem_cache.run_pending_tasks().await;
        }

        self.suspended_collections
            .write()
            .expect("cache suspended collections lock poisoned")
            .remove(collection_path);

        Ok(Some(removed.listener_target))
    }

    async fn begin_collection_resync(&self, collection_path: &str) -> FirestoreResult<()> {
        if !self.config().collections.contains_key(collection_path) {
            return Ok(());
        }
        self.suspend_collection(collection_path);
        self.invalidate_collection(collection_path).await;
        Ok(())
    }

    async fn authoritative_doc_count(&self, collection_path: &str) -> FirestoreResult<Option<u64>> {
        let config = self.config();
        let Some(collection_config) = config.collections.get(collection_path) else {
            return Ok(None);
        };
        if !self.count_authoritative || !collection_config.collection_load_mode.is_preloading() {
            return Ok(None);
        }

        let Some(mem_cache) = self.collection_cache(collection_path) else {
            return Ok(None);
        };

        // `entry_count` is otherwise an estimate.
        mem_cache.run_pending_tasks().await;
        let count = mem_cache.entry_count();

        // At capacity a shortfall is eviction, not divergence.
        Ok((count < self.max_capacity).then_some(count))
    }

    async fn shutdown(&self) -> Result<(), FirestoreError> {
        // Handles to the backend outlive the cache - `read_through_cache` clones one into every
        // `FirestoreDb` it is attached to - so releasing the documents here rather than waiting to
        // be dropped is what actually frees the memory.
        debug!("Releasing the cached documents of the in-memory cache.");
        self.invalidate_all().await
    }

    async fn on_listen_event(&self, event: FirestoreListenEvent) -> FirestoreResult<()> {
        match event {
            FirestoreListenEvent::DocumentChange(doc_change) => {
                if let Some(doc) = doc_change.document {
                    let (collection_path, document_id) = split_document_path(&doc.name);
                    if !self
                        .config()
                        .is_document_cached(collection_path, document_id)
                    {
                        return Ok(());
                    }
                    if let Some(mem_cache) = self.collection_cache(collection_path) {
                        trace!(
                            doc_name = ?doc.name,
                            "Writing document to cache due to listener event.",
                        );
                        mem_cache.insert(document_id.to_string(), doc).await;
                    }
                }
                Ok(())
            }
            FirestoreListenEvent::DocumentDelete(doc_deleted) => {
                self.evict_doc_by_path(&doc_deleted.document).await;
                Ok(())
            }
            // The document went out of view of the target. Firestore sends this instead of a
            // delete when it cannot send the new value, so keeping the document would leave the
            // cache serving something the caller can no longer read.
            FirestoreListenEvent::DocumentRemove(doc_removed) => {
                self.evict_doc_by_path(&doc_removed.document).await;
                Ok(())
            }
            FirestoreListenEvent::TargetChange(ref target_change) => {
                match crate::cache::cache_target_change_action(&self.config(), target_change) {
                    crate::cache::FirestoreCacheTargetChangeAction::SuspendAndInvalidate(
                        invalidations,
                    ) => {
                        {
                            let mut suspended = self
                                .suspended_collections
                                .write()
                                .expect("cache suspended collections lock poisoned");
                            suspended.extend(
                                invalidations
                                    .iter()
                                    .map(|invalidation| invalidation.collection_path().to_string()),
                            );
                        }
                        for invalidation in &invalidations {
                            match invalidation {
                                crate::cache::FirestoreCacheInvalidation::Collection(
                                    collection_path,
                                ) => self.invalidate_collection(collection_path).await,
                                crate::cache::FirestoreCacheInvalidation::Documents {
                                    collection_path,
                                    document_ids,
                                } => {
                                    if let Some(mem_cache) = self.collection_cache(collection_path)
                                    {
                                        for document_id in document_ids {
                                            mem_cache.remove(document_id).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    crate::cache::FirestoreCacheTargetChangeAction::Resume(paths) => {
                        let mut suspended = self
                            .suspended_collections
                            .write()
                            .expect("cache suspended collections lock poisoned");
                        for collection_path in &paths {
                            suspended.remove(collection_path);
                        }
                    }
                    crate::cache::FirestoreCacheTargetChangeAction::Ignore => {}
                }
                Ok(())
            }
            _ => {
                trace!(?event, "Ignoring a listen event the cache does not act on.");
                Ok(())
            }
        }
    }
}

#[async_trait]
impl FirestoreCacheDocsByPathSupport for FirestoreMemoryCacheBackend {
    async fn get_doc_by_path(
        &self,
        document_path: &str,
    ) -> FirestoreResult<Option<FirestoreDocument>> {
        let (collection_path, document_id) = split_document_path(document_path);

        match self.collection_cache(collection_path) {
            Some(mem_cache) => Ok(mem_cache.get(document_id).await),
            None => Ok(None),
        }
    }

    async fn update_doc_by_path(&self, document: &FirestoreDocument) -> FirestoreResult<()> {
        let (collection_path, document_id) = split_document_path(&document.name);

        if !self
            .config()
            .is_document_cached(collection_path, document_id)
        {
            return Ok(());
        }

        match self.collection_cache(collection_path) {
            Some(mem_cache) => {
                mem_cache
                    .insert(document_id.to_string(), document.clone())
                    .await;
                Ok(())
            }
            None => Ok(()),
        }
    }

    async fn list_all_docs<'b>(
        &self,
        collection_path: &str,
    ) -> FirestoreResult<FirestoreCachedValue<BoxStream<'b, FirestoreResult<FirestoreDocument>>>>
    {
        if !self.config().is_collection_listable(collection_path)
            || self.is_suspended(collection_path)
        {
            return Ok(FirestoreCachedValue::SkipCache);
        }

        match self.collection_cache(collection_path) {
            Some(mem_cache) => {
                let all_docs: Vec<FirestoreResult<FirestoreDocument>> =
                    mem_cache.iter().map(|(_, doc)| Ok(doc)).collect();
                Ok(FirestoreCachedValue::UseCached(Box::pin(
                    futures::stream::iter(all_docs),
                )))
            }
            None => Ok(FirestoreCachedValue::SkipCache),
        }
    }

    async fn query_docs<'b>(
        &self,
        collection_path: &str,
        query: &FirestoreQueryParams,
    ) -> FirestoreResult<FirestoreCachedValue<BoxStream<'b, FirestoreResult<FirestoreDocument>>>>
    {
        if !self.config().is_collection_listable(collection_path)
            || self.is_suspended(collection_path)
        {
            return Ok(FirestoreCachedValue::SkipCache);
        }

        let simple_query_engine = FirestoreCacheQueryEngine::new(query);
        if simple_query_engine.params_supported() {
            Ok(FirestoreCachedValue::UseCached(
                self.query_cached_docs(collection_path, simple_query_engine)
                    .await?,
            ))
        } else {
            Ok(FirestoreCachedValue::SkipCache)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcloud_sdk::google::firestore::v1::{
        DocumentChange, DocumentDelete, DocumentRemove, TargetChange,
    };
    use std::time::Duration;

    const DOCS: &str = "projects/test/databases/(default)/documents";

    fn backend(
        collections: &[(&str, u32, FirestoreCacheCollectionLoadMode)],
    ) -> FirestoreMemoryCacheBackend {
        let config = collections.iter().fold(
            FirestoreCacheConfiguration::new(),
            |config, (name, target, load_mode)| {
                config.add_collection_config_at(
                    DOCS,
                    FirestoreCacheCollectionConfiguration::new(
                        name,
                        FirestoreListenerTarget::new(*target),
                        *load_mode,
                    ),
                )
            },
        );

        FirestoreMemoryCacheBackend::new(config).expect("backend")
    }

    fn preloaded(collections: &[(&str, u32)]) -> FirestoreMemoryCacheBackend {
        let collections: Vec<_> = collections
            .iter()
            .map(|(name, target)| {
                (
                    *name,
                    *target,
                    FirestoreCacheCollectionLoadMode::PreloadAllDocs,
                )
            })
            .collect();
        backend(&collections)
    }

    fn doc_path(collection: &str, id: &str) -> String {
        format!("{DOCS}/{collection}/{id}")
    }

    /// Writes straight into the collection's cache, bypassing the watch filter, so that tests can
    /// set up documents a target does not cover.
    async fn seed(backend: &FirestoreMemoryCacheBackend, collection: &str, id: &str) {
        let collection_path = format!("{DOCS}/{collection}");
        let mem_cache = backend
            .collection_cache(&collection_path)
            .expect("configured collection");
        mem_cache
            .insert(
                id.to_string(),
                FirestoreDocument {
                    name: doc_path(collection, id),
                    ..Default::default()
                },
            )
            .await;
    }

    async fn cached(backend: &FirestoreMemoryCacheBackend, collection: &str, id: &str) -> bool {
        backend
            .get_doc_by_path(&doc_path(collection, id))
            .await
            .expect("cache lookup")
            .is_some()
    }

    fn target_change(
        change_type: FirestoreListenerTargetChangeType,
        target_ids: Vec<i32>,
    ) -> FirestoreListenEvent {
        FirestoreListenEvent::TargetChange(TargetChange {
            target_change_type: change_type as i32,
            target_ids,
            ..Default::default()
        })
    }

    async fn is_listable(backend: &FirestoreMemoryCacheBackend, collection: &str) -> bool {
        matches!(
            backend
                .list_all_docs(&format!("{DOCS}/{collection}"))
                .await
                .expect("listing"),
            FirestoreCachedValue::UseCached(_)
        )
    }

    #[tokio::test]
    async fn document_remove_evicts_the_document() {
        let backend = preloaded(&[("a", 1000)]);
        seed(&backend, "a", "one").await;

        backend
            .on_listen_event(FirestoreListenEvent::DocumentRemove(DocumentRemove {
                document: doc_path("a", "one"),
                ..Default::default()
            }))
            .await
            .expect("event handled");

        assert!(!cached(&backend, "a", "one").await);
    }

    #[tokio::test]
    async fn document_delete_still_evicts_the_document() {
        let backend = preloaded(&[("a", 1000)]);
        seed(&backend, "a", "one").await;

        backend
            .on_listen_event(FirestoreListenEvent::DocumentDelete(DocumentDelete {
                document: doc_path("a", "one"),
                ..Default::default()
            }))
            .await
            .expect("event handled");

        assert!(!cached(&backend, "a", "one").await);
    }

    #[tokio::test]
    async fn a_reset_empties_only_the_collection_of_that_target() {
        let backend = preloaded(&[("a", 1000), ("b", 1001)]);
        seed(&backend, "a", "one").await;
        seed(&backend, "b", "one").await;

        backend
            .on_listen_event(target_change(
                FirestoreListenerTargetChangeType::Reset,
                vec![1000],
            ))
            .await
            .expect("event handled");

        assert!(!cached(&backend, "a", "one").await);
        assert!(cached(&backend, "b", "one").await);
    }

    #[tokio::test]
    async fn a_reset_stops_the_collection_answering_listings_until_it_is_current() {
        let backend = preloaded(&[("a", 1000), ("b", 1001)]);

        assert!(is_listable(&backend, "a").await);

        backend
            .on_listen_event(target_change(
                FirestoreListenerTargetChangeType::Reset,
                vec![1000],
            ))
            .await
            .expect("event handled");

        // Serving a listing here would look complete while holding a half-replayed collection.
        assert!(!is_listable(&backend, "a").await);
        assert!(is_listable(&backend, "b").await);

        backend
            .on_listen_event(target_change(
                FirestoreListenerTargetChangeType::Current,
                vec![1000],
            ))
            .await
            .expect("event handled");

        assert!(is_listable(&backend, "a").await);
    }

    #[tokio::test]
    async fn a_reset_without_target_ids_affects_every_collection() {
        let backend = preloaded(&[("a", 1000), ("b", 1001)]);
        seed(&backend, "a", "one").await;
        seed(&backend, "b", "one").await;

        backend
            .on_listen_event(target_change(
                FirestoreListenerTargetChangeType::Reset,
                vec![],
            ))
            .await
            .expect("event handled");

        assert!(!cached(&backend, "a", "one").await);
        assert!(!cached(&backend, "b", "one").await);
        assert!(!is_listable(&backend, "a").await);
        assert!(!is_listable(&backend, "b").await);
    }

    fn watched(collections: &[(&str, u32, &[&str])]) -> FirestoreMemoryCacheBackend {
        let config = collections.iter().fold(
            FirestoreCacheConfiguration::new(),
            |config, (name, target, documents)| {
                config.add_collection_config_at(
                    DOCS,
                    FirestoreCacheCollectionConfiguration::new(
                        name,
                        FirestoreListenerTarget::new(*target),
                        FirestoreCacheCollectionLoadMode::PreloadAllDocs,
                    )
                    .with_documents(documents.iter()),
                )
            },
        );

        FirestoreMemoryCacheBackend::new(config).expect("backend")
    }

    #[tokio::test]
    async fn a_documents_watch_is_never_listable_even_when_preloaded() {
        let backend = watched(&[("a", 1000, &["one", "two"])]);

        // It holds a chosen subset, so answering a listing would look complete but not be.
        assert!(!is_listable(&backend, "a").await);
    }

    #[tokio::test]
    async fn a_documents_watch_ignores_documents_outside_its_set() {
        let backend = watched(&[("a", 1000, &["one"])]);

        for document_id in ["one", "three"] {
            backend
                .on_listen_event(FirestoreListenEvent::DocumentChange(DocumentChange {
                    document: Some(FirestoreDocument {
                        name: doc_path("a", document_id),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
                .await
                .expect("event handled");
        }

        assert!(cached(&backend, "a", "one").await);
        assert!(!cached(&backend, "a", "three").await);
    }

    #[tokio::test]
    async fn resetting_a_documents_target_drops_only_the_watched_documents() {
        let backend = watched(&[("a", 1000, &["one"])]);
        seed(&backend, "a", "one").await;
        // Present despite not being watched, as it would be if another target also covered this
        // collection: the reset must not take it with it.
        seed(&backend, "a", "other").await;

        backend
            .on_listen_event(target_change(
                FirestoreListenerTargetChangeType::Reset,
                vec![1000],
            ))
            .await
            .expect("event handled");

        assert!(!cached(&backend, "a", "one").await);
        assert!(cached(&backend, "a", "other").await);
    }

    #[tokio::test]
    async fn a_preloaded_collection_reports_a_comparable_document_count() {
        let backend = preloaded(&[("a", 1000)]);
        seed(&backend, "a", "one").await;
        seed(&backend, "a", "two").await;

        assert_eq!(
            backend
                .authoritative_doc_count(&format!("{DOCS}/a"))
                .await
                .unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn a_lazily_filled_collection_reports_no_comparable_count() {
        let backend = backend(&[("a", 1000, FirestoreCacheCollectionLoadMode::PreloadNone)]);
        seed(&backend, "a", "one").await;

        // It holds whatever happened to be read, so its count says nothing about Firestore's.
        assert_eq!(
            backend
                .authoritative_doc_count(&format!("{DOCS}/a"))
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn a_cache_that_expires_entries_reports_no_comparable_count() {
        let config = FirestoreCacheConfiguration::new().add_collection_config_at(
            DOCS,
            FirestoreCacheCollectionConfiguration::new(
                "a",
                FirestoreListenerTarget::new(1000),
                FirestoreCacheCollectionLoadMode::PreloadAllDocs,
            ),
        );
        let backend = FirestoreMemoryCacheBackend::with_options(
            config,
            FirestoreMemoryCacheOptions::new().with_time_to_live(Duration::from_secs(60)),
        )
        .expect("backend");

        // A shortfall here is this cache expiring entries, not Firestore having fewer documents.
        assert_eq!(
            backend
                .authoritative_doc_count(&format!("{DOCS}/a"))
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn events_for_unconfigured_collections_are_ignored() {
        let backend = preloaded(&[("a", 1000)]);

        backend
            .on_listen_event(FirestoreListenEvent::DocumentChange(DocumentChange {
                document: Some(FirestoreDocument {
                    name: doc_path("elsewhere", "one"),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .expect("event handled");

        assert!(!cached(&backend, "elsewhere", "one").await);
    }
}
