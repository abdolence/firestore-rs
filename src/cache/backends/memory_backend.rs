use crate::errors::*;
use crate::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use moka::future::{Cache, CacheBuilder};

use crate::cache::cache_query_engine::FirestoreCacheQueryEngine;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Set by `shutdown`. The listener has stopped by then, so whatever the cache still held or
    /// took in afterwards would go stale, and an emptied collection would read as a complete one.
    /// A shut down cache therefore answers nothing and stores nothing.
    shut_down: AtomicBool,
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
            shut_down: AtomicBool::new(false),
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

    fn is_shut_down(&self) -> bool {
        self.shut_down.load(Ordering::Acquire)
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
        let read_from_time = FirestoreCacheCollectionConfiguration::listener_read_time();

        self.preload_collections(db).await?;

        Ok(self
            .config()
            .collections
            .values()
            .map(|collection_config| {
                collection_config.listener_target_params(Some(
                    FirestoreListenerTargetResumeType::ReadTime(read_from_time),
                ))
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
        let read_from_time = FirestoreCacheCollectionConfiguration::listener_read_time();
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

        let target_params = collection_config.listener_target_params(Some(
            FirestoreListenerTargetResumeType::ReadTime(read_from_time),
        ));

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
        if self.is_shut_down()
            || !self.count_authoritative
            || !collection_config.collection_load_mode.is_preloading()
        {
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
        self.shut_down.store(true, Ordering::Release);
        self.invalidate_all().await
    }

    async fn on_listen_event(&self, event: FirestoreListenEvent) -> FirestoreResult<()> {
        if self.is_shut_down() {
            return Ok(());
        }

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
                match self.config().target_change_action(target_change) {
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
        if self.is_shut_down() {
            return Ok(None);
        }

        let (collection_path, document_id) = split_document_path(document_path);

        match self.collection_cache(collection_path) {
            Some(mem_cache) => Ok(mem_cache.get(document_id).await),
            None => Ok(None),
        }
    }

    async fn update_doc_by_path(&self, document: &FirestoreDocument) -> FirestoreResult<()> {
        if self.is_shut_down() {
            return Ok(());
        }

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
        if self.is_shut_down()
            || !self.config().is_collection_listable(collection_path)
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
        if self.is_shut_down()
            || !self.config().is_collection_listable(collection_path)
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
    use futures::TryStreamExt;
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
    async fn a_shut_down_cache_reports_no_comparable_count() {
        let backend = preloaded(&[("a", 1000)]);
        seed(&backend, "a", "one").await;
        backend.shutdown().await.unwrap();

        assert_eq!(
            backend
                .authoritative_doc_count(&format!("{DOCS}/a"))
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn a_shut_down_cache_stores_nothing() {
        let backend = preloaded(&[("a", 1000)]);
        backend.shutdown().await.unwrap();
        let document = FirestoreDocument {
            name: doc_path("a", "one"),
            ..Default::default()
        };

        backend
            .on_listen_event(FirestoreListenEvent::DocumentChange(DocumentChange {
                document: Some(document.clone()),
                ..Default::default()
            }))
            .await
            .expect("event handled");
        backend
            .update_doc_by_path(&document)
            .await
            .expect("write-back");

        let mem_cache = backend
            .collection_cache(&format!("{DOCS}/a"))
            .expect("configured collection");
        assert!(mem_cache.get("one").await.is_none());
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

    async fn seed_documents<const N: usize>(
        backend: &FirestoreMemoryCacheBackend,
        documents: [(&str, Vec<(&str, FirestoreValue)>); N],
    ) {
        for (id, fields) in documents {
            let document =
                FirestoreDb::serialize_map_to_doc(doc_path("a", id), fields).expect("document");
            backend
                .update_doc_by_path(&document)
                .await
                .expect("seeded document");
        }
    }

    async fn seed_values<const N: usize>(
        backend: &FirestoreMemoryCacheBackend,
        documents: [(&str, &str, FirestoreValue); N],
    ) {
        seed_documents(
            backend,
            documents.map(|(id, field, value)| (id, vec![(field, value)])),
        )
        .await
    }

    fn query() -> FirestoreQueryParams {
        FirestoreQueryParams::new("a".into())
    }

    fn matching(filter: FirestoreQueryFilterCompare) -> FirestoreQueryParams {
        query().with_filter(FirestoreQueryFilter::Compare(Some(filter)))
    }

    fn ordered_by(field: &str, direction: FirestoreQueryDirection) -> FirestoreQueryParams {
        query().with_order_by(vec![FirestoreQueryOrder::new(field.into(), direction)])
    }

    /// Runs every query against the seeded collection and compares the ids it returns, in the
    /// order it returns them.
    async fn assert_queries_return(
        backend: &FirestoreMemoryCacheBackend,
        cases: impl IntoIterator<Item = (FirestoreQueryParams, Vec<&'static str>)>,
    ) {
        for (query, expected) in cases {
            let FirestoreCachedValue::UseCached(stream) = backend
                .query_docs(&format!("{DOCS}/a"), &query)
                .await
                .expect("query")
            else {
                panic!("the collection is preloaded, so the query is served from the cache");
            };
            let documents: Vec<FirestoreDocument> = stream.try_collect().await.expect("documents");
            let ids: Vec<&str> = documents
                .iter()
                .map(|document| document.name.rsplit('/').next().unwrap())
                .collect();
            assert_eq!(ids, expected, "{query:?}");
        }
    }

    fn timestamp(instant: &str) -> FirestoreValue {
        FirestoreTimestamp(instant.parse().expect("instant")).into()
    }

    fn typed(value_type: gcloud_sdk::google::firestore::v1::value::ValueType) -> FirestoreValue {
        FirestoreValue::from(gcloud_sdk::google::firestore::v1::Value {
            value_type: Some(value_type),
        })
    }

    fn null() -> FirestoreValue {
        typed(gcloud_sdk::google::firestore::v1::value::ValueType::NullValue(0))
    }

    fn bytes(content: &[u8]) -> FirestoreValue {
        typed(gcloud_sdk::google::firestore::v1::value::ValueType::BytesValue(content.to_vec()))
    }

    fn geo_point(latitude: f64, longitude: f64) -> FirestoreValue {
        typed(
            gcloud_sdk::google::firestore::v1::value::ValueType::GeoPointValue(
                gcloud_sdk::google::r#type::LatLng {
                    latitude,
                    longitude,
                },
            ),
        )
    }

    fn list(values: impl IntoIterator<Item = FirestoreValue>) -> FirestoreValue {
        typed(
            gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(
                gcloud_sdk::google::firestore::v1::ArrayValue {
                    values: values.into_iter().map(|value| value.value).collect(),
                },
            ),
        )
    }

    fn vector(components: Vec<f64>) -> FirestoreValue {
        FirestoreValue::from_map([
            ("__type__", "__vector__".into()),
            ("value", components.into()),
        ])
    }

    #[tokio::test]
    async fn timestamps_compare_by_seconds_then_nanoseconds() {
        let backend = preloaded(&[("a", 1000)]);
        seed_values(
            &backend,
            [
                ("early", "at", timestamp("2026-01-01T00:00:01.9Z")),
                ("whole", "at", timestamp("2026-01-01T00:00:02Z")),
                ("late", "at", timestamp("2026-01-01T00:00:02.1Z")),
            ],
        )
        .await;
        let whole = || timestamp("2026-01-01T00:00:02Z");

        assert_queries_return(
            &backend,
            [
                (
                    matching(FirestoreQueryFilterCompare::LessThan("at".into(), whole())),
                    vec!["early"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::LessThanOrEqual(
                        "at".into(),
                        whole(),
                    )),
                    vec!["early", "whole"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::GreaterThan(
                        "at".into(),
                        whole(),
                    )),
                    vec!["late"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::GreaterThanOrEqual(
                        "at".into(),
                        whole(),
                    )),
                    vec!["whole", "late"],
                ),
            ],
        )
        .await;
    }

    /// Firestore stores a timestamp at the microsecond. A query compares a lone timestamp at the
    /// nanosecond, except in `in`, `not-in` and `array-contains-any`, and compares a timestamp
    /// inside an array or a map at the microsecond.
    #[tokio::test]
    async fn timestamps_compare_at_the_microsecond_unless_a_lone_value_is_compared() {
        let backend = preloaded(&[("a", 1000)]);
        let stored = || timestamp("2026-01-01T00:00:00.000001Z");
        let later = || timestamp("2026-01-01T00:00:00.0000015Z");
        seed_documents(
            &backend,
            [(
                "stored",
                vec![
                    ("at", stored()),
                    ("list", list([stored()])),
                    ("map", FirestoreValue::from_map([("at", stored())])),
                ],
            )],
        )
        .await;

        assert_queries_return(
            &backend,
            [
                (
                    matching(FirestoreQueryFilterCompare::Equal("at".into(), later())),
                    vec![],
                ),
                (
                    matching(FirestoreQueryFilterCompare::LessThan("at".into(), later())),
                    vec!["stored"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::ArrayContains(
                        "list".into(),
                        later(),
                    )),
                    vec![],
                ),
                (
                    matching(FirestoreQueryFilterCompare::In(
                        "at".into(),
                        list([later()]),
                    )),
                    vec!["stored"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::NotIn(
                        "at".into(),
                        list([later()]),
                    )),
                    vec![],
                ),
                (
                    matching(FirestoreQueryFilterCompare::ArrayContainsAny(
                        "list".into(),
                        list([later()]),
                    )),
                    vec!["stored"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::Equal(
                        "list".into(),
                        list([later()]),
                    )),
                    vec!["stored"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::LessThan(
                        "list".into(),
                        list([later()]),
                    )),
                    vec![],
                ),
                (
                    matching(FirestoreQueryFilterCompare::Equal(
                        "map".into(),
                        FirestoreValue::from_map([("at", later())]),
                    )),
                    vec!["stored"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::LessThan(
                        "map".into(),
                        FirestoreValue::from_map([("at", later())]),
                    )),
                    vec![],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn integers_and_doubles_compare_by_numeric_value() {
        let backend = preloaded(&[("a", 1000)]);
        seed_values(
            &backend,
            [
                ("integer-one", "n", 1_i64.into()),
                ("double-one", "n", 1.0_f64.into()),
                ("double-one-and-a-half", "n", 1.5_f64.into()),
                ("integer-two", "n", 2_i64.into()),
                ("integers", "list", vec![1_i64, 3].into()),
                ("doubles", "list", vec![1.0_f64, 3.0].into()),
            ],
        )
        .await;

        assert_queries_return(
            &backend,
            [
                (
                    matching(FirestoreQueryFilterCompare::Equal(
                        "n".into(),
                        1.0_f64.into(),
                    )),
                    vec!["double-one", "integer-one"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::Equal("n".into(), 2_i64.into())),
                    vec!["integer-two"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::NotEqual(
                        "n".into(),
                        1_i64.into(),
                    )),
                    vec!["double-one-and-a-half", "integer-two"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::LessThan(
                        "n".into(),
                        1.5_f64.into(),
                    )),
                    vec!["double-one", "integer-one"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::GreaterThanOrEqual(
                        "n".into(),
                        1.5_f64.into(),
                    )),
                    vec!["double-one-and-a-half", "integer-two"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::ArrayContainsAny(
                        "list".into(),
                        vec![3.0_f64].into(),
                    )),
                    vec!["doubles", "integers"],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn in_matches_a_scalar_field_equal_to_a_listed_value() {
        let backend = preloaded(&[("a", 1000)]);
        seed_values(
            &backend,
            [
                ("integer-one", "n", 1_i64.into()),
                ("double-one", "n", 1.0_f64.into()),
                ("integer-two", "n", 2_i64.into()),
                ("text", "n", "x".into()),
                ("null", "n", null()),
                ("absent", "other", 1_i64.into()),
            ],
        )
        .await;

        assert_queries_return(
            &backend,
            [
                (
                    matching(FirestoreQueryFilterCompare::In(
                        "n".into(),
                        serde_json::json!([1, "x"]).into(),
                    )),
                    vec!["double-one", "integer-one", "text"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::NotIn(
                        "n".into(),
                        serde_json::json!(["x", 2.0]).into(),
                    )),
                    vec!["double-one", "integer-one"],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn array_contains_matches_an_array_field_holding_the_value() {
        let backend = preloaded(&[("a", 1000)]);
        seed_values(
            &backend,
            [
                ("integers", "list", vec![1_i64, 2].into()),
                ("doubles", "list", vec![1.0_f64, 3.0].into()),
                ("texts", "list", vec!["x"].into()),
                ("empty", "list", Vec::<i64>::new().into()),
                ("scalar", "list", 1_i64.into()),
                ("absent", "other", 1_i64.into()),
            ],
        )
        .await;

        assert_queries_return(
            &backend,
            [
                (
                    matching(FirestoreQueryFilterCompare::ArrayContains(
                        "list".into(),
                        1_i64.into(),
                    )),
                    vec!["doubles", "integers"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::ArrayContains(
                        "list".into(),
                        "x".into(),
                    )),
                    vec!["texts"],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn arrays_and_maps_are_equal_when_their_elements_are_equal() {
        let backend = preloaded(&[("a", 1000)]);
        let fields =
            |x: FirestoreValue, y: FirestoreValue| FirestoreValue::from_map([("x", x), ("y", y)]);
        seed_values(
            &backend,
            [
                ("integers", "v", vec![1_i64, 2].into()),
                ("doubles", "v", vec![1.0_f64, 2.0].into()),
                ("reversed", "v", vec![2_i64, 1].into()),
                ("shorter", "v", vec![1_i64].into()),
                ("nested", "v", list([vec![1_i64, 2].into(), 3_i64.into()])),
                ("not-a-number", "v", vec![f64::NAN].into()),
                ("map-integers", "v", fields(1_i64.into(), 2_i64.into())),
                ("map-doubles", "v", fields(1.0_f64.into(), 2.0_f64.into())),
                (
                    "map-fewer",
                    "v",
                    FirestoreValue::from_map([("x", 1_i64.into())]),
                ),
                (
                    "map-nested",
                    "v",
                    FirestoreValue::from_map([("x", fields(1_i64.into(), null()))]),
                ),
                ("scalar", "v", 1_i64.into()),
            ],
        )
        .await;

        assert_queries_return(
            &backend,
            [
                (
                    matching(FirestoreQueryFilterCompare::Equal(
                        "v".into(),
                        vec![1_i64, 2].into(),
                    )),
                    vec!["doubles", "integers"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::ArrayContains(
                        "v".into(),
                        vec![1_i64, 2].into(),
                    )),
                    vec!["nested"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::Equal(
                        "v".into(),
                        vec![f64::NAN].into(),
                    )),
                    vec!["not-a-number"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::Equal(
                        "v".into(),
                        fields(1_i64.into(), 2_i64.into()),
                    )),
                    vec!["map-doubles", "map-integers"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::Equal(
                        "v".into(),
                        FirestoreValue::from_map([("x", fields(1.0_f64.into(), null()))]),
                    )),
                    vec!["map-nested"],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn bytes_references_and_geo_points_are_equal_only_to_the_same_value() {
        let backend = preloaded(&[("a", 1000)]);
        seed_values(
            &backend,
            [
                ("bytes", "v", bytes(&[1, 2, 3])),
                ("other-bytes", "v", bytes(&[1, 2, 4])),
                ("text-bytes", "v", "AQID".into()),
                (
                    "reference",
                    "v",
                    FirestoreReference(doc_path("a", "target")).into(),
                ),
                (
                    "other-reference",
                    "v",
                    FirestoreReference(doc_path("a", "elsewhere")).into(),
                ),
                ("text-reference", "v", doc_path("a", "target").into()),
                ("geo-point", "v", geo_point(1.0, 2.0)),
                ("other-geo-point", "v", geo_point(1.0, 3.0)),
            ],
        )
        .await;

        assert_queries_return(
            &backend,
            [
                (
                    matching(FirestoreQueryFilterCompare::Equal(
                        "v".into(),
                        bytes(&[1, 2, 3]),
                    )),
                    vec!["bytes"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::Equal(
                        "v".into(),
                        FirestoreReference(doc_path("a", "target")).into(),
                    )),
                    vec!["reference"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::Equal(
                        "v".into(),
                        geo_point(1.0, 2.0),
                    )),
                    vec!["geo-point"],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn null_in_a_filter_value_matches_nothing_and_null_fields_need_the_null_filter() {
        let backend = preloaded(&[("a", 1000)]);
        seed_values(
            &backend,
            [
                ("null", "v", null()),
                ("one", "v", 1_i64.into()),
                ("not-a-number", "v", f64::NAN.into()),
                ("listed", "v", list([null()])),
                ("absent", "other", 1_i64.into()),
            ],
        )
        .await;

        assert_queries_return(
            &backend,
            [
                (
                    matching(FirestoreQueryFilterCompare::Equal("v".into(), null())),
                    vec![],
                ),
                (
                    matching(FirestoreQueryFilterCompare::NotEqual("v".into(), null())),
                    vec![],
                ),
                (
                    matching(FirestoreQueryFilterCompare::In(
                        "v".into(),
                        list([1_i64.into(), null()]),
                    )),
                    vec!["one"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::NotIn(
                        "v".into(),
                        list([1_i64.into(), null()]),
                    )),
                    vec![],
                ),
                (
                    matching(FirestoreQueryFilterCompare::ArrayContains(
                        "v".into(),
                        null(),
                    )),
                    vec![],
                ),
                (
                    query().with_filter(FirestoreQueryFilter::Unary(
                        FirestoreQueryFilterUnary::IsNull("v".into()),
                    )),
                    vec!["null"],
                ),
                (
                    query().with_filter(FirestoreQueryFilter::Unary(
                        FirestoreQueryFilterUnary::IsNotNan("v".into()),
                    )),
                    vec!["one", "listed"],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn values_of_every_type_order_and_compare_in_firestores_type_order() {
        let backend = preloaded(&[("a", 1000)]);
        let in_type_order = [
            ("null", null()),
            ("false", false.into()),
            ("true", true.into()),
            ("nan", f64::NAN.into()),
            ("minus-one-and-a-half", (-1.5_f64).into()),
            ("one", 1_i64.into()),
            ("two", 2.0_f64.into()),
            ("early", timestamp("2026-01-01T00:00:00Z")),
            ("late", timestamp("2026-01-01T00:00:01Z")),
            ("text-a", "a".into()),
            ("text-ab", "ab".into()),
            ("text-b", "b".into()),
            ("text-accented", "\u{e9}".into()),
            ("bytes-0", bytes(&[0])),
            ("bytes-0-1", bytes(&[0, 1])),
            ("bytes-1", bytes(&[1])),
            (
                "reference-a",
                FirestoreReference(doc_path("a", "one")).into(),
            ),
            (
                "reference-a-nested",
                FirestoreReference(doc_path("a", "one/sub/two")).into(),
            ),
            (
                "reference-a-b",
                FirestoreReference(doc_path("a-b", "one")).into(),
            ),
            ("geo-point-south", geo_point(0.0, 5.0)),
            ("geo-point-north", geo_point(1.0, 0.0)),
            ("array-empty", Vec::<i64>::new().into()),
            ("array-1", vec![1_i64].into()),
            ("array-1-2", vec![1_i64, 2].into()),
            ("array-2", vec![2_i64].into()),
            ("vector-short", vector(vec![5.0, 5.0])),
            ("vector-long", vector(vec![0.0, 0.0, 0.0])),
            ("vector-long-larger", vector(vec![0.0, 0.0, 1.0])),
            ("map-a1", FirestoreValue::from_map([("a", 1_i64.into())])),
            (
                "map-a1-b2",
                FirestoreValue::from_map([("a", 1_i64.into()), ("b", 2_i64.into())]),
            ),
            ("map-a2", FirestoreValue::from_map([("a", 2_i64.into())])),
            ("map-b0", FirestoreValue::from_map([("b", 0_i64.into())])),
        ];
        let ids: Vec<&'static str> = in_type_order.iter().map(|(id, _)| *id).collect();
        for (id, value) in in_type_order.into_iter().rev() {
            seed_values(&backend, [(id, "v", value)]).await;
        }
        seed_values(&backend, [("absent", "other", 1_i64.into())]).await;

        assert_queries_return(
            &backend,
            [
                (
                    ordered_by("v", FirestoreQueryDirection::Ascending),
                    ids.clone(),
                ),
                (
                    ordered_by("v", FirestoreQueryDirection::Descending),
                    ids.iter().rev().copied().collect(),
                ),
                (
                    ordered_by("v", FirestoreQueryDirection::Ascending)
                        .with_start_at(FirestoreQueryCursor::AfterValue(vec!["ab".into()]))
                        .with_end_at(FirestoreQueryCursor::BeforeValue(vec![bytes(&[1])])),
                    vec!["text-b", "text-accented", "bytes-0", "bytes-0-1"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::GreaterThan(
                        "v".into(),
                        "a".into(),
                    )),
                    vec!["text-ab", "text-b", "text-accented"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::LessThanOrEqual(
                        "v".into(),
                        1_i64.into(),
                    )),
                    vec!["minus-one-and-a-half", "one"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::GreaterThanOrEqual(
                        "v".into(),
                        bytes(&[0, 1]),
                    )),
                    vec!["bytes-0-1", "bytes-1"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::LessThan(
                        "v".into(),
                        FirestoreReference(doc_path("a-b", "one")).into(),
                    )),
                    vec!["reference-a", "reference-a-nested"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::GreaterThan(
                        "v".into(),
                        vec![1_i64].into(),
                    )),
                    vec!["array-1-2", "array-2"],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn the_first_order_by_field_that_differs_decides_and_documents_missing_one_are_left_out()
    {
        let backend = preloaded(&[("a", 1000)]);
        let pair = |a: i64, b: FirestoreValue| vec![("a", a.into()), ("b", b)];
        seed_documents(
            &backend,
            [
                ("p", pair(1, 2_i64.into())),
                ("q", pair(2, 1_i64.into())),
                ("r", pair(1, 1_i64.into())),
                ("s", pair(1, "x".into())),
                ("same-as-r", pair(1, 1_i64.into())),
                ("only-a", vec![("a", 1_i64.into())]),
                ("only-b", vec![("b", 3_i64.into())]),
            ],
        )
        .await;
        let order = |a, b| {
            query().with_order_by(vec![
                FirestoreQueryOrder::new("a".into(), a),
                FirestoreQueryOrder::new("b".into(), b),
            ])
        };
        use FirestoreQueryDirection::{Ascending, Descending};

        assert_queries_return(
            &backend,
            [
                (
                    order(Ascending, Ascending),
                    vec!["r", "same-as-r", "p", "s", "q"],
                ),
                (
                    order(Ascending, Descending),
                    vec!["s", "p", "same-as-r", "r", "q"],
                ),
                (
                    order(Descending, Ascending),
                    vec!["q", "r", "same-as-r", "p", "s"],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn cursors_apply_before_offset_and_offset_before_limit() {
        let backend = preloaded(&[("a", 1000)]);
        seed_values(
            &backend,
            [
                ("f", "n", 1_i64.into()),
                ("e", "n", 2_i64.into()),
                ("d", "n", 3_i64.into()),
                ("c", "n", 4_i64.into()),
                ("b", "n", 5_i64.into()),
                ("a", "n", 6_i64.into()),
            ],
        )
        .await;
        let ordered = || ordered_by("n", FirestoreQueryDirection::Ascending);
        let at = |n: i64| vec![n.into()];

        assert_queries_return(
            &backend,
            [
                (ordered().with_offset(1).with_limit(2), vec!["e", "d"]),
                (
                    ordered()
                        .with_start_at(FirestoreQueryCursor::AfterValue(at(2)))
                        .with_limit(2),
                    vec!["d", "c"],
                ),
                (
                    ordered()
                        .with_start_at(FirestoreQueryCursor::BeforeValue(at(2)))
                        .with_end_at(FirestoreQueryCursor::AfterValue(at(4)))
                        .with_offset(1),
                    vec!["d", "c"],
                ),
                (
                    matching(FirestoreQueryFilterCompare::GreaterThan(
                        "n".into(),
                        2_i64.into(),
                    ))
                    .with_limit(2),
                    vec!["d", "c"],
                ),
                (query().with_limit(2), vec!["a", "b"]),
            ],
        )
        .await;
    }
}
