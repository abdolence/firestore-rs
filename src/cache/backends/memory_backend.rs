use crate::errors::*;
use crate::FirestoreInstant;
use crate::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use moka::future::{Cache, CacheBuilder};

use crate::cache::cache_query_engine::FirestoreCacheQueryEngine;
use futures::StreamExt;
use std::collections::HashMap;
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
    pub config: FirestoreCacheConfiguration,
    collection_caches: HashMap<String, FirestoreMemCache>,
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
        Self::with_collection_options(config, move |_| {
            let mut builder = FirestoreMemCache::builder().max_capacity(options.max_capacity);
            if let Some(time_to_live) = options.time_to_live {
                builder = builder.time_to_live(time_to_live);
            }
            if let Some(time_to_idle) = options.time_to_idle {
                builder = builder.time_to_idle(time_to_idle);
            }
            builder
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
        FN: Fn(&str) -> FirestoreMemCacheOptions,
    {
        let collection_caches = config
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
            config,
            collection_caches,
        })
    }

    async fn preload_collections(&self, db: &FirestoreDb) -> Result<(), FirestoreError> {
        for (collection_path, config) in &self.config.collections {
            match config.collection_load_mode {
                FirestoreCacheCollectionLoadMode::PreloadAllDocs
                | FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty => {
                    if let Some(mem_cache) = self.collection_caches.get(collection_path.as_str()) {
                        debug!(collection_path, "Preloading collection.");

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
                            .enumerate()
                            .map(|(index, docs)| {
                                if index > 0 && index % 5000 == 0 {
                                    debug!(
                                        collection_path = collection_path.as_str(),
                                        entries_loaded = index,
                                        "Collection preload in progress...",
                                    );
                                }
                                docs
                            })
                            .for_each_concurrent(1, |doc| async move {
                                let (_, document_id) = split_document_path(&doc.name);
                                mem_cache.insert(document_id.to_string(), doc).await;
                            })
                            .await;

                        mem_cache.run_pending_tasks().await;

                        info!(
                            collection_path = collection_path.as_str(),
                            entry_count = mem_cache.entry_count(),
                            "Preloading collection has been finished.",
                        );
                    }
                }
                FirestoreCacheCollectionLoadMode::PreloadNone => {}
            }
        }
        Ok(())
    }

    async fn query_cached_docs<'b>(
        &self,
        collection_path: &str,
        query_engine: FirestoreCacheQueryEngine,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        match self.collection_caches.get(collection_path) {
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
            .config
            .collections
            .values()
            .map(|collection_config| {
                FirestoreListenerTargetParams::new(
                    collection_config.listener_target.clone(),
                    FirestoreTargetType::Query(
                        FirestoreQueryParams::new(
                            collection_config.collection_name.as_str().into(),
                        )
                        .opt_parent(collection_config.parent.clone()),
                    ),
                    HashMap::new(),
                )
                .with_resume_type(FirestoreListenerTargetResumeType::ReadTime(read_from_time))
            })
            .collect())
    }

    async fn invalidate_all(&self) -> FirestoreResult<()> {
        for (collection_path, mem_cache) in &self.collection_caches {
            debug!(collection_path, "Invalidating cache for collection.");
            mem_cache.invalidate_all();
            mem_cache.run_pending_tasks().await;
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), FirestoreError> {
        Ok(())
    }

    async fn on_listen_event(&self, event: FirestoreListenEvent) -> FirestoreResult<()> {
        match event {
            FirestoreListenEvent::DocumentChange(doc_change) => {
                if let Some(doc) = doc_change.document {
                    let (collection_path, document_id) = split_document_path(&doc.name);
                    if let Some(mem_cache) = self.collection_caches.get(collection_path) {
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
                let (collection_path, document_id) = split_document_path(&doc_deleted.document);
                if let Some(mem_cache) = self.collection_caches.get(collection_path) {
                    trace!(
                        deleted_doc = ?doc_deleted.document.as_str(),
                        "Removing document from cache due to listener event.",
                    );
                    mem_cache.remove(document_id).await;
                }
                Ok(())
            }
            _ => Ok(()),
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

        match self.collection_caches.get(collection_path) {
            Some(mem_cache) => Ok(mem_cache.get(document_id).await),
            None => Ok(None),
        }
    }

    async fn update_doc_by_path(&self, document: &FirestoreDocument) -> FirestoreResult<()> {
        let (collection_path, document_id) = split_document_path(&document.name);

        match self.collection_caches.get(collection_path) {
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
        if !self.config.is_collection_listable(collection_path) {
            return Ok(FirestoreCachedValue::SkipCache);
        }

        match self.collection_caches.get(collection_path) {
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
        if !self.config.is_collection_listable(collection_path) {
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
