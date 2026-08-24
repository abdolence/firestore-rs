//! Caching for Firestore collections and documents.
//!
//! A cache keeps a copy of the documents you read most often, so that repeated reads are served
//! locally instead of being charged and waited for. A Firestore listener keeps that copy current:
//! when a document changes - from your application or anywhere else - the change is pushed to the
//! cache, including across distributed instances.
//!
//! Caching is opt-in through cargo features:
//!
//! - `caching-memory` for an in-memory cache;
//! - `caching-persistent` for a disk-backed cache.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use firestore::*;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let db = FirestoreDb::new("my-project-id").await?;
//!
//! // Builds the cache, loads it and starts listening for changes.
//! let cache = FirestoreCache::memory(&db)
//!     .preloaded_collection("countries")
//!     .build()
//!     .await?;
//!
//! // Reads go through the cache and fall back to Firestore.
//! let country: Option<String> = db
//!     .read_through_cache(&cache)
//!     .fluent()
//!     .select()
//!     .by_id_in("countries")
//!     .obj()
//!     .one("SE")
//!     .await?;
//!
//! cache.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Reading through a cache
//!
//! A cache is attached to reads by cloning the database handle:
//!
//! - [`FirestoreDb::read_through_cache`] serves what it can from the cache and goes to Firestore
//!   for the rest. This is the mode to reach for by default.
//! - [`FirestoreDb::read_cached_only`] never contacts Firestore. Reads by ID return `None` on a
//!   miss, and requests the cache cannot answer completely return an error.
//!
//! Which operations use the cache:
//!
//! | Operation | Cached |
//! | --- | --- |
//! | Read by ID, batch read by IDs | yes, for any cached collection |
//! | `list` a collection | only for **preloaded** collections |
//! | `query` a collection | only for **preloaded** collections, and only for supported filters |
//! | Paged listing, query with metadata, aggregations, transactions, writes | never |
//!
//! # Preloading, and why listings need it
//!
//! A collection added with [`FirestoreCacheBuilder::collection`] is filled lazily: it holds only
//! the documents that happened to be read through it. Answering `list` or `query` from such a
//! collection would return a subset while looking like a complete answer, so the library refuses
//! to do it - `read_through_cache` quietly falls back to Firestore, and `read_cached_only`
//! returns an error naming the collection.
//!
//! Use [`FirestoreCacheBuilder::preloaded_collection`] when you need cached listings. See
//! [`FirestoreCacheCollectionLoadMode`] for the individual modes, and
//! [`FirestoreCacheIncompleteCollectionPolicy`] if you would rather accept partial results.
//!
//! # Consistency
//!
//! Cached results are **eventually consistent**. They reflect the last state the listener
//! delivered, so a write may take a moment to appear. Cached listings are never *partial* by
//! construction, but they are not guaranteed to be current. Do not cache data that must be read at
//! strong consistency - read that through Firestore directly, or inside a transaction.
//!
//! Firestore also reports how many documents a target matches. When that disagrees with what a
//! preloaded collection holds - which means changes were missed, typically deletes that happened
//! while the listener was disconnected - the cache drops the collection and has Firestore replay
//! it. The count is only compared when it can be trusted: a collection the cache expires entries
//! from, or fills lazily, is never checked this way, because a shortfall there is the cache doing
//! its job rather than a divergence.
//!
//! When Firestore resets or removes a listener target - after a reconnect, or when a stored resume
//! token has expired - the cache drops what it holds for that collection and Firestore replays it.
//! While that replay is in progress the collection stops answering `list` and `query` from the
//! cache, because what it holds in the meantime is a partial view that would otherwise look like a
//! complete one. `read_through_cache` falls back to Firestore for the duration;
//! `read_cached_only` returns a [`FirestoreError::CacheError`](crate::errors::FirestoreError::CacheError).
//! Reads by ID are unaffected beyond behaving like a cache miss.
//!
//! # Caching named documents instead of a whole collection
//!
//! `.collection(name)` subscribes the listener to the **entire** collection, even though it does
//! not preload it - "lazy" only means the initial download is skipped. When you know which
//! documents you care about, say so:
//!
//! ```rust,no_run
//! # use firestore::*;
//! # async fn example(db: &FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let cache = FirestoreCache::memory(db)
//!     .collection_with("configs", |c| c.documents(["site", "billing"]).preload_all())
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! The listener then watches exactly those documents, so unrelated changes in the collection are
//! never streamed to your process or written into the cache, and preloading reads just those IDs.
//! This is the shape to reach for with configuration, feature flags and reference data.
//!
//! Such a collection is never listable, whatever its load mode: it holds a chosen subset, so
//! `list` and `query` would return a partial answer that looks complete.
//!
//! # Changing the cached collections at runtime
//!
//! The set of cached collections does not have to be fixed when the cache is built:
//!
//! ```rust,no_run
//! # use firestore::*;
//! # async fn example(cache: &FirestoreMemoryCache) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! cache
//!     .add_collection(FirestoreCacheCollection::new("currencies").preload_all())
//!     .await?;
//!
//! cache.remove_collection("currencies").await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`FirestoreCache::add_collection`] downloads the collection first if it is preloaded, publishes
//! it only once it is complete, and then extends the listener - so a listing never observes it half
//! filled, and nothing written during the download is missed.
//! [`FirestoreCache::remove_collection`] does the reverse, and also forgets the collection's resume
//! token so its listener target ID cannot be reused against a different query.
//!
//! `FirestoreDb` handles created earlier with [`FirestoreDb::read_through_cache`] or
//! [`FirestoreDb::read_cached_only`] pick both up immediately: they share the cache's backend
//! rather than a copy of it. A cache can also be built with no collections at all and populated
//! entirely at runtime.
//!
//! # Lifecycle
//!
//! [`FirestoreCacheBuilder::build`] creates the cache, preloads it and starts the listener. Call
//! [`FirestoreCache::shutdown`] when you are done - it stops the listener and releases the
//! backend's resources, dropping the cached documents and, for the persistent backend, closing its
//! database file. Because `load` and `shutdown` take `&self`, a built cache can be shared as
//! `Arc<FirestoreMemoryCache>` in your application state.
//!
//! # Custom backends
//!
//! To store the cache somewhere else, implement [`FirestoreCacheBackend`] and its supertrait
//! [`FirestoreCacheDocsByPathSupport`], then construct [`FirestoreCache`] with it.

use crate::errors::{FirestoreCacheError, FirestoreErrorPublicGenericDetails};
use crate::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Builds the error returned when a `read_cached_only` session asks for a `list`/`query` that the
/// cache cannot answer completely.
///
/// Returning an error rather than a partial result is deliberate: a silently incomplete
/// collection is far more damaging than a loud failure.
pub(crate) fn cache_incomplete_collection_error(
    collection_id: &str,
    reason: &str,
) -> FirestoreError {
    FirestoreError::CacheError(FirestoreCacheError::new(
        FirestoreErrorPublicGenericDetails::new("CacheIncompleteCollection".into()),
        format!(
            "The cache cannot serve this request for collection `{collection_id}` completely: \
             {reason}. Reading it from the cache would silently return partial results. \
             Either configure the collection with FirestoreCacheCollectionLoadMode::PreloadAllDocs \
             (or PreloadAllIfEmpty), or use `db.read_through_cache(&cache)` to fall back to \
             Firestore. To opt back into the previous partial-result behaviour, set \
             FirestoreCacheIncompleteCollectionPolicy::PartialResults on the cache configuration."
        ),
    ))
}

mod options;
pub use options::*;

mod configuration;
pub use configuration::*;

mod builder;
pub use builder::*;

mod backends;
pub use backends::*;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use tracing::*;

mod cache_filter_engine;
mod cache_query_engine;

/// Manages a cache of Firestore data.
///
/// `FirestoreCache` listens to changes in Firestore for specified targets and updates
/// a cache backend accordingly. It provides methods to load initial data, manage the
/// listener lifecycle, and access the underlying cache backend.
///
/// # Type Parameters
/// * `B`: The type of the cache backend, implementing [`FirestoreCacheBackend`].
/// * `LS`: The type of storage for the listener's resume state, implementing
///   [`FirestoreResumeStateStorage`](crate::FirestoreResumeStateStorage).
pub struct FirestoreCache<B, LS>
where
    B: FirestoreCacheBackend + Send + Sync + 'static,
    LS: FirestoreResumeStateStorage,
{
    inner: FirestoreCacheInner<B, LS>,
}

/// Inner state of the `FirestoreCache`.
struct FirestoreCacheInner<B, LS>
where
    B: FirestoreCacheBackend + Send + Sync + 'static,
    LS: FirestoreResumeStateStorage,
{
    /// Configuration options for the cache.
    pub options: FirestoreCacheOptions,
    /// The cache backend implementation.
    pub backend: Arc<B>,
    /// The Firestore listener for real-time updates.
    ///
    /// Behind a mutex so that `load`/`shutdown` can take `&self`, which lets a built cache be
    /// shared directly as `Arc<FirestoreCache<..>>` in application state.
    pub listener: tokio::sync::Mutex<FirestoreListener<FirestoreDb, LS>>,
    /// A clone of the Firestore database client.
    pub db: FirestoreDb,
    /// Serialises adding and removing collections.
    ///
    /// Without it two concurrent `add_collection` calls could both pass the "already cached" check
    /// before either published its entry, leaving one of the two listener targets orphaned.
    pub collection_mutations: tokio::sync::Mutex<()>,
    /// The next listener target ID to hand out to a collection added at runtime.
    ///
    /// Monotonic on purpose: an ID freed by `remove_collection` is never reissued in this process,
    /// so a resume token that outlived its target cannot be applied to a different query.
    pub next_listener_target: std::sync::Mutex<u32>,
}

/// A ready-to-use in-memory cache.
///
/// Use this alias to store a cache in your own types without spelling out its generic
/// parameters:
///
/// ```rust,no_run
/// # use firestore::*;
/// struct AppState {
///     db: FirestoreDb,
///     cache: std::sync::Arc<FirestoreMemoryCache>,
/// }
/// ```
#[cfg(feature = "caching-memory")]
pub type FirestoreMemoryCache =
    FirestoreCache<FirestoreMemoryCacheBackend, FirestoreMemListenStateStorage>;

/// A ready-to-use persistent cache. See [`FirestoreMemoryCache`] for how to use the alias.
#[cfg(feature = "caching-persistent")]
pub type FirestorePersistentCache =
    FirestoreCache<FirestorePersistentCacheBackend, FirestoreTempFilesListenStateStorage>;

#[cfg(feature = "caching-memory")]
impl FirestoreMemoryCache {
    /// Starts building an in-memory cache.
    ///
    /// ```rust,no_run
    /// # use firestore::*;
    /// # async fn example(db: &FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let cache = FirestoreCache::memory(db)
    ///     .preloaded_collection("countries")
    ///     .build()
    ///     .await?;
    ///
    /// let country: Option<String> = db
    ///     .read_through_cache(&cache)
    ///     .fluent()
    ///     .select()
    ///     .by_id_in("countries")
    ///     .obj()
    ///     .one("SE")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn memory(db: &FirestoreDb) -> FirestoreCacheBuilder<FirestoreMemoryCacheKind> {
        FirestoreCacheBuilder::new(db)
    }

    /// Starts building an in-memory cache. Equivalent to [`FirestoreCache::memory`].
    #[inline]
    pub fn builder(db: &FirestoreDb) -> FirestoreCacheBuilder<FirestoreMemoryCacheKind> {
        FirestoreCacheBuilder::new(db)
    }
}

#[cfg(feature = "caching-persistent")]
impl FirestorePersistentCache {
    /// Starts building a persistent, disk-backed cache.
    ///
    /// ```rust,no_run
    /// # use firestore::*;
    /// # async fn example(db: &FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let cache = FirestoreCache::persistent(db)
    ///     .data_dir("/var/cache/my-app")
    ///     .preloaded_collection("countries")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn persistent(db: &FirestoreDb) -> FirestoreCacheBuilder<FirestorePersistentCacheKind> {
        FirestoreCacheBuilder::new(db)
    }

    /// Starts building a persistent cache. Equivalent to [`FirestoreCache::persistent`].
    #[inline]
    pub fn builder(db: &FirestoreDb) -> FirestoreCacheBuilder<FirestorePersistentCacheKind> {
        FirestoreCacheBuilder::new(db)
    }
}

/// Represents a value that might be retrieved from the cache.
pub enum FirestoreCachedValue<T> {
    /// The value was found and retrieved from the cache.
    UseCached(T),
    /// The cache should be skipped for this request; the caller should fetch directly from Firestore.
    SkipCache,
}

impl<B, LS> FirestoreCache<B, LS>
where
    B: FirestoreCacheBackend + Send + Sync + 'static,
    LS: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
{
    /// Creates a new `FirestoreCache` with default options for the given name.
    ///
    /// # Arguments
    /// * `name`: A unique name for this cache instance.
    /// * `db`: A reference to the [`FirestoreDb`](crate::FirestoreDb) client.
    /// * `backend`: The cache backend implementation.
    /// * `listener_storage`: Storage for the listener's resume state.
    ///
    /// # Returns
    /// A `FirestoreResult` containing the new `FirestoreCache`.
    #[deprecated(
        since = "0.52.0",
        note = "Use the cache builder instead: `FirestoreCache::memory(&db)` or \
                `FirestoreCache::persistent(&db)`. It assigns listener targets automatically, \
                picks a matching listener state storage and loads the cache for you. \
                This constructor keeps working and will not be removed in 0.x."
    )]
    pub async fn new(
        name: FirestoreCacheName,
        db: &FirestoreDb,
        backend: B,
        listener_storage: LS,
    ) -> FirestoreResult<Self>
    where
        B: FirestoreCacheBackend + Send + Sync + 'static,
    {
        let options = FirestoreCacheOptions::new(name);
        Self::create(options, db, backend, listener_storage).await
    }

    /// Creates a new `FirestoreCache` with the specified options.
    ///
    /// # Arguments
    /// * `options`: [`FirestoreCacheOptions`] to configure the cache.
    /// * `db`: A reference to the [`FirestoreDb`](crate::FirestoreDb) client.
    /// * `backend`: The cache backend implementation.
    /// * `listener_storage`: Storage for the listener's resume state.
    ///
    /// # Returns
    /// A `FirestoreResult` containing the new `FirestoreCache`.
    #[deprecated(
        since = "0.52.0",
        note = "Use the cache builder instead: `FirestoreCache::memory(&db)` or \
                `FirestoreCache::persistent(&db)`. It assigns listener targets automatically, \
                picks a matching listener state storage and loads the cache for you. \
                This constructor keeps working and will not be removed in 0.x."
    )]
    pub async fn with_options(
        options: FirestoreCacheOptions,
        db: &FirestoreDb,
        backend: B,
        listener_storage: LS,
    ) -> FirestoreResult<Self>
    where
        B: FirestoreCacheBackend + Send + Sync + 'static,
    {
        Self::create(options, db, backend, listener_storage).await
    }

    /// Creates a cache without loading it. Shared by the builder and the deprecated
    /// constructors, so that neither of them calls a deprecated item.
    pub(crate) async fn create(
        options: FirestoreCacheOptions,
        db: &FirestoreDb,
        backend: B,
        listener_storage: LS,
    ) -> FirestoreResult<Self>
    where
        B: FirestoreCacheBackend + Send + Sync + 'static,
    {
        let listener = if let Some(ref listener_params) = options.listener_params {
            db.create_listener_with_params(listener_storage, listener_params.clone())
                .await?
        } else {
            db.create_listener(listener_storage).await?
        };

        let next_listener_target = backend
            .cache_configuration()
            .max_listener_target()
            .map(|max| max.saturating_add(1))
            .unwrap_or(FIRESTORE_CACHE_DEFAULT_LISTENER_TARGET_BASE);

        Ok(Self {
            inner: FirestoreCacheInner {
                options,
                backend: Arc::new(backend),
                listener: tokio::sync::Mutex::new(listener),
                db: db.clone(),
                collection_mutations: tokio::sync::Mutex::new(()),
                next_listener_target: std::sync::Mutex::new(next_listener_target),
            },
        })
    }

    /// Returns the name of this cache instance.
    pub fn name(&self) -> &FirestoreCacheName {
        &self.inner.options.name
    }

    /// Loads initial data into the cache and starts the Firestore listener.
    ///
    /// This method typically calls the backend's `load` method to determine which
    /// Firestore targets to listen to, adds them to the internal listener, and then
    /// starts the listener. The listener will then call the backend's `on_listen_event`
    /// method for incoming changes.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    pub async fn load(&self) -> Result<(), FirestoreError> {
        let backend_target_params = self
            .inner
            .backend
            .load(&self.inner.options, &self.inner.db)
            .await?;

        let mut listener = self.inner.listener.lock().await;

        for target_params in backend_target_params {
            listener.add_target(target_params)?;
        }

        let handler = Arc::new(FirestoreCacheListenHandler::new(
            self.inner.backend.clone(),
            listener.control_handle(),
        ));

        listener
            .start(move |event| {
                let handler = handler.clone();
                async move {
                    handler.on_listen_event(event).await;
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    /// Shuts down the Firestore listener and releases the cache backend's resources.
    ///
    /// The in-memory backend drops its cached documents; the persistent backend closes its
    /// database, releasing the exclusive lock on the file so that another cache can be opened over
    /// the same directory. This happens here rather than on drop because handles to the backend
    /// outlive the cache - [`read_through_cache`](crate::FirestoreDb::read_through_cache) clones
    /// one into every `FirestoreDb` it is attached to.
    ///
    /// Reads through a cache that has been shut down do not fail: they behave as a cache miss, so
    /// `read_through_cache` falls back to Firestore. `read_cached_only` reports the miss as an
    /// error, as it does for anything else it cannot answer.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    pub async fn shutdown(&self) -> Result<(), FirestoreError> {
        self.inner.listener.lock().await.shutdown().await?;
        self.inner.backend.shutdown().await?;
        Ok(())
    }

    /// Starts caching a collection on a running cache, without rebuilding it.
    ///
    /// The collection is downloaded first if it is configured to be preloaded, and only becomes
    /// visible to readers once it is fully populated - so a listing never sees it half filled.
    /// The listener then picks it up from the moment the download started, so nothing written in
    /// the meantime is missed.
    ///
    /// `FirestoreDb` handles created earlier with
    /// [`read_through_cache`](crate::FirestoreDb::read_through_cache) or
    /// [`read_cached_only`](crate::FirestoreDb::read_cached_only) see the new collection
    /// immediately: they share this cache's backend rather than a copy of it.
    ///
    /// ```rust,no_run
    /// # use firestore::*;
    /// # async fn example(cache: &FirestoreMemoryCache) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// cache
    ///     .add_collection(FirestoreCacheCollection::new("currencies").preload_all())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Returns an error if the collection is already cached, or if the backend does not support
    /// changing its collections at runtime.
    pub async fn add_collection(
        &self,
        collection: FirestoreCacheCollection,
    ) -> FirestoreResult<()> {
        let _mutation = self.inner.collection_mutations.lock().await;

        let documents_path = self.inner.db.get_documents_path();
        let config = self.inner.backend.cache_configuration();

        let listener_target = match collection.requested_listener_target() {
            Some(target) => {
                let target = FirestoreListenerTarget::new(target);
                target.validate()?;
                target
            }
            None => {
                // Never reuse an ID freed by `remove_collection`: a resume token belongs to one
                // target's query, so handing the ID to a different one would resume it wrongly.
                let mut next = self
                    .inner
                    .next_listener_target
                    .lock()
                    .expect("cache listener target counter poisoned");
                let allocated = config.allocate_listener_target(*next)?;
                *next = allocated.value().saturating_add(1);
                allocated
            }
        };

        let collection_config = collection.into_configuration(listener_target);
        let collection_path = collection_config.resolve_collection_path(documents_path);

        if config.collections.contains_key(&collection_path) {
            return Err(FirestoreError::CacheError(FirestoreCacheError::new(
                FirestoreErrorPublicGenericDetails::new("CacheCollectionAlreadyCached".into()),
                format!("The collection `{collection_path}` is already cached."),
            )));
        }

        let target_params = self
            .inner
            .backend
            .add_collection(&self.inner.options, &self.inner.db, collection_config)
            .await?;

        self.inner.listener.lock().await.add_target(target_params)?;

        info!(collection_path, "Added a collection to the cache.");
        Ok(())
    }

    /// Stops caching a collection, drops its documents and stops listening to it.
    ///
    /// Returns `false` if the collection was not cached. Use
    /// [`remove_collection_at`](Self::remove_collection_at) for a sub-collection, whose absolute
    /// path a bare name cannot address.
    pub async fn remove_collection<S>(&self, collection_name: S) -> FirestoreResult<bool>
    where
        S: AsRef<str>,
    {
        let collection_path = format!(
            "{}/{}",
            self.inner.db.get_documents_path(),
            collection_name.as_ref()
        );
        self.remove_collection_at(&collection_path).await
    }

    /// Stops caching the collection at an absolute path. See
    /// [`remove_collection`](Self::remove_collection).
    pub async fn remove_collection_at(&self, collection_path: &str) -> FirestoreResult<bool> {
        let Some(target) = self
            .inner
            .backend
            .remove_collection(collection_path)
            .await?
        else {
            return Ok(false);
        };

        self.inner
            .listener
            .lock()
            .await
            .remove_target(&target)
            .await?;

        info!(collection_path, "Removed a collection from the cache.");
        Ok(true)
    }

    /// The absolute paths of the collections this cache currently holds.
    pub fn cached_collections(&self) -> Vec<String> {
        self.inner
            .backend
            .cache_configuration()
            .collections
            .keys()
            .cloned()
            .collect()
    }

    /// Returns a thread-safe reference-counted pointer to the cache backend.
    pub fn backend(&self) -> Arc<B> {
        self.inner.backend.clone()
    }

    /// Invalidates all data in the cache.
    ///
    /// This calls the `invalidate_all` method on the cache backend.
    ///
    /// # Returns
    /// A `FirestoreResult` indicating success or failure.
    pub async fn invalidate_all(&self) -> FirestoreResult<()> {
        self.inner.backend.invalidate_all().await
    }
}

/// Defines the contract for a Firestore cache backend.
///
/// Implementors of this trait are responsible for storing, retrieving, and updating
/// cached Firestore data.
#[async_trait]
pub trait FirestoreCacheBackend: FirestoreCacheDocsByPathSupport {
    /// Loads initial data or configuration for the cache.
    ///
    /// This method is called when [`FirestoreCache::load()`] is invoked. It should
    /// determine which Firestore targets the cache needs to listen to and return
    /// them as a `Vec<FirestoreListenerTargetParams>`. These targets will be added
    /// to the `FirestoreCache`'s internal listener.
    ///
    /// # Arguments
    /// * `options`: The cache options.
    /// * `db`: A reference to the Firestore database client.
    ///
    /// # Returns
    /// A `Result` containing the listener target parameters or an error.
    async fn load(
        &self,
        options: &FirestoreCacheOptions,
        db: &FirestoreDb,
    ) -> Result<Vec<FirestoreListenerTargetParams>, FirestoreError>;

    /// Invalidates all data stored in the cache.
    ///
    /// # Returns
    /// A `FirestoreResult` indicating success or failure.
    async fn invalidate_all(&self) -> FirestoreResult<()>;

    /// Performs any necessary cleanup or shutdown procedures for the cache backend.
    ///
    /// This is called when [`FirestoreCache::shutdown()`] is invoked.
    ///
    /// # Returns
    /// A `FirestoreResult` indicating success or failure.
    async fn shutdown(&self) -> FirestoreResult<()>;

    /// Handles a listen event from Firestore.
    ///
    /// This method is called by the `FirestoreCache`'s listener when a change
    /// occurs for one of the listened targets. The backend should update its
    /// cached data based on the event.
    ///
    /// # Arguments
    /// * `event`: The [`FirestoreListenEvent`](crate::FirestoreListenEvent) received from Firestore.
    ///
    /// # Returns
    /// A `FirestoreResult` indicating success or failure of processing the event.
    async fn on_listen_event(&self, event: FirestoreListenEvent) -> FirestoreResult<()>;

    /// A snapshot of the collections this backend currently caches.
    ///
    /// [`FirestoreCache`] is generic over the backend, so this is the only way it can see the
    /// configuration - it uses it to reject a collection that is already cached and to pick a free
    /// listener target ID for a new one.
    ///
    /// The default returns an empty configuration, which disables adding and removing collections
    /// at runtime.
    fn cache_configuration(&self) -> Arc<FirestoreCacheConfiguration> {
        Arc::new(FirestoreCacheConfiguration::new())
    }

    /// Starts caching a collection on a running cache.
    ///
    /// The backend creates the collection's storage, preloads it if the load mode asks for it, and
    /// publishes it into its configuration only once it is fully populated. It returns the target
    /// the cache should then start listening on.
    ///
    /// The default reports that the backend does not support this, so that a backend which cannot
    /// do it fails loudly rather than quietly ignoring the request.
    async fn add_collection(
        &self,
        options: &FirestoreCacheOptions,
        db: &FirestoreDb,
        collection_config: FirestoreCacheCollectionConfiguration,
    ) -> FirestoreResult<FirestoreListenerTargetParams> {
        let _ = (options, db, collection_config);
        Err(cache_dynamic_collections_unsupported_error(
            "add_collection",
        ))
    }

    /// Drops everything cached for one collection and stops it answering `list`/`query` until
    /// Firestore reports it consistent again.
    ///
    /// Called when the cache is found to have diverged from the server and the collection is about
    /// to be replayed. The default invalidates the entire cache, which is correct but far coarser
    /// than it needs to be - override it.
    async fn begin_collection_resync(&self, collection_path: &str) -> FirestoreResult<()> {
        let _ = collection_path;
        self.invalidate_all().await
    }

    /// How many documents the backend holds for a collection, **when that number can be trusted
    /// to equal the number of documents Firestore holds**.
    ///
    /// Used to act on Firestore's existence filter, which reports the server's count so a client
    /// can notice that its view has diverged - typically because deletes were missed while the
    /// listener was disconnected.
    ///
    /// Returning `None` (the default) disables that check for the collection, and is the right
    /// answer whenever the backend evicts documents on its own: an undercount caused by the
    /// backend's own eviction is not a divergence, and treating it as one would re-download the
    /// collection over and over.
    async fn authoritative_doc_count(&self, collection_path: &str) -> FirestoreResult<Option<u64>> {
        let _ = collection_path;
        Ok(None)
    }

    /// Stops caching a collection and drops its data.
    ///
    /// Returns the listener target that was keeping it up to date, or `None` if the collection was
    /// not cached. See [`add_collection`](Self::add_collection) for the default behaviour.
    async fn remove_collection(
        &self,
        collection_path: &str,
    ) -> FirestoreResult<Option<FirestoreListenerTarget>> {
        let _ = collection_path;
        Err(cache_dynamic_collections_unsupported_error(
            "remove_collection",
        ))
    }
}

/// The shortest interval between two resynchronisations of the same target.
///
/// A target whose count keeps disagreeing with Firestore would otherwise re-download its
/// collection in a loop.
const FIRESTORE_CACHE_MIN_RESYNC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Applies listen events to a cache backend, and acts on the ones that say the cache has diverged.
///
/// This sits between the listener and the backend so that the divergence policy lives in one place
/// rather than in each backend, and so that it can reach the listener - which a backend cannot.
struct FirestoreCacheListenHandler<B> {
    backend: Arc<B>,
    listener: FirestoreListenerControlHandle,
    state: tokio::sync::Mutex<FirestoreCacheListenState>,
}

#[derive(Default)]
struct FirestoreCacheListenState {
    /// Existence filters awaiting the end of their snapshot, keyed by target.
    pending_filters: HashMap<FirestoreListenerTarget, i32>,
    /// When each target was last resynchronised, for the rate limit.
    last_resync: HashMap<FirestoreListenerTarget, std::time::Instant>,
}

impl<B> FirestoreCacheListenHandler<B>
where
    B: FirestoreCacheBackend + Send + Sync + 'static,
{
    fn new(backend: Arc<B>, listener: FirestoreListenerControlHandle) -> Self {
        Self {
            backend,
            listener,
            state: tokio::sync::Mutex::new(FirestoreCacheListenState::default()),
        }
    }

    async fn on_listen_event(&self, event: FirestoreListenEvent) {
        match &event {
            // Firestore sends the filter and the changes of the same snapshot as a group.
            // Comparing counts the moment it arrives would compare a half-applied snapshot, and a
            // false mismatch costs a full re-download - so hold it until the snapshot closes.
            FirestoreListenEvent::Filter(filter) => {
                if let Ok(target) = FirestoreListenerTarget::try_from(filter.target_id) {
                    if filter.unchanged_names.is_some() {
                        debug!(
                            ?target,
                            "Firestore sent a bloom filter with its existence filter; this cache                              resynchronises on a count mismatch instead of using it.",
                        );
                    }
                    self.state
                        .lock()
                        .await
                        .pending_filters
                        .insert(target, filter.count);
                }
                return;
            }
            FirestoreListenEvent::TargetChange(target_change) => {
                // A reset or removed target is about to be replayed, so a filter counting the copy
                // we are dropping says nothing about the copy that replaces it.
                if matches!(
                    FirestoreListenerTargetChangeType::try_from(target_change.target_change_type),
                    Ok(FirestoreListenerTargetChangeType::Reset)
                        | Ok(FirestoreListenerTargetChangeType::Remove)
                ) {
                    self.discard_pending_filters(&target_change.target_ids)
                        .await;
                }

                // NO_CHANGE and CURRENT with a read time are the snapshot boundaries.
                let settles_snapshot = matches!(
                    FirestoreListenerTargetChangeType::try_from(target_change.target_change_type),
                    Ok(FirestoreListenerTargetChangeType::NoChange)
                        | Ok(FirestoreListenerTargetChangeType::Current)
                );
                if settles_snapshot && target_change.read_time.is_some() {
                    self.settle_pending_filters(&target_change.target_ids).await;
                }
            }
            _ => {}
        }

        if let Err(err) = self.backend.on_listen_event(event).await {
            error!(?err, "Error occurred while updating cache.");
        }
    }

    /// Forgets the filters waiting on targets whose contents are being thrown away anyway.
    async fn discard_pending_filters(&self, target_ids: &[i32]) {
        let mut state = self.state.lock().await;
        if target_ids.is_empty() {
            state.pending_filters.clear();
            return;
        }
        for target in target_ids
            .iter()
            .filter_map(|id| FirestoreListenerTarget::try_from(*id).ok())
        {
            state.pending_filters.remove(&target);
        }
    }

    /// Compares the filters that were waiting on this snapshot, and resynchronises what diverged.
    async fn settle_pending_filters(&self, target_ids: &[i32]) {
        let settled: Vec<(FirestoreListenerTarget, i32)> = {
            let mut state = self.state.lock().await;
            if state.pending_filters.is_empty() {
                return;
            }
            if target_ids.is_empty() {
                state.pending_filters.drain().collect()
            } else {
                target_ids
                    .iter()
                    .filter_map(|id| FirestoreListenerTarget::try_from(*id).ok())
                    .filter_map(|target| {
                        state
                            .pending_filters
                            .remove(&target)
                            .map(|count| (target, count))
                    })
                    .collect()
            }
        };

        let config = self.backend.cache_configuration();
        for (target, remote_count) in settled {
            let Some(collection_path) = config
                .collections
                .iter()
                .find(|(_, c)| c.listener_target == target)
                .map(|(path, _)| path.clone())
            else {
                continue;
            };

            let local_count = match self.backend.authoritative_doc_count(&collection_path).await {
                Ok(Some(count)) => count,
                Ok(None) => {
                    debug!(
                        collection_path,
                        "Cannot compare this collection's document count with Firestore's, so                          the existence filter is ignored.",
                    );
                    continue;
                }
                Err(err) => {
                    warn!(?err, collection_path, "Could not count cached documents.");
                    continue;
                }
            };

            if i64::from(remote_count) == local_count as i64 {
                trace!(
                    collection_path,
                    local_count,
                    "The cache agrees with Firestore."
                );
                continue;
            }

            self.resync(target, &collection_path, local_count, remote_count)
                .await;
        }
    }

    async fn resync(
        &self,
        target: FirestoreListenerTarget,
        collection_path: &str,
        local_count: u64,
        remote_count: i32,
    ) {
        {
            let mut state = self.state.lock().await;
            if let Some(last) = state.last_resync.get(&target) {
                if last.elapsed() < FIRESTORE_CACHE_MIN_RESYNC_INTERVAL {
                    warn!(
                        collection_path,
                        local_count,
                        remote_count,
                        "The cache still disagrees with Firestore, but it was resynchronised                          recently. Skipping this one.",
                    );
                    return;
                }
            }
            state
                .last_resync
                .insert(target.clone(), std::time::Instant::now());
        }

        warn!(
            collection_path,
            local_count,
            remote_count,
            "The cache holds a different number of documents than Firestore reports, so it has              missed changes. Resynchronising the collection.",
        );

        // Drop the stale copy first. Firestore replays a target that is re-added without a resume
        // token, but a replay only says which documents exist - never which ones no longer do - so
        // without this the documents whose deletion we missed would survive the resync, and the
        // count would keep disagreeing.
        if let Err(err) = self.backend.begin_collection_resync(collection_path).await {
            error!(
                ?err,
                collection_path,
                "Could not drop a diverged collection, so it was not resynchronised."
            );
            return;
        }

        // The listener then discards the target's resume state and reopens the stream. The
        // collection serves listings again once Firestore reports the target current.
        self.listener.resync_target(target);
    }
}

/// How far back a target's read time is set from the local clock.
///
/// A resume `read_time` in the server's future is rejected as invalid, and the client's clock can
/// easily be a little ahead. The cost of the margin is a few redundant document changes, which are
/// idempotent; the cost of being wrong the other way is a rejected listen request.
const FIRESTORE_CACHE_READ_TIME_SKEW_MARGIN: std::time::Duration =
    std::time::Duration::from_secs(5);

/// The point in time a newly attached target should be resumed from.
///
/// Deliberately a little in the past - see [`FIRESTORE_CACHE_READ_TIME_SKEW_MARGIN`].
pub(crate) fn cache_target_read_time() -> FirestoreInstant {
    let now = FirestoreInstant::now();
    jiff::SignedDuration::try_from(FIRESTORE_CACHE_READ_TIME_SKEW_MARGIN)
        .ok()
        .and_then(|margin| now.checked_sub(margin).ok())
        .unwrap_or(now)
}

/// Builds the listener target that keeps one cached collection up to date.
///
/// Shared by the backends and by their runtime `add_collection`, so that a collection added later
/// is listened to exactly like one configured up front.
pub(crate) fn target_params_for_collection(
    collection_config: &FirestoreCacheCollectionConfiguration,
    resume_type: Option<FirestoreListenerTargetResumeType>,
) -> FirestoreListenerTargetParams {
    let target_type = match collection_config.watched_document_ids() {
        // Watching named documents keeps unrelated changes in the collection off the wire
        // entirely, instead of streaming them here only to be filtered out.
        Some(document_ids) => FirestoreTargetType::Documents(
            FirestoreCollectionDocuments::new(
                collection_config.collection_name.clone(),
                document_ids.to_vec(),
            )
            .opt_parent(collection_config.parent.clone()),
        ),
        None => FirestoreTargetType::Query(
            FirestoreQueryParams::new(collection_config.collection_name.as_str().into())
                .opt_parent(collection_config.parent.clone()),
        ),
    };

    FirestoreListenerTargetParams::new(
        collection_config.listener_target.clone(),
        target_type,
        std::collections::HashMap::new(),
    )
    .opt_resume_type(resume_type)
}

/// Builds the error returned when a backend does not support changing its collections at runtime.
pub(crate) fn cache_dynamic_collections_unsupported_error(operation: &str) -> FirestoreError {
    FirestoreError::CacheError(FirestoreCacheError::new(
        FirestoreErrorPublicGenericDetails::new("CacheDynamicCollectionsUnsupported".into()),
        format!(
            "This cache backend does not support `{operation}`. Implement it on your \
             FirestoreCacheBackend to add and remove cached collections while the cache runs."
        ),
    ))
}

/// What a backend should do with the collections a Firestore target change affects.
///
/// Firestore uses an empty set of target IDs to mean *all* targets, which this resolves for the
/// caller.
pub enum FirestoreCacheTargetChangeAction {
    /// The listener targets are being replayed from scratch: drop what is cached for them and stop
    /// answering `list`/`query` from their collections until the replay completes.
    SuspendAndInvalidate(Vec<FirestoreCacheInvalidation>),
    /// The listener targets now reflect a consistent snapshot again: their collections may serve
    /// `list`/`query` once more.
    Resume(Vec<String>),
    /// Nothing to do.
    Ignore,
}

/// What one target change asks the backend to drop.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FirestoreCacheInvalidation {
    /// Drop every document cached for this collection.
    Collection(String),
    /// Drop only these documents of the collection - the target watches nothing else, so wiping
    /// the whole collection would throw away documents it never covered.
    Documents {
        collection_path: String,
        document_ids: Vec<String>,
    },
}

impl FirestoreCacheInvalidation {
    /// The collection this invalidation belongs to.
    pub fn collection_path(&self) -> &str {
        match self {
            Self::Collection(collection_path) => collection_path,
            Self::Documents {
                collection_path, ..
            } => collection_path,
        }
    }
}

/// Decides what a target change means for the cache.
///
/// Shared by the backends so that they cannot drift apart, and kept free of any backend state so
/// that it can be unit tested on its own. Reachable publicly as
/// [`FirestoreCacheConfiguration::target_change_action`].
pub(crate) fn cache_target_change_action(
    config: &FirestoreCacheConfiguration,
    target_change: &gcloud_sdk::google::firestore::v1::TargetChange,
) -> FirestoreCacheTargetChangeAction {
    use gcloud_sdk::google::firestore::v1::target_change::TargetChangeType;

    let Ok(change_type) = TargetChangeType::try_from(target_change.target_change_type) else {
        return FirestoreCacheTargetChangeAction::Ignore;
    };

    match change_type {
        TargetChangeType::Reset | TargetChangeType::Remove => {
            FirestoreCacheTargetChangeAction::SuspendAndInvalidate(
                affected_scopes(config, &target_change.target_ids)
                    .into_iter()
                    .map(|scope| match scope {
                        FirestoreCacheTargetScope::Collection(collection_path) => {
                            FirestoreCacheInvalidation::Collection(collection_path.to_string())
                        }
                        FirestoreCacheTargetScope::Documents {
                            collection_path,
                            document_ids,
                        } => FirestoreCacheInvalidation::Documents {
                            collection_path: collection_path.to_string(),
                            document_ids: document_ids.to_vec(),
                        },
                    })
                    .collect(),
            )
        }
        TargetChangeType::Current => FirestoreCacheTargetChangeAction::Resume(
            affected_scopes(config, &target_change.target_ids)
                .into_iter()
                .map(|scope| scope.collection_path().to_string())
                .collect(),
        ),
        TargetChangeType::NoChange | TargetChangeType::Add => {
            FirestoreCacheTargetChangeAction::Ignore
        }
    }
}

fn affected_scopes<'a>(
    config: &'a FirestoreCacheConfiguration,
    target_ids: &[i32],
) -> Vec<FirestoreCacheTargetScope<'a>> {
    if target_ids.is_empty() {
        return config.all_target_scopes();
    }

    target_ids
        .iter()
        .filter_map(|target_id_num| FirestoreListenerTarget::try_from(*target_id_num).ok())
        .filter_map(|target| config.target_scope(&target))
        .collect()
}

/// Defines support for retrieving and updating cached documents by their full path.
#[async_trait]
pub trait FirestoreCacheDocsByPathSupport {
    /// Retrieves a single document from the cache by its full Firestore path.
    ///
    /// # Arguments
    /// * `document_path`: The full path to the document (e.g., "projects/P/databases/D/documents/C/ID").
    ///
    /// # Returns
    /// A `FirestoreResult` containing an `Option<FirestoreDocument>`.
    /// `None` if the document is not found in the cache.
    async fn get_doc_by_path(
        &self,
        document_path: &str,
    ) -> FirestoreResult<Option<FirestoreDocument>>;

    /// Retrieves multiple documents from the cache by their full Firestore paths.
    ///
    /// This default implementation iterates over `full_doc_ids` and calls `get_doc_by_path`
    /// for each. Backends may provide a more optimized batch implementation.
    ///
    /// # Arguments
    /// * `full_doc_ids`: A slice of full document paths.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a stream of `FirestoreResult<(String, Option<FirestoreDocument>)>`.
    /// The `String` in the tuple is the document ID (last segment of the path).
    async fn get_docs_by_paths<'a>(
        &'a self,
        full_doc_ids: &'a [String],
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<FirestoreDocument>)>>>
    where
        Self: Sync,
    {
        Ok(Box::pin(futures::stream::iter(full_doc_ids).filter_map({
            move |document_path| async move {
                match self.get_doc_by_path(document_path.as_str()).await {
                    Ok(maybe_doc) => maybe_doc.map(|document| {
                        let doc_id = document
                            .name
                            .split('/')
                            .next_back()
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| document.name.clone());
                        Ok((doc_id, Some(document)))
                    }),
                    Err(err) => {
                        error!(%err, "Error occurred while reading from cache.");
                        None
                    }
                }
            }
        })))
    }

    /// Updates or inserts a document in the cache.
    ///
    /// The document's full path is typically derived from `document.name`.
    ///
    /// # Arguments
    /// * `document`: The [`FirestoreDocument`](crate::FirestoreDocument) to update/insert.
    ///
    /// # Returns
    /// A `FirestoreResult` indicating success or failure.
    async fn update_doc_by_path(&self, document: &FirestoreDocument) -> FirestoreResult<()>;

    /// Lists all documents in the cache for a given collection path.
    ///
    /// # Arguments
    /// * `collection_path`: The full path to the collection (e.g., "projects/P/databases/D/documents/C").
    ///
    /// # Returns
    /// A `FirestoreResult` containing a [`FirestoreCachedValue`]. If `UseCached`, it holds
    /// a stream of `FirestoreResult<FirestoreDocument>`. If `SkipCache`, the caller
    /// should fetch directly from Firestore.
    async fn list_all_docs<'b>(
        &self,
        collection_path: &str,
    ) -> FirestoreResult<FirestoreCachedValue<BoxStream<'b, FirestoreResult<FirestoreDocument>>>>;

    /// Queries documents in the cache for a given collection path and query parameters.
    ///
    /// The backend is responsible for applying the filters and ordering defined in `query`
    /// to its cached data.
    ///
    /// # Arguments
    /// * `collection_path`: The full path to the collection.
    /// * `query`: The [`FirestoreQueryParams`](crate::FirestoreQueryParams) to apply.
    ///
    /// # Returns
    /// A `FirestoreResult` containing a [`FirestoreCachedValue`]. If `UseCached`, it holds
    /// a stream of `FirestoreResult<FirestoreDocument>`. If `SkipCache`, the caller
    /// should fetch directly from Firestore.
    async fn query_docs<'b>(
        &self,
        collection_path: &str,
        query: &FirestoreQueryParams,
    ) -> FirestoreResult<FirestoreCachedValue<BoxStream<'b, FirestoreResult<FirestoreDocument>>>>;
}

#[cfg(test)]
mod target_change_tests {
    use super::*;
    use gcloud_sdk::google::firestore::v1::TargetChange;

    const DOCS: &str = "projects/test/databases/(default)/documents";

    fn config_with(collections: &[(&str, u32)]) -> FirestoreCacheConfiguration {
        collections.iter().fold(
            FirestoreCacheConfiguration::new(),
            |config, (name, target)| {
                config.add_collection_config_at(
                    DOCS,
                    FirestoreCacheCollectionConfiguration::new(
                        name,
                        FirestoreListenerTarget::new(*target),
                        FirestoreCacheCollectionLoadMode::PreloadAllDocs,
                    ),
                )
            },
        )
    }

    fn change(
        change_type: FirestoreListenerTargetChangeType,
        target_ids: Vec<i32>,
    ) -> TargetChange {
        TargetChange {
            target_change_type: change_type as i32,
            target_ids,
            ..Default::default()
        }
    }

    fn suspended(action: FirestoreCacheTargetChangeAction) -> Vec<String> {
        match action {
            FirestoreCacheTargetChangeAction::SuspendAndInvalidate(invalidations) => sorted(
                invalidations
                    .into_iter()
                    .map(|invalidation| invalidation.collection_path().to_string())
                    .collect(),
            ),
            other => panic!(
                "expected SuspendAndInvalidate, got {}",
                describe_action(&other)
            ),
        }
    }

    fn resumed(action: FirestoreCacheTargetChangeAction) -> Vec<String> {
        match action {
            FirestoreCacheTargetChangeAction::Resume(paths) => sorted(paths),
            other => panic!("expected Resume, got {}", describe_action(&other)),
        }
    }

    fn sorted(mut paths: Vec<String>) -> Vec<String> {
        paths.sort();
        paths
    }

    fn describe_action(action: &FirestoreCacheTargetChangeAction) -> &'static str {
        match action {
            FirestoreCacheTargetChangeAction::SuspendAndInvalidate(_) => "SuspendAndInvalidate",
            FirestoreCacheTargetChangeAction::Resume(_) => "Resume",
            FirestoreCacheTargetChangeAction::Ignore => "Ignore",
        }
    }

    #[test]
    fn reset_suspends_only_the_collections_of_the_named_targets() {
        let config = config_with(&[("a", 1000), ("b", 1001)]);

        let action = cache_target_change_action(
            &config,
            &change(FirestoreListenerTargetChangeType::Reset, vec![1000]),
        );

        assert_eq!(suspended(action), vec![format!("{DOCS}/a")]);
    }

    #[test]
    fn remove_suspends_like_reset_does() {
        let config = config_with(&[("a", 1000), ("b", 1001)]);

        let action = cache_target_change_action(
            &config,
            &change(FirestoreListenerTargetChangeType::Remove, vec![1001]),
        );

        assert_eq!(suspended(action), vec![format!("{DOCS}/b")]);
    }

    #[test]
    fn an_empty_target_id_set_means_every_target() {
        let config = config_with(&[("a", 1000), ("b", 1001)]);

        let action = cache_target_change_action(
            &config,
            &change(FirestoreListenerTargetChangeType::Reset, vec![]),
        );

        assert_eq!(
            suspended(action),
            vec![format!("{DOCS}/a"), format!("{DOCS}/b")]
        );
    }

    #[test]
    fn current_resumes_the_collection() {
        let config = config_with(&[("a", 1000)]);

        let action = cache_target_change_action(
            &config,
            &change(FirestoreListenerTargetChangeType::Current, vec![1000]),
        );

        assert_eq!(resumed(action), vec![format!("{DOCS}/a")]);
    }

    #[test]
    fn keepalives_and_adds_do_nothing() {
        let config = config_with(&[("a", 1000)]);

        for change_type in [
            FirestoreListenerTargetChangeType::NoChange,
            FirestoreListenerTargetChangeType::Add,
        ] {
            assert!(matches!(
                cache_target_change_action(&config, &change(change_type, vec![1000])),
                FirestoreCacheTargetChangeAction::Ignore
            ));
        }
    }

    #[test]
    fn targets_belonging_to_another_listener_are_ignored() {
        let config = config_with(&[("a", 1000)]);

        let action = cache_target_change_action(
            &config,
            &change(FirestoreListenerTargetChangeType::Reset, vec![42]),
        );

        assert!(suspended(action).is_empty());
    }
}
