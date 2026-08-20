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
//! delivered, so a write may take a moment to appear, and a stalled or reset listener can leave
//! the cache stale without saying so. Cached listings are never *partial* by construction, but
//! they are not guaranteed to be current. Do not cache data that must be read at strong
//! consistency - read that through Firestore directly, or inside a transaction.
//!
//! # Lifecycle
//!
//! [`FirestoreCacheBuilder::build`] creates the cache, preloads it and starts the listener. Call
//! [`FirestoreCache::shutdown`] when you are done. Because `load` and `shutdown` take `&self`, a
//! built cache can be shared as `Arc<FirestoreMemoryCache>` in your application state.
//!
//! # Custom backends
//!
//! To store the cache somewhere else, implement [`FirestoreCacheBackend`] and its supertrait
//! [`FirestoreCacheDocsByPathSupport`], then construct [`FirestoreCache`] with it.

use crate::errors::{FirestoreCacheError, FirestoreErrorPublicGenericDetails};
use crate::*;
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

        Ok(Self {
            inner: FirestoreCacheInner {
                options,
                backend: Arc::new(backend),
                listener: tokio::sync::Mutex::new(listener),
                db: db.clone(),
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

        let backend = self.inner.backend.clone();
        listener
            .start(move |event| {
                let backend = backend.clone();
                async move {
                    if let Err(err) = backend.on_listen_event(event).await {
                        error!(?err, "Error occurred while updating cache.");
                    };
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    /// Shuts down the Firestore listener and the cache backend.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    pub async fn shutdown(&self) -> Result<(), FirestoreError> {
        self.inner.listener.lock().await.shutdown().await?;
        self.inner.backend.shutdown().await?;
        Ok(())
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
