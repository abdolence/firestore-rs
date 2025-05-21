//! Provides caching capabilities for Firestore data.
//!
//! This module allows for caching Firestore documents and query results to reduce
//! latency and the number of reads to the Firestore database. It defines traits
//! for cache backends and provides a `FirestoreCache` struct that orchestrates
//! listening to Firestore changes and updating the cache.
//!
//! # Key Components
//! - [`FirestoreCache`]: The main struct for managing a cache. It uses a
//!   [`FirestoreListener`](crate::FirestoreListener) to receive real-time updates
//!   from Firestore and a [`FirestoreCacheBackend`] to store and retrieve cached data.
//! - [`FirestoreCacheBackend`]: A trait that defines the interface for different
//!   cache storage mechanisms (e.g., in-memory, persistent).
//! - [`FirestoreCacheOptions`]: Configuration options for the cache, such as its name
//!   and listener parameters.
//! - [`FirestoreCachedValue`]: An enum indicating whether a value was retrieved from
//!   the cache or if the cache should be skipped for a particular query.
//!
//! # Usage
//! To use the caching functionality, you typically:
//! 1. Implement the [`FirestoreCacheBackend`] trait for your chosen storage.
//! 2. Create a [`FirestoreDb`](crate::FirestoreDb) instance.
//! 3. Instantiate [`FirestoreCache`] with the database, backend, and a
//!    [`FirestoreResumeStateStorage`](crate::FirestoreResumeStateStorage) for the listener.
//! 4. Call [`FirestoreCache::load()`] to initialize the cache and start listening for updates.
//! 5. Use methods on the cache backend (e.g., `get_doc_by_path`, `query_docs`) to retrieve data.
//!    These methods might return cached data or indicate that the cache should be bypassed.
//!
//! The cache automatically updates in the background as changes occur in Firestore,
//! based on the targets added to its internal listener (often configured by the backend's `load` method).

use crate::*;
use std::sync::Arc;

mod options;
pub use options::*;

mod configuration;
pub use configuration::*;

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
    pub listener: FirestoreListener<FirestoreDb, LS>,
    /// A clone of the Firestore database client.
    pub db: FirestoreDb,
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
        Self::with_options(options, db, backend, listener_storage).await
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
    pub async fn with_options(
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
                listener,
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
    pub async fn load(&mut self) -> Result<(), FirestoreError> {
        let backend_target_params = self
            .inner
            .backend
            .load(&self.inner.options, &self.inner.db)
            .await?;

        for target_params in backend_target_params {
            self.inner.listener.add_target(target_params)?;
        }

        let backend = self.inner.backend.clone();
        self.inner
            .listener
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
    pub async fn shutdown(&mut self) -> Result<(), FirestoreError> {
        self.inner.listener.shutdown().await?;
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
                            .last()
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
