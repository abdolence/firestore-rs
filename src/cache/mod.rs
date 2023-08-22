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

pub struct FirestoreCache<B>
where
    B: FirestoreCacheBackend + Send + Sync + 'static,
{
    inner: FirestoreCacheInner<B>,
}

struct FirestoreCacheInner<B>
where
    B: FirestoreCacheBackend + Send + Sync + 'static,
{
    pub options: FirestoreCacheOptions,
    pub backend: Arc<B>,
    pub listener: FirestoreListener<FirestoreDb, FirestoreTempFilesListenStateStorage>,
    pub db: FirestoreDb,
}

pub enum FirestoreCachedValue<T> {
    UseCached(T),
    SkipCache,
}

impl<B> FirestoreCache<B>
where
    B: FirestoreCacheBackend + Send + Sync + 'static,
{
    pub async fn new(
        name: FirestoreCacheName,
        db: &FirestoreDb,
        backend: B,
    ) -> FirestoreResult<Self>
    where
        B: FirestoreCacheBackend + Send + Sync + 'static,
    {
        let temp_dir = std::env::temp_dir();
        let firestore_cache_dir = temp_dir.join("firestore_cache");
        let cache_dir = firestore_cache_dir.join(name.value().clone());
        if !cache_dir.exists() {
            debug!(
                "Creating a temp directory to store listener state: {}",
                cache_dir.display()
            );
            std::fs::create_dir_all(&cache_dir)?;
        } else {
            debug!(
                "Using a temp directory to store listener state: {}",
                cache_dir.display()
            );
        }

        let options = FirestoreCacheOptions::new(name, cache_dir);
        Self::with_options(options, backend, db).await
    }

    pub async fn with_options(
        options: FirestoreCacheOptions,
        backend: B,
        db: &FirestoreDb,
    ) -> FirestoreResult<Self>
    where
        B: FirestoreCacheBackend + Send + Sync + 'static,
    {
        let listener_storage =
            FirestoreTempFilesListenStateStorage::with_temp_dir(options.cache_dir.clone());

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

    pub fn name(&self) -> &FirestoreCacheName {
        &self.inner.options.name
    }

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
                        error!("Error occurred while updating cache: {}", err);
                    };
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), FirestoreError> {
        self.inner.listener.shutdown().await?;
        self.inner.backend.shutdown().await?;
        Ok(())
    }

    pub fn backend(&self) -> Arc<B> {
        self.inner.backend.clone()
    }
}

#[async_trait]
pub trait FirestoreCacheBackend: FirestoreCacheDocsByPathSupport {
    async fn load(
        &self,
        options: &FirestoreCacheOptions,
        db: &FirestoreDb,
    ) -> Result<Vec<FirestoreListenerTargetParams>, FirestoreError>;

    async fn shutdown(&self) -> FirestoreResult<()>;

    async fn on_listen_event(&self, event: FirestoreListenEvent) -> FirestoreResult<()>;
}

#[async_trait]
pub trait FirestoreCacheDocsByPathSupport {
    async fn get_doc_by_path(
        &self,
        collection_id: &str,
        document_path: &str,
    ) -> FirestoreResult<Option<FirestoreDocument>>;

    async fn get_docs_by_paths<'a>(
        &'a self,
        collection_id: &str,
        full_doc_ids: &'a [String],
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<FirestoreDocument>)>>>
    where
        Self: Sync,
    {
        let collection_id = collection_id.to_string();
        Ok(Box::pin(
            futures::stream::iter(full_doc_ids.clone()).filter_map({
                move |document_path| {
                    let collection_id = collection_id.to_string();
                    async move {
                        match self
                            .get_doc_by_path(collection_id.as_str(), document_path.as_str())
                            .await
                        {
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
                                error!("Error occurred while reading from cache: {}", err);
                                None
                            }
                        }
                    }
                }
            }),
        ))
    }

    async fn update_doc_by_path(
        &self,
        collection_id: &str,
        document: &FirestoreDocument,
    ) -> FirestoreResult<()>;

    async fn list_all_docs(
        &self,
        collection_id: &str,
    ) -> FirestoreResult<BoxStream<FirestoreResult<FirestoreDocument>>>;
}
