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

pub struct FirestoreCache {
    inner: FirestoreCacheInner,
}

struct FirestoreCacheInner {
    pub options: FirestoreCacheOptions,
    pub config: FirestoreCacheConfiguration,
    pub backend: Arc<Box<dyn FirestoreCacheBackend + Send + Sync + 'static>>,
    pub listener: FirestoreListener<FirestoreDb, FirestoreTempFilesListenStateStorage>,
}

pub enum FirestoreCachedValue<T> {
    UseCached(T),
    SkipCache,
}

impl FirestoreCache {
    pub async fn new<B>(
        name: FirestoreCacheName,
        config: FirestoreCacheConfiguration,
        backend: B,
        db: &FirestoreDb,
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
        Self::with_options(options, config, backend, db).await
    }

    pub async fn with_options<B>(
        options: FirestoreCacheOptions,
        config: FirestoreCacheConfiguration,
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
                config,
                backend: Arc::new(Box::new(backend)),
                listener,
            },
        })
    }

    pub async fn load(&mut self, db: &FirestoreDb) -> Result<(), FirestoreError> {
        let backend_target_params = self
            .inner
            .backend
            .load(&self.inner.options, &self.inner.config, db)
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

    #[inline]
    pub fn enabled_for_collection(&self, collection_name: &str) -> bool {
        self.inner.config.collections.contains_key(collection_name)
    }
}

#[async_trait]
pub trait FirestoreCacheBackend: FirestoreCacheDocsByPathSupport {
    async fn load(
        &self,
        options: &FirestoreCacheOptions,
        config: &FirestoreCacheConfiguration,
        db: &FirestoreDb,
    ) -> Result<Vec<FirestoreListenerTargetParams>, FirestoreError>;

    async fn shutdown(&self) -> FirestoreResult<()>;

    async fn on_listen_event(&self, event: FirestoreListenEvent) -> FirestoreResult<()>;
}

#[async_trait]
pub trait FirestoreCacheDocsByPathSupport {
    async fn get_doc_by_path(
        &self,
        document_path: &str,
    ) -> FirestoreResult<Option<FirestoreDocument>>;

    async fn get_docs_by_paths<'a>(
        &'a self,
        full_doc_ids: &'a [String],
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<FirestoreDocument>)>>>
    where
        Self: Sync,
    {
        Ok(Box::pin(
            futures::stream::iter(full_doc_ids.clone()).filter_map({
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
                            error!("[DB]: Error occurred while reading from cache: {}", err);
                            None
                        }
                    }
                }
            }),
        ))
    }

    async fn update_doc_by_path(&self, document: &FirestoreDocument) -> FirestoreResult<()>;
}

#[async_trait]
impl FirestoreCacheDocsByPathSupport for FirestoreCache {
    async fn get_doc_by_path(
        &self,
        document_path: &str,
    ) -> FirestoreResult<Option<FirestoreDocument>> {
        self.inner.backend.get_doc_by_path(document_path).await
    }

    async fn get_docs_by_paths<'a>(
        &'a self,
        full_doc_ids: &'a [String],
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<FirestoreDocument>)>>> {
        self.inner.backend.get_docs_by_paths(full_doc_ids).await
    }

    async fn update_doc_by_path(&self, document: &FirestoreDocument) -> FirestoreResult<()> {
        self.inner.backend.update_doc_by_path(document).await
    }
}
