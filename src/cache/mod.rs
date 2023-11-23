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

pub struct FirestoreCache<B, LS>
where
    B: FirestoreCacheBackend + Send + Sync + 'static,
    LS: FirestoreResumeStateStorage,
{
    inner: FirestoreCacheInner<B, LS>,
}

struct FirestoreCacheInner<B, LS>
where
    B: FirestoreCacheBackend + Send + Sync + 'static,
    LS: FirestoreResumeStateStorage,
{
    pub options: FirestoreCacheOptions,
    pub backend: Arc<B>,
    pub listener: FirestoreListener<FirestoreDb, LS>,
    pub db: FirestoreDb,
}

pub enum FirestoreCachedValue<T> {
    UseCached(T),
    SkipCache,
}

impl<B, LS> FirestoreCache<B, LS>
where
    B: FirestoreCacheBackend + Send + Sync + 'static,
    LS: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
{
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
                        error!(?err, "Error occurred while updating cache.");
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

    pub async fn invalidate_all(&self) -> FirestoreResult<()> {
        self.inner.backend.invalidate_all().await
    }
}

#[async_trait]
pub trait FirestoreCacheBackend: FirestoreCacheDocsByPathSupport {
    async fn load(
        &self,
        options: &FirestoreCacheOptions,
        db: &FirestoreDb,
    ) -> Result<Vec<FirestoreListenerTargetParams>, FirestoreError>;

    async fn invalidate_all(&self) -> FirestoreResult<()>;

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

    async fn update_doc_by_path(&self, document: &FirestoreDocument) -> FirestoreResult<()>;

    async fn list_all_docs(
        &self,
        collection_path: &str,
    ) -> FirestoreResult<FirestoreCachedValue<BoxStream<FirestoreResult<FirestoreDocument>>>>;

    async fn query_docs(
        &self,
        collection_path: &str,
        query: &FirestoreQueryParams,
    ) -> FirestoreResult<FirestoreCachedValue<BoxStream<FirestoreResult<FirestoreDocument>>>>;
}
