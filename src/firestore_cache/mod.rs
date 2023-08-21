use crate::*;
use std::sync::atomic::AtomicU32;

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
    pub backend: Box<dyn FirestoreCacheBackend + Send + Sync + 'static>,
    pub current_target_id: AtomicU32,
}

impl FirestoreCache {
    pub fn new<B>(name: FirestoreCacheName, config: FirestoreCacheConfiguration, backend: B) -> Self
    where
        B: FirestoreCacheBackend + Send + Sync + 'static,
    {
        let temp_dir = std::env::temp_dir();
        let firestore_cache_dir = temp_dir.join("firestore_cache").join(name.value().clone());

        let options = FirestoreCacheOptions::new(name, firestore_cache_dir);
        Self::with_options(options, config, backend)
    }

    pub fn with_options<B>(
        options: FirestoreCacheOptions,
        config: FirestoreCacheConfiguration,
        backend: B,
    ) -> Self
    where
        B: FirestoreCacheBackend + Send + Sync + 'static,
    {
        let initial_target_id = *options.allocated_target_id_range.start().value();
        Self {
            inner: FirestoreCacheInner {
                options,
                config,
                backend: Box::new(backend),
                current_target_id: AtomicU32::new(initial_target_id),
            },
        }
    }

    pub async fn load(&mut self) -> Result<(), FirestoreError> {
        let allocated_target_id: FirestoreListenerTarget = self.allocate_new_target_id();

        self.inner
            .backend
            .load(&self.inner.options, &self.inner.config, allocated_target_id)
            .await?;
        Ok(())
    }

    fn allocate_new_target_id(&self) -> FirestoreListenerTarget {
        self.inner
            .current_target_id
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |current_value| {
                    if current_value < *self.inner.options.allocated_target_id_range.end().value() {
                        Some(current_value + 1)
                    } else {
                        Some(*self.inner.options.allocated_target_id_range.start().value())
                    }
                },
            )
            .unwrap_or_else(|_| *self.inner.options.allocated_target_id_range.start().value())
            .into()
    }

    pub async fn shutdown(&mut self) -> Result<(), FirestoreError> {
        self.inner.backend.shutdown().await?;
        Ok(())
    }

    #[inline]
    pub fn enabled_for_collection(&self, collection_name: &str) -> bool {
        self.inner
            .config
            .collections
            .contains_key(collection_name.into())
    }
}

#[async_trait]
pub trait FirestoreCacheBackend: FirestoreCacheDocsByPathSupport {
    async fn load(
        &mut self,
        options: &FirestoreCacheOptions,
        config: &FirestoreCacheConfiguration,
        allocated_target_id: FirestoreListenerTarget,
    ) -> Result<Vec<FirestoreListenerTargetParams>, FirestoreError>;

    async fn shutdown(&mut self) -> Result<(), FirestoreError>;
}

#[async_trait]
pub trait FirestoreCacheDocsByPathSupport {
    async fn get_doc_by_path(
        &self,
        collection_id: &str,
        document_path: &str,
        return_only_fields: &Option<Vec<String>>,
    ) -> FirestoreResult<Option<FirestoreDocument>>;

    async fn get_docs_by_paths<'a>(
        &'a self,
        collection_id: &'a str,
        full_doc_ids: &'a Vec<String>,
        return_only_fields: &'a Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<FirestoreDocument>)>>>
    where
        Self: Sync,
    {
        Ok(Box::pin(
            futures::stream::iter(full_doc_ids.clone()).filter_map({
                move |document_path| {
                    let return_only_fields = return_only_fields.clone();
                    let collection_id = collection_id.to_string();
                    async move {
                        match self
                            .get_doc_by_path(
                                collection_id.as_str(),
                                document_path.as_str(),
                                &return_only_fields,
                            )
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
                                error!("[DB]: Error occurred while reading from cache: {}", err);
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
}

#[async_trait]
impl FirestoreCacheDocsByPathSupport for FirestoreCache {
    async fn get_doc_by_path(
        &self,
        collection_id: &str,
        document_path: &str,
        return_only_fields: &Option<Vec<String>>,
    ) -> FirestoreResult<Option<FirestoreDocument>> {
        self.inner
            .backend
            .get_doc_by_path(collection_id, document_path, return_only_fields)
            .await
    }

    async fn get_docs_by_paths<'a>(
        &'a self,
        collection_id: &'a str,
        full_doc_ids: &'a Vec<String>,
        return_only_fields: &'a Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<FirestoreDocument>)>>> {
        self.inner
            .backend
            .get_docs_by_paths(collection_id, full_doc_ids, return_only_fields)
            .await
    }

    async fn update_doc_by_path(
        &self,
        collection_id: &str,
        document: &FirestoreDocument,
    ) -> FirestoreResult<()> {
        self.inner
            .backend
            .update_doc_by_path(collection_id, document)
            .await
    }
}
