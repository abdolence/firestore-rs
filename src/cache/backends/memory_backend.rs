use crate::errors::*;
use crate::*;
use async_trait::async_trait;
use chrono::Utc;
use futures::stream::BoxStream;
use moka::future::{Cache, CacheBuilder};

use crate::cache::cache_query_engine::FirestoreCacheQueryEngine;
use futures::TryStreamExt;
use futures::{future, StreamExt};
use std::collections::HashMap;
use tracing::*;

pub type FirestoreMemCache = Cache<String, FirestoreDocument>;

pub type FirestoreMemCacheOptions = CacheBuilder<String, FirestoreDocument, FirestoreMemCache>;

pub struct FirestoreMemoryCacheBackend {
    pub config: FirestoreCacheConfiguration,
    collection_caches: HashMap<String, FirestoreMemCache>,
}

const FIRESTORE_MEMORY_CACHE_DEFAULT_MAX_CAPACITY: u64 = 50000;

impl FirestoreMemoryCacheBackend {
    pub fn new(config: FirestoreCacheConfiguration) -> FirestoreResult<Self> {
        Self::with_max_capacity(config, FIRESTORE_MEMORY_CACHE_DEFAULT_MAX_CAPACITY)
    }

    pub fn with_max_capacity(
        config: FirestoreCacheConfiguration,
        max_capacity: u64,
    ) -> FirestoreResult<Self> {
        Self::with_collection_options(config, |_| {
            FirestoreMemCache::builder().max_capacity(max_capacity)
        })
    }

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
                        debug!("Preloading {}", collection_path.as_str());

                        let params = if let Some(parent) = &config.parent {
                            db.fluent()
                                .list()
                                .from(&config.collection_name)
                                .parent(parent)
                        } else {
                            db.fluent().list().from(&config.collection_name)
                        };

                        let stream = params.page_size(1000).stream_all_with_errors().await?;

                        stream
                            .try_for_each_concurrent(2, |doc| async move {
                                let (_, document_id) = split_document_path(&doc.name);
                                mem_cache.insert(document_id.to_string(), doc).await;
                                Ok(())
                            })
                            .await?;

                        mem_cache.run_pending_tasks().await;

                        info!(
                            "Preloading collection `{}` has been finished. Loaded: {} entries",
                            collection_path.as_str(),
                            mem_cache.entry_count()
                        );
                    }
                }
                FirestoreCacheCollectionLoadMode::PreloadNone => {}
            }
        }
        Ok(())
    }

    async fn query_cached_docs(
        &self,
        collection_path: &str,
        query_engine: FirestoreCacheQueryEngine,
    ) -> FirestoreResult<BoxStream<FirestoreResult<FirestoreDocument>>> {
        match self.collection_caches.get(collection_path) {
            Some(mem_cache) => {
                let filtered_stream = Box::pin(
                    futures::stream::unfold(
                        (query_engine.clone(), mem_cache.iter()),
                        |(query_engine, mut iter)| async move {
                            match iter.next() {
                                Some((_, doc)) => {
                                    if query_engine.matches_doc(&doc) {
                                        Some((Ok(Some(doc)), (query_engine, iter)))
                                    } else {
                                        Some((Ok(None), (query_engine, iter)))
                                    }
                                }
                                None => None,
                            }
                        },
                    )
                    .filter_map(|doc_res| {
                        future::ready(match doc_res {
                            Ok(Some(doc)) => Some(Ok(doc)),
                            Ok(None) => None,
                            Err(err) => Some(Err(err)),
                        })
                    }),
                );

                let output_stream = query_engine.process_query_stream(filtered_stream).await?;

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
        let read_from_time = Utc::now();

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
            debug!("Invalidating cache for {}", collection_path);
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
                            "Writing document to cache due to listener event: {:?}",
                            doc.name
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
                        "Removing document from cache due to listener event: {:?}",
                        doc_deleted.document.as_str()
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

    async fn list_all_docs(
        &self,
        collection_path: &str,
    ) -> FirestoreResult<BoxStream<FirestoreResult<FirestoreDocument>>> {
        match self.collection_caches.get(collection_path) {
            Some(mem_cache) => Ok(Box::pin(futures::stream::iter(
                mem_cache.iter().map(|(_, doc)| Ok(doc)),
            ))),
            None => Ok(Box::pin(futures::stream::empty())),
        }
    }

    async fn query_docs(
        &self,
        collection_path: &str,
        query: &FirestoreQueryParams,
    ) -> FirestoreResult<FirestoreCachedValue<BoxStream<FirestoreResult<FirestoreDocument>>>> {
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
