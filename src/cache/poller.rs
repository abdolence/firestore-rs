use crate::*;
use async_trait::async_trait;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::*;

/// What a poller needs from the backend of its cache.
///
/// Public inside a private module, so that only this crate's backends can implement it, and the
/// builder of a cache, which knows its backend is one of them, hands it to the poller.
#[async_trait]
pub trait FirestoreCachePolledBackend: FirestoreCacheBackend {
    /// Loads the backend as [`FirestoreCacheBackend::load`] does, and returns the collections whose
    /// documents the load kept from an earlier run without downloading them.
    async fn load_for_polling(
        &self,
        options: &FirestoreCacheOptions,
        db: &FirestoreDb,
    ) -> FirestoreResult<Vec<String>>;

    /// Reads one cached collection from Firestore again and brings the backend's copy in line with
    /// it: a collection watched by document ID reads those documents, any other preloaded one all
    /// its documents, removing those the read did not return, and any other one the documents the
    /// backend holds.
    ///
    /// A document present before and after stays readable throughout. A document is stored only
    /// over an earlier version or none, and removed only while the backend still holds the version
    /// it held before the read, so that a version a read through the cache stored in the meantime
    /// survives. On an error the backend keeps what it holds. A collection the backend does not
    /// cache is left alone.
    async fn poll_collection(&self, db: &FirestoreDb, collection_path: &str)
        -> FirestoreResult<()>;
}

/// Keeps a cache in [`FirestoreCacheSyncMode::Poll`] current with one background task, which
/// reads the cache's collections one after another, however many there are.
pub(crate) struct FirestoreCachePoller {
    cache_name: FirestoreCacheName,
    interval: Duration,
    backend: Arc<dyn FirestoreCachePolledBackend + Send + Sync>,
    /// The task stops once this is set, or once the poller is dropped and the sender with it, so
    /// that a cache dropped without `shutdown` does not go on reading Firestore. A load or an
    /// added collection in progress stops at it too.
    shutdown_signal: watch::Sender<bool>,
    /// Held while a load or an added collection is in progress, which `shutdown` waits for before
    /// the backend is released.
    state: tokio::sync::Mutex<FirestoreCachePollerState>,
}

enum FirestoreCachePollerState {
    Created,
    /// Kept until the task has been joined: a `shutdown` cancelled while it waits leaves the task
    /// to the next one, rather than letting the backend be released under a running task.
    Polling(JoinHandle<()>),
    ShutDown,
}

impl FirestoreCachePoller {
    /// `params.interval` is not zero: the builder refuses that before it creates the backend.
    pub(crate) fn new(
        cache_name: FirestoreCacheName,
        params: &FirestoreCachePollParams,
        backend: Arc<dyn FirestoreCachePolledBackend + Send + Sync>,
    ) -> Self {
        Self {
            cache_name,
            interval: params.interval,
            backend,
            shutdown_signal: watch::Sender::new(false),
            state: tokio::sync::Mutex::new(FirestoreCachePollerState::Created),
        }
    }

    /// Loads the backend, then starts polling. A cache is loaded once: a second load, or a load
    /// after `shutdown`, fails before the backend reads anything. A load that is cancelled or
    /// fails leaves the cache to be loaded again.
    ///
    /// The collections whose documents the load kept from an earlier run are polled at once, and
    /// every collection one interval later and every interval after that.
    pub(crate) async fn load(
        &self,
        db: &FirestoreDb,
        options: &FirestoreCacheOptions,
    ) -> FirestoreResult<()> {
        let mut state = self.state.lock().await;
        match *state {
            FirestoreCachePollerState::Created => {}
            FirestoreCachePollerState::Polling(_) => {
                return Err(FirestoreError::invalid_parameters(
                    "cache",
                    "This cache has already been loaded.",
                ))
            }
            FirestoreCachePollerState::ShutDown => return Err(FirestoreError::cache_shut_down()),
        }

        let kept_collections = self
            .until_shut_down(self.backend.load_for_polling(options, db))
            .await?;

        let rounds = FirestoreCachePollRounds {
            cache_name: self.cache_name.clone(),
            interval: self.interval,
            backend: self.backend.clone(),
            db: db.clone(),
        };
        let mut shutdown = self.shutdown_signal.subscribe();
        *state = FirestoreCachePollerState::Polling(tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.wait_for(|stopped| *stopped) => {}
                () = rounds.run(kept_collections) => {}
            }
        }));
        Ok(())
    }

    /// Runs `add`, which adds a collection to the backend, once polling is found running or yet to
    /// start, so that a collection is never downloaded for a cache that nothing keeps current.
    pub(crate) async fn add_collection<T>(
        &self,
        add: impl Future<Output = FirestoreResult<T>>,
    ) -> FirestoreResult<T> {
        let state = self.state.lock().await;
        match &*state {
            FirestoreCachePollerState::ShutDown => Err(FirestoreError::cache_shut_down()),
            FirestoreCachePollerState::Polling(task) if task.is_finished() => Err(
                FirestoreError::system_error("The poller of this cache has stopped."),
            ),
            _ => self.until_shut_down(add).await,
        }
    }

    /// Drops `operation` where it stands once `shutdown` begins.
    async fn until_shut_down<T>(
        &self,
        operation: impl Future<Output = FirestoreResult<T>>,
    ) -> FirestoreResult<T> {
        let mut shutdown = self.shutdown_signal.subscribe();
        tokio::select! {
            result = operation => result,
            _ = shutdown.wait_for(|stopped| *stopped) => Err(FirestoreError::cache_shut_down()),
        }
    }

    /// Stops polling, and a load or an added collection in progress, and returns once all of them
    /// have. Returns a [`FirestoreError::SystemError`] if the task panicked.
    pub(crate) async fn shutdown(&self) -> FirestoreResult<()> {
        self.shutdown_signal.send_replace(true);
        let mut state = self.state.lock().await;
        let joined = match &mut *state {
            FirestoreCachePollerState::Polling(task) => task.await.map_err(|err| {
                FirestoreError::system_error(format!("The cache poller panicked: {err}"))
            }),
            _ => Ok(()),
        };
        *state = FirestoreCachePollerState::ShutDown;
        joined
    }
}

struct FirestoreCachePollRounds {
    cache_name: FirestoreCacheName,
    interval: Duration,
    backend: Arc<dyn FirestoreCachePolledBackend + Send + Sync>,
    db: FirestoreDb,
}

impl FirestoreCachePollRounds {
    async fn run(self, kept_collections: Vec<String>) {
        for collection_path in &kept_collections {
            self.poll(collection_path).await;
        }
        loop {
            tokio::time::sleep(self.interval).await;
            self.poll_round().await;
        }
    }

    /// Takes the collections from the backend's configuration as the round starts, so that one
    /// added or removed since the last round is polled or left alone without the poller being
    /// told.
    async fn poll_round(&self) {
        let config = self.backend.cache_configuration();
        debug!(
            cache = self.cache_name.value(),
            collections = config.collections.len(),
            "Polling the cached collections."
        );
        for collection_path in config.collections.keys() {
            self.poll(collection_path).await;
        }
    }

    /// A collection that fails keeps what it holds, and the next round reads it again.
    async fn poll(&self, collection_path: &str) {
        if let Err(error) = self
            .backend
            .poll_collection(&self.db, collection_path)
            .await
        {
            error!(
                ?error,
                cache = self.cache_name.value(),
                collection_path,
                "Could not poll a cached collection. It keeps the documents it holds until a \
                 later round succeeds."
            );
        }
    }
}

#[cfg(all(test, feature = "caching-memory"))]
mod tests {
    use crate::db::fake_firestore::{
        batch_get_found_message, batch_get_missing_message, run_query_message, FakeFirestore,
        FakeResponse,
    };
    use crate::*;
    use gcloud_sdk::google::firestore::v1::{
        run_query_request, value::ValueType, BatchGetDocumentsRequest, RunQueryRequest, Value,
    };
    use gcloud_sdk::prost::Message;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    const FAKE_DOCS: &str = "projects/fake-firestore/databases/(default)/documents";
    const POLL_INTERVAL: Duration = Duration::from_millis(20);

    fn document(collection: &str, id: &str, version: i64) -> FirestoreDocument {
        FirestoreDocument {
            name: format!("{FAKE_DOCS}/{collection}/{id}"),
            fields: HashMap::from([(
                "version".to_string(),
                Value {
                    value_type: Some(ValueType::IntegerValue(version)),
                },
            )]),
            update_time: Some(gcloud_sdk::prost_types::Timestamp {
                seconds: version,
                nanos: 0,
            }),
            ..Default::default()
        }
    }

    /// A server holding the documents `documents_at(n)` returns while answering its `n`th read,
    /// counting from zero. A query answers the documents of its collection, and a batch get each
    /// named document, found or missing. Calls are logged as `RunQuery <collection>`,
    /// `BatchGet <collection>: <sorted ids>` and `Listen`, with ` at a read time` after a read
    /// that names one.
    async fn server_holding(
        documents_at: impl Fn(usize) -> Vec<FirestoreDocument> + Send + Sync + 'static,
    ) -> FakeFirestore {
        let reads = AtomicUsize::new(0);
        FakeFirestore::start(move |method, request| {
            if method.ends_with("/Listen") {
                return ("Listen".to_string(), FakeResponse::empty());
            }
            let documents = documents_at(reads.fetch_add(1, Ordering::SeqCst));
            if method.ends_with("/RunQuery") {
                let request = RunQueryRequest::decode(request).expect("query request");
                let Some(run_query_request::QueryType::StructuredQuery(query)) = request.query_type
                else {
                    panic!("a structured query");
                };
                let collection = &query.from[0].collection_id;
                let collection_path = format!("{}/{collection}", request.parent);
                let read_time = read_time_note(request.consistency_selector.is_some());
                let messages = documents
                    .into_iter()
                    .filter(|doc| split_document_path(&doc.name).0 == collection_path)
                    .map(run_query_message)
                    .collect();
                (
                    format!("RunQuery {collection}{read_time}"),
                    FakeResponse::Paced(messages, Duration::ZERO),
                )
            } else if method.ends_with("/BatchGetDocuments") {
                let request = BatchGetDocumentsRequest::decode(request).expect("batch request");
                let mut ids: Vec<&str> = request
                    .documents
                    .iter()
                    .map(|name| split_document_path(name).1)
                    .collect();
                ids.sort();
                let collection =
                    split_document_path(split_document_path(&request.documents[0]).0).1;
                let read_time = read_time_note(request.consistency_selector.is_some());
                let messages = request
                    .documents
                    .iter()
                    .map(
                        |name| match documents.iter().find(|doc| &doc.name == name) {
                            Some(doc) => batch_get_found_message(doc.clone()),
                            None => batch_get_missing_message(name.clone()),
                        },
                    )
                    .collect();
                (
                    format!("BatchGet {collection}: {}{read_time}", ids.join(", ")),
                    FakeResponse::Paced(messages, Duration::ZERO),
                )
            } else {
                panic!("a cache that polls makes no {method} call");
            }
        })
        .await
    }

    fn read_time_note(at_read_time: bool) -> &'static str {
        if at_read_time {
            " at a read time"
        } else {
            ""
        }
    }

    async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .expect("the cache polled in time")
    }

    async fn cached(
        cache: &FirestoreMemoryCache,
        collection: &str,
        id: &str,
    ) -> Option<FirestoreDocument> {
        cache
            .backend()
            .get_doc_by_path(&format!("{FAKE_DOCS}/{collection}/{id}"))
            .await
            .expect("cache lookup")
    }

    #[tokio::test]
    async fn a_poll_stores_changed_documents_and_evicts_deleted_ones() {
        let server = server_holding(|read| match read {
            0 => vec![document("a", "kept", 1), document("a", "deleted", 1)],
            _ => vec![document("a", "kept", 2), document("a", "created", 1)],
        })
        .await;
        let cache = FirestoreCache::memory(&server.db)
            .preloaded_collection("a")
            .poll_changes(POLL_INTERVAL)
            .build()
            .await
            .expect("cache");

        // The preload, then the first round, then the start of the second one.
        within(server.wait_for_calls(3)).await;

        assert_eq!(
            cached(&cache, "a", "kept").await,
            Some(document("a", "kept", 2))
        );
        assert_eq!(
            cached(&cache, "a", "created").await,
            Some(document("a", "created", 1))
        );
        assert_eq!(cached(&cache, "a", "deleted").await, None);
        cache.shutdown().await.expect("shutdown");
        // A listen stream is logged once the client has closed it, which shutdown does.
        assert!(!server.calls().iter().any(|call| call == "Listen"));
    }

    #[tokio::test]
    async fn watched_and_lazy_collections_poll_only_the_documents_they_hold() {
        let server = server_holding(|_| {
            vec![
                document("configs", "site", 2),
                document("users", "read", 2),
                document("users", "never_read", 1),
            ]
        })
        .await;
        let cache = FirestoreCache::memory(&server.db)
            .collection_with("configs", |c| c.documents(["site", "billing"]))
            .collection("users")
            .poll_changes(POLL_INTERVAL)
            .build_without_load()
            .await
            .expect("cache");
        for id in ["read", "deleted"] {
            cache
                .backend()
                .update_doc_by_path(&document("users", id, 1))
                .await
                .expect("a document read through the cache");
        }
        cache.load().await.expect("loaded");

        // A round of two batch gets, then the start of the next.
        within(server.wait_for_calls(3)).await;

        let mut first_round = server.calls()[0..2].to_vec();
        first_round.sort();
        assert_eq!(
            first_round,
            vec![
                "BatchGet configs: billing, site",
                "BatchGet users: deleted, read"
            ]
        );
        assert_eq!(
            cached(&cache, "configs", "site").await,
            Some(document("configs", "site", 2))
        );
        assert_eq!(cached(&cache, "configs", "billing").await, None);
        assert_eq!(
            cached(&cache, "users", "read").await,
            Some(document("users", "read", 2))
        );
        assert_eq!(cached(&cache, "users", "deleted").await, None);
        assert_eq!(cached(&cache, "users", "never_read").await, None);
        cache.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn shutdown_stops_polling() {
        let server = server_holding(|_| Vec::new()).await;
        let cache = FirestoreCache::memory(&server.db)
            .preloaded_collection("a")
            .poll_changes(POLL_INTERVAL)
            .build()
            .await
            .expect("cache");
        within(server.wait_for_calls(2)).await;

        cache.shutdown().await.expect("shutdown");

        assert_no_calls_after_settling(&server).await;
    }

    /// Lets a call that was already on its way land in the log first, so that only a poller still
    /// running adds calls afterwards.
    async fn assert_no_calls_after_settling(server: &FakeFirestore) {
        tokio::time::sleep(POLL_INTERVAL * 5).await;
        let settled_calls = server.calls().len();
        tokio::time::sleep(POLL_INTERVAL * 10).await;

        assert_eq!(server.calls().len(), settled_calls);
    }

    #[tokio::test]
    async fn dropping_the_cache_stops_polling() {
        let server = server_holding(|_| Vec::new()).await;
        let cache = FirestoreCache::memory(&server.db)
            .preloaded_collection("a")
            .poll_changes(POLL_INTERVAL)
            .build()
            .await
            .expect("cache");
        within(server.wait_for_calls(2)).await;

        drop(cache);

        assert_no_calls_after_settling(&server).await;
    }

    #[tokio::test]
    async fn a_collection_added_at_runtime_is_polled() {
        let server = server_holding(|read| match read {
            0 => vec![document("b", "one", 1)],
            _ => vec![document("b", "one", 2)],
        })
        .await;
        let cache = FirestoreCache::memory(&server.db)
            .poll_changes(POLL_INTERVAL)
            .build()
            .await
            .expect("cache");

        cache
            .add_collection(FirestoreCacheCollection::new("b").preload_all())
            .await
            .expect("added");
        within(server.wait_for_calls(3)).await;

        assert_eq!(
            cached(&cache, "b", "one").await,
            Some(document("b", "one", 2))
        );
        cache.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn polls_read_the_latest_state_whatever_the_handle_reads() {
        let server = server_holding(|_| Vec::new()).await;
        let reading_at_a_read_time =
            server
                .db
                .clone_with_consistency_selector(FirestoreConsistencySelector::ReadTime(
                    FirestoreInstant::now(),
                ));
        let cache = FirestoreCache::memory(&reading_at_a_read_time)
            .preloaded_collection("a")
            .poll_changes(POLL_INTERVAL)
            .build()
            .await
            .expect("cache");

        within(server.wait_for_calls(2)).await;

        assert_eq!(server.calls()[0..2], ["RunQuery a", "RunQuery a"]);
        cache.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn a_second_load_is_refused_before_it_reads_firestore() {
        let server = server_holding(|_| Vec::new()).await;
        let cache = FirestoreCache::memory(&server.db)
            .preloaded_collection("a")
            .poll_changes(Duration::from_secs(3600))
            .build()
            .await
            .expect("cache");

        let error = cache.load().await.unwrap_err();

        assert!(matches!(error, FirestoreError::InvalidParametersError(_)));
        assert_eq!(server.calls(), vec!["RunQuery a"]);
        cache.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn adding_a_collection_once_polling_has_stopped_fails_and_keeps_nothing() {
        let server = server_holding(|_| Vec::new()).await;
        let cache = FirestoreCache::memory(&server.db)
            .poll_changes(Duration::from_secs(3600))
            .build()
            .await
            .expect("cache");
        let crate::cache::FirestoreCacheSync::Poller(poller) = &cache.inner.sync else {
            panic!("the cache polls");
        };
        poller.shutdown().await.expect("poller stopped");

        let FirestoreError::CacheError(error) = cache
            .add_collection(FirestoreCacheCollection::new("b").preload_all())
            .await
            .unwrap_err()
        else {
            panic!("the collection was not refused with a cache error");
        };

        assert_eq!(error.public.code, "CacheShutdown");
        assert!(server.calls().is_empty());
        assert!(cache.cached_collections().is_empty());
    }

    #[tokio::test]
    async fn listener_params_and_poll_changes_are_refused_together() {
        let server = server_holding(|_| Vec::new()).await;

        let error = FirestoreCache::memory(&server.db)
            .listener_params(FirestoreListenerParams::new())
            .poll_changes(POLL_INTERVAL)
            .build()
            .await
            .err()
            .expect("the two are refused");

        assert!(matches!(error, FirestoreError::InvalidParametersError(_)));
    }

    #[tokio::test]
    async fn the_deprecated_constructors_refuse_to_poll() {
        let server = server_holding(|_| Vec::new()).await;
        let options = FirestoreCacheOptions::new("custom".into()).with_sync_mode(
            FirestoreCacheSyncMode::Poll(FirestoreCachePollParams::new(POLL_INTERVAL)),
        );

        // The deprecated constructor is the subject of this test.
        #[allow(deprecated)]
        let refused = FirestoreCache::with_options(
            options,
            &server.db,
            FirestoreMemoryCacheBackend::new(FirestoreCacheConfiguration::new()).expect("backend"),
            FirestoreMemListenStateStorage::new(),
        )
        .await;

        assert!(matches!(
            refused.err(),
            Some(FirestoreError::InvalidParametersError(_))
        ));
    }

    /// A server whose query of `a` takes 200 ms to answer, so that a load of `a` is in progress
    /// for that long.
    async fn slowly_serving_a() -> FakeFirestore {
        FakeFirestore::start(|_, _| {
            (
                "RunQuery a".to_string(),
                FakeResponse::Paced(
                    vec![
                        run_query_message(document("a", "one", 1)),
                        run_query_message(document("a", "two", 1)),
                    ],
                    Duration::from_millis(200),
                ),
            )
        })
        .await
    }

    async fn unloaded_cache_of_a(server: &FakeFirestore) -> FirestoreMemoryCache {
        FirestoreCache::memory(&server.db)
            .preloaded_collection("a")
            .poll_changes(Duration::from_secs(3600))
            .build_without_load()
            .await
            .expect("cache")
    }

    #[tokio::test]
    async fn a_cancelled_load_leaves_the_cache_to_be_loaded_again() {
        let server = slowly_serving_a().await;
        let cache = unloaded_cache_of_a(&server).await;

        let cancelled = tokio::time::timeout(Duration::from_millis(50), cache.load()).await;

        assert!(cancelled.is_err());
        within(cache.load()).await.expect("loaded");
        cache.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn shutdown_returns_once_a_load_in_progress_has_stopped() {
        let server = slowly_serving_a().await;
        let cache = unloaded_cache_of_a(&server).await;
        let loading = std::sync::atomic::AtomicBool::new(true);

        let (loaded, loading_once_shut_down) = tokio::join!(
            async {
                let loaded = cache.load().await;
                loading.store(false, Ordering::SeqCst);
                loaded
            },
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                cache.shutdown().await.expect("shutdown");
                loading.load(Ordering::SeqCst)
            },
        );

        assert!(loaded.is_err());
        assert!(!loading_once_shut_down);
    }
}
