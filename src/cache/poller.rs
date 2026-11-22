use crate::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::*;

/// Keeps a cache in [`FirestoreCacheSyncMode::Poll`] current with one background task, which
/// reads the cache's collections one after another, however many there are.
pub(crate) struct FirestoreCachePoller {
    cache_name: FirestoreCacheName,
    interval: Duration,
    /// The task stops once this is set, or once the poller is dropped and the sender with it, so
    /// that a cache dropped without `shutdown` does not go on reading Firestore.
    shutdown_signal: watch::Sender<bool>,
    state: tokio::sync::Mutex<FirestoreCachePollerState>,
}

enum FirestoreCachePollerState {
    Created,
    Loading,
    /// Kept until the task has been joined: a `shutdown` cancelled while it waits leaves the task
    /// to the next one, rather than letting the backend be released under a running task.
    Polling(JoinHandle<()>),
    ShutDown,
}

impl FirestoreCachePoller {
    /// Refuses a zero interval, which would read Firestore in a busy loop.
    pub(crate) fn new(
        cache_name: FirestoreCacheName,
        params: &FirestoreCachePollParams,
    ) -> FirestoreResult<Self> {
        if params.interval.is_zero() {
            return Err(FirestoreError::invalid_parameters(
                "interval",
                "The interval between two polls of a cache must not be zero.",
            ));
        }
        Ok(Self {
            cache_name,
            interval: params.interval,
            shutdown_signal: watch::Sender::new(false),
            state: tokio::sync::Mutex::new(FirestoreCachePollerState::Created),
        })
    }

    /// Loads the backend, then starts polling. A cache is loaded once: a second load, or a load
    /// after `shutdown`, fails before the backend reads anything.
    ///
    /// The collections the backend kept documents of at load are polled at once, and every
    /// collection one interval later and every interval after that.
    pub(crate) async fn load<B>(
        &self,
        backend: &Arc<B>,
        db: &FirestoreDb,
        options: &FirestoreCacheOptions,
    ) -> FirestoreResult<()>
    where
        B: FirestoreCacheBackend + Send + Sync + 'static,
    {
        {
            let mut state = self.state.lock().await;
            match *state {
                FirestoreCachePollerState::Created => *state = FirestoreCachePollerState::Loading,
                FirestoreCachePollerState::Loading | FirestoreCachePollerState::Polling(_) => {
                    return Err(FirestoreError::invalid_parameters(
                        "cache",
                        "This cache has already been loaded.",
                    ))
                }
                FirestoreCachePollerState::ShutDown => return Err(Self::shut_down_error()),
            }
        }

        // Not under the lock, so that a slow preload does not hold up `shutdown`.
        let loaded = backend.load(options, db).await;

        let mut state = self.state.lock().await;
        if let Err(error) = loaded {
            if matches!(*state, FirestoreCachePollerState::Loading) {
                *state = FirestoreCachePollerState::Created;
            }
            return Err(error);
        }
        if !matches!(*state, FirestoreCachePollerState::Loading) {
            return Err(Self::shut_down_error());
        }

        let kept_collections: Vec<String> = backend
            .cache_configuration()
            .collections
            .keys()
            .filter(|collection_path| backend.kept_documents_at_load(collection_path))
            .cloned()
            .collect();
        let rounds = FirestoreCachePollRounds {
            cache_name: self.cache_name.clone(),
            interval: self.interval,
            backend: backend.clone(),
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

    /// Fails once polling has stopped, so that a collection added then is refused rather than
    /// cached with nothing reading it again.
    pub(crate) async fn accepts_collections(&self) -> FirestoreResult<()> {
        match &*self.state.lock().await {
            FirestoreCachePollerState::ShutDown => Err(Self::shut_down_error()),
            FirestoreCachePollerState::Polling(task) if task.is_finished() => Err(
                FirestoreError::system_error("The poller of this cache has stopped."),
            ),
            _ => Ok(()),
        }
    }

    /// Stops polling, interrupting a round in progress, and returns once the task has exited.
    /// Returns a [`FirestoreError::SystemError`] if the task panicked.
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

    fn shut_down_error() -> FirestoreError {
        FirestoreError::invalid_parameters("cache", "This cache has been shut down.")
    }
}

struct FirestoreCachePollRounds<B> {
    cache_name: FirestoreCacheName,
    interval: Duration,
    backend: Arc<B>,
    db: FirestoreDb,
}

impl<B> FirestoreCachePollRounds<B>
where
    B: FirestoreCacheBackend + Send + Sync + 'static,
{
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
    }

    #[tokio::test]
    async fn watched_and_lazy_collections_poll_only_the_documents_they_hold() {
        let server = server_holding(|read| match read {
            0 => vec![
                document("configs", "site", 1),
                document("configs", "billing", 1),
            ],
            _ => vec![
                document("configs", "site", 2),
                document("users", "read", 2),
                document("users", "never_read", 1),
            ],
        })
        .await;
        let cache = FirestoreCache::memory(&server.db)
            .collection_with("configs", |c| {
                c.documents(["site", "billing"]).preload_all()
            })
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

        // The preload of `configs`, then a round of two batch gets, then the start of the next.
        within(server.wait_for_calls(4)).await;

        let mut first_round = server.calls()[1..3].to_vec();
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
    async fn a_cache_that_polls_never_listens() {
        let server = server_holding(|_| vec![document("a", "one", 1)]).await;
        let cache = FirestoreCache::memory(&server.db)
            .preloaded_collection("a")
            .poll_changes(POLL_INTERVAL)
            .build()
            .await
            .expect("cache");
        // The preload, then a round. A listener's stream would be logged by now instead, once
        // the client closed it at the end of the empty response.
        within(server.wait_for_calls(2)).await;

        cache.shutdown().await.expect("shutdown");

        assert!(!server.calls().iter().any(|call| call == "Listen"));
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
    async fn a_zero_poll_interval_is_refused() {
        let server = server_holding(|_| Vec::new()).await;

        let error = FirestoreCache::memory(&server.db)
            .poll_changes(Duration::ZERO)
            .build()
            .await
            .err()
            .expect("a zero interval is refused");

        assert!(matches!(error, FirestoreError::InvalidParametersError(_)));
    }

    #[tokio::test]
    async fn a_watched_collection_polls_its_documents_whatever_its_load_mode() {
        let server = server_holding(|_| vec![document("flags", "on", 1)]).await;
        let cache = FirestoreCache::memory(&server.db)
            .collection_with("flags", |c| c.documents(["on", "off"]))
            .poll_changes(POLL_INTERVAL)
            .build()
            .await
            .expect("cache");

        // A round of one batch get, then the start of the next.
        within(server.wait_for_calls(2)).await;

        assert_eq!(server.calls()[0], "BatchGet flags: off, on");
        assert_eq!(
            cached(&cache, "flags", "on").await,
            Some(document("flags", "on", 1))
        );
        cache.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn a_lazy_collection_is_read_in_chunks() {
        let users = || {
            (0..150)
                .map(|id| document("users", &id.to_string(), 1))
                .collect::<Vec<_>>()
        };
        let server = server_holding(move |_| users()).await;
        let cache = FirestoreCache::memory(&server.db)
            .collection("users")
            .poll_changes(POLL_INTERVAL)
            .build_without_load()
            .await
            .expect("cache");
        for user in users() {
            cache
                .backend()
                .update_doc_by_path(&user)
                .await
                .expect("a document read through the cache");
        }
        cache.load().await.expect("loaded");

        // A round of two batch gets, then the start of the next.
        within(server.wait_for_calls(3)).await;

        let mut documents_per_read: Vec<usize> = server.calls()[0..2]
            .iter()
            .map(|call| call.split(", ").count())
            .collect();
        documents_per_read.sort();
        assert_eq!(documents_per_read, vec![50, 100]);
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

        let error = cache
            .add_collection(FirestoreCacheCollection::new("b").preload_all())
            .await
            .unwrap_err();

        assert!(matches!(error, FirestoreError::InvalidParametersError(_)));
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

    /// A custom backend that implements nothing a polling cache needs. Building the cache must
    /// fail before any of these is called.
    struct BackendThatCannotPoll;

    #[async_trait::async_trait]
    impl FirestoreCacheBackend for BackendThatCannotPoll {
        async fn load(
            &self,
            _: &FirestoreCacheOptions,
            _: &FirestoreDb,
        ) -> FirestoreResult<Vec<FirestoreListenerTargetParams>> {
            unreachable!("the cache is never built")
        }

        async fn invalidate_all(&self) -> FirestoreResult<()> {
            unreachable!("the cache is never built")
        }

        async fn shutdown(&self) -> FirestoreResult<()> {
            unreachable!("the cache is never built")
        }

        async fn on_listen_event(&self, _: FirestoreListenEvent) -> FirestoreResult<()> {
            unreachable!("the cache is never built")
        }
    }

    #[async_trait::async_trait]
    impl FirestoreCacheDocsByPathSupport for BackendThatCannotPoll {
        async fn get_doc_by_path(&self, _: &str) -> FirestoreResult<Option<FirestoreDocument>> {
            unreachable!("the cache is never built")
        }

        async fn update_doc_by_path(&self, _: &FirestoreDocument) -> FirestoreResult<()> {
            unreachable!("the cache is never built")
        }

        async fn list_all_docs<'b>(
            &self,
            _: &str,
        ) -> FirestoreResult<
            FirestoreCachedValue<
                futures::stream::BoxStream<'b, FirestoreResult<FirestoreDocument>>,
            >,
        > {
            unreachable!("the cache is never built")
        }

        async fn query_docs<'b>(
            &self,
            _: &str,
            _: &FirestoreQueryParams,
        ) -> FirestoreResult<
            FirestoreCachedValue<
                futures::stream::BoxStream<'b, FirestoreResult<FirestoreDocument>>,
            >,
        > {
            unreachable!("the cache is never built")
        }
    }

    #[tokio::test]
    async fn a_backend_that_cannot_poll_fails_a_polling_cache_when_it_is_created() {
        let server = server_holding(|_| Vec::new()).await;
        let options = FirestoreCacheOptions::new("custom".into()).with_sync_mode(
            FirestoreCacheSyncMode::Poll(FirestoreCachePollParams::new(POLL_INTERVAL)),
        );

        let error = FirestoreCache::create(
            options,
            &server.db,
            BackendThatCannotPoll,
            FirestoreMemListenStateStorage::new(),
        )
        .await
        .err()
        .expect("the cache is refused");

        let FirestoreError::CacheError(error) = error else {
            panic!("the cache was not refused with a cache error");
        };
        assert_eq!(error.public.code, "CacheBackendUnsupported");
    }
}
