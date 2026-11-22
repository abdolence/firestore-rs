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
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
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
            task: tokio::sync::Mutex::new(None),
        })
    }

    /// Spawns the task, whose first round starts one interval from now. Fails if the task is
    /// already running.
    pub(crate) async fn start<B>(&self, backend: Arc<B>, db: FirestoreDb) -> FirestoreResult<()>
    where
        B: FirestoreCacheBackend + Send + Sync + 'static,
    {
        let mut task = self.task.lock().await;
        if task.is_some() {
            return Err(FirestoreError::invalid_parameters(
                "sync_mode",
                "This cache is already polling.",
            ));
        }

        let rounds = FirestoreCachePollRounds {
            cache_name: self.cache_name.clone(),
            interval: self.interval,
            backend,
            db,
        };
        let mut shutdown = self.shutdown_signal.subscribe();
        *task = Some(tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.wait_for(|stopped| *stopped) => {}
                () = rounds.run() => {}
            }
        }));
        Ok(())
    }

    /// Stops the task, interrupting a round in progress, and waits for it to exit. Returns a
    /// [`FirestoreError::SystemError`] if the task panicked.
    pub(crate) async fn shutdown(&self) -> FirestoreResult<()> {
        self.shutdown_signal.send_replace(true);
        let Some(task) = self.task.lock().await.take() else {
            return Ok(());
        };
        task.await.map_err(|err| {
            FirestoreError::system_error(format!("The cache poller panicked: {err}"))
        })
    }
}

/// What the poller's task owns.
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
    async fn run(self) {
        loop {
            tokio::time::sleep(self.interval).await;
            self.poll_round().await;
        }
    }

    /// Takes the collections from the backend's configuration as the round starts, so that one
    /// added or removed since the last round is polled or left alone without the poller being
    /// told. A collection that fails keeps what it holds, and the next round tries it again.
    async fn poll_round(&self) {
        let config = self.backend.cache_configuration();
        debug!(
            cache = self.cache_name.value(),
            collections = config.collections.len(),
            "Polling the cached collections."
        );
        for (collection_path, collection_config) in &config.collections {
            if let Err(error) = self
                .poll_collection(collection_path, collection_config)
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

    async fn poll_collection(
        &self,
        collection_path: &str,
        config: &FirestoreCacheCollectionConfiguration,
    ) -> FirestoreResult<()> {
        let document_ids = match (
            config.collection_load_mode.is_preloading(),
            config.watched_document_ids(),
        ) {
            (true, None) => {
                let documents = config.read_all_documents(&self.db).await?;
                return self
                    .backend
                    .reconcile_collection(collection_path, documents)
                    .await;
            }
            (true, Some(watched_document_ids)) => watched_document_ids.to_vec(),
            // A lazily filled collection holds what was read through it: polling refreshes those
            // documents and never fills it further.
            (false, _) => self.backend.cached_document_ids(collection_path).await?,
        };
        if document_ids.is_empty() {
            return Ok(());
        }

        let documents = config.read_documents(&self.db, document_ids).await?;
        self.backend
            .reconcile_documents(collection_path, documents)
            .await
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
            ..Default::default()
        }
    }

    /// A server holding the documents `documents_at(n)` returns while answering its `n`th read,
    /// counting from zero. A query answers the documents of its collection, and a batch get each
    /// named document, found or missing. Calls are logged as `RunQuery <collection>`,
    /// `BatchGet <collection>: <sorted ids>` and `Listen`.
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
                let messages = documents
                    .into_iter()
                    .filter(|doc| split_document_path(&doc.name).0 == collection_path)
                    .map(run_query_message)
                    .collect();
                (
                    format!("RunQuery {collection}"),
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
                    format!("BatchGet {collection}: {}", ids.join(", ")),
                    FakeResponse::Paced(messages, Duration::ZERO),
                )
            } else {
                panic!("a cache that polls makes no {method} call");
            }
        })
        .await
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
    async fn one_round_polls_every_collection() {
        let server = server_holding(|_| Vec::new()).await;
        let cache = FirestoreCache::memory(&server.db)
            .preloaded_collection("a")
            .preloaded_collection("b")
            .preloaded_collection("c")
            .poll_changes(POLL_INTERVAL)
            .build()
            .await
            .expect("cache");

        // Three preloads, a round of three queries, then the start of the next round.
        within(server.wait_for_calls(7)).await;

        let mut first_round = server.calls()[3..6].to_vec();
        first_round.sort();
        assert_eq!(first_round, vec!["RunQuery a", "RunQuery b", "RunQuery c"]);
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
}
