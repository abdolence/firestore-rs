use super::*;
use crate::db::fake_firestore::{FakeFirestore, FakeResponse};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;

/// Where the listener's task stops until the test adds a `release` permit.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Wait {
    /// The connect never answers.
    Connect,
    /// The response ends at once, so the task sits in the reconnect delay; nothing releases it.
    Reconnect,
    /// The resume-state read inside the loop, after the one `start` makes.
    Read,
    Callback,
    Storage,
}

#[derive(Clone)]
struct TestDb {
    wait: Wait,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    stored: Arc<AtomicBool>,
    reads: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
}

impl TestDb {
    async fn wait_for_release(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }
}

#[async_trait]
impl FirestoreListenSupport for TestDb {
    async fn listen_doc_changes<'a, 'b>(
        &'a self,
        _: Vec<FirestoreListenerTargetParams>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>> {
        self.connections.fetch_add(1, Ordering::Relaxed);
        match self.wait {
            Wait::Connect => {
                self.wait_for_release().await;
                Ok(futures::stream::empty().boxed())
            }
            // Signals from the poll that ends the stream, so the task is already in the
            // reconnect delay when the test wakes.
            Wait::Reconnect => {
                let entered = self.entered.clone();
                Ok(futures::stream::poll_fn(move |_| {
                    entered.add_permits(1);
                    std::task::Poll::Ready(None)
                })
                .boxed())
            }
            Wait::Read | Wait::Callback | Wait::Storage => {
                Ok(futures::stream::iter([Ok(ListenResponse {
                    response_type: Some(FirestoreListenEvent::TargetChange(TargetChange {
                        target_change_type: target_change::TargetChangeType::Current as i32,
                        target_ids: vec![1],
                        resume_token: vec![1],
                        ..Default::default()
                    })),
                })])
                .chain(futures::stream::pending())
                .boxed())
            }
        }
    }
}

#[async_trait]
impl FirestoreResumeStateStorage for TestDb {
    async fn read_resume_state(
        &self,
        _: &FirestoreListenerTarget,
    ) -> AnyBoxedErrResult<Option<FirestoreListenerTargetResumeType>> {
        if self.wait == Wait::Read && self.reads.fetch_add(1, Ordering::Relaxed) > 0 {
            self.wait_for_release().await;
        }
        Ok(None)
    }

    async fn update_resume_token(
        &self,
        _: &FirestoreListenerTarget,
        _: FirestoreListenerToken,
    ) -> AnyBoxedErrResult<()> {
        if self.wait == Wait::Storage {
            self.wait_for_release().await;
        }
        self.stored.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn forget_resume_state(&self, _: &FirestoreListenerTarget) -> AnyBoxedErrResult<()> {
        Ok(())
    }
}

async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("listener operation timed out")
}

/// A listener with one target that has not been started.
async fn listener(wait: Wait) -> (FirestoreListener<TestDb, TestDb>, TestDb) {
    let db = TestDb {
        wait,
        entered: Arc::new(Semaphore::new(0)),
        release: Arc::new(Semaphore::new(0)),
        stored: Arc::new(AtomicBool::new(false)),
        reads: Arc::new(AtomicUsize::new(0)),
        connections: Arc::new(AtomicUsize::new(0)),
    };
    let listener = FirestoreListener::new(
        db.clone(),
        db.clone(),
        FirestoreListenerParams::new().with_retry_delay(Duration::from_secs(3600)),
    )
    .await
    .unwrap();
    listener
        .add_target(FirestoreListenerTargetParams::new(
            FirestoreListenerTarget::new(1),
            FirestoreTargetType::Documents(FirestoreCollectionDocuments::new(
                "test".into(),
                vec!["one".into()],
            )),
            HashMap::new(),
        ))
        .unwrap();
    (listener, db)
}

/// A started listener whose task is stopped at `wait`.
async fn started(wait: Wait) -> (FirestoreListener<TestDb, TestDb>, TestDb) {
    let (mut listener, db) = listener(wait).await;
    let callback_db = db.clone();
    listener
        .start(move |_| {
            let db = callback_db.clone();
            async move {
                if db.wait == Wait::Callback {
                    db.wait_for_release().await;
                }
                Ok(())
            }
        })
        .await
        .unwrap();
    within(db.entered.acquire()).await.unwrap().forget();
    (listener, db)
}

#[tokio::test]
async fn shutdown_waits_for_the_callback_and_for_resume_token_storage() {
    for wait in [Wait::Callback, Wait::Storage] {
        for timeout in [None, Some(Duration::from_secs(3600))] {
            let (mut listener, db) = started(wait).await;
            {
                let shutdown = listener.shutdown_with(timeout);
                futures::pin_mut!(shutdown);
                assert!(futures::poll!(shutdown.as_mut()).is_pending());
                tokio::task::yield_now().await;
                assert!(futures::poll!(shutdown.as_mut()).is_pending());
                assert!(!db.stored.load(Ordering::Relaxed));
                db.release.add_permits(1);
                within(shutdown).await.unwrap();
            }
            assert!(db.stored.load(Ordering::Relaxed));
        }
    }
}

#[tokio::test]
async fn shutdown_interrupts_connecting_and_the_reconnect_delay() {
    for wait in [Wait::Connect, Wait::Reconnect] {
        let (mut listener, _db) = started(wait).await;
        within(listener.shutdown()).await.unwrap();
    }
}

#[tokio::test]
async fn shutdown_before_start_prevents_connections() {
    let (mut listener, db) = listener(Wait::Connect).await;
    listener.shutdown().await.unwrap();
    listener.start(|_| async { Ok(()) }).await.unwrap();
    within(listener.shutdown_handle.as_mut().unwrap())
        .await
        .unwrap();
    assert_eq!(db.connections.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn shutdown_while_reading_resume_state_prevents_connection() {
    let (mut listener, db) = started(Wait::Read).await;
    {
        let shutdown = listener.shutdown();
        futures::pin_mut!(shutdown);
        assert!(futures::poll!(shutdown.as_mut()).is_pending());
        db.release.add_permits(1);
        within(shutdown).await.unwrap();
    }
    assert_eq!(db.connections.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn shutdown_timeout_aborts_and_joins_a_stuck_read_callback_or_storage() {
    for wait in [Wait::Read, Wait::Callback, Wait::Storage] {
        let (mut listener, db) = started(wait).await;
        let timeout = Duration::from_millis(10);
        let began = tokio::time::Instant::now();
        within(listener.shutdown_with_timeout(timeout))
            .await
            .unwrap();
        assert!(began.elapsed() >= timeout);
        assert!(!db.stored.load(Ordering::Relaxed));
        assert!(listener.shutdown_handle.is_none());
    }
}

#[tokio::test]
async fn cancelled_shutdown_keeps_the_task_for_a_later_join() {
    for timeout in [None, Some(Duration::from_secs(3600)), Some(Duration::ZERO)] {
        let aborted = timeout == Some(Duration::ZERO);
        let (mut listener, db) = started(Wait::Callback).await;
        {
            let shutdown = listener.shutdown_with(timeout);
            futures::pin_mut!(shutdown);
            assert!(futures::poll!(shutdown.as_mut()).is_pending());
        }
        assert!(listener.shutdown_handle.is_some());
        if !aborted {
            db.release.add_permits(1);
        }
        within(listener.shutdown()).await.unwrap();
        assert_eq!(db.stored.load(Ordering::Relaxed), !aborted);
        assert!(listener.shutdown_handle.is_none());
    }
}

/// A fake server that logs `Listen closed` once the client closes a Listen request; see
/// [`FakeFirestore`] for why the log line is the proof.
async fn listen_server() -> FakeFirestore {
    FakeFirestore::start(|method, _bytes| {
        assert!(method.ends_with("/Listen"));
        ("Listen closed".to_string(), FakeResponse::empty())
    })
    .await
}

#[tokio::test]
async fn dropping_response_closes_http2_request() {
    let server = listen_server().await;
    let response = within(server.db.listen_doc_changes(vec![])).await.unwrap();
    assert!(server.calls().is_empty());
    drop(response);
    within(server.wait_for_calls(1)).await;
    assert_eq!(server.calls(), vec!["Listen closed"]);
}

// A listener holds its response stream through the reconnect delay after the stream ends, so
// the request must close when the response ends, not only when the stream is dropped.
#[tokio::test]
async fn ended_response_closes_http2_request_while_held() {
    let server = listen_server().await;
    let mut response = within(server.db.listen_doc_changes(vec![])).await.unwrap();
    assert!(within(response.next()).await.is_none());
    within(server.wait_for_calls(1)).await;
    assert_eq!(server.calls(), vec!["Listen closed"]);
}
