use super::*;
use crate::db::fake_firestore::{FakeFirestore, FakeResponse};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Wait {
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
    dropped: Arc<AtomicBool>,
    reads: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
}

struct PendingOperation(Arc<AtomicBool>);

impl Drop for PendingOperation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl TestDb {
    async fn wait_for_release(&self) {
        let _operation = PendingOperation(self.dropped.clone());
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

async fn started(wait: Wait) -> (FirestoreListener<TestDb, TestDb>, TestDb) {
    let db = TestDb {
        wait,
        entered: Arc::new(Semaphore::new(0)),
        release: Arc::new(Semaphore::new(0)),
        stored: Arc::new(AtomicBool::new(false)),
        dropped: Arc::new(AtomicBool::new(false)),
        reads: Arc::new(AtomicUsize::new(0)),
        connections: Arc::new(AtomicUsize::new(0)),
    };
    let mut listener =
        FirestoreListener::new(db.clone(), db.clone(), FirestoreListenerParams::new())
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

async fn assert_graceful_shutdown(wait: Wait, timeout: Option<Duration>) {
    let (mut listener, db) = started(wait).await;
    {
        let shutdown = async {
            match timeout {
                Some(timeout) => listener.shutdown_with_timeout(timeout).await,
                None => listener.shutdown().await,
            }
        };
        futures::pin_mut!(shutdown);
        assert!(futures::poll!(shutdown.as_mut()).is_pending());
        tokio::task::yield_now().await;
        assert!(futures::poll!(shutdown.as_mut()).is_pending());
        assert!(!db.stored.load(Ordering::Relaxed));
        db.release.add_permits(1);
        within(shutdown).await.unwrap();
    }
    assert!(db.stored.load(Ordering::Relaxed));
    listener.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_the_callback_and_for_resume_token_storage() {
    for wait in [Wait::Callback, Wait::Storage] {
        for timeout in [None, Some(Duration::from_secs(3600))] {
            assert_graceful_shutdown(wait, timeout).await;
        }
    }
}

#[tokio::test]
async fn interrupted_shutdown_retains_task_for_join() {
    let (mut listener, db) = started(Wait::Callback).await;
    {
        let shutdown = listener.shutdown();
        futures::pin_mut!(shutdown);
        assert!(futures::poll!(shutdown).is_pending());
    }
    assert!(listener.shutdown_handle.is_some());
    db.release.add_permits(1);
    within(listener.shutdown()).await.unwrap();
    assert!(db.stored.load(Ordering::Relaxed));
    assert!(listener.shutdown_handle.is_none());
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

#[tokio::test]
async fn shutdown_timeout_aborts_and_joins_callback_and_storage() {
    for wait in [Wait::Read, Wait::Callback, Wait::Storage] {
        let (mut listener, db) = started(wait).await;
        let timeout = Duration::from_millis(10);
        let began = tokio::time::Instant::now();
        within(listener.shutdown_with_timeout(timeout))
            .await
            .unwrap();
        assert!(began.elapsed() >= timeout);
        assert!(!db.stored.load(Ordering::Relaxed));
        assert!(db.dropped.load(Ordering::Relaxed));
        assert!(listener.shutdown_handle.is_none());
        listener
            .shutdown_with_timeout(Duration::ZERO)
            .await
            .unwrap();
        listener.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn interrupted_shutdown_with_timeout_retains_task_before_and_after_abort() {
    for timeout in [
        Duration::from_secs(3600),
        Duration::ZERO,
        Duration::from_millis(10),
    ] {
        let (mut listener, db) = started(Wait::Callback).await;
        let aborted = timeout < Duration::from_secs(1);
        {
            let shutdown = listener.shutdown_with_timeout(timeout);
            futures::pin_mut!(shutdown);
            assert!(futures::poll!(shutdown.as_mut()).is_pending());
            if aborted && !timeout.is_zero() {
                tokio::time::sleep(timeout * 2).await;
                assert!(futures::poll!(shutdown.as_mut()).is_pending());
            }
        }
        assert!(listener.shutdown_handle.is_some());
        if !aborted {
            db.release.add_permits(1);
        }
        within(listener.shutdown()).await.unwrap();
        assert_eq!(db.stored.load(Ordering::Relaxed), !aborted);
        assert!(db.dropped.load(Ordering::Relaxed));
        assert!(listener.shutdown_handle.is_none());
    }
}

#[tokio::test]
async fn shutdown_during_resume_state_read_prevents_connection() {
    let (mut listener, db) = started(Wait::Read).await;
    {
        let shutdown = listener.shutdown();
        futures::pin_mut!(shutdown);
        assert!(futures::poll!(shutdown.as_mut()).is_pending());
        db.release.add_permits(1);
        within(shutdown).await.unwrap();
    }
    assert_eq!(db.connections.load(Ordering::Relaxed), 0);
    assert!(db.dropped.load(Ordering::Relaxed));
    assert!(!db.stored.load(Ordering::Relaxed));
}

#[tokio::test]
async fn shutdown_with_timeout_joins_an_already_finished_task() {
    let (mut listener, db) = started(Wait::Callback).await;
    {
        let shutdown = listener.shutdown();
        futures::pin_mut!(shutdown);
        assert!(futures::poll!(shutdown.as_mut()).is_pending());
    }
    db.release.add_permits(1);
    within(async {
        while !listener.shutdown_handle.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    listener
        .shutdown_with_timeout(Duration::ZERO)
        .await
        .unwrap();
    assert!(db.stored.load(Ordering::Relaxed));
    assert!(listener.shutdown_handle.is_none());
}
