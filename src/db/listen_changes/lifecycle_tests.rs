use super::*;
use crate::db::fake_firestore::{FakeFirestore, FakeResponse};
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Wait {
    Callback,
    Storage,
}

#[derive(Clone)]
struct TestDb {
    wait: Wait,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    stored: Arc<AtomicBool>,
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

async fn assert_graceful_shutdown(wait: Wait) {
    let (mut listener, db) = started(wait).await;
    {
        let shutdown = listener.shutdown();
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
        assert_graceful_shutdown(wait).await;
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

// The fake server's handler only runs once the request body ends, which for a Listen call
// happens only when the client drops its response - so a logged call is itself the proof that
// dropping the response closed the request stream.
#[tokio::test]
async fn dropping_response_closes_http2_request() {
    let server = FakeFirestore::start(|method, _bytes| {
        assert!(method.ends_with("/Listen"));
        ("Listen closed".to_string(), FakeResponse::Drop)
    })
    .await;
    let response = within(server.db.listen_doc_changes(vec![])).await.unwrap();
    assert!(server.calls().is_empty());
    drop(response);
    within(async {
        while server.calls().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(server.calls(), vec!["Listen closed"]);
}
