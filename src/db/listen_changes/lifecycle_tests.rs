use super::*;
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
async fn shutdown_waits_for_callback_and_resume_token() {
    assert_graceful_shutdown(Wait::Callback).await;
}

#[tokio::test]
async fn shutdown_waits_for_resume_token_storage() {
    assert_graceful_shutdown(Wait::Storage).await;
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

#[tokio::test]
async fn dropping_response_closes_http2_request() {
    let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = server.local_addr().unwrap();
    let (closed, mut request_closed) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let (socket, _) = server.accept().await.unwrap();
        let mut connection = h2::server::handshake(socket).await.unwrap();
        let (request, mut respond) = connection.accept().await.unwrap().unwrap();
        let _response = respond
            .send_response(
                hyper::Response::builder()
                    .header("content-type", "application/grpc")
                    .body(())
                    .unwrap(),
                false,
            )
            .unwrap();
        let mut body = request.into_body();
        tokio::select! {
            _ = async { while connection.accept().await.is_some() {} } => {}
            _ = async {
                while let Some(Ok(data)) = body.data().await {
                    body.flow_control().release_capacity(data.len()).unwrap();
                }
            } => {}
        }
        closed.send(()).unwrap();
    });
    let db = FirestoreDb::with_options_token_source(
        crate::FirestoreDbOptions::new("test".into())
            .with_firebase_api_url(format!("http://{address}")),
        vec![],
        gcloud_sdk::TokenSourceType::ExternalSource(Box::new(
            crate::db::FirestoreEmulatorTokenSource,
        )),
    )
    .await
    .unwrap();
    let response = within(db.listen_doc_changes(vec![])).await.unwrap();
    assert!(matches!(
        request_closed.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    drop(response);
    within(request_closed).await.unwrap();
    server_task.await.unwrap();
}
