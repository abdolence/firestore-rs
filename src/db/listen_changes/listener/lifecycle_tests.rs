use super::*;
use crate::db::fake_firestore::{FakeFirestore, FakeResponse};
use crate::db::support::FirestoreListenSupport;
use crate::{
    FirestoreCollectionDocuments, FirestoreListenerTargetResumeType, FirestoreListenerToken,
    FirestoreMemListenStateStorage, FirestoreRequestOptions, FirestoreResult,
    FirestoreResumeStateStorage, FirestoreTargetType,
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use gcloud_sdk::tonic::{Code, Status};
use listen_request::TargetChange as RequestedChange;
use rvstruct::ValueStruct;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use target_change::TargetChangeType;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

/// Where the listener's task stops until the test adds a `release` permit.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Wait {
    /// The connect, which then opens a stream with `events`.
    Connect,
    /// The response ends at once, so the task sits in the reconnect delay; nothing releases it.
    Reconnect,
    /// The resume-state read inside the loop, after the one `start` makes.
    Read,
    Callback,
    Storage,
    /// Forgetting a target's resume state. Every connect signals, as for `Never`.
    Forget,
    /// Nowhere. Every connect signals that it happened.
    Never,
}

#[derive(Clone)]
struct TestDb {
    wait: Wait,
    /// What every stream sends before it goes quiet, unless `wait` stops the task earlier.
    events: Vec<FirestoreListenEvent>,
    /// What the test feeds the next stream after `events`, from [`TestDb::feed`].
    feed: Arc<Mutex<Option<UnboundedReceiver<FirestoreListenEvent>>>>,
    /// Fails the fed stream with a status, from [`TestDb::fail_stream`].
    stream_failure: Arc<Mutex<Option<UnboundedSender<Code>>>>,
    stream_failure_reader: Arc<Mutex<Option<UnboundedReceiver<Code>>>>,
    /// Fails every connection with this status instead.
    connect_error: Option<Code>,
    /// The request side of the latest stream.
    requests: Arc<Mutex<Option<UnboundedReceiver<ListenRequest>>>>,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    /// Every event the callback has been given, in order.
    seen: watch::Sender<Vec<FirestoreListenEvent>>,
    markers: Arc<AtomicI32>,
    /// The targets whose resume token was stored, in order.
    stored: Arc<Mutex<Vec<FirestoreListenerTarget>>>,
    /// What the resume-state storage holds.
    tokens: Arc<Mutex<HashMap<FirestoreListenerTarget, FirestoreListenerToken>>>,
    /// The targets whose resume state was forgotten, in order.
    forgotten: Arc<Mutex<Vec<FirestoreListenerTarget>>>,
    reads: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
}

impl TestDb {
    fn new(wait: Wait) -> Self {
        Self {
            wait,
            events: vec![FirestoreListenEvent::TargetChange(TargetChange {
                target_change_type: TargetChangeType::Current as i32,
                target_ids: vec![1],
                resume_token: vec![1],
                ..Default::default()
            })],
            feed: Arc::new(Mutex::new(None)),
            stream_failure: Arc::new(Mutex::new(None)),
            stream_failure_reader: Arc::new(Mutex::new(None)),
            connect_error: None,
            requests: Arc::new(Mutex::new(None)),
            entered: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            seen: watch::Sender::new(Vec::new()),
            markers: Arc::new(AtomicI32::new(0)),
            stored: Arc::new(Mutex::new(Vec::new())),
            tokens: Arc::new(Mutex::new(HashMap::new())),
            forgotten: Arc::new(Mutex::new(Vec::new())),
            reads: Arc::new(AtomicUsize::new(0)),
            connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// A stream with no events of its own, which sends what the test feeds it and ends once the
    /// returned sender is dropped.
    fn fed(wait: Wait) -> (Self, UnboundedSender<FirestoreListenEvent>) {
        let db = Self {
            events: Vec::new(),
            ..Self::new(wait)
        };
        let (feed, feed_reader) = tokio::sync::mpsc::unbounded_channel();
        *db.feed.lock().unwrap() = Some(feed_reader);
        let (stream_failure, stream_failure_reader) = tokio::sync::mpsc::unbounded_channel();
        *db.stream_failure.lock().unwrap() = Some(stream_failure);
        *db.stream_failure_reader.lock().unwrap() = Some(stream_failure_reader);
        (db, feed)
    }

    /// Fails the fed stream with `code`, ahead of every fed event the listener has not read yet.
    fn fail_stream(&self, code: Code) {
        self.stream_failure
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .send(code)
            .unwrap();
    }

    async fn wait_for_release(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }

    fn take_requests(&self) -> UnboundedReceiver<ListenRequest> {
        self.requests.lock().unwrap().take().unwrap()
    }

    /// Waits until the listener has acted on everything fed to it and asked of it before.
    ///
    /// Two markers, because a request made while the loop is between polling its control channel
    /// and its stream can still be behind the first.
    async fn settle(&self, feed: &UnboundedSender<FirestoreListenEvent>) {
        for _ in 0..2 {
            let marker = FirestoreListenEvent::Filter(ExistenceFilter {
                count: self.markers.fetch_add(1, Ordering::Relaxed),
                ..Default::default()
            });
            feed.send(marker.clone()).unwrap();
            within(
                self.seen
                    .subscribe()
                    .wait_for(|seen| seen.contains(&marker)),
            )
            .await
            .unwrap();
        }
    }
}

#[async_trait]
impl FirestoreListenSupport for TestDb {
    async fn listen_doc_changes<'a, 'b>(
        &'a self,
        requests: UnboundedReceiver<ListenRequest>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>> {
        self.connections.fetch_add(1, Ordering::Relaxed);
        *self.requests.lock().unwrap() = Some(requests);
        if let Some(code) = self.connect_error {
            return Err(Status::new(code, "rejected by the test").into());
        }
        match self.wait {
            Wait::Connect => self.wait_for_release().await,
            Wait::Forget | Wait::Never => self.entered.add_permits(1),
            Wait::Reconnect | Wait::Read | Wait::Callback | Wait::Storage => {}
        }
        let response = |event| -> FirestoreResult<ListenResponse> {
            Ok(ListenResponse {
                response_type: Some(event),
            })
        };
        match self.wait {
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
            Wait::Connect
            | Wait::Read
            | Wait::Callback
            | Wait::Storage
            | Wait::Forget
            | Wait::Never => {
                let events = futures::stream::iter(self.events.clone()).map(response);
                Ok(match self.feed.lock().unwrap().take() {
                    Some(mut feed) => {
                        let mut failures = self.stream_failure_reader.lock().unwrap().take();
                        let fed = futures::stream::poll_fn(move |cx| {
                            if let Some(std::task::Poll::Ready(Some(code))) =
                                failures.as_mut().map(|failures| failures.poll_recv(cx))
                            {
                                return std::task::Poll::Ready(Some(Err(Status::new(
                                    code,
                                    "failed by the test",
                                )
                                .into())));
                            }
                            feed.poll_recv(cx).map(|event| event.map(response))
                        });
                        events.chain(fed).boxed()
                    }
                    None => events.chain(futures::stream::pending()).boxed(),
                })
            }
        }
    }

    fn add_target_request(
        &self,
        target_params: FirestoreListenerTargetParams,
    ) -> FirestoreResult<ListenRequest> {
        Ok(ListenRequest {
            target_change: Some(RequestedChange::AddTarget(Target {
                target_id: target_params.target.try_into()?,
                once: target_params.add_target_once.unwrap_or(false),
                target_type: match target_params.target_type {
                    FirestoreTargetType::Documents(documents) => {
                        Some(target::TargetType::Documents(target::DocumentsTarget {
                            documents: documents.documents,
                        }))
                    }
                    FirestoreTargetType::Query(_) => None,
                },
                resume_type: target_params
                    .resume_type
                    .and_then(|resume_type| match resume_type {
                        FirestoreListenerTargetResumeType::Token(token) => {
                            Some(target::ResumeType::ResumeToken(token.into_value()))
                        }
                        FirestoreListenerTargetResumeType::ReadTime(_) => None,
                    }),
                ..Default::default()
            })),
            ..Default::default()
        })
    }

    fn remove_target_request(
        &self,
        target: &FirestoreListenerTarget,
        _: Option<&FirestoreRequestOptions>,
    ) -> FirestoreResult<ListenRequest> {
        Ok(ListenRequest {
            target_change: Some(RequestedChange::RemoveTarget(target.clone().try_into()?)),
            ..Default::default()
        })
    }
}

#[async_trait]
impl FirestoreResumeStateStorage for TestDb {
    async fn read_resume_state(
        &self,
        target: &FirestoreListenerTarget,
    ) -> AnyBoxedErrResult<Option<FirestoreListenerTargetResumeType>> {
        if self.wait == Wait::Read && self.reads.fetch_add(1, Ordering::Relaxed) > 0 {
            self.wait_for_release().await;
        }
        Ok(self
            .tokens
            .lock()
            .unwrap()
            .get(target)
            .cloned()
            .map(FirestoreListenerTargetResumeType::Token))
    }

    async fn update_resume_token(
        &self,
        target: &FirestoreListenerTarget,
        token: FirestoreListenerToken,
    ) -> AnyBoxedErrResult<()> {
        if self.wait == Wait::Storage {
            self.wait_for_release().await;
        }
        self.stored.lock().unwrap().push(target.clone());
        self.tokens.lock().unwrap().insert(target.clone(), token);
        Ok(())
    }

    async fn forget_resume_state(&self, target: &FirestoreListenerTarget) -> AnyBoxedErrResult<()> {
        if self.wait == Wait::Forget {
            self.wait_for_release().await;
        }
        self.forgotten.lock().unwrap().push(target.clone());
        self.tokens.lock().unwrap().remove(target);
        Ok(())
    }
}

async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("listener operation timed out")
}

/// The target change of the next request the listener sends, or `None` if it sends none in time.
async fn next_request(requests: &mut UnboundedReceiver<ListenRequest>) -> Option<RequestedChange> {
    tokio::time::timeout(Duration::from_secs(2), requests.recv())
        .await
        .ok()
        .flatten()
        .and_then(|request| request.target_change)
}

/// The `AddTarget` that [`TestDb`] makes of a target on `document`, listened to from scratch.
fn add_request(target: i32, document: &str) -> RequestedChange {
    RequestedChange::AddTarget(Target {
        target_id: target,
        target_type: Some(target::TargetType::Documents(target::DocumentsTarget {
            documents: vec![document.to_string()],
        })),
        ..Default::default()
    })
}

/// The targets of the next `count` requests, which must all be `AddTarget`s, in ID order.
async fn added_targets(requests: &mut UnboundedReceiver<ListenRequest>, count: usize) -> Vec<i32> {
    let mut added = Vec::with_capacity(count);
    for _ in 0..count {
        match next_request(requests).await {
            Some(RequestedChange::AddTarget(target)) => added.push(target.target_id),
            other => panic!("expected an AddTarget, got {other:?}"),
        }
    }
    added.sort();
    added
}

fn target_change(change_type: TargetChangeType, target_ids: Vec<i32>) -> FirestoreListenEvent {
    FirestoreListenEvent::TargetChange(TargetChange {
        target_change_type: change_type as i32,
        target_ids,
        ..Default::default()
    })
}

fn removal_with_cause(target_ids: Vec<i32>, code: Code) -> FirestoreListenEvent {
    FirestoreListenEvent::TargetChange(TargetChange {
        target_change_type: TargetChangeType::Remove as i32,
        target_ids,
        cause: Some(gcloud_sdk::google::rpc::Status {
            code: code as i32,
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn target_params(target: u32) -> FirestoreListenerTargetParams {
    target_params_for_document(target, &format!("document-{target}"))
}

fn target_params_for_document(target: u32, document: &str) -> FirestoreListenerTargetParams {
    FirestoreListenerTargetParams::new(
        FirestoreListenerTarget::new(target),
        FirestoreTargetType::Documents(FirestoreCollectionDocuments::new(
            "test".into(),
            vec![document.to_string()],
        )),
        HashMap::new(),
    )
}

/// Listener params whose reconnects and add-backs never come within a test.
fn never_retrying() -> FirestoreListenerParams {
    FirestoreListenerParams::new().with_retry_delay(Duration::from_secs(3600))
}

fn retrying_at_once() -> FirestoreListenerParams {
    FirestoreListenerParams::new().with_retry_delay(Duration::ZERO)
}

/// A listener on `db` with target 1 that has not been started.
async fn listener(
    db: TestDb,
    params: FirestoreListenerParams,
) -> (FirestoreListener<TestDb, TestDb>, TestDb) {
    let listener = FirestoreListener::new(db.clone(), db.clone(), params)
        .await
        .unwrap();
    listener.add_target(target_params(1)).unwrap();
    (listener, db)
}

/// Starts `listener` with a callback that records every event, and waits until its task stops
/// at `db.wait`.
async fn start(listener: &mut FirestoreListener<TestDb, TestDb>, db: &TestDb) {
    let callback_db = db.clone();
    listener
        .start(move |event| {
            let db = callback_db.clone();
            async move {
                if db.wait == Wait::Callback {
                    db.wait_for_release().await;
                }
                db.seen.send_modify(|seen| seen.push(event));
                Ok(())
            }
        })
        .await
        .unwrap();
    within(db.entered.acquire()).await.unwrap().forget();
}

/// A started listener on `db` with target 1, whose task is stopped at `db.wait`.
async fn started(
    db: TestDb,
    params: FirestoreListenerParams,
) -> (FirestoreListener<TestDb, TestDb>, TestDb) {
    let (mut listener, db) = listener(db, params).await;
    start(&mut listener, &db).await;
    (listener, db)
}

/// Waits until the listener's task has ended, however it ended.
async fn until_finished(listener: &FirestoreListener<TestDb, TestDb>) {
    while !listener
        .shutdown_handle
        .as_ref()
        .is_some_and(JoinHandle::is_finished)
    {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test]
async fn shutdown_waits_for_the_callback_and_for_resume_token_storage() {
    for wait in [Wait::Callback, Wait::Storage] {
        for timeout in [None, Some(Duration::from_secs(3600))] {
            let (mut listener, db) = started(TestDb::new(wait), never_retrying()).await;
            {
                let shutdown = listener.shutdown_with(timeout);
                futures::pin_mut!(shutdown);
                assert!(futures::poll!(shutdown.as_mut()).is_pending());
                tokio::task::yield_now().await;
                assert!(futures::poll!(shutdown.as_mut()).is_pending());
                assert!(db.stored.lock().unwrap().is_empty());
                db.release.add_permits(1);
                within(shutdown).await.unwrap();
            }
            assert!(!db.stored.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn shutdown_interrupts_connecting_and_the_reconnect_delay() {
    for wait in [Wait::Connect, Wait::Reconnect] {
        let (mut listener, _db) = started(TestDb::new(wait), never_retrying()).await;
        within(listener.shutdown()).await.unwrap();
    }
}

#[tokio::test]
async fn shutdown_before_start_prevents_connections() {
    let (mut listener, db) = listener(TestDb::new(Wait::Connect), never_retrying()).await;
    listener.shutdown().await.unwrap();
    listener.start(|_| async { Ok(()) }).await.unwrap();
    within(listener.shutdown_handle.as_mut().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db.connections.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn shutdown_while_reading_resume_state_prevents_connection() {
    let (mut listener, db) = started(TestDb::new(Wait::Read), never_retrying()).await;
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
        let (mut listener, db) = started(TestDb::new(wait), never_retrying()).await;
        let timeout = Duration::from_millis(10);
        let began = tokio::time::Instant::now();
        within(listener.shutdown_with_timeout(timeout))
            .await
            .unwrap();
        assert!(began.elapsed() >= timeout);
        assert!(db.stored.lock().unwrap().is_empty());
        assert!(listener.shutdown_handle.is_none());
    }
}

#[tokio::test]
async fn cancelled_shutdown_keeps_the_task_for_a_later_join() {
    for timeout in [None, Some(Duration::from_secs(3600)), Some(Duration::ZERO)] {
        let aborted = timeout == Some(Duration::ZERO);
        let (mut listener, db) = started(TestDb::new(Wait::Callback), never_retrying()).await;
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
        assert_eq!(db.stored.lock().unwrap().is_empty(), aborted);
        assert!(listener.shutdown_handle.is_none());
    }
}

#[tokio::test]
async fn a_panicking_callback_stops_the_listener_with_an_error() {
    let (mut listener, _db) = listener(TestDb::new(Wait::Never), never_retrying()).await;
    listener
        .start(|_| async { panic!("the callback panicked") })
        .await
        .unwrap();

    within(until_finished(&listener)).await;

    assert!(matches!(
        listener.add_target(target_params(2)),
        Err(FirestoreError::SystemError(_))
    ));
    assert!(matches!(
        within(listener.shutdown()).await,
        Err(FirestoreError::SystemError(_))
    ));
}

#[tokio::test]
async fn stream_wide_token_is_stored_only_at_a_snapshot_for_targets_on_the_stream() {
    let db = TestDb {
        events: vec![
            target_change(TargetChangeType::Add, vec![1]),
            // Without a read time it is not a consistent snapshot of the stream.
            FirestoreListenEvent::TargetChange(TargetChange {
                target_change_type: TargetChangeType::NoChange as i32,
                resume_token: vec![1],
                ..Default::default()
            }),
            FirestoreListenEvent::TargetChange(TargetChange {
                target_change_type: TargetChangeType::NoChange as i32,
                resume_token: vec![2],
                read_time: Some(gcloud_sdk::prost_types::Timestamp::default()),
                ..Default::default()
            }),
        ],
        ..TestDb::new(Wait::Callback)
    };
    let (mut listener, db) = started(db, never_retrying()).await;
    for _ in 0..2 {
        db.release.add_permits(1);
        within(db.entered.acquire()).await.unwrap().forget();
    }

    listener.add_target(target_params(2)).unwrap();
    db.release.add_permits(1);
    within(listener.shutdown()).await.unwrap();

    assert_eq!(
        *db.stored.lock().unwrap(),
        vec![FirestoreListenerTarget::new(1)]
    );
}

#[tokio::test]
async fn a_stream_wide_token_read_before_a_target_is_registered_again_is_not_stored_for_it() {
    let db = TestDb {
        events: vec![
            target_change(TargetChangeType::Add, vec![1]),
            FirestoreListenEvent::TargetChange(TargetChange {
                target_change_type: TargetChangeType::NoChange as i32,
                resume_token: vec![9],
                read_time: Some(gcloud_sdk::prost_types::Timestamp::default()),
                ..Default::default()
            }),
        ],
        ..TestDb::new(Wait::Callback)
    };
    let (mut listener, db) = started(db, never_retrying()).await;
    db.release.add_permits(1);
    within(db.entered.acquire()).await.unwrap().forget();

    listener
        .remove_target(&FirestoreListenerTarget::new(1))
        .await
        .unwrap();
    listener
        .add_target(target_params_for_document(1, "document-other"))
        .unwrap();
    db.release.add_permits(1);
    within(listener.shutdown()).await.unwrap();

    assert!(db.stored.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_token_written_while_its_target_is_removed_is_not_resumed_by_the_next_registration() {
    let (db, feed) = TestDb::fed(Wait::Storage);
    let db = TestDb {
        events: vec![FirestoreListenEvent::TargetChange(TargetChange {
            target_change_type: TargetChangeType::Current as i32,
            target_ids: vec![1],
            resume_token: vec![1],
            ..Default::default()
        })],
        ..db
    };
    let (mut listener, db) = started(db, never_retrying()).await;
    let mut requests = db.take_requests();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-1"))
    );

    listener
        .remove_target(&FirestoreListenerTarget::new(1))
        .await
        .unwrap();
    listener
        .add_target(target_params_for_document(1, "document-other"))
        .unwrap();
    db.release.add_permits(1);
    assert_eq!(
        next_request(&mut requests).await,
        Some(RequestedChange::RemoveTarget(1))
    );
    feed.send(target_change(TargetChangeType::Remove, vec![1]))
        .unwrap();

    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-other"))
    );
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn invalid_listen_retries_without_tokens_then_stops_with_its_error() {
    let db = TestDb {
        connect_error: Some(Code::InvalidArgument),
        ..TestDb::new(Wait::Connect)
    };
    let (mut listener, db) = listener(db, retrying_at_once()).await;
    listener.start(|_| async { Ok(()) }).await.unwrap();

    within(until_finished(&listener)).await;

    assert!(matches!(
        within(listener.shutdown()).await,
        Err(FirestoreError::DatabaseError(err)) if err.public.code == "InvalidArgument"
    ));
    assert_eq!(db.connections.load(Ordering::Relaxed), 2);
    assert_eq!(
        *db.forgotten.lock().unwrap(),
        vec![FirestoreListenerTarget::new(1)]
    );
}

#[tokio::test]
async fn a_target_that_cannot_be_listened_on_is_rejected_when_added() {
    let server = listen_server().await;
    let listener = FirestoreListener::new(
        server.db.clone(),
        FirestoreMemListenStateStorage::new(),
        never_retrying(),
    )
    .await
    .unwrap();

    let added = listener.add_target(target_params_for_document(1, "not/a-document-id"));

    assert!(matches!(
        added,
        Err(FirestoreError::InvalidParametersError(_))
    ));
    assert!(listener.targets().is_empty());
}

#[tokio::test]
async fn a_target_added_to_a_running_listener_joins_the_open_stream() {
    let db = TestDb {
        events: Vec::new(),
        ..TestDb::new(Wait::Connect)
    };
    let (mut listener, db) = started(db, never_retrying()).await;
    let mut requests = db.take_requests();
    db.release.add_permits(1);

    listener.add_target(target_params(2)).unwrap();

    assert_eq!(added_targets(&mut requests, 2).await, vec![1, 2]);
    assert_eq!(db.connections.load(Ordering::Relaxed), 1);
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_target_removed_and_added_again_waits_for_firestore_to_acknowledge_the_removal() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let (mut listener, db) = started(db, never_retrying()).await;
    let mut requests = db.take_requests();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-1"))
    );

    listener
        .remove_target(&FirestoreListenerTarget::new(1))
        .await
        .unwrap();
    assert_eq!(
        next_request(&mut requests).await,
        Some(RequestedChange::RemoveTarget(1))
    );
    listener
        .add_target(target_params_for_document(1, "document-other"))
        .unwrap();
    feed.send(target_change(TargetChangeType::Add, vec![1]))
        .unwrap();
    db.settle(&feed).await;
    assert!(requests.try_recv().is_err());

    feed.send(target_change(TargetChangeType::Remove, vec![1]))
        .unwrap();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-other"))
    );
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_removal_cancelled_while_forgetting_its_resume_state_still_reaches_the_stream() {
    let (db, feed) = TestDb::fed(Wait::Forget);
    let (mut listener, db) = started(db, never_retrying()).await;
    let mut requests = db.take_requests();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-1"))
    );

    let target = FirestoreListenerTarget::new(1);
    {
        let removal = listener.remove_target(&target);
        futures::pin_mut!(removal);
        assert!(futures::poll!(removal.as_mut()).is_pending());
    }
    listener
        .add_target(target_params_for_document(1, "document-other"))
        .unwrap();

    assert_eq!(
        next_request(&mut requests).await,
        Some(RequestedChange::RemoveTarget(1))
    );
    feed.send(target_change(TargetChangeType::Remove, vec![1]))
        .unwrap();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-other"))
    );
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_removal_crossed_by_firestore_removing_the_target_reconnects_at_once_on_not_found() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let (mut listener, db) = started(db, never_retrying()).await;
    let mut requests = db.take_requests();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-1"))
    );
    feed.send(target_change(TargetChangeType::Add, vec![1]))
        .unwrap();

    listener
        .remove_target(&FirestoreListenerTarget::new(1))
        .await
        .unwrap();
    assert_eq!(
        next_request(&mut requests).await,
        Some(RequestedChange::RemoveTarget(1))
    );
    listener
        .add_target(target_params_for_document(1, "document-other"))
        .unwrap();
    feed.send(removal_with_cause(vec![1], Code::Unavailable))
        .unwrap();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-other"))
    );

    // Firestore's answer to a `RemoveTarget` for a target it has already removed.
    db.fail_stream(Code::NotFound);
    within(db.entered.acquire()).await.unwrap().forget();
    let mut requests = db.take_requests();

    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-other"))
    );
    assert_eq!(db.connections.load(Ordering::Relaxed), 2);
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_not_found_stream_error_without_a_crossed_removal_waits_for_the_retry_delay() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let (mut listener, db) = started(db, never_retrying()).await;
    db.settle(&feed).await;

    db.fail_stream(Code::NotFound);

    assert!(
        tokio::time::timeout(Duration::from_millis(200), db.entered.acquire())
            .await
            .is_err()
    );
    assert_eq!(db.connections.load(Ordering::Relaxed), 1);
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_target_firestore_removes_is_added_back_alone_on_the_open_stream() {
    let db = TestDb {
        events: vec![removal_with_cause(vec![1], Code::Unavailable)],
        ..TestDb::new(Wait::Connect)
    };
    let (mut listener, db) = listener(db, retrying_at_once()).await;
    listener.add_target(target_params(2)).unwrap();
    listener.start(|_| async { Ok(()) }).await.unwrap();
    within(db.entered.acquire()).await.unwrap().forget();
    let mut requests = db.take_requests();
    db.release.add_permits(1);

    assert_eq!(added_targets(&mut requests, 3).await, vec![1, 1, 2]);
    assert!(requests.try_recv().is_err());
    assert_eq!(db.connections.load(Ordering::Relaxed), 1);
    assert_eq!(
        *db.forgotten.lock().unwrap(),
        vec![FirestoreListenerTarget::new(1)]
    );
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_target_firestore_removes_for_good_is_unregistered_and_not_added_back() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let (mut listener, db) = listener(db, retrying_at_once()).await;
    listener.add_target(target_params(2)).unwrap();
    start(&mut listener, &db).await;
    let mut requests = db.take_requests();
    assert_eq!(added_targets(&mut requests, 2).await, vec![1, 2]);

    feed.send(removal_with_cause(vec![1], Code::PermissionDenied))
        .unwrap();
    db.settle(&feed).await;

    assert!(!listener.has_target(&FirestoreListenerTarget::new(1)));
    assert!(requests.try_recv().is_err());
    assert_eq!(
        *db.forgotten.lock().unwrap(),
        vec![FirestoreListenerTarget::new(1)]
    );
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn the_callback_sees_whether_a_removed_target_will_be_added_back() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let (mut listener, db) = listener(db, never_retrying()).await;
    listener.add_target(target_params(2)).unwrap();
    let handle = listener.control_handle();
    let registered_at_removal = Arc::new(Mutex::new(Vec::new()));
    let callback_registered_at_removal = registered_at_removal.clone();
    let callback_db = db.clone();
    listener
        .start(move |event| {
            if let FirestoreListenEvent::TargetChange(change) = &event {
                if change.target_change_type() == TargetChangeType::Remove {
                    for target_id in &change.target_ids {
                        let target = FirestoreListenerTarget::try_from(*target_id).unwrap();
                        let registered = handle.has_target(&target);
                        callback_registered_at_removal
                            .lock()
                            .unwrap()
                            .push((*target_id, registered));
                    }
                }
            }
            callback_db.seen.send_modify(|seen| seen.push(event));
            async { Ok(()) }
        })
        .await
        .unwrap();
    within(db.entered.acquire()).await.unwrap().forget();
    let mut requests = db.take_requests();
    assert_eq!(added_targets(&mut requests, 2).await, vec![1, 2]);

    feed.send(removal_with_cause(vec![1], Code::PermissionDenied))
        .unwrap();
    feed.send(removal_with_cause(vec![2], Code::Unavailable))
        .unwrap();
    db.settle(&feed).await;

    assert_eq!(
        *registered_at_removal.lock().unwrap(),
        vec![(1, false), (2, true)]
    );
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_target_the_callback_removes_through_the_control_handle_leaves_the_stream() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let (mut listener, db) = listener(db, never_retrying()).await;
    let handle = listener.control_handle();
    listener
        .start(move |event| {
            let handle = handle.clone();
            async move {
                if event == target_change(TargetChangeType::Current, vec![1]) {
                    handle
                        .remove_target(&FirestoreListenerTarget::new(1))
                        .await
                        .unwrap();
                }
                Ok(())
            }
        })
        .await
        .unwrap();
    within(db.entered.acquire()).await.unwrap().forget();
    let mut requests = db.take_requests();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-1"))
    );

    feed.send(target_change(TargetChangeType::Add, vec![1]))
        .unwrap();
    feed.send(target_change(TargetChangeType::Current, vec![1]))
        .unwrap();

    assert_eq!(
        next_request(&mut requests).await,
        Some(RequestedChange::RemoveTarget(1))
    );
    assert_eq!(
        *db.forgotten.lock().unwrap(),
        vec![FirestoreListenerTarget::new(1)]
    );
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_target_the_callback_removes_on_its_transient_removal_is_not_added_back() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let (mut listener, db) = listener(db, retrying_at_once()).await;
    listener.add_target(target_params(2)).unwrap();
    let handle = listener.control_handle();
    let removed_by_callback = Arc::new(Mutex::new(Vec::new()));
    let callback_removed = removed_by_callback.clone();
    let callback_db = db.clone();
    listener
        .start(move |event| {
            let handle = handle.clone();
            let callback_removed = callback_removed.clone();
            let callback_db = callback_db.clone();
            async move {
                if event == removal_with_cause(vec![1], Code::Unavailable) {
                    let removed = handle
                        .remove_target(&FirestoreListenerTarget::new(1))
                        .await
                        .unwrap();
                    callback_removed.lock().unwrap().push(removed);
                }
                callback_db.seen.send_modify(|seen| seen.push(event));
                Ok(())
            }
        })
        .await
        .unwrap();
    within(db.entered.acquire()).await.unwrap().forget();
    let mut requests = db.take_requests();
    assert_eq!(added_targets(&mut requests, 2).await, vec![1, 2]);

    feed.send(removal_with_cause(vec![1], Code::Unavailable))
        .unwrap();
    db.settle(&feed).await;

    assert_eq!(*removed_by_callback.lock().unwrap(), vec![true]);
    assert!(requests.try_recv().is_err());
    assert_eq!(
        *db.forgotten.lock().unwrap(),
        vec![FirestoreListenerTarget::new(1)]
    );
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_once_target_ends_with_its_removal() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let (mut listener, db) = listener(db, retrying_at_once()).await;
    listener
        .add_target(target_params(2).with_add_target_once(true))
        .unwrap();
    start(&mut listener, &db).await;
    let mut requests = db.take_requests();
    assert_eq!(added_targets(&mut requests, 2).await, vec![1, 2]);

    feed.send(target_change(TargetChangeType::Add, vec![2]))
        .unwrap();
    feed.send(target_change(TargetChangeType::Current, vec![2]))
        .unwrap();
    feed.send(target_change(TargetChangeType::Remove, vec![2]))
        .unwrap();
    db.settle(&feed).await;

    assert!(!listener.has_target(&FirestoreListenerTarget::new(2)));
    assert!(requests.try_recv().is_err());
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_once_target_ending_across_our_removal_reconnects_at_once_on_not_found() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let mut listener = FirestoreListener::new(db.clone(), db.clone(), never_retrying())
        .await
        .unwrap();
    listener
        .add_target(target_params(1).with_add_target_once(true))
        .unwrap();
    start(&mut listener, &db).await;
    let mut requests = db.take_requests();
    assert_eq!(added_targets(&mut requests, 1).await, vec![1]);
    feed.send(target_change(TargetChangeType::Add, vec![1]))
        .unwrap();

    listener
        .remove_target(&FirestoreListenerTarget::new(1))
        .await
        .unwrap();
    assert_eq!(
        next_request(&mut requests).await,
        Some(RequestedChange::RemoveTarget(1))
    );
    listener
        .add_target(target_params_for_document(1, "document-other"))
        .unwrap();
    // The once target ends before Firestore reads our `RemoveTarget`.
    feed.send(target_change(TargetChangeType::Current, vec![1]))
        .unwrap();
    feed.send(target_change(TargetChangeType::Remove, vec![1]))
        .unwrap();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-other"))
    );

    db.fail_stream(Code::NotFound);
    within(db.entered.acquire()).await.unwrap().forget();
    let mut requests = db.take_requests();

    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-other"))
    );
    assert_eq!(db.connections.load(Ordering::Relaxed), 2);
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_once_target_removal_acknowledged_before_current_leaves_a_later_not_found_to_the_retry_delay(
) {
    let (db, feed) = TestDb::fed(Wait::Never);
    let (mut listener, db) = listener(db, never_retrying()).await;
    listener
        .add_target(target_params(2).with_add_target_once(true))
        .unwrap();
    start(&mut listener, &db).await;
    let mut requests = db.take_requests();
    assert_eq!(added_targets(&mut requests, 2).await, vec![1, 2]);
    feed.send(target_change(TargetChangeType::Add, vec![1, 2]))
        .unwrap();

    listener
        .remove_target(&FirestoreListenerTarget::new(2))
        .await
        .unwrap();
    assert_eq!(
        next_request(&mut requests).await,
        Some(RequestedChange::RemoveTarget(2))
    );
    feed.send(target_change(TargetChangeType::Remove, vec![2]))
        .unwrap();
    db.settle(&feed).await;

    db.fail_stream(Code::NotFound);

    assert!(
        tokio::time::timeout(Duration::from_millis(200), db.entered.acquire())
            .await
            .is_err()
    );
    assert_eq!(db.connections.load(Ordering::Relaxed), 1);
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_removal_crossed_by_a_stream_wide_removal_reconnects_at_once_on_not_found() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let (mut listener, db) = listener(db, never_retrying()).await;
    listener.add_target(target_params(2)).unwrap();
    start(&mut listener, &db).await;
    let mut requests = db.take_requests();
    assert_eq!(added_targets(&mut requests, 2).await, vec![1, 2]);
    feed.send(target_change(TargetChangeType::Add, vec![1, 2]))
        .unwrap();

    listener
        .remove_target(&FirestoreListenerTarget::new(1))
        .await
        .unwrap();
    assert_eq!(
        next_request(&mut requests).await,
        Some(RequestedChange::RemoveTarget(1))
    );
    feed.send(removal_with_cause(Vec::new(), Code::Unavailable))
        .unwrap();
    db.settle(&feed).await;

    db.fail_stream(Code::NotFound);
    within(db.entered.acquire()).await.unwrap().forget();
    let mut requests = db.take_requests();

    assert_eq!(added_targets(&mut requests, 1).await, vec![2]);
    assert_eq!(db.connections.load(Ordering::Relaxed), 2);
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_current_once_target_is_removed_without_a_remove_target() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let mut listener = FirestoreListener::new(db.clone(), db.clone(), never_retrying())
        .await
        .unwrap();
    listener
        .add_target(target_params(1).with_add_target_once(true))
        .unwrap();
    start(&mut listener, &db).await;
    let mut requests = db.take_requests();
    assert_eq!(added_targets(&mut requests, 1).await, vec![1]);
    feed.send(target_change(TargetChangeType::Add, vec![1]))
        .unwrap();
    feed.send(target_change(TargetChangeType::Current, vec![1]))
        .unwrap();
    db.settle(&feed).await;

    listener
        .remove_target(&FirestoreListenerTarget::new(1))
        .await
        .unwrap();
    listener
        .add_target(target_params_for_document(1, "document-other"))
        .unwrap();
    db.settle(&feed).await;
    assert!(requests.try_recv().is_err());

    // The once target's end completes the removal.
    feed.send(target_change(TargetChangeType::Remove, vec![1]))
        .unwrap();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-other"))
    );
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_resumed_target_firestore_rejects_as_failed_precondition_is_added_back_from_scratch() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let mut listener = FirestoreListener::new(db.clone(), db.clone(), retrying_at_once())
        .await
        .unwrap();
    listener
        .add_target(
            target_params(1)
                .with_resume_type(FirestoreListenerTargetResumeType::Token(vec![1].into())),
        )
        .unwrap();
    start(&mut listener, &db).await;
    let mut requests = db.take_requests();
    assert_eq!(added_targets(&mut requests, 1).await, vec![1]);

    feed.send(removal_with_cause(vec![1], Code::FailedPrecondition))
        .unwrap();

    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-1"))
    );
    assert!(listener.has_target(&FirestoreListenerTarget::new(1)));
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_reset_keeps_the_resume_state_of_a_target_registered_again_during_its_callback() {
    let (db, feed) = TestDb::fed(Wait::Callback);
    let (mut listener, db) = listener(db, never_retrying()).await;
    feed.send(target_change(TargetChangeType::Add, vec![1]))
        .unwrap();
    start(&mut listener, &db).await;
    let mut requests = db.take_requests();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-1"))
    );
    feed.send(target_change(TargetChangeType::Reset, vec![1]))
        .unwrap();
    db.release.add_permits(1);
    within(db.entered.acquire()).await.unwrap().forget();

    listener
        .remove_target(&FirestoreListenerTarget::new(1))
        .await
        .unwrap();
    listener
        .add_target(
            target_params_for_document(1, "document-other")
                .with_resume_type(FirestoreListenerTargetResumeType::Token(vec![42].into())),
        )
        .unwrap();
    db.release.add_permits(Semaphore::MAX_PERMITS / 2);
    assert_eq!(
        next_request(&mut requests).await,
        Some(RequestedChange::RemoveTarget(1))
    );
    feed.send(target_change(TargetChangeType::Remove, vec![1]))
        .unwrap();

    match next_request(&mut requests).await {
        Some(RequestedChange::AddTarget(target)) => assert_eq!(
            target.resume_type,
            Some(target::ResumeType::ResumeToken(vec![42]))
        ),
        other => panic!("expected an AddTarget, got {other:?}"),
    }
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_resynchronised_target_is_removed_and_added_back_from_scratch() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let (mut listener, db) = started(db, retrying_at_once()).await;
    let mut requests = db.take_requests();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-1"))
    );
    feed.send(target_change(TargetChangeType::Add, vec![1]))
        .unwrap();

    listener
        .control_handle()
        .resync_target(FirestoreListenerTarget::new(1));
    assert_eq!(
        next_request(&mut requests).await,
        Some(RequestedChange::RemoveTarget(1))
    );
    feed.send(target_change(TargetChangeType::Remove, vec![1]))
        .unwrap();

    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-1"))
    );
    assert_eq!(
        *db.forgotten.lock().unwrap(),
        vec![FirestoreListenerTarget::new(1)]
    );
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_reconnect_adds_every_registered_target_whatever_its_transition() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let (mut listener, db) = listener(db, retrying_at_once()).await;
    listener.add_target(target_params(2)).unwrap();
    start(&mut listener, &db).await;
    let mut requests = db.take_requests();
    assert_eq!(added_targets(&mut requests, 2).await, vec![1, 2]);
    feed.send(target_change(TargetChangeType::Add, vec![1, 2]))
        .unwrap();

    listener
        .remove_target(&FirestoreListenerTarget::new(1))
        .await
        .unwrap();
    assert_eq!(
        next_request(&mut requests).await,
        Some(RequestedChange::RemoveTarget(1))
    );
    listener
        .add_target(target_params_for_document(1, "document-other"))
        .unwrap();
    listener.add_target(target_params(3)).unwrap();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(3, "document-3"))
    );

    drop(feed);
    within(db.entered.acquire()).await.unwrap().forget();
    let mut requests = db.take_requests();
    let mut added = Vec::new();
    for _ in 0..3 {
        added.extend(next_request(&mut requests).await);
    }
    added.sort_by_key(|change| match change {
        RequestedChange::AddTarget(target) => target.target_id,
        RequestedChange::RemoveTarget(target_id) => *target_id,
    });

    assert_eq!(
        added,
        vec![
            add_request(1, "document-other"),
            add_request(2, "document-2"),
            add_request(3, "document-3"),
        ]
    );
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_listener_whose_last_target_is_removed_while_it_is_added_does_not_connect() {
    let (mut listener, db) = started(TestDb::new(Wait::Read), never_retrying()).await;

    listener
        .remove_target(&FirestoreListenerTarget::new(1))
        .await
        .unwrap();
    db.release.add_permits(1);
    listener.add_target(target_params(2)).unwrap();
    within(db.entered.acquire()).await.unwrap().forget();

    assert_eq!(db.connections.load(Ordering::Relaxed), 0);
    db.release.add_permits(1);
    within(listener.shutdown()).await.unwrap();
}

#[tokio::test]
async fn a_listener_closes_its_stream_once_its_last_target_is_removed() {
    let (db, feed) = TestDb::fed(Wait::Never);
    let (mut listener, db) = started(db, never_retrying()).await;
    let mut requests = db.take_requests();
    assert_eq!(
        next_request(&mut requests).await,
        Some(add_request(1, "document-1"))
    );

    listener
        .remove_target(&FirestoreListenerTarget::new(1))
        .await
        .unwrap();
    assert_eq!(
        next_request(&mut requests).await,
        Some(RequestedChange::RemoveTarget(1))
    );
    feed.send(target_change(TargetChangeType::Remove, vec![1]))
        .unwrap();

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), requests.recv()).await,
        Ok(None)
    ));
    within(listener.shutdown()).await.unwrap();
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
    let (_requests, request_reader) = tokio::sync::mpsc::unbounded_channel();
    let response = within(server.db.listen_doc_changes(request_reader))
        .await
        .unwrap();
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
    let (_requests, request_reader) = tokio::sync::mpsc::unbounded_channel();
    let mut response = within(server.db.listen_doc_changes(request_reader))
        .await
        .unwrap();
    assert!(within(response.next()).await.is_none());
    within(server.wait_for_calls(1)).await;
    assert_eq!(server.calls(), vec!["Listen closed"]);
}
