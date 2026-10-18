//! A listener's targets against the real Firestore: targets added to and removed from an open
//! stream, Firestore's acknowledgements of them, and the targets Firestore ends on its own.

mod common;

use crate::common::{eventually_async, setup};
use firestore::gcloud_sdk::google::firestore::v1::{
    listen_request, target, ListenRequest, StructuredQuery, Target, TargetChange,
};
use firestore::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use tracing::info;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const TEST_COLLECTION_NAME: &str = "integration-test-crud";
const LISTENER_RETRY_DELAY: Duration = Duration::from_secs(1);
const FIRESTORE_ANSWER_TIMEOUT: Duration = Duration::from_secs(60);
/// The first wait between checks for Firestore's answer; `eventually_async` grows it
/// quadratically.
const FIRESTORE_ANSWER_POLL: Duration = Duration::from_millis(100);
/// About `FIRESTORE_ANSWER_TIMEOUT` of checks from `FIRESTORE_ANSWER_POLL`.
const FIRESTORE_ANSWER_RETRIES: u32 = 12;
/// Several retry delays long, so that a target the listener adds back would show up within it.
const LISTENER_QUIET_PERIOD: Duration = Duration::from_secs(5);
/// A field no index covers: ordering by it after an equality filter on another field needs a
/// composite index, which this collection does not have, so Firestore rejects the query.
const UNINDEXED_ORDER_FIELD: &str = "listen_targets_unindexed_rank";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaggedDocument {
    tag: String,
    revision: u32,
}

/// Every event the listener's callback received, in order.
#[derive(Clone, Default)]
struct RecordedEvents(Arc<Mutex<Vec<FirestoreListenEvent>>>);

impl RecordedEvents {
    fn snapshot(&self) -> Vec<FirestoreListenEvent> {
        self.0.lock().unwrap().clone()
    }

    fn record(&self, event: FirestoreListenEvent) {
        self.0.lock().unwrap().push(event);
    }
}

type TestListener = FirestoreListener<FirestoreDb, FirestoreMemListenStateStorage>;

/// A document ID no other run uses, so that concurrent runs do not see each other's writes.
fn unique_doc_id(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past the Unix epoch")
        .as_nanos();
    format!("listen-targets-{name}-{nanos}")
}

async fn start_listener(
    db: &FirestoreDb,
    retry_delay: Duration,
    targets: Vec<FirestoreListenerTargetParams>,
) -> TestResult<(TestListener, RecordedEvents)> {
    let mut listener = db
        .create_listener_with_params(
            FirestoreMemListenStateStorage::new(),
            FirestoreListenerParams::new().with_retry_delay(retry_delay),
        )
        .await?;
    for target in targets {
        listener.add_target(target)?;
    }
    let events = RecordedEvents::default();
    let recorder = events.clone();
    listener
        .start(move |event| {
            recorder.record(event);
            async { Ok(()) }
        })
        .await?;
    Ok((listener, events))
}

fn document_target(target: i32, doc_id: &str) -> FirestoreResult<FirestoreListenerTargetParams> {
    Ok(FirestoreListenerTargetParams::new(
        FirestoreListenerTarget::try_from(target)?,
        FirestoreTargetType::Documents(FirestoreCollectionDocuments::new(
            TEST_COLLECTION_NAME.to_string(),
            vec![doc_id.to_string()],
        )),
        HashMap::new(),
    ))
}

fn tag_query(tag: &str) -> FirestoreQueryParams {
    FirestoreQueryParams::new(TEST_COLLECTION_NAME.into()).with_filter(
        FirestoreQueryFilter::Compare(Some(FirestoreQueryFilterCompare::Equal(
            "tag".to_string(),
            tag.into(),
        ))),
    )
}

fn tag_query_target(target: i32, tag: &str) -> FirestoreResult<FirestoreListenerTargetParams> {
    Ok(FirestoreListenerTargetParams::new(
        FirestoreListenerTarget::try_from(target)?,
        FirestoreTargetType::Query(tag_query(tag)),
        HashMap::new(),
    ))
}

/// A query Firestore can only answer with a composite index that does not exist.
fn unindexed_query(tag: &str) -> FirestoreQueryParams {
    tag_query(tag).with_order_by(vec![FirestoreQueryOrder::new(
        UNINDEXED_ORDER_FIELD.to_string(),
        FirestoreQueryDirection::Ascending,
    )])
}

async fn write_tagged(
    db: &FirestoreDb,
    doc_id: &str,
    tag: &str,
    revision: u32,
) -> FirestoreResult<()> {
    db.fluent()
        .update()
        .in_col(TEST_COLLECTION_NAME)
        .document_id(doc_id)
        .object(&TaggedDocument {
            tag: tag.to_string(),
            revision,
        })
        .execute::<()>()
        .await
}

async fn delete_documents(db: &FirestoreDb, doc_ids: &[&str]) -> FirestoreResult<()> {
    for doc_id in doc_ids {
        db.fluent()
            .delete()
            .from(TEST_COLLECTION_NAME)
            .document_id(*doc_id)
            .execute()
            .await?;
    }
    Ok(())
}

fn as_target_change(event: &FirestoreListenEvent) -> Option<&TargetChange> {
    match event {
        FirestoreListenEvent::TargetChange(change) => Some(change),
        _ => None,
    }
}

fn is_target_change(
    event: &FirestoreListenEvent,
    change_type: FirestoreListenerTargetChangeType,
    target: i32,
) -> bool {
    as_target_change(event).is_some_and(|change| {
        change.target_change_type == i32::from(change_type) && change.target_ids.contains(&target)
    })
}

fn count_target_changes(
    events: &[FirestoreListenEvent],
    change_type: FirestoreListenerTargetChangeType,
    target: i32,
) -> usize {
    events
        .iter()
        .filter(|event| is_target_change(event, change_type, target))
        .count()
}

fn is_removal(event: &FirestoreListenEvent, target: i32, with_cause: bool) -> bool {
    is_target_change(event, FirestoreListenerTargetChangeType::Remove, target)
        && as_target_change(event).is_some_and(|change| change.cause.is_some() == with_cause)
}

fn is_document_change(event: &FirestoreListenEvent, target: i32, doc_id: &str) -> bool {
    match event {
        FirestoreListenEvent::DocumentChange(change) => {
            change.target_ids.contains(&target)
                && change
                    .document
                    .as_ref()
                    .is_some_and(|document| document.name.ends_with(&format!("/{doc_id}")))
        }
        _ => false,
    }
}

/// A target added to a running listener receives changes, and is added on the open stream:
/// reopening it would add every target again, which Firestore answers with a second `ADD` and
/// `CURRENT` for the first one.
#[tokio::test]
async fn target_added_to_running_listener_receives_changes() -> TestResult {
    const FIRST_TARGET: i32 = 1;
    const ADDED_TARGET: i32 = 2;

    let db = setup().await?;
    let first_doc_id = unique_doc_id("running-first");
    let added_doc_id = unique_doc_id("running-added");

    let (mut listener, events) = start_listener(
        &db,
        LISTENER_RETRY_DELAY,
        vec![document_target(FIRST_TARGET, &first_doc_id)?],
    )
    .await?;
    let first_current = eventually_async(FIRESTORE_ANSWER_RETRIES, FIRESTORE_ANSWER_POLL, || {
        std::future::ready(Ok(count_target_changes(
            &events.snapshot(),
            FirestoreListenerTargetChangeType::Current,
            FIRST_TARGET,
        ) > 0))
    })
    .await?;
    assert!(first_current, "the first target never became current");

    let added_at = Instant::now();
    listener.add_target(document_target(ADDED_TARGET, &added_doc_id)?)?;
    let added_current = eventually_async(FIRESTORE_ANSWER_RETRIES, FIRESTORE_ANSWER_POLL, || {
        std::future::ready(Ok(count_target_changes(
            &events.snapshot(),
            FirestoreListenerTargetChangeType::Current,
            ADDED_TARGET,
        ) > 0))
    })
    .await?;
    info!(elapsed = ?added_at.elapsed(), added_current, "Waited for the added target to become current.");

    let written_at = Instant::now();
    write_tagged(&db, &added_doc_id, "added-target", 0).await?;
    let received = eventually_async(FIRESTORE_ANSWER_RETRIES, FIRESTORE_ANSWER_POLL, || {
        std::future::ready(Ok(events
            .snapshot()
            .iter()
            .any(|event| is_document_change(event, ADDED_TARGET, &added_doc_id))))
    })
    .await?;
    info!(elapsed = ?written_at.elapsed(), received, "Waited for the added target's change.");

    let stopped = listener.shutdown().await;
    delete_documents(&db, &[&added_doc_id]).await?;
    stopped?;

    assert!(
        added_current,
        "the target added to the running listener never became current"
    );
    assert!(received, "the added target never received its document");
    let events = events.snapshot();
    assert_eq!(
        count_target_changes(
            &events,
            FirestoreListenerTargetChangeType::Add,
            FIRST_TARGET
        ),
        1,
        "the stream was reopened for the first target"
    );
    assert_eq!(
        count_target_changes(
            &events,
            FirestoreListenerTargetChangeType::Current,
            FIRST_TARGET
        ),
        1,
        "the stream was reopened for the first target"
    );
    Ok(())
}

/// A removed target ID registered again with another query is added back only after Firestore
/// acknowledges the removal, from scratch, and receives only the new query's changes.
#[tokio::test]
async fn reused_target_id_follows_only_new_query() -> TestResult {
    const REUSED_TARGET: i32 = 1;

    let db = setup().await?;
    let old_doc_id = unique_doc_id("reuse-old");
    let new_doc_id = unique_doc_id("reuse-new");
    let old_tag = format!("old-{old_doc_id}");
    let new_tag = format!("new-{new_doc_id}");

    let observed = async {
        // Both written before the first registration becomes current, so that any resume token
        // it leaves behind is later than the new query's document. Resuming the new query from it
        // would leave that document out of the new registration's initial snapshot.
        write_tagged(&db, &old_doc_id, &old_tag, 0).await?;
        write_tagged(&db, &new_doc_id, &new_tag, 0).await?;

        let (mut listener, events) = start_listener(
            &db,
            LISTENER_RETRY_DELAY,
            vec![tag_query_target(REUSED_TARGET, &old_tag)?],
        )
        .await?;
        let current_count = |events: &RecordedEvents| {
            count_target_changes(
                &events.snapshot(),
                FirestoreListenerTargetChangeType::Current,
                REUSED_TARGET,
            )
        };
        let first_current =
            eventually_async(FIRESTORE_ANSWER_RETRIES, FIRESTORE_ANSWER_POLL, || {
                std::future::ready(Ok(current_count(&events) > 0))
            })
            .await?;

        // Re-registered without waiting, so that the listener has to hold the new `AddTarget`
        // back until the removal is acknowledged.
        listener
            .remove_target(&FirestoreListenerTarget::try_from(REUSED_TARGET)?)
            .await?;
        listener.add_target(tag_query_target(REUSED_TARGET, &new_tag)?)?;
        let second_current =
            eventually_async(FIRESTORE_ANSWER_RETRIES, FIRESTORE_ANSWER_POLL, || {
                std::future::ready(Ok(current_count(&events) > 1))
            })
            .await?;

        // A change of field, so that Firestore reports both writes as changes.
        write_tagged(&db, &old_doc_id, &old_tag, 1).await?;
        write_tagged(&db, &new_doc_id, &new_tag, 1).await?;
        let new_change_received =
            eventually_async(FIRESTORE_ANSWER_RETRIES, FIRESTORE_ANSWER_POLL, || {
                std::future::ready(Ok(events
                    .snapshot()
                    .iter()
                    .filter(|event| is_document_change(event, REUSED_TARGET, &new_doc_id))
                    .count()
                    > 1))
            })
            .await?;
        tokio::time::sleep(Duration::from_secs(2)).await;

        listener.shutdown().await?;
        TestResult::Ok((
            first_current,
            second_current,
            new_change_received,
            events.snapshot(),
        ))
    }
    .await;
    delete_documents(&db, &[&old_doc_id, &new_doc_id]).await?;
    let (first_current, second_current, new_change_received, events) = observed?;

    assert!(first_current, "the first registration never became current");
    assert!(
        second_current,
        "the second registration never became current"
    );
    assert!(new_change_received, "the new query's change never arrived");

    let acknowledged = events
        .iter()
        .position(|event| is_removal(event, REUSED_TARGET, false))
        .expect("Firestore acknowledged the removal before the second registration was current");
    let added_again = events
        .iter()
        .enumerate()
        .filter(|(_, event)| {
            is_target_change(event, FirestoreListenerTargetChangeType::Add, REUSED_TARGET)
        })
        .map(|(position, _)| position)
        .nth(1)
        .expect("the second registration became current, so it was added");
    let current_again = events
        .iter()
        .enumerate()
        .filter(|(_, event)| {
            is_target_change(
                event,
                FirestoreListenerTargetChangeType::Current,
                REUSED_TARGET,
            )
        })
        .map(|(position, _)| position)
        .nth(1)
        .expect("checked above");
    assert!(
        acknowledged < added_again,
        "the ID was added again before Firestore acknowledged its removal"
    );

    let second_registration = &events[added_again..];
    assert!(
        events[added_again..current_again]
            .iter()
            .any(|event| is_document_change(event, REUSED_TARGET, &new_doc_id)),
        "the new query's existing document was not in its initial snapshot, so it resumed from \
         the old query's token"
    );
    assert!(
        !second_registration.iter().any(|event| is_document_change(
            event,
            REUSED_TARGET,
            &old_doc_id
        )),
        "the reused ID received a change for the old query"
    );
    Ok(())
}

fn listen_request(db: &FirestoreDb, change: listen_request::TargetChange) -> ListenRequest {
    ListenRequest {
        database: db.get_database_path().clone(),
        target_change: Some(change),
        ..Default::default()
    }
}

/// Firestore answers a `RemoveTarget` for a target it has already removed on its own by failing
/// the whole stream with `NOT_FOUND`, not with an acknowledgement. The listener relies on this to
/// tell that failure from any other; this drives the listen protocol directly to pin it.
#[tokio::test]
async fn firestore_fails_stream_on_removal_of_target_it_ended() -> TestResult {
    const ENDED_TARGET: i32 = 1;

    let db = setup().await?;
    let (requests, mut request_reader) = tokio::sync::mpsc::unbounded_channel::<ListenRequest>();
    requests.send(listen_request(
        &db,
        listen_request::TargetChange::AddTarget(Target {
            target_id: ENDED_TARGET,
            target_type: Some(target::TargetType::Query(target::QueryTarget {
                parent: db.get_documents_path().clone(),
                query_type: Some(target::query_target::QueryType::StructuredQuery(
                    StructuredQuery::try_from(unindexed_query("never-indexed"))?,
                )),
            })),
            ..Default::default()
        }),
    ))?;

    // Firestore answers the call only once a first request has arrived, so that request is
    // queued before the call is made.
    let mut client = db.client().get();
    let call = client.listen(firestore::gcloud_sdk::tonic::Request::new(
        futures::stream::poll_fn(move |cx| request_reader.poll_recv(cx)),
    ));
    let mut responses = tokio::time::timeout(FIRESTORE_ANSWER_TIMEOUT, call)
        .await??
        .into_inner();

    loop {
        let response = tokio::time::timeout(FIRESTORE_ANSWER_TIMEOUT, responses.message())
            .await??
            .and_then(|response| response.response_type)
            .ok_or("the listen stream closed before Firestore removed the target")?;
        if is_removal(&response, ENDED_TARGET, true) {
            break;
        }
    }

    requests.send(listen_request(
        &db,
        listen_request::TargetChange::RemoveTarget(ENDED_TARGET),
    ))?;
    let mut acknowledged = false;
    let failure = loop {
        match tokio::time::timeout(FIRESTORE_ANSWER_TIMEOUT, responses.message()).await? {
            Ok(Some(response)) => {
                acknowledged = response
                    .response_type
                    .is_some_and(|response| is_removal(&response, ENDED_TARGET, false));
                if acknowledged {
                    break None;
                }
            }
            Ok(None) => break None,
            Err(status) => break Some(status.code()),
        }
    };
    drop(requests);

    assert!(
        !acknowledged,
        "Firestore acknowledged the removal of a target it had already removed"
    );
    assert_eq!(failure, Some(firestore::gcloud_sdk::tonic::Code::NotFound));
    Ok(())
}

/// A `RemoveTarget` that crosses the end of a target added once makes Firestore fail the stream
/// with `NOT_FOUND`. The listener reconnects at once rather than after its retry delay, and the
/// other target keeps receiving changes.
///
/// The callback holds the once target's `ADD` until the listener has been asked to remove it.
/// Firestore sends the rest of its answer meanwhile, ending the target, and the listener reads
/// the removal request before that answer, so its `RemoveTarget` goes out for a target Firestore
/// has already ended.
#[tokio::test]
async fn listener_reconnects_at_once_after_removal_crosses_once_target_end() -> TestResult {
    const HEALTHY_TARGET: i32 = 1;
    const ONCE_TARGET: i32 = 2;
    /// Far longer than the test runs, so a reconnect seen at all was not made after the delay.
    const LONG_RETRY_DELAY: Duration = Duration::from_secs(600);
    /// Long enough for Firestore to report the once target current and end it.
    const ONCE_TARGET_END_WAIT: Duration = Duration::from_secs(2);
    /// About nine seconds of checks: the `NOT_FOUND` and the reconnect after it.
    const RECONNECT_RETRIES: u32 = 6;

    let db = setup().await?;
    let healthy_doc_id = unique_doc_id("crossed-healthy");

    let mut listener = db
        .create_listener_with_params(
            FirestoreMemListenStateStorage::new(),
            FirestoreListenerParams::new().with_retry_delay(LONG_RETRY_DELAY),
        )
        .await?;
    listener.add_target(document_target(HEALTHY_TARGET, &healthy_doc_id)?)?;
    let events = RecordedEvents::default();
    let once_target_added = Arc::new(Notify::new());
    let once_target_released = Arc::new(Notify::new());
    {
        let events = events.clone();
        let once_target_added = once_target_added.clone();
        let once_target_released = once_target_released.clone();
        listener
            .start(move |event| {
                let held =
                    is_target_change(&event, FirestoreListenerTargetChangeType::Add, ONCE_TARGET);
                events.record(event);
                let once_target_added = once_target_added.clone();
                let once_target_released = once_target_released.clone();
                async move {
                    if held {
                        once_target_added.notify_one();
                        once_target_released.notified().await;
                    }
                    Ok(())
                }
            })
            .await?;
    }

    let healthy_current = eventually_async(FIRESTORE_ANSWER_RETRIES, FIRESTORE_ANSWER_POLL, || {
        std::future::ready(Ok(count_target_changes(
            &events.snapshot(),
            FirestoreListenerTargetChangeType::Current,
            HEALTHY_TARGET,
        ) > 0))
    })
    .await?;
    assert!(healthy_current, "the healthy target never became current");

    let once_target = FirestoreListenerTarget::try_from(ONCE_TARGET)?;
    listener.add_target(
        document_target(ONCE_TARGET, &unique_doc_id("crossed-once"))?.with_add_target_once(true),
    )?;
    tokio::time::timeout(FIRESTORE_ANSWER_TIMEOUT, once_target_added.notified()).await?;
    tokio::time::sleep(ONCE_TARGET_END_WAIT).await;
    listener.remove_target(&once_target).await?;
    let removed_at = Instant::now();
    once_target_released.notify_one();

    let reconnected = eventually_async(RECONNECT_RETRIES, FIRESTORE_ANSWER_POLL, || {
        std::future::ready(Ok(count_target_changes(
            &events.snapshot(),
            FirestoreListenerTargetChangeType::Add,
            HEALTHY_TARGET,
        ) > 1))
    })
    .await?;
    info!(elapsed = ?removed_at.elapsed(), reconnected, "Waited for the reconnect.");

    write_tagged(&db, &healthy_doc_id, "healthy", 0).await?;
    let healthy_received =
        eventually_async(FIRESTORE_ANSWER_RETRIES, FIRESTORE_ANSWER_POLL, || {
            std::future::ready(Ok(events
                .snapshot()
                .iter()
                .any(|event| is_document_change(event, HEALTHY_TARGET, &healthy_doc_id))))
        })
        .await?;

    let stopped = listener.shutdown().await;
    delete_documents(&db, &[&healthy_doc_id]).await?;
    stopped?;

    assert!(
        reconnected,
        "the listener did not reconnect at once after the crossed removal"
    );
    assert!(
        healthy_received,
        "the healthy target stopped receiving changes after the reconnect"
    );
    Ok(())
}

/// A target Firestore rejects for good ends on its own: the listener unregisters it and does not
/// add it back, while the other target on the stream keeps receiving changes.
#[tokio::test]
async fn permanently_rejected_target_ends_alone() -> TestResult {
    const HEALTHY_TARGET: i32 = 1;
    const REJECTED_TARGET: i32 = 2;

    let db = setup().await?;
    let healthy_doc_id = unique_doc_id("rejected-healthy");

    let (mut listener, events) = start_listener(
        &db,
        LISTENER_RETRY_DELAY,
        vec![
            document_target(HEALTHY_TARGET, &healthy_doc_id)?,
            FirestoreListenerTargetParams::new(
                FirestoreListenerTarget::try_from(REJECTED_TARGET)?,
                FirestoreTargetType::Query(unindexed_query(&healthy_doc_id)),
                HashMap::new(),
            ),
        ],
    )
    .await?;
    let rejected = eventually_async(FIRESTORE_ANSWER_RETRIES, FIRESTORE_ANSWER_POLL, || {
        std::future::ready(Ok(events
            .snapshot()
            .iter()
            .any(|event| is_removal(event, REJECTED_TARGET, true))))
    })
    .await?;
    let rejected_cause = events
        .snapshot()
        .iter()
        .find(|event| is_removal(event, REJECTED_TARGET, true))
        .and_then(as_target_change)
        .and_then(|change| change.cause.clone());
    info!(?rejected_cause, "Firestore removed the rejected target.");

    tokio::time::sleep(LISTENER_QUIET_PERIOD).await;
    let unregistered = !listener.has_target(&FirestoreListenerTarget::try_from(REJECTED_TARGET)?);

    write_tagged(&db, &healthy_doc_id, "healthy", 0).await?;
    let healthy_received =
        eventually_async(FIRESTORE_ANSWER_RETRIES, FIRESTORE_ANSWER_POLL, || {
            std::future::ready(Ok(events
                .snapshot()
                .iter()
                .any(|event| is_document_change(event, HEALTHY_TARGET, &healthy_doc_id))))
        })
        .await?;

    let stopped = listener.shutdown().await;
    delete_documents(&db, &[&healthy_doc_id]).await?;
    stopped?;

    assert!(rejected, "Firestore never removed the unindexed query");
    assert_eq!(
        rejected_cause.map(|cause| firestore::gcloud_sdk::tonic::Code::from(cause.code)),
        Some(firestore::gcloud_sdk::tonic::Code::FailedPrecondition)
    );
    assert!(unregistered, "the rejected target is still registered");
    let events = events.snapshot();
    assert_eq!(
        events
            .iter()
            .filter(|event| is_removal(event, REJECTED_TARGET, true))
            .count(),
        1,
        "the rejected target was added back"
    );
    assert_eq!(
        count_target_changes(
            &events,
            FirestoreListenerTargetChangeType::Add,
            HEALTHY_TARGET
        ),
        1,
        "the stream was reopened for the healthy target"
    );
    assert!(
        healthy_received,
        "the healthy target stopped receiving changes"
    );
    Ok(())
}

/// A target added once ends after its initial snapshot, and the listener does not add it back.
#[tokio::test]
async fn once_target_ends_after_initial_snapshot() -> TestResult {
    const ONCE_TARGET: i32 = 1;
    const KEEPER_TARGET: i32 = 2;

    let db = setup().await?;
    let once_doc_id = unique_doc_id("once");

    let observed = async {
        write_tagged(&db, &once_doc_id, "once", 0).await?;

        // The second target keeps the stream open, so a re-added once target would show up on it.
        let (mut listener, events) = start_listener(
            &db,
            LISTENER_RETRY_DELAY,
            vec![
                document_target(ONCE_TARGET, &once_doc_id)?.with_add_target_once(true),
                document_target(KEEPER_TARGET, &unique_doc_id("once-keeper"))?,
            ],
        )
        .await?;
        let ended = eventually_async(FIRESTORE_ANSWER_RETRIES, FIRESTORE_ANSWER_POLL, || {
            std::future::ready(Ok(events
                .snapshot()
                .iter()
                .any(|event| is_removal(event, ONCE_TARGET, false))))
        })
        .await?;
        tokio::time::sleep(LISTENER_QUIET_PERIOD).await;
        let unregistered = !listener.has_target(&FirestoreListenerTarget::try_from(ONCE_TARGET)?);

        listener.shutdown().await?;
        TestResult::Ok((ended, unregistered, events.snapshot()))
    }
    .await;
    delete_documents(&db, &[&once_doc_id]).await?;
    let (ended, unregistered, events) = observed?;

    assert!(ended, "the once target never ended");
    assert!(unregistered, "the ended once target is still registered");
    let removed_at = events
        .iter()
        .position(|event| is_removal(event, ONCE_TARGET, false))
        .expect("checked above");
    assert!(
        events[..removed_at].iter().any(|event| is_document_change(
            event,
            ONCE_TARGET,
            &once_doc_id
        )),
        "the once target ended without its initial snapshot"
    );
    assert_eq!(
        count_target_changes(&events, FirestoreListenerTargetChangeType::Add, ONCE_TARGET),
        1,
        "the once target was added back"
    );
    Ok(())
}
