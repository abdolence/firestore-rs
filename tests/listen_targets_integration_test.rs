//! A listener's targets against the real Firestore: targets added to and removed from an open
//! stream, Firestore's acknowledgements of them, and the targets Firestore ends on its own.

mod common;

use crate::common::config_env_var;
use firestore::gcloud_sdk::google::firestore::v1::{
    listen_request, listen_response, target, ListenRequest, StructuredQuery, Target, TargetChange,
};
use firestore::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::info;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const TEST_COLLECTION_NAME: &str = "integration-test-crud";
const LISTENER_RETRY_DELAY: Duration = Duration::from_secs(1);
const FIRESTORE_ANSWER_TIMEOUT: Duration = Duration::from_secs(60);
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

    /// Waits until `done` holds for the events received so far, and returns whether it did
    /// before `timeout`.
    async fn wait_for(
        &self,
        timeout: Duration,
        done: impl Fn(&[FirestoreListenEvent]) -> bool,
    ) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if done(&self.0.lock().unwrap()) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

type TestListener = FirestoreListener<FirestoreDb, FirestoreMemListenStateStorage>;

async fn connect() -> Result<FirestoreDb, Box<dyn std::error::Error + Send + Sync>> {
    // Every test in this file connects, and only the first can install the subscriber.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();
    Ok(FirestoreDb::new(&config_env_var("GCP_PROJECT")?).await?)
}

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
    targets: Vec<FirestoreListenerTargetParams>,
) -> Result<(TestListener, RecordedEvents), Box<dyn std::error::Error + Send + Sync>> {
    let mut listener = db
        .create_listener_with_params(
            FirestoreMemListenStateStorage::new(),
            FirestoreListenerParams::new().with_retry_delay(LISTENER_RETRY_DELAY),
        )
        .await?;
    for target in targets {
        listener.add_target(target)?;
    }
    let events = RecordedEvents::default();
    let recorder = events.clone();
    listener
        .start(move |event| {
            let recorder = recorder.clone();
            async move {
                recorder.0.lock().unwrap().push(event);
                Ok(())
            }
        })
        .await?;
    Ok((listener, events))
}

fn document_target(target: u32, doc_id: &str) -> FirestoreListenerTargetParams {
    FirestoreListenerTargetParams::new(
        FirestoreListenerTarget::new(target),
        FirestoreTargetType::Documents(FirestoreCollectionDocuments::new(
            TEST_COLLECTION_NAME.to_string(),
            vec![doc_id.to_string()],
        )),
        HashMap::new(),
    )
}

fn tag_query(tag: &str) -> FirestoreQueryParams {
    FirestoreQueryParams::new(TEST_COLLECTION_NAME.into()).with_filter(
        FirestoreQueryFilter::Compare(Some(FirestoreQueryFilterCompare::Equal(
            "tag".to_string(),
            tag.into(),
        ))),
    )
}

fn tag_query_target(target: u32, tag: &str) -> FirestoreListenerTargetParams {
    FirestoreListenerTargetParams::new(
        FirestoreListenerTarget::new(target),
        FirestoreTargetType::Query(tag_query(tag)),
        HashMap::new(),
    )
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

/// Starts a listener on target 1 and, once Firestore reports it current, adds target 2 to the
/// running listener and waits until Firestore reports that one current too.
async fn listen_then_add_second_target(
    db: &FirestoreDb,
    first_doc_id: &str,
    second_doc_id: &str,
) -> Result<(TestListener, RecordedEvents), Box<dyn std::error::Error + Send + Sync>> {
    let (listener, events) = start_listener(db, vec![document_target(1, first_doc_id)]).await?;
    assert!(
        events
            .wait_for(FIRESTORE_ANSWER_TIMEOUT, |events| {
                count_target_changes(events, FirestoreListenerTargetChangeType::Current, 1) > 0
            })
            .await,
        "the first target never became current"
    );

    let added_at = Instant::now();
    listener.add_target(document_target(2, second_doc_id))?;
    assert!(
        events
            .wait_for(FIRESTORE_ANSWER_TIMEOUT, |events| {
                count_target_changes(events, FirestoreListenerTargetChangeType::Current, 2) > 0
            })
            .await,
        "the target added to the running listener never became current"
    );
    info!(elapsed = ?added_at.elapsed(), "The added target became current.");
    Ok((listener, events))
}

/// A target added to a running listener receives changes without the listener restarting.
#[tokio::test]
async fn target_added_to_running_listener_receives_changes() -> TestResult {
    let db = connect().await?;
    let first_doc_id = unique_doc_id("running-first");
    let second_doc_id = unique_doc_id("running-second");

    let (mut listener, events) =
        listen_then_add_second_target(&db, &first_doc_id, &second_doc_id).await?;

    let written_at = Instant::now();
    write_tagged(&db, &second_doc_id, "added-target", 0).await?;
    let received = events
        .wait_for(FIRESTORE_ANSWER_TIMEOUT, |events| {
            events
                .iter()
                .any(|event| is_document_change(event, 2, &second_doc_id))
        })
        .await;
    info!(elapsed = ?written_at.elapsed(), received, "Waited for the added target's change.");

    let stopped = listener.shutdown().await;
    delete_documents(&db, &[&second_doc_id]).await?;

    assert!(received, "the added target never received its document");
    stopped?;
    Ok(())
}

/// Adding a target sends `AddTarget` on the open stream. Reopening the stream would add every
/// target again, which Firestore answers with a second `ADD` and `CURRENT` for the first one.
#[tokio::test]
async fn adding_target_does_not_reopen_stream() -> TestResult {
    let db = connect().await?;
    let first_doc_id = unique_doc_id("reopen-first");
    let second_doc_id = unique_doc_id("reopen-second");

    let (mut listener, events) =
        listen_then_add_second_target(&db, &first_doc_id, &second_doc_id).await?;
    tokio::time::sleep(LISTENER_QUIET_PERIOD).await;
    listener.shutdown().await?;

    let events = events.snapshot();
    assert_eq!(
        count_target_changes(&events, FirestoreListenerTargetChangeType::Add, 1),
        1
    );
    assert_eq!(
        count_target_changes(&events, FirestoreListenerTargetChangeType::Current, 1),
        1
    );
    Ok(())
}

/// A removed target ID registered again with another query is added back only after Firestore
/// acknowledges the removal, from scratch, and receives only the new query's changes.
#[tokio::test]
async fn reused_target_id_follows_only_new_query() -> TestResult {
    const REUSED_TARGET: i32 = 1;

    let db = connect().await?;
    let old_doc_id = unique_doc_id("reuse-old");
    let new_doc_id = unique_doc_id("reuse-new");
    let old_tag = format!("old-{old_doc_id}");
    let new_tag = format!("new-{new_doc_id}");
    // Both written before the first registration becomes current, so that any resume token it
    // leaves behind is later than the new query's document. Resuming the new query from it would
    // leave that document out of the new registration's initial snapshot.
    write_tagged(&db, &old_doc_id, &old_tag, 0).await?;
    write_tagged(&db, &new_doc_id, &new_tag, 0).await?;

    let (mut listener, events) = start_listener(&db, vec![tag_query_target(1, &old_tag)]).await?;
    let first_current = events
        .wait_for(FIRESTORE_ANSWER_TIMEOUT, |events| {
            count_target_changes(events, FirestoreListenerTargetChangeType::Current, 1) > 0
        })
        .await;

    // Re-registered without waiting, so that the listener has to hold the new `AddTarget` back
    // until the removal is acknowledged.
    listener
        .remove_target(&FirestoreListenerTarget::new(1))
        .await?;
    listener.add_target(tag_query_target(1, &new_tag))?;
    let second_current = events
        .wait_for(FIRESTORE_ANSWER_TIMEOUT, |events| {
            count_target_changes(events, FirestoreListenerTargetChangeType::Current, 1) > 1
        })
        .await;

    // A change of field, so that Firestore reports both writes as changes.
    write_tagged(&db, &old_doc_id, &old_tag, 1).await?;
    write_tagged(&db, &new_doc_id, &new_tag, 1).await?;
    let new_doc_changes = |events: &[FirestoreListenEvent]| {
        events
            .iter()
            .filter(|event| is_document_change(event, REUSED_TARGET, &new_doc_id))
            .count()
    };
    let new_change_received = events
        .wait_for(FIRESTORE_ANSWER_TIMEOUT, |events| {
            new_doc_changes(events) > 1
        })
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let stopped = listener.shutdown().await;
    delete_documents(&db, &[&old_doc_id, &new_doc_id]).await?;
    stopped?;

    assert!(first_current, "the first registration never became current");
    assert!(
        second_current,
        "the second registration never became current"
    );
    assert!(new_change_received, "the new query's change never arrived");

    let events = events.snapshot();
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

/// The listener counts a `RemoveTarget` crossed by Firestore removing the target on its own as
/// still owed an acknowledgement. This drives the listen protocol directly to check that
/// Firestore does acknowledge a `RemoveTarget` for a target it has already removed itself.
#[tokio::test]
async fn firestore_acknowledges_removal_of_target_it_ended() -> TestResult {
    const ENDED_TARGET: i32 = 1;
    const LATER_TARGET: i32 = 2;

    let db = connect().await?;
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

    let mut received = Vec::new();
    let mut ended_cause = None;
    while ended_cause.is_none() {
        let response = tokio::time::timeout(FIRESTORE_ANSWER_TIMEOUT, responses.message())
            .await??
            .and_then(|response| response.response_type)
            .ok_or("the listen stream closed before Firestore removed the target")?;
        if let listen_response::ResponseType::TargetChange(change) = &response {
            if is_removal(&response, ENDED_TARGET, true) {
                ended_cause = change.cause.clone();
            }
        }
        received.push(response);
    }
    let ended_received = received.len();

    requests.send(listen_request(
        &db,
        listen_request::TargetChange::RemoveTarget(ENDED_TARGET),
    ))?;
    requests.send(listen_request(
        &db,
        listen_request::TargetChange::AddTarget(Target {
            target_id: LATER_TARGET,
            target_type: Some(target::TargetType::Documents(target::DocumentsTarget {
                documents: vec![format!(
                    "{}/{TEST_COLLECTION_NAME}/{}",
                    db.get_documents_path(),
                    unique_doc_id("ack-later")
                )],
            })),
            ..Default::default()
        }),
    ))?;

    // Firestore answers a stream's requests in order, so an acknowledgement of the removal comes
    // before the answer to the later `AddTarget`. The quiet period after `CURRENT` catches one
    // that arrives late all the same.
    let mut later_current_at = None;
    loop {
        let wait = match later_current_at {
            Some(at) => LISTENER_QUIET_PERIOD.saturating_sub(Instant::elapsed(&at)),
            None => FIRESTORE_ANSWER_TIMEOUT,
        };
        let Ok(next) = tokio::time::timeout(wait, responses.message()).await else {
            break;
        };
        let Some(response) = next?.and_then(|response| response.response_type) else {
            break;
        };
        if later_current_at.is_none()
            && is_target_change(
                &response,
                FirestoreListenerTargetChangeType::Current,
                LATER_TARGET,
            )
        {
            later_current_at = Some(Instant::now());
        }
        received.push(response);
    }
    drop(requests);

    let after_removal = &received[ended_received..];
    let acknowledgement = after_removal
        .iter()
        .position(|event| is_removal(event, ENDED_TARGET, false));
    let later_added = after_removal.iter().position(|event| {
        is_target_change(event, FirestoreListenerTargetChangeType::Add, LATER_TARGET)
    });
    info!(
        ?ended_cause,
        ?acknowledgement,
        ?later_added,
        "Firestore's answer to a RemoveTarget for a target it had already removed."
    );

    assert!(
        later_current_at.is_some(),
        "the later target never became current"
    );
    assert!(
        acknowledgement.is_some(),
        "Firestore did not acknowledge the RemoveTarget for a target it had already removed"
    );
    assert!(
        acknowledgement < later_added,
        "the acknowledgement arrived after the answer to a later request"
    );
    Ok(())
}

/// A target Firestore rejects for good ends on its own: the listener unregisters it and does not
/// add it back, while the other target on the stream keeps receiving changes.
#[tokio::test]
async fn permanently_rejected_target_ends_alone() -> TestResult {
    const HEALTHY_TARGET: i32 = 1;
    const REJECTED_TARGET: i32 = 2;

    let db = connect().await?;
    let healthy_doc_id = unique_doc_id("rejected-healthy");

    let (mut listener, events) = start_listener(
        &db,
        vec![
            document_target(1, &healthy_doc_id),
            FirestoreListenerTargetParams::new(
                FirestoreListenerTarget::new(2),
                FirestoreTargetType::Query(unindexed_query(&healthy_doc_id)),
                HashMap::new(),
            ),
        ],
    )
    .await?;
    let rejected = events
        .wait_for(FIRESTORE_ANSWER_TIMEOUT, |events| {
            events
                .iter()
                .any(|event| is_removal(event, REJECTED_TARGET, true))
        })
        .await;
    let rejected_cause = events
        .snapshot()
        .iter()
        .find(|event| is_removal(event, REJECTED_TARGET, true))
        .and_then(as_target_change)
        .and_then(|change| change.cause.clone());
    info!(?rejected_cause, "Firestore removed the rejected target.");

    tokio::time::sleep(LISTENER_QUIET_PERIOD).await;
    let unregistered = !listener.has_target(&FirestoreListenerTarget::new(2));

    write_tagged(&db, &healthy_doc_id, "healthy", 0).await?;
    let healthy_received = events
        .wait_for(FIRESTORE_ANSWER_TIMEOUT, |events| {
            events
                .iter()
                .any(|event| is_document_change(event, HEALTHY_TARGET, &healthy_doc_id))
        })
        .await;

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

/// One stream carries 100 targets and still takes another one.
#[tokio::test]
async fn stream_with_hundred_targets_takes_one_more() -> TestResult {
    const INITIAL_TARGETS: u32 = 100;
    const EXTRA_TARGET: i32 = 101;

    let db = connect().await?;
    let extra_doc_id = unique_doc_id("scale-extra");

    let started_at = Instant::now();
    let (mut listener, events) = start_listener(
        &db,
        (1..=INITIAL_TARGETS)
            .map(|target| document_target(target, &unique_doc_id(&format!("scale-{target}"))))
            .collect(),
    )
    .await?;
    let current_targets = |events: &[FirestoreListenEvent]| {
        events
            .iter()
            .filter_map(as_target_change)
            .filter(|change| {
                change.target_change_type == i32::from(FirestoreListenerTargetChangeType::Current)
            })
            .flat_map(|change| change.target_ids.iter().copied())
            .collect::<HashSet<i32>>()
    };
    let all_current = events
        .wait_for(FIRESTORE_ANSWER_TIMEOUT, |events| {
            current_targets(events).len() == INITIAL_TARGETS as usize
        })
        .await;
    info!(elapsed = ?started_at.elapsed(), all_current, "Waited for the initial targets.");

    let added_at = Instant::now();
    listener.add_target(document_target(101, &extra_doc_id))?;
    let extra_added = events
        .wait_for(FIRESTORE_ANSWER_TIMEOUT, |events| {
            count_target_changes(events, FirestoreListenerTargetChangeType::Add, EXTRA_TARGET) > 0
        })
        .await;
    info!(elapsed = ?added_at.elapsed(), extra_added, "Waited for the extra target's ADD.");

    write_tagged(&db, &extra_doc_id, "scale", 0).await?;
    let extra_received = events
        .wait_for(FIRESTORE_ANSWER_TIMEOUT, |events| {
            events
                .iter()
                .any(|event| is_document_change(event, EXTRA_TARGET, &extra_doc_id))
        })
        .await;
    info!(elapsed = ?added_at.elapsed(), extra_received, "Waited for the extra target's change.");

    let stopped = listener.shutdown().await;
    delete_documents(&db, &[&extra_doc_id]).await?;
    stopped?;

    assert!(all_current, "not every initial target became current");
    assert!(extra_added, "Firestore never acknowledged the extra target");
    assert!(
        extra_received,
        "the extra target never received its document"
    );
    Ok(())
}

/// A target added once ends after its initial snapshot, and the listener does not add it back.
#[tokio::test]
async fn once_target_ends_after_initial_snapshot() -> TestResult {
    const ONCE_TARGET: i32 = 1;

    let db = connect().await?;
    let once_doc_id = unique_doc_id("once");
    write_tagged(&db, &once_doc_id, "once", 0).await?;

    // The second target keeps the stream open, so a re-added once target would show up on it.
    let (mut listener, events) = start_listener(
        &db,
        vec![
            document_target(1, &once_doc_id).with_add_target_once(true),
            document_target(2, &unique_doc_id("once-keeper")),
        ],
    )
    .await?;
    let ended = events
        .wait_for(FIRESTORE_ANSWER_TIMEOUT, |events| {
            events
                .iter()
                .any(|event| is_removal(event, ONCE_TARGET, false))
        })
        .await;
    tokio::time::sleep(LISTENER_QUIET_PERIOD).await;
    let unregistered = !listener.has_target(&FirestoreListenerTarget::new(1));

    let stopped = listener.shutdown().await;
    delete_documents(&db, &[&once_doc_id]).await?;
    stopped?;

    assert!(ended, "the once target never ended");
    assert!(unregistered, "the ended once target is still registered");
    let events = events.snapshot();
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
