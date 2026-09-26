use super::*;
use crate::db::fake_firestore::{begin_response, read_transaction_id, FakeFirestore, FakeResponse};
use crate::FirestoreGetByIdSupport;
use gcloud_sdk::google::firestore::v1::{CommitRequest, Document, RollbackRequest};
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::Code;
use std::sync::atomic::AtomicU8;

/// Begins transactions with sequential IDs and answers every read, rolling back with
/// `rollback_code` and committing with `commit_code`. A rollback's call-log entry carries its
/// request tags, so a test can assert them without separate bookkeeping. One retry keeps a
/// persistently failing commit short.
async fn fixture(rollback_code: Code, commit_code: Code) -> FakeFirestore {
    let begins = AtomicU8::new(0);
    FakeFirestore::start_with_max_retries(1, move |method, bytes| {
        if method.ends_with("/BeginTransaction") {
            begin_response(&begins)
        } else if method.ends_with("/GetDocument") {
            (
                format!("Get({})", read_transaction_id(bytes)),
                FakeResponse::Message(Document::default().encode_to_vec()),
            )
        } else if method.ends_with("/Rollback") {
            let request = RollbackRequest::decode(bytes).unwrap();
            let tags = request
                .request_options
                .map(|options| options.request_tags)
                .unwrap_or_default();
            let label = if tags.is_empty() {
                format!("Rollback({})", request.transaction[0])
            } else {
                format!("Rollback({}, tags={tags:?})", request.transaction[0])
            };
            let response = if rollback_code == Code::Ok {
                FakeResponse::empty()
            } else {
                FakeResponse::Status(rollback_code)
            };
            (label, response)
        } else {
            let request = CommitRequest::decode(bytes).unwrap();
            let call = format!("Commit({})", request.transaction[0]);
            let response = if commit_code == Code::Ok {
                FakeResponse::committed()
            } else {
                FakeResponse::Status(commit_code)
            };
            (call, response)
        }
    })
    .await
}

// The fixture's two rollback answers are what the table below relies on: one a success, the
// other a failure.
#[tokio::test]
async fn fixture_rollback_answers_succeed_and_fail() {
    for (rollback_code, succeeds) in [(Code::Ok, true), (Code::Unavailable, false)] {
        let server = fixture(rollback_code, Code::Ok).await;
        let transaction = server.db.begin_transaction().await.unwrap();
        let result = transaction.rollback().await;
        assert_eq!(result.is_ok(), succeeds, "{rollback_code:?}: {result:?}");
    }
}

// A rollback failing must not replace the callback's own error, so the same assertions run
// whether Firestore accepts or rejects the cleanup rollback.
#[tokio::test]
async fn callback_failures_roll_back_each_attempt_before_retrying() {
    for rollback_code in [Code::Ok, Code::Unavailable] {
        let server = fixture(rollback_code, Code::Ok).await;
        let options = FirestoreTransactionOptions::new()
            .with_request_options(FirestoreRequestOptions::from_tags(["cleanup"]));
        let result: FirestoreResult<()> = server
            .db
            .run_transaction_with_options(
                |db, transaction| {
                    Box::pin(async move {
                        // A transaction-scoped read holds a lock even with no queued writes.
                        db.get_doc("items", "one", None).await.unwrap();
                        let error = std::io::Error::other("callback failed");
                        if transaction.transaction_id() == &[1] {
                            Err(BackoffError::retry_after(error, Duration::ZERO))
                        } else {
                            Err(BackoffError::permanent(error))
                        }
                    })
                },
                options,
            )
            .await;
        let Err(FirestoreError::ErrorInTransaction(error)) = result else {
            panic!("expected a wrapped callback error, got {rollback_code:?}");
        };
        assert_eq!(error.transaction_id, vec![2]);
        assert_eq!(error.source.to_string(), "callback failed");
        assert_eq!(
            server.calls(),
            vec![
                "Begin→1",
                "Get(1)",
                "Rollback(1, tags=[\"cleanup\"])",
                "Begin→2",
                "Get(2)",
                "Rollback(2, tags=[\"cleanup\"])",
            ]
        );
    }
}

// A callback that always fails transiently gets `1 + max_retries` attempts, whether it names
// its own `retry_after` or leaves the delay to the backoff, and the last one is rolled back too.
#[tokio::test]
async fn exhausted_retries_roll_back_the_last_attempt() {
    for retry_after in [None, Some(Duration::ZERO)] {
        let server = fixture(Code::Ok, Code::Ok).await;
        let options = FirestoreTransactionOptions::new().with_max_retries(2);
        let result: FirestoreResult<()> = tokio::time::timeout(
            Duration::from_secs(10),
            server.db.run_transaction_with_options(
                move |_, _| {
                    Box::pin(async move {
                        let err = std::io::Error::other("retry");
                        Err(match retry_after {
                            Some(delay) => BackoffError::retry_after(err, delay),
                            None => BackoffError::transient(err),
                        })
                    })
                },
                options,
            ),
        )
        .await
        .expect("retries must be bounded by max_retries");
        assert!(
            matches!(result, Err(FirestoreError::ErrorInTransaction(_))),
            "{retry_after:?}: {result:?}"
        );
        assert_eq!(
            server.calls(),
            vec![
                "Begin→1",
                "Rollback(1)",
                "Begin→2",
                "Rollback(2)",
                "Begin→3",
                "Rollback(3)"
            ],
            "{retry_after:?}"
        );
    }
}

#[tokio::test]
async fn failed_commit_in_the_retry_loop_opens_no_extra_transaction() {
    let server = fixture(Code::Ok, Code::Unavailable).await;
    let options =
        FirestoreTransactionOptions::new().with_max_elapsed_time(crate::FirestoreDuration::ZERO);
    let result: FirestoreResult<()> = server
        .db
        .run_transaction_with_options(
            |_, transaction| {
                Box::pin(async move {
                    if transaction.transaction_id() == &[1] {
                        Err(BackoffError::retry_after(
                            std::io::Error::other("retry"),
                            Duration::ZERO,
                        ))
                    } else {
                        Ok(())
                    }
                })
            },
            options,
        )
        .await;
    assert!(
        matches!(result, Err(FirestoreError::DatabaseError(ref error)) if error.public.code == "Unavailable")
    );
    assert_eq!(
        server.calls(),
        vec!["Begin→1", "Rollback(1)", "Begin→2", "Commit(2)"]
    );
}

// A callback's `retry_after` is the delay before the attempt that retries it, the first
// retry included.
#[tokio::test]
async fn retry_after_delays_the_next_attempt() {
    let server = fixture(Code::Ok, Code::Ok).await;
    let retry_after = Duration::from_millis(300);
    let started = std::time::Instant::now();
    let result: FirestoreResult<()> = server
        .db
        .run_transaction(move |_, transaction| {
            Box::pin(async move {
                if transaction.transaction_id() == &[1] {
                    Err(BackoffError::retry_after(
                        std::io::Error::other("busy"),
                        retry_after,
                    ))
                } else {
                    Ok(())
                }
            })
        })
        .await;
    let elapsed = started.elapsed();
    result.unwrap();
    assert!(elapsed >= retry_after, "retried after {elapsed:?}");
    assert_eq!(
        server.calls(),
        vec!["Begin→1", "Rollback(1)", "Begin→2", "Commit(2)"]
    );
}
