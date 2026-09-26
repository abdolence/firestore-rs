use super::*;
use crate::db::fake_firestore::{FakeFirestore, FakeResponse};
use gcloud_sdk::google::firestore::v1::{BeginTransactionResponse, CommitResponse};
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::Code;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// One queued answer to a `Commit` RPC: a real response, an error code, or a connection dropped
/// mid-call - the ambiguous case where the writes may already have landed.
#[derive(Clone, Copy, Debug)]
enum CommitOutcome {
    Ok,
    Status(Code),
    Drop,
}

/// `begin_failure` is the one allowed `BeginTransaction` failure, as (attempt number, code).
async fn run_transaction(
    commits: &[CommitOutcome],
    begin_failure: Option<(u32, Code)>,
    transient_callback: bool,
) -> (FirestoreResult<usize>, usize, usize) {
    let commit_results = Mutex::new(VecDeque::from(commits.to_vec()));
    let begin_count = AtomicU32::new(0);
    let server = FakeFirestore::start(move |method, bytes| {
        if method.ends_with("/BeginTransaction") {
            let count = begin_count.fetch_add(1, Ordering::SeqCst) + 1;
            let code = begin_failure
                .filter(|(attempt, _)| *attempt == count)
                .map_or(Code::Ok, |(_, code)| code);
            let response = if code == Code::Ok {
                FakeResponse::Message(
                    BeginTransactionResponse {
                        transaction: count.to_be_bytes().to_vec(),
                    }
                    .encode_to_vec(),
                )
            } else {
                FakeResponse::Status(code)
            };
            (format!("Begin({count}, {code:?})"), response)
        } else if method.ends_with("/Commit") {
            let request = CommitRequest::decode(bytes).unwrap();
            assert_eq!(request.writes.len(), 1);
            let outcome = commit_results.lock().unwrap().pop_front().unwrap();
            let response = match outcome {
                CommitOutcome::Ok => {
                    FakeResponse::Message(CommitResponse::default().encode_to_vec())
                }
                CommitOutcome::Status(code) => FakeResponse::Status(code),
                CommitOutcome::Drop => FakeResponse::Drop,
            };
            (format!("Commit({outcome:?})"), response)
        } else {
            assert!(method.ends_with("/Rollback"));
            // A failed cleanup must not replace the original commit error.
            (
                "Rollback".to_string(),
                FakeResponse::Status(Code::Unavailable),
            )
        }
    })
    .await;
    let callbacks = Arc::new(Mutex::new(0));
    let observed_callbacks = callbacks.clone();
    let allowed_callbacks = commits.len() + usize::from(transient_callback);
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        server.db.run_transaction(move |_, transaction| {
            let mut callbacks = observed_callbacks.lock().unwrap();
            *callbacks += 1;
            let attempt = *callbacks;
            Box::pin(async move {
                if transient_callback && attempt == 1 {
                    return Err(BackoffError::transient(std::io::Error::other("retry read")));
                }
                if attempt > allowed_callbacks {
                    return Err(BackoffError::permanent(std::io::Error::other(
                        "later callback rejected the input",
                    )));
                }
                transaction
                    .delete_by_id("items", "one", None)
                    .map_err(|error| BackoffError::permanent(std::io::Error::other(error)))?;
                Ok(attempt)
            })
        }),
    )
    .await
    .expect("transaction did not finish");
    let callbacks = *callbacks.lock().unwrap();
    let commit_attempts = server
        .calls()
        .iter()
        .filter(|call| call.starts_with("Commit"))
        .count();
    (result, callbacks, commit_attempts)
}

#[tokio::test]
async fn ambiguous_commit_stops_without_repeating_the_callback() {
    for outcome in [
        CommitOutcome::Status(Code::Unavailable),
        CommitOutcome::Status(Code::Cancelled),
        CommitOutcome::Status(Code::ResourceExhausted),
        CommitOutcome::Status(Code::DeadlineExceeded),
        CommitOutcome::Status(Code::Unknown),
        CommitOutcome::Drop,
    ] {
        for initial_abort in [false, true] {
            let mut commits = Vec::new();
            if initial_abort {
                commits.push(CommitOutcome::Status(Code::Aborted));
            }
            commits.push(outcome);
            let (result, callbacks, commit_count) = run_transaction(&commits, None, false).await;
            let FirestoreError::DatabaseError(error) = result.unwrap_err() else {
                panic!("{outcome:?} commit outcome replaced by a later callback error");
            };
            assert!(
                !error.retry_possible,
                "{outcome:?} must not be retried: {error:?}"
            );
            if let CommitOutcome::Status(code) = outcome {
                assert_eq!(error.public.code, format!("{code:?}"));
            }
            assert_eq!(
                (callbacks, commit_count),
                (commits.len(), commits.len()),
                "{outcome:?}"
            );
        }
    }
}

// Repeated ABORTED commits retry until one succeeds; a BeginTransaction failure or a transient
// callback error ahead of the commit keeps its own, independent retry policy.
#[tokio::test]
async fn precommit_failures_keep_their_own_retry_policy() {
    let aborted = CommitOutcome::Status(Code::Aborted);
    for (commits, begin_failure, transient_callback, want_commits) in [
        (vec![aborted, aborted, CommitOutcome::Ok], None, false, 3),
        (
            vec![aborted, CommitOutcome::Ok],
            Some((2, Code::Unavailable)),
            true,
            2,
        ),
    ] {
        let (result, callbacks, commits_seen) =
            run_transaction(&commits, begin_failure, transient_callback).await;
        assert_eq!(result.unwrap(), 3);
        assert_eq!((callbacks, commits_seen), (3, want_commits));
    }
}
