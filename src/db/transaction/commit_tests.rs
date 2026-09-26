use super::*;
use crate::db::fake_firestore::{begin_failure, begin_response, FakeFirestore, FakeResponse};
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::Code;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

/// One queued answer to a `Commit` RPC: a real response, an error code, or a connection dropped
/// mid-call - the ambiguous case where the writes may already have landed.
#[derive(Clone, Copy, Debug)]
enum CommitOutcome {
    Ok,
    Status(Code),
    Drop,
}

impl CommitOutcome {
    /// The public error code the client reports for this outcome.
    fn error_code(self) -> String {
        match self {
            Self::Ok => panic!("a successful commit has no error code"),
            Self::Status(code) => format!("{code:?}"),
            Self::Drop => "CONNECTION_ERROR".to_string(),
        }
    }
}

/// `failing_begin` fails, once, the `BeginTransaction` that would open the given transaction ID.
/// Returns the transaction result, the number of callback runs, and one `Commit(<transaction>)`
/// entry per `Commit` RPC, in order.
async fn run_transaction(
    commits: &[CommitOutcome],
    failing_begin: Option<(u8, Code)>,
    transient_callback: bool,
) -> (FirestoreResult<usize>, usize, Vec<String>) {
    let commit_results = Mutex::new(VecDeque::from(commits.to_vec()));
    let begins = AtomicU8::new(0);
    let failing_begin = Mutex::new(failing_begin);
    // One retry keeps a persistently failing commit, and its backoff, short.
    let server = FakeFirestore::start_with_max_retries(1, move |method, bytes| {
        if method.ends_with("/BeginTransaction") {
            let next_id = begins.load(Ordering::SeqCst) + 1;
            match failing_begin
                .lock()
                .unwrap()
                .take_if(|(id, _)| *id == next_id)
            {
                Some((_, code)) => begin_failure(code),
                None => begin_response(&begins),
            }
        } else if method.ends_with("/Commit") {
            let request = CommitRequest::decode(bytes).unwrap();
            assert_eq!(request.writes.len(), 1);
            let response = match commit_results.lock().unwrap().pop_front().unwrap() {
                CommitOutcome::Ok => FakeResponse::committed(),
                CommitOutcome::Status(code) => FakeResponse::Status(code),
                CommitOutcome::Drop => FakeResponse::Drop,
            };
            (format!("Commit({})", request.transaction[0]), response)
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
    let commits = server
        .calls()
        .into_iter()
        .filter(|call| call.starts_with("Commit"))
        .collect();
    (result, callbacks, commits)
}

#[tokio::test]
async fn ambiguous_commit_stops_without_repeating_the_callback() {
    for outcome in [
        CommitOutcome::Status(Code::Cancelled),
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
            let (result, callbacks, commits_seen) = run_transaction(&commits, None, false).await;
            let FirestoreError::DatabaseError(error) = result.unwrap_err() else {
                panic!("{outcome:?} commit outcome replaced by a later callback error");
            };
            assert!(
                !error.retry_possible,
                "{outcome:?} must not be retried: {error:?}"
            );
            assert_eq!(error.public.code, outcome.error_code());
            assert_eq!(
                (callbacks, commits_seen.len()),
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
    for (commits, failing_begin, transient_callback, want_commits) in [
        (vec![aborted, aborted, CommitOutcome::Ok], None, false, 3),
        (
            vec![aborted, CommitOutcome::Ok],
            Some((2, Code::Unavailable)),
            true,
            2,
        ),
    ] {
        let (result, callbacks, commits_seen) =
            run_transaction(&commits, failing_begin, transient_callback).await;
        assert_eq!(result.unwrap(), 3);
        assert_eq!((callbacks, commits_seen.len()), (3, want_commits));
    }
}

#[tokio::test]
async fn unavailable_commit_is_resent_with_the_same_transaction() {
    for code in [Code::Unavailable, Code::ResourceExhausted] {
        let commits = [CommitOutcome::Status(code), CommitOutcome::Ok];
        let (result, callbacks, commits_seen) = run_transaction(&commits, None, false).await;
        assert_eq!(result.unwrap(), 1, "{code:?}");
        assert_eq!(callbacks, 1, "{code:?}");
        assert_eq!(commits_seen, ["Commit(1)", "Commit(1)"], "{code:?}");
    }
}

// Firestore answers a Commit for a transaction that already committed with ABORTED, so after a
// resent Commit an ABORTED cannot tell a lost success from a real conflict.
#[tokio::test]
async fn resent_commit_failures_do_not_rerun_the_callback() {
    for (first, last) in [
        (Code::Unavailable, Code::Unavailable),
        (Code::ResourceExhausted, Code::ResourceExhausted),
        (Code::Unavailable, Code::Aborted),
    ] {
        let commits = [CommitOutcome::Status(first), CommitOutcome::Status(last)];
        let (result, callbacks, commits_seen) = run_transaction(&commits, None, false).await;
        let Err(FirestoreError::DatabaseError(error)) = result else {
            panic!("{first:?} then {last:?}: expected the commit error, got {result:?}");
        };
        assert_eq!(error.public.code, format!("{last:?}"));
        assert!(!error.retry_possible, "{first:?} then {last:?}");
        assert_eq!(callbacks, 1, "{first:?} then {last:?}");
        assert_eq!(
            commits_seen,
            ["Commit(1)", "Commit(1)"],
            "{first:?} then {last:?}"
        );
    }
}
