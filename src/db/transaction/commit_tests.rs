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

// Only ABORTED proves Firestore wrote nothing; after any other failure the writes may already
// be applied, so the commit is neither resent nor the callback run again.
#[tokio::test]
async fn ambiguous_commit_stops_without_repeating_the_callback() {
    for outcome in [
        CommitOutcome::Status(Code::Unavailable),
        CommitOutcome::Status(Code::ResourceExhausted),
        CommitOutcome::Status(Code::Cancelled),
        CommitOutcome::Status(Code::DeadlineExceeded),
        CommitOutcome::Status(Code::Unknown),
        CommitOutcome::Drop,
    ] {
        for (commits, want_commits) in [
            (vec![outcome], vec!["Commit(1)"]),
            (
                vec![CommitOutcome::Status(Code::Aborted), outcome],
                vec!["Commit(1)", "Commit(2)"],
            ),
        ] {
            let (result, callbacks, commits_seen) = run_transaction(&commits, None, false).await;
            let FirestoreError::DatabaseError(error) = result.unwrap_err() else {
                panic!("{outcome:?} commit outcome replaced by a later callback error");
            };
            assert!(
                !error.retry_possible,
                "{outcome:?} must not be retried: {error:?}"
            );
            assert_eq!(error.public.code, outcome.error_code());
            assert_eq!(commits_seen, want_commits, "{outcome:?}");
            assert_eq!(callbacks, commits.len(), "{outcome:?}");
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
