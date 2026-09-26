use super::*;
use crate::db::transaction_test_server::TestServer;
use gcloud_sdk::google::firestore::v1::{BeginTransactionResponse, CommitResponse};
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::Code;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

async fn run_transaction(
    commits: &[Code],
    begins: &[Code],
    transient_callback: bool,
) -> (FirestoreResult<usize>, usize, usize) {
    let commit_results = Mutex::new(VecDeque::from(commits.to_vec()));
    let begin_results = Mutex::new(VecDeque::from(begins.to_vec()));
    let commit_count = Arc::new(Mutex::new(0));
    let observed_commits = commit_count.clone();
    let begin_count = Mutex::new(0_u32);
    let server = TestServer::start(move |method, bytes| {
        if method.ends_with("/BeginTransaction") {
            let mut count = begin_count.lock().unwrap();
            *count += 1;
            Some((
                begin_results
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(Code::Ok),
                BeginTransactionResponse {
                    transaction: count.to_be_bytes().to_vec(),
                }
                .encode_to_vec(),
            ))
        } else if method.ends_with("/Commit") {
            let request = CommitRequest::decode(bytes).unwrap();
            assert_eq!(request.writes.len(), 1);
            *observed_commits.lock().unwrap() += 1;
            Some((
                commit_results.lock().unwrap().pop_front().unwrap(),
                CommitResponse::default().encode_to_vec(),
            ))
        } else {
            assert!(method.ends_with("/Rollback"));
            // A failed cleanup must not replace the original commit error.
            Some((Code::Unavailable, Vec::new()))
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
    let commits = *commit_count.lock().unwrap();
    (result, callbacks, commits)
}

#[tokio::test]
async fn ambiguous_commit_stops_without_repeating_the_callback() {
    for code in [
        Code::Unavailable,
        Code::Cancelled,
        Code::ResourceExhausted,
        Code::DeadlineExceeded,
        Code::Unknown,
    ] {
        for initial_abort in [false, true] {
            let mut commits = Vec::new();
            if initial_abort {
                commits.push(Code::Aborted);
            }
            commits.push(code);
            let (result, callbacks, commit_count) = run_transaction(&commits, &[], false).await;
            let FirestoreError::DatabaseError(error) = result.unwrap_err() else {
                panic!("{code:?} commit outcome replaced by a later callback error");
            };
            assert_eq!(error.public.code, format!("{code:?}"));
            assert!(!error.retry_possible);
            assert_eq!(callbacks, commits.len());
            assert_eq!(commit_count, commits.len());
        }
    }
}

#[tokio::test]
async fn aborted_commits_still_retry_until_success() {
    let (result, callbacks, commits) =
        run_transaction(&[Code::Aborted, Code::Aborted, Code::Ok], &[], false).await;
    assert_eq!(result.unwrap(), 3);
    assert_eq!((callbacks, commits), (3, 3));
}

#[tokio::test]
async fn precommit_failures_keep_their_retry_policy() {
    let (result, callbacks, commits) = run_transaction(
        &[Code::Aborted, Code::Ok],
        &[Code::Ok, Code::Unavailable, Code::Ok],
        true,
    )
    .await;
    assert_eq!(result.unwrap(), 3);
    assert_eq!((callbacks, commits), (3, 2));
}
