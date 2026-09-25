use super::*;
use crate::db::transaction_test_server::TestServer;
use crate::FirestoreGetByIdSupport;
use gcloud_sdk::google::firestore::v1::{
    get_document_request, BeginTransactionResponse, Document, GetDocumentRequest,
};
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::Code;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    begins: u8,
    reads: Vec<Vec<u8>>,
    rollbacks: Vec<RollbackRequest>,
    commit_code: Option<Code>,
}

struct Fixture {
    server: TestServer,
    state: Arc<Mutex<State>>,
}

impl Fixture {
    async fn start(rollback_code: Code) -> Self {
        let state = Arc::new(Mutex::new(State::default()));
        let shared = state.clone();
        let server = TestServer::start(move |method, bytes| {
            let mut state = shared.lock().unwrap();
            if method.ends_with("/BeginTransaction") {
                state.begins += 1;
                Some((
                    Code::Ok,
                    BeginTransactionResponse {
                        transaction: vec![state.begins],
                    }
                    .encode_to_vec(),
                ))
            } else if method.ends_with("/GetDocument") {
                let request = GetDocumentRequest::decode(bytes).unwrap();
                let Some(get_document_request::ConsistencySelector::Transaction(id)) =
                    request.consistency_selector
                else {
                    panic!("read must belong to a transaction");
                };
                state.reads.push(id);
                Some((Code::Ok, Document::default().encode_to_vec()))
            } else if method.ends_with("/Rollback") {
                state
                    .rollbacks
                    .push(RollbackRequest::decode(bytes).unwrap());
                Some((rollback_code, Vec::new()))
            } else {
                assert!(method.ends_with("/Commit"));
                Some((state.commit_code.unwrap_or(Code::Ok), Vec::new()))
            }
        })
        .await;
        Self { server, state }
    }

    fn rolled_back(&self) -> Vec<Vec<u8>> {
        self.state
            .lock()
            .unwrap()
            .rollbacks
            .iter()
            .map(|request| request.transaction.clone())
            .collect()
    }
}

#[tokio::test]
async fn callback_errors_release_each_attempt_and_preserve_the_original_error() {
    for rollback_code in [Code::Ok, Code::Unavailable] {
        let fixture = Fixture::start(rollback_code).await;
        let result: FirestoreResult<()> = fixture
            .server
            .db
            .run_transaction(|db, transaction| {
                Box::pin(async move {
                    // Reads acquire locks even when no writes have been queued.
                    db.get_doc("test", "one", None).await.unwrap();
                    let error = std::io::Error::other("callback failed");
                    if transaction.transaction_id() == &[1] {
                        Err(BackoffError::retry_after(error, Duration::ZERO))
                    } else {
                        Err(BackoffError::permanent(error))
                    }
                })
            })
            .await;
        let Err(FirestoreError::ErrorInTransaction(error)) = result else {
            panic!("expected callback error");
        };
        assert_eq!(error.transaction_id, vec![2]);
        assert_eq!(error.source.to_string(), "callback failed");
        assert_eq!(fixture.rolled_back(), vec![vec![1], vec![2]]);
        let state = fixture.state.lock().unwrap();
        assert_eq!(state.reads, vec![vec![1], vec![2]]);
        assert_eq!(state.begins, 2);
    }
}

#[tokio::test]
async fn exhausted_callback_retries_release_the_last_attempt() {
    let fixture = Fixture::start(Code::Ok).await;
    let options =
        FirestoreTransactionOptions::new().with_max_elapsed_time(crate::FirestoreDuration::ZERO);
    let result: FirestoreResult<()> = fixture
        .server
        .db
        .run_transaction_with_options(
            |_, _| Box::pin(async { Err(BackoffError::transient(std::io::Error::other("retry"))) }),
            options,
        )
        .await;
    assert!(matches!(result, Err(FirestoreError::ErrorInTransaction(_))));
    assert_eq!(fixture.rolled_back(), vec![vec![1], vec![2]]);
    assert_eq!(fixture.state.lock().unwrap().begins, 2);
}

#[tokio::test]
async fn rollback_preserves_the_failed_attempts_request_options() {
    let fixture = Fixture::start(Code::Ok).await;
    let options = FirestoreTransactionOptions::new()
        .with_request_options(FirestoreRequestOptions::from_tags(["cleanup"]));
    let result: FirestoreResult<()> = fixture
        .server
        .db
        .run_transaction_with_options(
            |_, _| {
                Box::pin(async { Err(BackoffError::permanent(std::io::Error::other("failed"))) })
            },
            options,
        )
        .await;
    assert!(result.is_err());
    assert_eq!(fixture.rolled_back(), vec![vec![1]]);
    let state = fixture.state.lock().unwrap();
    assert_eq!(
        state.rollbacks[0]
            .request_options
            .as_ref()
            .unwrap()
            .request_tags,
        vec!["cleanup"]
    );
}

#[tokio::test]
async fn failed_commit_does_not_open_another_transaction_to_roll_back() {
    let fixture = Fixture::start(Code::Ok).await;
    fixture.state.lock().unwrap().commit_code = Some(Code::Unavailable);
    let options =
        FirestoreTransactionOptions::new().with_max_elapsed_time(crate::FirestoreDuration::ZERO);
    let result: FirestoreResult<()> = fixture
        .server
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
        matches!(result, Err(FirestoreError::DatabaseError(ref error))
        if error.public.code == "Unavailable")
    );
    assert_eq!(fixture.rolled_back(), vec![vec![1]]);
    assert_eq!(fixture.state.lock().unwrap().begins, 2);
}
