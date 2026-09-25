use super::*;
use crate::errors::{firestore_err_to_backoff, BackoffError};
use futures::TryStreamExt;
use gcloud_sdk::tonic::Code;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

#[derive(Clone, Copy, Debug)]
enum Read {
    Get,
    List,
    Query,
    Aggregate,
    AggregateStream,
}

impl Read {
    async fn execute(self, db: &FirestoreDb) -> FirestoreResult<()> {
        match self {
            Self::Get => {
                db.get_doc("items", "one", None).await?;
            }
            Self::List => {
                db.list_doc(FirestoreListDocParams::new("items".into()))
                    .await?;
            }
            Self::Query => {
                db.query_doc(FirestoreQueryParams::new("items".into()))
                    .await?;
            }
            Self::Aggregate | Self::AggregateStream => {
                let query = FirestoreAggregatedQueryParams::new(
                    FirestoreQueryParams::new("items".into()),
                    vec![FirestoreAggregation::new("count".into())],
                );
                if matches!(self, Self::Aggregate) {
                    db.aggregated_query_doc(query).await?;
                } else {
                    db.stream_aggregated_query_doc_with_errors(query)
                        .await?
                        .try_collect::<Vec<_>>()
                        .await?;
                }
            }
        }
        Ok(())
    }
}

async fn check_read_retries(read: Read) {
    for (code, in_transaction, expected_attempts) in [
        (Code::Aborted, true, 1),
        (Code::Aborted, false, 2),
        (Code::Unavailable, true, 2),
    ] {
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut options = FirestoreDbOptions::new("test-project".into());
        options.firebase_api_url = Some(format!("http://{}", socket.local_addr().unwrap()));
        options.max_retries = 1;
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        // JoinSet aborts the server and its connections even if an assertion fails.
        let mut server = JoinSet::new();
        server.spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                let (socket, _) = socket.accept().await.unwrap();
                let observed = observed.clone();
                connections.spawn(async move {
                    let mut connection = h2::server::handshake(socket).await.unwrap();
                    while let Some(request) = connection.accept().await {
                        let (_, mut respond) = request.unwrap();
                        observed.fetch_add(1, Ordering::SeqCst);
                        let response = hyper::Response::builder()
                            .header("content-type", "application/grpc")
                            .header("grpc-status", (code as i32).to_string())
                            .body(())
                            .unwrap();
                        respond.send_response(response, true).unwrap();
                    }
                });
            }
        });
        let db = FirestoreDb::with_options_token_source(
            options,
            Vec::new(),
            TokenSourceType::ExternalSource(Box::new(FirestoreEmulatorTokenSource)),
        )
        .await
        .unwrap();
        let db = if in_transaction {
            db.clone_with_consistency_selector(FirestoreConsistencySelector::Transaction(vec![1]))
        } else {
            db
        };
        let error = tokio::time::timeout(Duration::from_secs(5), read.execute(&db))
            .await
            .expect("read retries must be bounded")
            .unwrap_err();
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            expected_attempts,
            "{read:?}, {code:?}, in_transaction={in_transaction}"
        );
        let FirestoreError::DatabaseError(ref db_error) = error else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(db_error.public.code, format!("{code:?}"));
        assert!(matches!(
            firestore_err_to_backoff(error),
            BackoffError::Transient { .. }
        ));
    }
}

#[tokio::test]
async fn get_retry_scope() {
    check_read_retries(Read::Get).await;
}

#[tokio::test]
async fn list_retry_scope() {
    check_read_retries(Read::List).await;
}

#[tokio::test]
async fn query_retry_scope() {
    check_read_retries(Read::Query).await;
}

#[tokio::test]
async fn aggregate_retry_scope() {
    check_read_retries(Read::Aggregate).await;
}

#[tokio::test]
async fn aggregate_stream_retry_scope() {
    check_read_retries(Read::AggregateStream).await;
}
