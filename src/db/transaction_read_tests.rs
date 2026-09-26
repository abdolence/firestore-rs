use super::*;
use crate::db::fake_firestore::{begin_response, read_transaction_id, FakeFirestore, FakeResponse};
use crate::errors::{firestore_err_to_backoff, BackoffError};
use futures::TryStreamExt;
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::Code;
use std::sync::atomic::AtomicU8;
use std::time::Duration;

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

// Every request in this suite gets the same status, with no message body: retry counting is by
// `calls().len()`, not by which RPC ran.
async fn check_read_retries(read: Read) {
    for (code, in_transaction, expected_attempts) in [
        (Code::Aborted, true, 1),
        (Code::Aborted, false, 2),
        (Code::Unavailable, true, 2),
    ] {
        let server = FakeFirestore::start_with_max_retries(1, move |_, _| {
            ("attempt".to_string(), FakeResponse::Status(code))
        })
        .await;
        let db = if in_transaction {
            server
                .db
                .clone_with_consistency_selector(FirestoreConsistencySelector::Transaction(vec![1]))
        } else {
            server.db.clone()
        };
        let error = tokio::time::timeout(Duration::from_secs(5), read.execute(&db))
            .await
            .expect("read retries must be bounded")
            .unwrap_err();
        assert_eq!(
            server.calls().len(),
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
async fn read_retry_scope() {
    for read in [
        Read::Get,
        Read::List,
        Read::Query,
        Read::Aggregate,
        Read::AggregateStream,
    ] {
        check_read_retries(read).await;
    }
}

#[tokio::test]
async fn aborted_read_retries_the_whole_transaction() {
    let begins = AtomicU8::new(0);
    let server = FakeFirestore::start(move |method, bytes| {
        if method.ends_with("/BeginTransaction") {
            begin_response(&begins)
        } else if method.ends_with("/GetDocument") {
            let id = read_transaction_id(bytes);
            if id == 1 {
                (
                    format!("Get({id}) aborted"),
                    FakeResponse::Status(Code::Aborted),
                )
            } else {
                (
                    format!("Get({id})"),
                    FakeResponse::Message(Document::default().encode_to_vec()),
                )
            }
        } else if method.ends_with("/Rollback") {
            ("Rollback".to_string(), FakeResponse::empty())
        } else {
            ("Commit".to_string(), FakeResponse::committed())
        }
    })
    .await;
    let result: FirestoreResult<()> = server
        .db
        .run_transaction(|db, transaction| {
            Box::pin(async move {
                db.get_doc("items", "one", None).await?;
                transaction.delete_by_id("items", "one", None)?;
                Ok(())
            })
        })
        .await;
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        server.calls(),
        vec![
            "Begin→1",
            "Get(1) aborted",
            "Rollback",
            "Begin→2",
            "Get(2)",
            "Commit"
        ]
    );
}
