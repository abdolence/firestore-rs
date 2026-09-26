//! [`FirestoreBulkDeleteSupport`] for [`FirestoreDb`]: starts and optionally waits on Firestore's
//! `BulkDeleteDocuments` admin RPC, on the same authenticated channel index management uses
//! ([`FirestoreDb::admin_client`](super::indexes)).

use crate::db::admin::operation_wait::{OperationAction, StartedOperation};
use crate::db::support::FirestoreBulkDeleteSupport;
use crate::errors::{FirestoreError, FirestoreErrorPublicGenericDetails, FirestoreSystemError};
use crate::{
    FirestoreBulkDeleteParams, FirestoreBulkDeleteProgress, FirestoreBulkDeleteResult,
    FirestoreCollectionId, FirestoreDb, FirestoreInstant, FirestoreOperationWaitOptions,
    FirestoreResult,
};
use async_trait::async_trait;
use gcloud_sdk::google::firestore::admin::v1::{
    BulkDeleteDocumentsMetadata, BulkDeleteDocumentsRequest, Progress as ProtoProgress,
};
use gcloud_sdk::prost::Message as _;
use gcloud_sdk::prost_types::Any;
use std::time::Instant;
use tracing::*;

/// A short, stable identifier for `BulkDeleteDocuments`, logged as a field alongside the
/// human-readable collection-group label.
const BULK_DELETE_ACTION_KIND: &str = "bulk_delete";

/// The declared collection groups, comma-joined, for logging and a timeout's error message.
fn bulk_delete_label(groups: &[FirestoreCollectionId]) -> String {
    groups
        .iter()
        .map(FirestoreCollectionId::as_str)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The one action a bulk delete's operation carries out, for the shared operation wait.
struct BulkDeleteAction<'a> {
    label: &'a str,
}

impl std::fmt::Display for BulkDeleteAction<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bulk delete {}", self.label)
    }
}

impl OperationAction for BulkDeleteAction<'_> {
    fn kind(&self) -> &'static str {
        BULK_DELETE_ACTION_KIND
    }
}

/// Decodes a long-running operation's metadata as `BulkDeleteDocumentsMetadata`. Not checked
/// against `any.type_url`: the generated proto type carries no [`prost::Name`] impl to check it
/// against, and every operation this crate polls here is one it itself started, so the wire shape
/// is already known.
fn decode_metadata(any: &Any) -> Option<BulkDeleteDocumentsMetadata> {
    BulkDeleteDocumentsMetadata::decode(any.value.as_slice()).ok()
}

fn progress_from(proto: Option<ProtoProgress>) -> Option<FirestoreBulkDeleteProgress> {
    proto.map(|p| FirestoreBulkDeleteProgress {
        estimated_work: p.estimated_work,
        completed_work: p.completed_work,
    })
}

/// Folds one polled `BulkDeleteDocumentsMetadata` into `result`, logging the snapshot time the
/// first time it is observed - it does not change once the server reports it, so logging it again
/// on every later poll would only repeat the same line.
fn apply_metadata(
    label: &str,
    result: &mut FirestoreBulkDeleteResult,
    metadata: &BulkDeleteDocumentsMetadata,
) {
    if result.snapshot_time.is_none() {
        if let Some(ts) = metadata.snapshot_time {
            if let Ok(instant) = crate::timestamp_utils::from_timestamp(ts) {
                info!(
                    collection_groups = label,
                    snapshot_time = %instant,
                    "Firestore reported the bulk delete's snapshot time.",
                );
                result.snapshot_time = Some(instant);
            }
        }
    }
    result.documents = progress_from(metadata.progress_documents);
    result.bytes = progress_from(metadata.progress_bytes);
}

impl FirestoreDb {
    async fn start_bulk_delete(
        &self,
        label: &str,
        collection_ids: Vec<String>,
    ) -> FirestoreResult<(String, Option<BulkDeleteDocumentsMetadata>)> {
        let span = span!(
            Level::INFO,
            "Start Bulk Delete",
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let outcome = async {
            let request = BulkDeleteDocumentsRequest {
                name: self.inner.database_path.clone(),
                collection_ids,
                namespace_ids: Vec::new(),
            };
            match self.admin_client().bulk_delete_documents(request).await {
                Ok(response) => {
                    let operation = response.into_inner();
                    info!(
                        operation = operation.name.as_str(),
                        action = BULK_DELETE_ACTION_KIND,
                        collection_groups = label,
                        "Started a bulk delete.",
                    );
                    let metadata = operation.metadata.as_ref().and_then(decode_metadata);
                    Ok((operation.name, metadata))
                }
                Err(status) => {
                    error!(
                        error = %status,
                        action = BULK_DELETE_ACTION_KIND,
                        collection_groups = label,
                        "Failed to start a bulk delete.",
                    );
                    Err(FirestoreError::from(status))
                }
            }
        }
        .instrument(span.clone())
        .await;
        let elapsed = FirestoreInstant::now().duration_since(began);
        span.record("/firestore/response_time", elapsed.as_millis());
        outcome
    }

    /// Waits for the started bulk delete through the shared operation wait, folding the
    /// `BulkDeleteDocumentsMetadata` of every poll into `result`, so progress is known even when
    /// the wait fails.
    async fn wait_for_bulk_delete(
        &self,
        operation: &StartedOperation<BulkDeleteAction<'_>>,
        result: &mut FirestoreBulkDeleteResult,
        options: &FirestoreOperationWaitOptions,
    ) -> FirestoreResult<()> {
        let span = span!(
            Level::INFO,
            "Firestore Bulk Delete Wait",
            "/firestore/operation" = operation.name.as_str(),
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let label = operation.action.label;
        let outcome = self
            .wait_for_operations(&[operation], options, |_, polled| {
                if let Some(metadata) = polled.metadata.as_ref().and_then(decode_metadata) {
                    debug!(
                        collection_groups = label,
                        documents = ?progress_from(metadata.progress_documents),
                        bytes = ?progress_from(metadata.progress_bytes),
                        "Bulk delete progress.",
                    );
                    apply_metadata(label, result, &metadata);
                }
            })
            .instrument(span.clone())
            .await;
        let elapsed = FirestoreInstant::now().duration_since(began);
        span.record("/firestore/response_time", elapsed.as_millis());
        outcome
    }
}

#[async_trait]
impl FirestoreBulkDeleteSupport for FirestoreDb {
    async fn bulk_delete_documents(
        &self,
        params: FirestoreBulkDeleteParams,
    ) -> FirestoreResult<FirestoreBulkDeleteResult> {
        crate::validate_bulk_delete_params(&params)?;
        let label = bulk_delete_label(&params.collection_groups);

        if self.inner.is_emulator {
            error!(
                collection_groups = label.as_str(),
                "Bulk delete is not available on the Firestore emulator.",
            );
            return Err(FirestoreError::SystemError(FirestoreSystemError::new(
                FirestoreErrorPublicGenericDetails::new(
                    "BULK_DELETE_UNAVAILABLE_ON_EMULATOR".to_string(),
                ),
                "bulk delete is not available on the Firestore emulator: BulkDeleteDocuments is \
                 not implemented there"
                    .to_string(),
            )));
        }

        let waited = params.wait.is_some();
        let root = span!(
            Level::INFO,
            "Firestore Bulk Delete",
            "/firestore/collection_groups" = label.as_str(),
            "/firestore/wait" = waited,
            "/firestore/response_time" = field::Empty,
        );
        let wall_start = Instant::now();
        let began = FirestoreInstant::now();
        let collection_ids: Vec<String> = params
            .collection_groups
            .iter()
            .map(|group| group.as_str().to_string())
            .collect();
        let outcome = async {
            let (operation_name, started_metadata) =
                self.start_bulk_delete(&label, collection_ids).await?;
            let mut result = FirestoreBulkDeleteResult {
                operation_name: operation_name.clone(),
                collection_groups: params.collection_groups.clone(),
                ..Default::default()
            };
            if let Some(metadata) = &started_metadata {
                apply_metadata(&label, &mut result, metadata);
            }
            if let Some(wait_options) = &params.wait {
                let started = StartedOperation {
                    name: operation_name,
                    action: BulkDeleteAction { label: &label },
                };
                if let Err(err) = self
                    .wait_for_bulk_delete(&started, &mut result, wait_options)
                    .await
                {
                    result.elapsed = wall_start.elapsed();
                    warn!(
                        collection_groups = label.as_str(),
                        "Waiting for the bulk delete failed; known so far: {result}",
                    );
                    return Err(err);
                }
            }
            Ok::<_, FirestoreError>(result)
        }
        .instrument(root.clone())
        .await;
        let elapsed = FirestoreInstant::now().duration_since(began);
        root.record("/firestore/response_time", elapsed.as_millis());

        let mut result = outcome?;
        result.elapsed = wall_start.elapsed();
        if waited {
            info!(collection_groups = label.as_str(), "{result}");
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::fake_firestore::{
        bulk_delete_operation_response, done_operation_response, failed_operation_response,
        pending_operation_response, FakeFirestore, FakeResponse,
    };
    use crate::db::FirestoreDbInner;
    use crate::{FirestoreCollectionId, FirestoreOperationWaitOptions};
    use gcloud_sdk::google::firestore::admin::v1::{
        BulkDeleteDocumentsRequest, Progress as ProtoProgress,
    };
    use gcloud_sdk::tonic::Code;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    const BULK_DELETE: &str = "/google.firestore.admin.v1.FirestoreAdmin/BulkDeleteDocuments";
    const GET_OPERATION: &str = "/google.longrunning.Operations/GetOperation";

    const DATABASE_PATH: &str = "projects/fake-firestore/databases/(default)";
    const OPERATION_NAME: &str = "projects/fake-firestore/databases/(default)/operations/bulk-1";

    fn groups() -> Vec<FirestoreCollectionId> {
        vec![
            FirestoreCollectionId::from_static("users"),
            FirestoreCollectionId::from_static("orders"),
        ]
    }

    fn no_writes_allowed(method: &str) -> ! {
        panic!("unexpected write RPC in a read-only scenario: {method}")
    }

    /// Serializes every test in this module against every other, for the same reason
    /// `src/db/admin/indexes.rs` does: `tracing`'s per-callsite interest cache is shared
    /// process-wide, and this module's tests share callsites (there is only one
    /// `span!(..., "Firestore Bulk Delete", ...)` call site in the source, for example).
    static MODULE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn request_carries_the_database_name_and_exactly_the_given_ids() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, bytes| match method {
            BULK_DELETE => {
                let request = BulkDeleteDocumentsRequest::decode(bytes).unwrap();
                assert_eq!(request.name, DATABASE_PATH);
                assert_eq!(request.collection_ids, vec!["users", "orders"]);
                assert!(request.namespace_ids.is_empty());
                (
                    "BulkDeleteDocuments".to_string(),
                    done_operation_response(OPERATION_NAME),
                )
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let result = fake
            .db
            .bulk_delete_documents(FirestoreBulkDeleteParams::new(groups()))
            .await
            .unwrap();
        assert_eq!(result.operation_name, OPERATION_NAME);
        assert_eq!(result.collection_groups, groups());
    }

    #[tokio::test]
    async fn empty_collection_groups_sends_no_rpc() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| no_writes_allowed(method)).await;
        let err = fake
            .db
            .bulk_delete_documents(FirestoreBulkDeleteParams::new(vec![]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("collection_groups"));
        assert!(fake.calls().is_empty());
    }

    #[tokio::test]
    async fn duplicate_collection_group_sends_no_rpc() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| no_writes_allowed(method)).await;
        let duplicated = vec![
            FirestoreCollectionId::from_static("users"),
            FirestoreCollectionId::from_static("users"),
        ];
        let err = fake
            .db
            .bulk_delete_documents(FirestoreBulkDeleteParams::new(duplicated))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("more than once"));
        assert!(fake.calls().is_empty());
    }

    #[tokio::test]
    async fn wait_polls_until_done_and_decodes_progress() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let polls = StdArc::new(AtomicU32::new(0));
        let polls_in_handler = polls.clone();
        let fake = FakeFirestore::start(move |method, _| match method {
            BULK_DELETE => (
                "BulkDeleteDocuments".to_string(),
                pending_operation_response(OPERATION_NAME),
            ),
            GET_OPERATION => {
                let count = polls_in_handler.fetch_add(1, Ordering::SeqCst) + 1;
                let metadata = BulkDeleteDocumentsMetadata {
                    progress_documents: Some(ProtoProgress {
                        estimated_work: 100,
                        completed_work: i64::from(count) * 10,
                    }),
                    ..Default::default()
                };
                let response = if count < 2 {
                    bulk_delete_operation_response(OPERATION_NAME, false, metadata)
                } else {
                    bulk_delete_operation_response(OPERATION_NAME, true, metadata)
                };
                (format!("GetOperation#{count}"), response)
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let params = FirestoreBulkDeleteParams::new(groups()).with_wait(
            FirestoreOperationWaitOptions::new(Duration::from_secs(5))
                .with_poll_interval(Duration::from_millis(5)),
        );
        let result = fake.db.bulk_delete_documents(params).await.unwrap();

        assert!(polls.load(Ordering::SeqCst) >= 2);
        assert_eq!(
            result.documents,
            Some(FirestoreBulkDeleteProgress {
                estimated_work: 100,
                completed_work: 20,
            })
        );
        assert!(fake.calls().contains(&"GetOperation#2".to_string()));
    }

    #[tokio::test]
    async fn without_wait_returns_once_started() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| match method {
            BULK_DELETE => (
                "BulkDeleteDocuments".to_string(),
                pending_operation_response(OPERATION_NAME),
            ),
            other => no_writes_allowed(other),
        })
        .await;

        let result = fake
            .db
            .bulk_delete_documents(FirestoreBulkDeleteParams::new(groups()))
            .await
            .unwrap();
        assert_eq!(result.operation_name, OPERATION_NAME);
        assert!(result.documents.is_none());
        assert!(fake.calls().len() == 1);
    }

    #[tokio::test]
    async fn a_transient_poll_error_is_retried_within_the_deadline() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let polls = StdArc::new(AtomicU32::new(0));
        let polls_in_handler = polls.clone();
        let fake = FakeFirestore::start(move |method, _| match method {
            BULK_DELETE => (
                "BulkDeleteDocuments".to_string(),
                pending_operation_response(OPERATION_NAME),
            ),
            GET_OPERATION => {
                let count = polls_in_handler.fetch_add(1, Ordering::SeqCst) + 1;
                if count == 1 {
                    (
                        "GetOperation#1 (unavailable)".to_string(),
                        FakeResponse::Status(Code::Unavailable),
                    )
                } else {
                    let metadata = BulkDeleteDocumentsMetadata {
                        progress_documents: Some(ProtoProgress {
                            estimated_work: 3,
                            completed_work: 3,
                        }),
                        ..Default::default()
                    };
                    (
                        format!("GetOperation#{count}"),
                        bulk_delete_operation_response(OPERATION_NAME, true, metadata),
                    )
                }
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let params = FirestoreBulkDeleteParams::new(groups()).with_wait(
            FirestoreOperationWaitOptions::new(Duration::from_secs(5))
                .with_poll_interval(Duration::from_millis(5)),
        );
        let result = fake
            .db
            .bulk_delete_documents(params)
            .await
            .expect("an UNAVAILABLE poll must be retried, not end the wait");

        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(result.documents.unwrap().completed_work, 3);
    }

    #[tokio::test]
    async fn a_failed_wait_logs_the_result_known_so_far() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| match method {
            BULK_DELETE => (
                "BulkDeleteDocuments".to_string(),
                pending_operation_response(OPERATION_NAME),
            ),
            GET_OPERATION => {
                let metadata = BulkDeleteDocumentsMetadata {
                    progress_documents: Some(ProtoProgress {
                        estimated_work: 10,
                        completed_work: 4,
                    }),
                    ..Default::default()
                };
                (
                    "GetOperation".to_string(),
                    bulk_delete_operation_response(OPERATION_NAME, false, metadata),
                )
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let params = FirestoreBulkDeleteParams::new(groups()).with_wait(
            FirestoreOperationWaitOptions::new(Duration::from_millis(20))
                .with_poll_interval(Duration::from_millis(5)),
        );
        let (subscriber, buffer) = capturing_subscriber();
        {
            let _guard = tracing::subscriber::set_default(subscriber);
            fake.db.bulk_delete_documents(params).await.unwrap_err();
        }
        let output = captured_text(&buffer);

        let result_line = output
            .lines()
            .find(|line| line.contains("Bulk delete users, orders"))
            .unwrap_or_else(|| panic!("no partial result logged in:\n{output}"));
        assert!(result_line.contains("WARN"), "{result_line}");
        assert!(result_line.contains("documents 4/10"), "{result_line}");
    }

    #[tokio::test]
    async fn a_failed_operation_surfaces_as_an_error() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| match method {
            BULK_DELETE => (
                "BulkDeleteDocuments".to_string(),
                pending_operation_response(OPERATION_NAME),
            ),
            GET_OPERATION => (
                "GetOperation".to_string(),
                failed_operation_response(OPERATION_NAME, 9, "bulk delete failed: quota exceeded"),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let params = FirestoreBulkDeleteParams::new(groups())
            .with_wait(FirestoreOperationWaitOptions::new(Duration::from_secs(5)));
        let err = fake.db.bulk_delete_documents(params).await.unwrap_err();
        assert!(err.to_string().contains("quota exceeded"));
    }

    #[tokio::test]
    async fn wait_timeout_returns_an_error_naming_the_operation() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| match method {
            BULK_DELETE => (
                "BulkDeleteDocuments".to_string(),
                pending_operation_response(OPERATION_NAME),
            ),
            GET_OPERATION => (
                "GetOperation".to_string(),
                pending_operation_response(OPERATION_NAME),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let params = FirestoreBulkDeleteParams::new(groups()).with_wait(
            FirestoreOperationWaitOptions::new(Duration::from_millis(20))
                .with_poll_interval(Duration::from_millis(5)),
        );
        let err = fake.db.bulk_delete_documents(params).await.unwrap_err();
        assert!(err.to_string().contains("timed out"));
        assert!(err.to_string().contains(OPERATION_NAME));
    }

    #[tokio::test]
    async fn the_emulator_path_errors_instead_of_skipping() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| no_writes_allowed(method)).await;

        let emulator_db = FirestoreDb {
            inner: StdArc::new(FirestoreDbInner {
                database_path: fake.db.get_database_path().clone(),
                doc_path: fake.db.get_documents_path().clone(),
                options: fake.db.get_options().clone(),
                client: fake.db.client().clone(),
                is_emulator: true,
            }),
            session_params: fake.db.get_session_params().clone().into(),
        };

        let err = emulator_db
            .bulk_delete_documents(FirestoreBulkDeleteParams::new(groups()))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("emulator"));
        assert!(fake.calls().is_empty());
    }

    fn capturing_subscriber() -> (
        impl tracing::Subscriber + Send + Sync,
        StdArc<std::sync::Mutex<Vec<u8>>>,
    ) {
        let buffer = StdArc::new(std::sync::Mutex::new(Vec::new()));
        let make_writer = {
            let buffer = buffer.clone();
            move || SharedBufferWriter(buffer.clone())
        };
        let subscriber = tracing_subscriber::fmt()
            .with_writer(make_writer)
            .with_ansi(false)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .with_env_filter(tracing_subscriber::EnvFilter::new("firestore=debug"))
            .finish();
        (subscriber, buffer)
    }

    struct SharedBufferWriter(StdArc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for SharedBufferWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn captured_text(buffer: &StdArc<std::sync::Mutex<Vec<u8>>>) -> String {
        String::from_utf8(buffer.lock().unwrap().clone()).unwrap()
    }

    #[tokio::test]
    async fn bulk_delete_logs_the_span_tree_and_the_named_lines() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let polls = StdArc::new(AtomicU32::new(0));
        let polls_in_handler = polls.clone();
        let fake = FakeFirestore::start(move |method, _| match method {
            BULK_DELETE => (
                "BulkDeleteDocuments".to_string(),
                bulk_delete_operation_response(
                    OPERATION_NAME,
                    false,
                    BulkDeleteDocumentsMetadata {
                        snapshot_time: Some(gcloud_sdk::prost_types::Timestamp {
                            seconds: 1_700_000_000,
                            nanos: 0,
                        }),
                        ..Default::default()
                    },
                ),
            ),
            GET_OPERATION => {
                let count = polls_in_handler.fetch_add(1, Ordering::SeqCst) + 1;
                let metadata = BulkDeleteDocumentsMetadata {
                    progress_documents: Some(ProtoProgress {
                        estimated_work: 10,
                        completed_work: 10,
                    }),
                    ..Default::default()
                };
                (
                    format!("GetOperation#{count}"),
                    bulk_delete_operation_response(OPERATION_NAME, true, metadata),
                )
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let (subscriber, buffer) = capturing_subscriber();
        let params = FirestoreBulkDeleteParams::new(groups())
            .with_wait(FirestoreOperationWaitOptions::new(Duration::from_secs(5)));
        let result = {
            let _guard = tracing::subscriber::set_default(subscriber);
            fake.db.bulk_delete_documents(params).await.unwrap()
        };
        let output = captured_text(&buffer);

        for expected in [
            "Firestore Bulk Delete",
            "Start Bulk Delete",
            "Firestore Bulk Delete Wait",
        ] {
            assert!(
                output.contains(expected),
                "missing span {expected:?} in:\n{output}"
            );
        }
        assert!(output.contains("Started a bulk delete."));
        assert!(output.contains("Firestore reported the bulk delete's snapshot time."));
        assert!(output.contains("Operation reached a terminal state."));
        assert!(output.contains("Bulk delete users, orders"));
        assert!(
            output.contains("/firestore/response_time"),
            "missing recorded response time in:\n{output}"
        );
        assert_eq!(result.documents.unwrap().completed_work, 10);
    }
}
