use crate::db::safe_document_path;
use crate::db::support::FirestoreUpdateSupport;
use crate::timestamp_utils::from_timestamp;
use crate::FirestoreInstant;
use crate::{
    FirestoreConsistencySelector, FirestoreDb, FirestoreError, FirestoreFieldTransform,
    FirestoreResult, FirestoreWritePrecondition,
};
use async_trait::async_trait;
use gcloud_sdk::google::firestore::v1::*;
use serde::{Deserialize, Serialize};
use tracing::*;

#[async_trait]
impl FirestoreUpdateSupport for FirestoreDb {
    async fn update_obj<I, O, S>(
        &self,
        collection_id: &str,
        document_id: S,
        obj: &I,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
        transforms: Vec<FirestoreFieldTransform>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        self.update_obj_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            obj,
            update_only,
            return_only_fields,
            precondition,
            transforms,
        )
        .await
    }

    async fn update_obj_at<I, O, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &I,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
        transforms: Vec<FirestoreFieldTransform>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        let firestore_doc = Self::serialize_to_doc(
            safe_document_path(parent, collection_id, document_id.as_ref())?.as_str(),
            obj,
        )?;

        let doc = self
            .update_doc(
                collection_id,
                firestore_doc,
                update_only,
                return_only_fields,
                precondition,
                transforms,
            )
            .await?;

        Self::deserialize_doc_to(&doc)
    }

    /// Without `transforms` this is one `UpdateDocument`. `UpdateDocument` cannot carry
    /// transforms, so with them the update is a `Write` in a `Commit`, followed by a read of the
    /// document at the commit time, which shows this write's result whatever other writers do
    /// meanwhile. A failure of that read is returned although the commit has been applied.
    async fn update_doc(
        &self,
        collection_id: &str,
        firestore_doc: Document,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
        transforms: Vec<FirestoreFieldTransform>,
    ) -> FirestoreResult<Document> {
        let document_id = firestore_doc.name.clone();

        let span = span!(
            Level::DEBUG,
            "Firestore Update Document",
            "/firestore/collection_name" = collection_id,
            "/firestore/document_name" = document_id,
            "/firestore/response_time" = field::Empty,
        );

        let update_mask = update_only.map(|field_paths| DocumentMask { field_paths });
        let current_document = precondition.map(|cond| cond.try_into()).transpose()?;

        let begin_query_utc: FirestoreInstant = FirestoreInstant::now();
        let document = if transforms.is_empty() {
            let update_document_request = UpdateDocumentRequest {
                update_mask,
                document: Some(firestore_doc),
                mask: return_only_fields.as_ref().map(|masks| DocumentMask {
                    field_paths: masks.clone(),
                }),
                current_document,
                request_options: self.resolve_request_options(None),
            };
            self.retry_write(
                &span,
                "update document",
                &update_document_request,
                |mut client, request| async move { client.update_document(request).await },
            )
            .await?
        } else {
            let commit_request = CommitRequest {
                database: self.get_database_path().clone(),
                writes: vec![Write {
                    update_mask,
                    update_transforms: transforms
                        .into_iter()
                        .map(|transform| transform.try_into())
                        .collect::<FirestoreResult<_>>()?,
                    current_document,
                    operation: Some(write::Operation::Update(firestore_doc)),
                }],
                transaction: Vec::new(),
                request_options: self.resolve_request_options(None),
            };
            let commit_response = self
                .retry_write(
                    &span,
                    "update document",
                    &commit_request,
                    |mut client, request| async move { client.commit(request).await },
                )
                .await?;
            let commit_time = commit_response
                .commit_time
                .ok_or_else(|| FirestoreError::system_error("Commit returned no commit time"))?;
            self.fetch_doc(
                collection_id.to_string(),
                document_id.clone(),
                return_only_fields,
                Some(&FirestoreConsistencySelector::ReadTime(from_timestamp(
                    commit_time,
                )?)),
            )
            .await?
        };
        let end_query_utc: FirestoreInstant = FirestoreInstant::now();
        let query_duration = end_query_utc.duration_since(begin_query_utc);

        span.record("/firestore/response_time", query_duration.as_millis());

        span.in_scope(|| {
            debug!(collection_id, document_id, "Updated the document.");
        });

        Ok(document)
    }
}

#[cfg(test)]
mod tests {
    use crate::db::fake_firestore::{FakeFirestore, FakeResponse};
    use crate::{path, FirestoreDb};
    use gcloud_sdk::google::firestore::v1::{
        get_document_request, CommitRequest, CommitResponse, GetDocumentRequest, WriteResult,
    };
    use gcloud_sdk::prost::Message as _;
    use gcloud_sdk::prost_types::Timestamp;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Counter {
        name: String,
        count: i64,
    }

    /// The update time Firestore reports for a write that changes nothing: the one of an earlier
    /// write, older than the commit.
    const PREVIOUS_UPDATE_TIME: Timestamp = Timestamp {
        seconds: 1_700_000_000,
        nanos: 5,
    };
    const COMMIT_TIME: Timestamp = Timestamp {
        seconds: 1_700_000_000,
        nanos: 9,
    };
    const COMMITTED_THEN_READ_AT_COMMIT_TIME: [&str; 2] =
        ["Commit with 1 transforms", "GetDocument at 1700000000.9"];

    /// A server that commits a write and then holds `count: 11`. Each call is logged with what a
    /// test asserts on: the number of transforms committed, the time a document is read at.
    async fn server_that_commits_and_reads_back() -> FakeFirestore {
        FakeFirestore::start(|method, request| {
            if method.ends_with("/Commit") {
                let writes = CommitRequest::decode(request).unwrap().writes;
                let response = CommitResponse {
                    write_results: vec![WriteResult {
                        update_time: Some(PREVIOUS_UPDATE_TIME),
                        transform_results: vec![],
                    }],
                    commit_time: Some(COMMIT_TIME),
                };
                (
                    format!(
                        "Commit with {} transforms",
                        writes[0].update_transforms.len()
                    ),
                    FakeResponse::Message(response.encode_to_vec()),
                )
            } else if method.ends_with("/GetDocument") {
                let request = GetDocumentRequest::decode(request).unwrap();
                let Some(get_document_request::ConsistencySelector::ReadTime(read_time)) =
                    request.consistency_selector
                else {
                    return (method.to_string(), FakeResponse::empty());
                };
                let document = FirestoreDb::serialize_to_doc(
                    request.name,
                    &Counter {
                        name: "a".into(),
                        count: 11,
                    },
                )
                .unwrap();
                (
                    format!("GetDocument at {}.{}", read_time.seconds, read_time.nanos),
                    FakeResponse::Message(document.encode_to_vec()),
                )
            } else {
                (method.to_string(), FakeResponse::empty())
            }
        })
        .await
    }

    #[tokio::test]
    async fn object_update_sends_transforms_and_reads_back_at_the_commit_time() {
        let server = server_that_commits_and_reads_back().await;

        let stored: Counter = server
            .db
            .fluent()
            .update()
            .in_col("items")
            .document_id("a")
            .object(&Counter {
                name: "a".into(),
                count: 10,
            })
            .transforms(|t| t.fields([t.field(path!(Counter::count)).increment(1)]))
            .execute()
            .await
            .unwrap();

        assert_eq!(stored.count, 11);
        assert_eq!(server.calls(), COMMITTED_THEN_READ_AT_COMMIT_TIME);
    }

    #[tokio::test]
    async fn document_update_sends_transforms_and_reads_back_at_the_commit_time() {
        let server = server_that_commits_and_reads_back().await;
        let document = FirestoreDb::serialize_to_doc(
            format!("{}/items/a", server.db.get_documents_path()),
            &Counter {
                name: "a".into(),
                count: 10,
            },
        )
        .unwrap();

        let stored = server
            .db
            .fluent()
            .update()
            .in_col("items")
            .transforms(|t| t.fields([t.field(path!(Counter::count)).increment(1)]))
            .document(document)
            .execute()
            .await
            .unwrap();

        assert_eq!(server.calls(), COMMITTED_THEN_READ_AT_COMMIT_TIME);
        assert_eq!(
            FirestoreDb::deserialize_doc_to::<Counter>(&stored)
                .unwrap()
                .count,
            11
        );
    }
}
