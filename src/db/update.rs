use crate::db::retry::CREATE_AND_UPDATE_RETRIED_CODES;
use crate::db::safe_document_path;
use crate::db::support::FirestoreUpdateSupport;
use crate::db::UpdateObjectOperation;
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
            )
            .await?;

        Self::deserialize_doc_to(&doc)
    }

    async fn update_obj_with_transforms<I, O, S>(
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
        self.update_obj_with_transforms_at(
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

    /// `UpdateDocument` cannot carry transforms, so this sends the update as a `Write` in a
    /// `Commit`, and then reads the document at the time of that write, which shows exactly this
    /// write's result whatever other writers do meanwhile.
    ///
    /// A failure of the read is returned although the commit has been applied.
    async fn update_obj_with_transforms_at<I, O, S>(
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
        let document_path = safe_document_path(parent, collection_id, document_id.as_ref())?;
        let write = UpdateObjectOperation {
            parent: parent.to_string(),
            collection_id: collection_id.to_string(),
            document_id: document_id.as_ref(),
            obj,
            update_only,
            precondition,
            update_transforms: transforms,
        }
        .try_into()?;

        let span = span!(
            Level::DEBUG,
            "Firestore Update Document With Transforms",
            "/firestore/collection_name" = collection_id,
            "/firestore/document_name" = document_path,
            "/firestore/response_time" = field::Empty,
        );

        let commit_request = CommitRequest {
            database: self.get_database_path().clone(),
            writes: vec![write],
            transaction: Vec::new(),
            request_options: self.resolve_request_options(None),
        };

        let begin_query_utc: FirestoreInstant = FirestoreInstant::now();
        let commit_response = self
            .retry_write(
                &span,
                "update document",
                &commit_request,
                &CREATE_AND_UPDATE_RETRIED_CODES,
                |mut client, request| async move { client.commit(request).await },
            )
            .await?;

        let written_at = commit_response
            .write_results
            .first()
            .and_then(|result| result.update_time)
            .or(commit_response.commit_time)
            .ok_or_else(|| FirestoreError::system_error("Commit returned no write time"))?;
        let document = self
            .fetch_doc(
                collection_id.to_string(),
                document_path.clone(),
                return_only_fields,
                Some(&FirestoreConsistencySelector::ReadTime(from_timestamp(
                    written_at,
                )?)),
            )
            .await?;

        let query_duration = FirestoreInstant::now().duration_since(begin_query_utc);
        span.record("/firestore/response_time", query_duration.as_millis());
        span.in_scope(|| {
            debug!(collection_id, document_path, "Updated the document.");
        });

        Self::deserialize_doc_to(&document)
    }

    async fn update_doc(
        &self,
        collection_id: &str,
        firestore_doc: Document,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<Document> {
        let document_id = firestore_doc.name.clone();

        let span = span!(
            Level::DEBUG,
            "Firestore Update Document",
            "/firestore/collection_name" = collection_id,
            "/firestore/document_name" = document_id,
            "/firestore/response_time" = field::Empty,
        );

        let update_document_request = UpdateDocumentRequest {
            update_mask: update_only.map({
                |vf| DocumentMask {
                    field_paths: vf.iter().map(|f| f.to_string()).collect(),
                }
            }),
            document: Some(firestore_doc),
            mask: return_only_fields.as_ref().map(|masks| DocumentMask {
                field_paths: masks.clone(),
            }),
            current_document: precondition.map(|cond| cond.try_into()).transpose()?,
            request_options: self.resolve_request_options(None),
        };

        let begin_query_utc: FirestoreInstant = FirestoreInstant::now();
        let update_response = self
            .retry_write(
                &span,
                "update document",
                &update_document_request,
                &CREATE_AND_UPDATE_RETRIED_CODES,
                |mut client, request| async move { client.update_document(request).await },
            )
            .await?;
        let end_query_utc: FirestoreInstant = FirestoreInstant::now();
        let query_duration = end_query_utc.duration_since(begin_query_utc);

        span.record("/firestore/response_time", query_duration.as_millis());

        span.in_scope(|| {
            debug!(collection_id, document_id, "Updated the document.");
        });

        Ok(update_response)
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
    use std::sync::{Arc, Mutex};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Counter {
        name: String,
        count: i64,
    }

    const UPDATE_TIME: Timestamp = Timestamp {
        seconds: 1_700_000_000,
        nanos: 5,
    };
    const COMMIT_TIME: Timestamp = Timestamp {
        seconds: 1_700_000_000,
        nanos: 9,
    };

    /// Runs `update().object(..).transforms(increment count)` against a server that answers
    /// `Commit` with `write_results` and reads back `count: 11`. Returns the result
    /// and the time the document was read at.
    async fn update_with_increment(
        write_results: Vec<WriteResult>,
    ) -> (Counter, Option<Timestamp>) {
        let read_at = Arc::new(Mutex::new(None));
        let observed_read_at = read_at.clone();
        let committed = Arc::new(Mutex::new(None));
        let observed_commit = committed.clone();
        let server = FakeFirestore::start(move |method, request| {
            if method.ends_with("/Commit") {
                *observed_commit.lock().unwrap() = Some(CommitRequest::decode(request).unwrap());
                let response = CommitResponse {
                    write_results: write_results.clone(),
                    commit_time: Some(COMMIT_TIME),
                };
                (
                    "Commit".to_string(),
                    FakeResponse::Message(response.encode_to_vec()),
                )
            } else if method.ends_with("/GetDocument") {
                let request = GetDocumentRequest::decode(request).unwrap();
                if let Some(get_document_request::ConsistencySelector::ReadTime(time)) =
                    request.consistency_selector
                {
                    *observed_read_at.lock().unwrap() = Some(time);
                }
                let document = FirestoreDb::serialize_to_doc(
                    request.name,
                    &Counter {
                        name: "a".into(),
                        count: 11,
                    },
                )
                .unwrap();
                (
                    "GetDocument".to_string(),
                    FakeResponse::Message(document.encode_to_vec()),
                )
            } else {
                ("Other".to_string(), FakeResponse::empty())
            }
        })
        .await;

        let result = server
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
            .execute::<Counter>()
            .await;

        let calls = server.calls();
        let written = committed.lock().unwrap().take();
        assert_eq!(
            calls,
            ["Commit", "GetDocument"],
            "the update must be one Commit and a read"
        );
        let writes = written.unwrap().writes;
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].update_transforms.len(), 1);
        let read_at = read_at.lock().unwrap().take();
        (result.unwrap(), read_at)
    }

    #[tokio::test]
    async fn execute_applies_the_transforms_and_returns_the_stored_document() {
        let (stored, read_at) = update_with_increment(vec![WriteResult {
            update_time: Some(UPDATE_TIME),
            transform_results: vec![],
        }])
        .await;

        assert_eq!(stored.count, 11);
        assert_eq!(read_at, Some(UPDATE_TIME));
    }

    #[tokio::test]
    async fn execute_reads_back_at_the_commit_time_when_the_write_has_no_update_time() {
        let (_, read_at) = update_with_increment(vec![WriteResult::default()]).await;

        assert_eq!(read_at, Some(COMMIT_TIME));
    }
}
