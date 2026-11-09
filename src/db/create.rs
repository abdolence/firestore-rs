use crate::db::retry::CREATE_AND_UPDATE_RETRIED_CODES;
use crate::db::support::FirestoreCreateSupport;
use crate::db::validate_path_segment;
use crate::FirestoreInstant;
use crate::{FirestoreDb, FirestoreResult};
use async_trait::async_trait;
use gcloud_sdk::google::firestore::v1::*;
use serde::{Deserialize, Serialize};
use tracing::*;

/// Validates the IDs for a create request before it reaches the server.
///
/// `document_id` is validated only when `Some`: `None` means "generate one", and that empty
/// placeholder must not be rejected as an invalid document ID.
fn validate_create_doc_ids<S: AsRef<str>>(
    collection_id: &str,
    document_id: Option<&S>,
) -> FirestoreResult<()> {
    validate_path_segment(collection_id, "collection_id")?;
    if let Some(document_id) = document_id {
        validate_path_segment(document_id.as_ref(), "document_id")?;
    }
    Ok(())
}

const GENERATED_ID_ALPHABET: &[u8; 62] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
const GENERATED_ID_LENGTH: usize = 20;

/// The highest random byte that maps onto the alphabet evenly: 248 values, four for each of its
/// 62 characters. A higher byte is dropped, or the first 8 characters would come up more often.
const GENERATED_ID_MAX_BYTE: u8 = 62 * 4 - 1;

/// Makes a document ID the way Firestore's own clients do: 20 characters from `A-Za-z0-9`, each
/// equally likely.
///
/// The ID is made before the request is sent, so that sending the request again after a failure
/// cannot create a second document.
fn generate_document_id() -> String {
    let mut id = String::with_capacity(GENERATED_ID_LENGTH);
    while id.len() < GENERATED_ID_LENGTH {
        let mut random_bytes = [0u8; 40];
        rand::fill(&mut random_bytes);
        id.extend(
            random_bytes
                .into_iter()
                .filter(|byte| *byte <= GENERATED_ID_MAX_BYTE)
                .map(|byte| {
                    char::from(
                        GENERATED_ID_ALPHABET[usize::from(byte) % GENERATED_ID_ALPHABET.len()],
                    )
                })
                .take(GENERATED_ID_LENGTH - id.len()),
        );
    }
    id
}

#[async_trait]
impl FirestoreCreateSupport for FirestoreDb {
    async fn create_doc<S>(
        &self,
        collection_id: &str,
        document_id: Option<S>,
        input_doc: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send,
    {
        self.create_doc_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            input_doc,
            return_only_fields,
        )
        .await
    }

    async fn create_doc_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: Option<S>,
        input_doc: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send,
    {
        validate_create_doc_ids(collection_id, document_id.as_ref())?;

        let span = span!(
            Level::DEBUG,
            "Firestore Create Document",
            "/firestore/collection_name" = collection_id,
            "/firestore/response_time" = field::Empty,
            "/firestore/document_name" = field::Empty,
        );

        let create_document_request = CreateDocumentRequest {
            parent: parent.into(),
            document_id: document_id
                .as_ref()
                .map(|id| id.as_ref().to_string())
                .unwrap_or_else(generate_document_id),
            mask: return_only_fields.as_ref().map(|masks| DocumentMask {
                field_paths: masks.clone(),
            }),
            collection_id: collection_id.into(),
            document: Some(input_doc),
            request_options: self.resolve_request_options(None),
        };

        let begin_query_utc: FirestoreInstant = FirestoreInstant::now();

        let create_response = self
            .retry_write(
                &span,
                "create document",
                &create_document_request,
                &CREATE_AND_UPDATE_RETRIED_CODES,
                |mut client, request| async move { client.create_document(request).await },
            )
            .await?;

        let end_query_utc: FirestoreInstant = FirestoreInstant::now();
        let query_duration = end_query_utc.duration_since(begin_query_utc);

        span.record("/firestore/response_time", query_duration.as_millis());

        span.record("/firestore/document_name", &create_response.name);

        span.in_scope(|| {
            debug!(
                collection_id,
                document_id = document_id.as_ref().map(|id| id.as_ref()),
                "Created a new document.",
            );
        });

        Ok(create_response)
    }

    async fn create_obj<I, O, S>(
        &self,
        collection_id: &str,
        document_id: Option<S>,
        obj: &I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        self.create_obj_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            obj,
            return_only_fields,
        )
        .await
    }

    async fn create_obj_at<I, O, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: Option<S>,
        obj: &I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        let input_doc = Self::serialize_to_doc("", obj)?;

        let doc = self
            .create_doc_at(
                parent,
                collection_id,
                document_id,
                input_doc,
                return_only_fields,
            )
            .await?;

        Self::deserialize_doc_to(&doc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_collection_id() {
        assert!(validate_create_doc_ids::<String>("users", None).is_ok());
        assert!(validate_create_doc_ids::<String>("a/b", None).is_err());
        assert!(validate_create_doc_ids::<String>("", None).is_err());
    }

    #[test]
    fn validates_document_id_only_when_some() {
        assert!(validate_create_doc_ids("users", Some(&"user-1".to_string())).is_ok());
        assert!(validate_create_doc_ids("users", Some(&"a/b".to_string())).is_err());
        assert!(
            validate_create_doc_ids("users", Some(&"".to_string())).is_err(),
            "an explicit empty document_id must not silently auto-generate"
        );
    }

    #[test]
    fn none_document_id_is_always_valid() {
        // `None` means "generate an ID"; it must never be validated as an ID.
        assert!(validate_create_doc_ids::<String>("users", None).is_ok());
    }
}
