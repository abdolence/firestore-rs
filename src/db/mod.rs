#![allow(clippy::too_many_arguments)]

mod get;

pub use get::*;

mod create;
pub use create::*;

mod update;
pub use update::*;

mod delete;
pub use delete::*;

mod query_models;
pub use query_models::*;
mod precondition_models;
pub use precondition_models::*;

mod query;
pub use query::*;

mod aggregated_query;
pub use aggregated_query::*;

mod list;
pub use list::*;

mod listen_changes;
pub use listen_changes::*;

mod listen_changes_state_storage;
pub use listen_changes_state_storage::*;

use crate::*;
use gcloud_sdk::google::firestore::v1::firestore_client::FirestoreClient;
use gcloud_sdk::google::firestore::v1::*;
use gcloud_sdk::*;
use serde::{Deserialize, Serialize};
use tracing::*;

mod options;
pub use options::*;

mod transaction;
pub use transaction::*;

mod transaction_models;
pub use transaction_models::*;

mod transaction_ops;
use transaction_ops::*;

mod session_params;
pub use session_params::*;

mod consistency_selector;
pub use consistency_selector::*;

mod parent_path_builder;
pub use parent_path_builder::*;

mod batch_writer;
pub use batch_writer::*;

mod batch_streaming_writer;
pub use batch_streaming_writer::*;
mod batch_simple_writer;
pub use batch_simple_writer::*;

use crate::errors::{
    FirestoreError, FirestoreInvalidParametersError, FirestoreInvalidParametersPublicDetails,
};
use std::fmt::Formatter;
use std::sync::Arc;

mod transform_models;
pub use transform_models::*;

struct FirestoreDbInner {
    database_path: String,
    doc_path: String,
    options: FirestoreDbOptions,
    client: GoogleApi<FirestoreClient<GoogleAuthMiddleware>>,
}

#[derive(Clone)]
pub struct FirestoreDb {
    inner: Arc<FirestoreDbInner>,
    session_params: Arc<FirestoreDbSessionParams>,
}

const GOOGLE_FIREBASE_API_URL: &str = "https://firestore.googleapis.com";
const GOOGLE_FIRESTORE_EMULATOR_HOST_ENV: &str = "FIRESTORE_EMULATOR_HOST";

impl FirestoreDb {
    pub async fn new<S>(google_project_id: S) -> FirestoreResult<Self>
    where
        S: AsRef<str>,
    {
        Self::with_options(FirestoreDbOptions::new(
            google_project_id.as_ref().to_string(),
        ))
        .await
    }

    pub async fn with_options(options: FirestoreDbOptions) -> FirestoreResult<Self> {
        Self::with_options_token_source(
            options,
            GCP_DEFAULT_SCOPES.clone(),
            TokenSourceType::Default,
        )
        .await
    }

    pub async fn for_default_project_id() -> FirestoreResult<Self> {
        match FirestoreDbOptions::for_default_project_id().await {
            Some(options) => Self::with_options(options).await,
            _ => Err(FirestoreError::InvalidParametersError(
                FirestoreInvalidParametersError::new(FirestoreInvalidParametersPublicDetails::new(
                    "google_project_id".to_string(),
                    "Unable to retrieve google_project_id".to_string(),
                )),
            )),
        }
    }

    pub async fn with_options_service_account_key_file(
        options: FirestoreDbOptions,
        service_account_key_path: std::path::PathBuf,
    ) -> FirestoreResult<Self> {
        Self::with_options_token_source(
            options,
            gcloud_sdk::GCP_DEFAULT_SCOPES.clone(),
            gcloud_sdk::TokenSourceType::File(service_account_key_path),
        )
        .await
    }

    pub async fn with_options_token_source(
        options: FirestoreDbOptions,
        token_scopes: Vec<String>,
        token_source_type: TokenSourceType,
    ) -> FirestoreResult<Self> {
        let firestore_database_path = format!(
            "projects/{}/databases/{}",
            options.google_project_id, options.database_id
        );
        let firestore_database_doc_path = format!("{firestore_database_path}/documents");

        let effective_firebase_api_url = options
            .firebase_api_url
            .clone()
            .or_else(|| {
                std::env::var(GOOGLE_FIRESTORE_EMULATOR_HOST_ENV)
                    .ok()
                    .map(ensure_url_scheme)
            })
            .unwrap_or_else(|| GOOGLE_FIREBASE_API_URL.to_string());

        info!(
            database_path = firestore_database_path,
            api_url = effective_firebase_api_url,
            token_scopes = token_scopes.join(", "),
            "Creating a new database client.",
        );

        let client = GoogleApiClient::from_function_with_token_source(
            FirestoreClient::new,
            effective_firebase_api_url,
            Some(firestore_database_path.clone()),
            token_scopes,
            token_source_type,
        )
        .await?;

        let inner = FirestoreDbInner {
            database_path: firestore_database_path,
            doc_path: firestore_database_doc_path,
            client,
            options,
        };

        Ok(Self {
            inner: Arc::new(inner),
            session_params: Arc::new(FirestoreDbSessionParams::new()),
        })
    }

    pub fn deserialize_doc_to<T>(doc: &Document) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
    {
        crate::firestore_serde::firestore_document_to_serializable(doc)
    }

    pub fn serialize_to_doc<S, T>(document_path: S, obj: &T) -> FirestoreResult<Document>
    where
        S: AsRef<str>,
        T: Serialize,
    {
        crate::firestore_serde::firestore_document_from_serializable(document_path, obj)
    }

    pub fn serialize_map_to_doc<S, I, IS>(
        document_path: S,
        fields: I,
    ) -> FirestoreResult<FirestoreDocument>
    where
        S: AsRef<str>,
        I: IntoIterator<Item = (IS, FirestoreValue)>,
        IS: AsRef<str>,
    {
        crate::firestore_serde::firestore_document_from_map(document_path, fields)
    }

    pub async fn ping(&self) -> FirestoreResult<()> {
        // Reading non-existing document just to check that database is available to read
        self.get_doc_by_path(
            "-ping-".to_string(),
            self.get_database_path().clone(),
            None,
            0,
        )
        .await
        .ok();
        Ok(())
    }

    #[inline]
    pub fn get_database_path(&self) -> &String {
        &self.inner.database_path
    }

    #[inline]
    pub fn get_documents_path(&self) -> &String {
        &self.inner.doc_path
    }

    #[inline]
    pub fn parent_path<S>(
        &self,
        collection_name: &str,
        document_id: S,
    ) -> FirestoreResult<ParentPathBuilder>
    where
        S: AsRef<str>,
    {
        Ok(ParentPathBuilder::new(safe_document_path(
            self.inner.doc_path.as_str(),
            collection_name,
            document_id.as_ref(),
        )?))
    }

    #[inline]
    pub fn get_options(&self) -> &FirestoreDbOptions {
        &self.inner.options
    }

    #[inline]
    pub fn get_session_params(&self) -> &FirestoreDbSessionParams {
        &self.session_params
    }

    #[inline]
    pub fn client(&self) -> &GoogleApi<FirestoreClient<GoogleAuthMiddleware>> {
        &self.inner.client
    }

    #[inline]
    pub fn clone_with_session_params(&self, session_params: FirestoreDbSessionParams) -> Self {
        Self {
            session_params: session_params.into(),
            ..self.clone()
        }
    }

    #[inline]
    pub fn with_session_params(self, session_params: FirestoreDbSessionParams) -> Self {
        Self {
            session_params: session_params.into(),
            ..self
        }
    }

    #[inline]
    pub fn clone_with_consistency_selector(
        &self,
        consistency_selector: FirestoreConsistencySelector,
    ) -> Self {
        let existing_session_params = (*self.session_params).clone();

        self.clone_with_session_params(
            existing_session_params.with_consistency_selector(consistency_selector),
        )
    }

    #[cfg(feature = "caching")]
    pub fn with_cache(&self, cache_mode: crate::FirestoreDbSessionCacheMode) -> Self {
        let existing_session_params = (*self.session_params).clone();

        self.clone_with_session_params(existing_session_params.with_cache_mode(cache_mode))
    }

    #[cfg(feature = "caching")]
    pub fn read_through_cache<B, LS>(&self, cache: &FirestoreCache<B, LS>) -> Self
    where
        B: FirestoreCacheBackend + Send + Sync + 'static,
        LS: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
    {
        self.with_cache(crate::FirestoreDbSessionCacheMode::ReadThroughCache(
            cache.backend(),
        ))
    }

    #[cfg(feature = "caching")]
    pub fn read_cached_only<B, LS>(&self, cache: &FirestoreCache<B, LS>) -> Self
    where
        B: FirestoreCacheBackend + Send + Sync + 'static,
        LS: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
    {
        self.with_cache(crate::FirestoreDbSessionCacheMode::ReadCachedOnly(
            cache.backend(),
        ))
    }
}

fn ensure_url_scheme(url: String) -> String {
    if !url.contains("://") {
        format!("http://{url}")
    } else {
        url
    }
}

impl std::fmt::Debug for FirestoreDb {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirestoreDb")
            .field("options", &self.inner.options)
            .field("database_path", &self.inner.database_path)
            .field("doc_path", &self.inner.doc_path)
            .finish()
    }
}

pub(crate) fn safe_document_path<S>(
    parent: &str,
    collection_id: &str,
    document_id: S,
) -> FirestoreResult<String>
where
    S: AsRef<str>,
{
    // All restrictions described here: https://firebase.google.com/docs/firestore/quotas#collections_documents_and_fields
    // Here we check only the most dangerous one for `/` to avoid document_id injections, leaving other validation to the server side.
    let document_id_ref = document_id.as_ref();
    if document_id_ref.chars().all(|c| c != '/') && document_id_ref.len() <= 1500 {
        Ok(format!("{parent}/{collection_id}/{document_id_ref}"))
    } else {
        Err(FirestoreError::InvalidParametersError(
            FirestoreInvalidParametersError::new(FirestoreInvalidParametersPublicDetails::new(
                "document_id".to_string(),
                format!("Invalid document ID provided: {document_id_ref}"),
            )),
        ))
    }
}

pub(crate) fn split_document_path(path: &str) -> (&str, &str) {
    // Return string range the last part after '/'
    let split_pos = path.rfind('/').map(|pos| pos + 1).unwrap_or(0);
    if split_pos == 0 {
        ("", path)
    } else {
        (&path[0..split_pos - 1], &path[split_pos..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_document_path() {
        assert_eq!(
            safe_document_path(
                "projects/test-project/databases/(default)/documents",
                "test",
                "test1"
            )
            .ok(),
            Some("projects/test-project/databases/(default)/documents/test/test1".to_string())
        );

        assert_eq!(
            safe_document_path(
                "projects/test-project/databases/(default)/documents",
                "test",
                "test1#test2"
            )
            .ok(),
            Some(
                "projects/test-project/databases/(default)/documents/test/test1#test2".to_string()
            )
        );

        assert_eq!(
            safe_document_path(
                "projects/test-project/databases/(default)/documents",
                "test",
                "test1/test2"
            )
            .ok(),
            None
        );
    }

    #[test]
    fn test_ensure_url_scheme() {
        assert_eq!(
            ensure_url_scheme("localhost:8080".into()),
            "http://localhost:8080"
        );
        assert_eq!(
            ensure_url_scheme("any://localhost:8080".into()),
            "any://localhost:8080"
        );
        assert_eq!(
            ensure_url_scheme("invalid:localhost:8080".into()),
            "http://invalid:localhost:8080"
        );
    }

    #[test]
    fn test_split_document_path() {
        assert_eq!(
            split_document_path("projects/test-project/databases/(default)/documents/test/test1"),
            (
                "projects/test-project/databases/(default)/documents/test",
                "test1"
            )
        );
    }
}
