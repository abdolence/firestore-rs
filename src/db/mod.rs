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

mod query;
pub use query::*;

mod aggregated_query;
pub use aggregated_query::*;

mod list_doc;
pub use list_doc::*;

mod listen_changes;
pub use listen_changes::*;

use crate::FirestoreResult;
use gcloud_sdk::google::firestore::v1::firestore_client::FirestoreClient;
use gcloud_sdk::google::firestore::v1::*;
use gcloud_sdk::*;
use serde::{Deserialize, Serialize};
use tracing::*;

mod options;
pub use options::*;

mod transaction;
pub use transaction::*;

mod transaction_ops;
pub use transaction_ops::*;

mod session_params;
pub use session_params::*;

mod consistency_selector;
pub use consistency_selector::*;

mod parent_path_builder;
pub use parent_path_builder::*;

use std::fmt::Formatter;

#[derive(Clone)]
pub struct FirestoreDb {
    database_path: String,
    doc_path: String,
    options: FirestoreDbOptions,
    client: GoogleApi<FirestoreClient<GoogleAuthMiddleware>>,
    session_params: FirestoreDbSessionParams,
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

    pub async fn with_options_token_source(
        options: FirestoreDbOptions,
        token_scopes: Vec<String>,
        token_source_type: TokenSourceType,
    ) -> FirestoreResult<Self> {
        let firestore_database_path =
            format!("projects/{}/databases/(default)", options.google_project_id);
        let firestore_database_doc_path = format!("{}/documents", firestore_database_path);

        let effective_firebase_api_url = options
            .firebase_api_url
            .clone()
            .or_else(|| std::env::var(GOOGLE_FIRESTORE_EMULATOR_HOST_ENV).ok())
            .unwrap_or_else(|| GOOGLE_FIREBASE_API_URL.to_string());

        info!(
            "Creating a new DB client: {}. API: {} Token scopes: {}",
            effective_firebase_api_url,
            firestore_database_path,
            token_scopes.join(", ")
        );

        let client = GoogleApiClient::from_function_with_token_source(
            FirestoreClient::new,
            effective_firebase_api_url,
            Some(firestore_database_path.clone()),
            token_scopes,
            token_source_type,
        )
        .await?;

        Ok(Self {
            database_path: firestore_database_path,
            doc_path: firestore_database_doc_path,
            client,
            options,
            session_params: FirestoreDbSessionParams::new(),
        })
    }

    pub fn deserialize_doc_to<T>(doc: &Document) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
    {
        crate::firestore_serde::firestore_document_to_serializable(doc)
    }

    pub fn serialize_to_doc<T>(document_path: &str, obj: &T) -> FirestoreResult<Document>
    where
        T: Serialize,
    {
        crate::firestore_serde::firestore_document_from_serializable(document_path, obj)
    }

    pub async fn ping(&self) -> FirestoreResult<()> {
        // Reading non-existing document just to check that database is available to read
        self.get_doc_by_path(self.get_database_path().clone(), 0)
            .await
            .ok();
        Ok(())
    }

    #[inline]
    pub const fn get_database_path(&self) -> &String {
        &self.database_path
    }

    #[inline]
    pub const fn get_documents_path(&self) -> &String {
        &self.doc_path
    }

    #[inline]
    pub fn parent_path<S>(
        &self,
        parent_collection_name: &str,
        parent_document_id: S,
    ) -> ParentPathBuilder
    where
        S: AsRef<str>,
    {
        ParentPathBuilder::new(format!(
            "{}/{}/{}",
            &self.doc_path.as_str(),
            parent_collection_name,
            parent_document_id.as_ref()
        ))
    }

    #[inline]
    pub const fn get_options(&self) -> &FirestoreDbOptions {
        &self.options
    }

    #[inline]
    pub const fn get_session_params(&self) -> &FirestoreDbSessionParams {
        &self.session_params
    }

    #[inline]
    pub const fn client(&self) -> &GoogleApi<FirestoreClient<GoogleAuthMiddleware>> {
        &self.client
    }

    #[inline]
    pub fn clone_with_session_params(&self, session_params: FirestoreDbSessionParams) -> Self {
        Self {
            session_params,
            ..self.clone()
        }
    }

    #[inline]
    pub fn with_session_params(self, session_params: FirestoreDbSessionParams) -> Self {
        Self {
            session_params,
            ..self
        }
    }

    #[inline]
    pub fn clone_with_consistency_selector(
        &self,
        consistency_selector: FirestoreConsistencySelector,
    ) -> Self {
        self.clone_with_session_params(
            self.session_params
                .clone()
                .with_consistency_selector(consistency_selector),
        )
    }
}

impl std::fmt::Debug for FirestoreDb {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirestoreDb")
            .field("options", &self.options)
            .field("database_path", &self.database_path)
            .field("doc_path", &self.doc_path)
            .field("session_params", &self.session_params)
            .finish()
    }
}
