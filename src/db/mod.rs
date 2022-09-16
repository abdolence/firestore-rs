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

pub type FirestoreCursor = gcloud_sdk::google::firestore::v1::Cursor;

#[derive(Clone)]
pub struct FirestoreDb {
    database_path: String,
    doc_path: String,
    options: FirestoreDbOptions,
    client: GoogleApi<FirestoreClient<GoogleAuthMiddleware>>,
    session_params: FirestoreDbSessionParams,
}

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
        let firestore_database_path =
            format!("projects/{}/databases/(default)", options.google_project_id);
        let firestore_database_doc_path = format!("{}/documents", firestore_database_path);

        info!("Creating a new DB client: {}", firestore_database_path);

        let client = GoogleApiClient::from_function(
            FirestoreClient::new,
            "https://firestore.googleapis.com",
            Some(firestore_database_path.clone()),
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
