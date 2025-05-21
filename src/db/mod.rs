// Linter allowance for functions that might have many arguments,
// often seen in builder patterns or comprehensive configuration methods.
#![allow(clippy::too_many_arguments)]

/// Module for document retrieval operations (get).
mod get;
pub use get::*;

/// Module for document creation operations.
mod create;
pub use create::*;

/// Module for document update operations.
mod update;
pub use update::*;

/// Module for document deletion operations.
mod delete;
pub use delete::*;

/// Module defining models used in queries (filters, orders, etc.).
mod query_models;
pub use query_models::*;

/// Module defining models for preconditions (e.g., last update time).
mod precondition_models;
pub use precondition_models::*;

/// Module for query execution.
mod query;
pub use query::*;

/// Module for aggregated query execution.
mod aggregated_query;
pub use aggregated_query::*;

/// Module for listing documents or collections.
mod list;
pub use list::*;

/// Module for listening to real-time document changes.
mod listen_changes;
pub use listen_changes::*;

/// Module for storing the state of listen operations (e.g., resume tokens).
mod listen_changes_state_storage;
pub use listen_changes_state_storage::*;

use crate::*;
use gcloud_sdk::google::firestore::v1::firestore_client::FirestoreClient;
use gcloud_sdk::google::firestore::v1::*;
use gcloud_sdk::*;
// Re-export serde for convenience as it's often used with Firestore documents.
use serde::{Deserialize, Serialize};
use tracing::*;

/// Module for database client options and configuration.
mod options;
pub use options::*;

/// Module for Firestore transactions.
mod transaction;
pub use transaction::*;

/// Module defining models related to transactions.
mod transaction_models;
pub use transaction_models::*;

/// Internal module for transaction operations.
mod transaction_ops;
use transaction_ops::*;

/// Module for session-specific parameters (e.g., consistency, caching).
mod session_params;
pub use session_params::*;

/// Module for defining read consistency (e.g., read_time, transaction_id).
mod consistency_selector;
pub use consistency_selector::*;

/// Module for building parent paths for sub-collections.
mod parent_path_builder;
pub use parent_path_builder::*;

/// Module for batch writing operations.
mod batch_writer;
pub use batch_writer::*;

/// Module for streaming batch write operations.
mod batch_streaming_writer;
pub use batch_streaming_writer::*;

/// Module for simple (non-streaming) batch write operations.
mod batch_simple_writer;
pub use batch_simple_writer::*;

use crate::errors::{
    FirestoreError, FirestoreInvalidParametersError, FirestoreInvalidParametersPublicDetails,
};
use std::fmt::Formatter;
use std::sync::Arc;

/// Module defining models for document transformations (e.g., server-side increments).
mod transform_models;
pub use transform_models::*;

/// Internal struct holding the core components of the Firestore database client.
/// This includes the database path, document path prefix, options, and the gRPC client.
struct FirestoreDbInner {
    database_path: String,
    doc_path: String,
    options: FirestoreDbOptions,
    client: GoogleApi<FirestoreClient<GoogleAuthMiddleware>>,
}

/// The main entry point for interacting with a Google Firestore database.
///
/// `FirestoreDb` provides methods for database operations such as creating, reading,
/// updating, and deleting documents, as well as querying collections and running transactions.
/// It manages the connection and authentication with the Firestore service.
///
/// Instances of `FirestoreDb` are cloneable and internally use `Arc` for shared state,
/// making them cheap to clone and safe to share across threads.
#[derive(Clone)]
pub struct FirestoreDb {
    inner: Arc<FirestoreDbInner>,
    session_params: Arc<FirestoreDbSessionParams>,
}

const GOOGLE_FIREBASE_API_URL: &str = "https://firestore.googleapis.com";
const GOOGLE_FIRESTORE_EMULATOR_HOST_ENV: &str = "FIRESTORE_EMULATOR_HOST";

impl FirestoreDb {
    /// Creates a new `FirestoreDb` instance with the specified Google Project ID.
    ///
    /// This is a convenience method that uses default [`FirestoreDbOptions`].
    /// For more control over configuration, use [`FirestoreDb::with_options`].
    ///
    /// # Arguments
    /// * `google_project_id`: The Google Cloud Project ID that owns the Firestore database.
    ///
    /// # Example
    /// ```rust,no_run
    /// use firestore::*; // Imports FirestoreDb, FirestoreResult, etc.
    ///
    /// # async fn run() -> FirestoreResult<()> {
    /// let db = FirestoreDb::new("my-gcp-project-id").await?;
    /// // Use db for Firestore operations
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new<S>(google_project_id: S) -> FirestoreResult<Self>
    where
        S: AsRef<str>,
    {
        Self::with_options(FirestoreDbOptions::new(
            google_project_id.as_ref().to_string(),
        ))
        .await
    }

    /// Creates a new `FirestoreDb` instance with the specified options.
    ///
    /// This method allows for detailed configuration of the Firestore client,
    /// such as setting a custom database ID or API URL.
    /// It uses default token scopes and token source.
    ///
    /// # Arguments
    /// * `options`: The [`FirestoreDbOptions`] to configure the client.
    pub async fn with_options(options: FirestoreDbOptions) -> FirestoreResult<Self> {
        Self::with_options_token_source(
            options,
            GCP_DEFAULT_SCOPES.clone(),
            TokenSourceType::Default,
        )
        .await
    }

    /// Creates a new `FirestoreDb` instance attempting to infer the Google Project ID
    /// from the environment (e.g., Application Default Credentials).
    ///
    /// This is useful in environments where the project ID is implicitly available.
    ///
    /// # Errors
    /// Returns an [`FirestoreError::InvalidParametersError`] if the project ID cannot be inferred.
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

    /// Creates a new `FirestoreDb` instance with specified options and a service account key file
    /// for authentication.
    ///
    /// # Arguments
    /// * `options`: The [`FirestoreDbOptions`] to configure the client.
    /// * `service_account_key_path`: Path to the JSON service account key file.
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

    /// Creates a new `FirestoreDb` instance with full control over options, token scopes,
    /// and token source type.
    ///
    /// This is the most flexible constructor, allowing customization of authentication
    /// and authorization aspects.
    ///
    /// # Arguments
    /// * `options`: The [`FirestoreDbOptions`] to configure the client.
    /// * `token_scopes`: A list of OAuth2 scopes required for Firestore access.
    /// * `token_source_type`: The [`TokenSourceType`](gcloud_sdk::TokenSourceType)
    ///   specifying how to obtain authentication tokens (e.g., default, file, metadata server).
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

    /// Deserializes a Firestore [`Document`] into a Rust type `T`.
    ///
    /// This function uses the custom Serde deserializer provided by this crate
    /// to map Firestore's native data types to Rust structs.
    ///
    /// # Arguments
    /// * `doc`: A reference to the Firestore [`Document`] to deserialize.
    ///
    /// # Type Parameters
    /// * `T`: The target Rust type that implements `serde::Deserialize`.
    ///
    /// # Errors
    /// Returns a [`FirestoreError::DeserializeError`] if deserialization fails.
    pub fn deserialize_doc_to<T>(doc: &Document) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
    {
        crate::firestore_serde::firestore_document_to_serializable(doc)
    }

    /// Serializes a Rust type `T` into a Firestore [`Document`].
    ///
    /// This function uses the custom Serde serializer to convert Rust structs
    /// into Firestore's native data format.
    ///
    /// # Arguments
    /// * `document_path`: The full path to the document (e.g., "projects/my-project/databases/(default)/documents/my-collection/my-doc").
    ///   This is used to set the `name` field of the resulting Firestore document.
    /// * `obj`: A reference to the Rust object to serialize.
    ///
    /// # Type Parameters
    /// * `S`: A type that can be converted into a string for the document path.
    /// * `T`: The source Rust type that implements `serde::Serialize`.
    ///
    /// # Errors
    /// Returns a [`FirestoreError::SerializeError`] if serialization fails.
    pub fn serialize_to_doc<S, T>(document_path: S, obj: &T) -> FirestoreResult<Document>
    where
        S: AsRef<str>,
        T: Serialize,
    {
        crate::firestore_serde::firestore_document_from_serializable(document_path, obj)
    }

    /// Serializes a map of field names to [`FirestoreValue`]s into a Firestore [`Document`].
    ///
    /// This is useful for constructing documents dynamically or when working with
    /// partially structured data.
    ///
    /// # Arguments
    /// * `document_path`: The full path to the document.
    /// * `fields`: An iterator yielding pairs of field names (as strings) and their
    ///   corresponding [`FirestoreValue`]s.
    ///
    /// # Type Parameters
    /// * `S`: A type that can be converted into a string for the document path.
    /// * `I`: An iterator type for the fields.
    /// * `IS`: A type that can be converted into a string for field names.
    ///
    /// # Errors
    /// Returns a [`FirestoreError::SerializeError`] if serialization fails.
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

    /// Performs a simple "ping" to the Firestore database to check connectivity.
    ///
    /// This method attempts to read a non-existent document. A successful outcome
    /// (even if the document is not found) indicates that the database is reachable
    /// and the client is authenticated.
    ///
    /// # Errors
    /// May return network or authentication errors if the database is unreachable.
    pub async fn ping(&self) -> FirestoreResult<()> {
        // Reading non-existing document just to check that database is available to read
        self.get_doc_by_path(
            "-ping-".to_string(),             // A document ID that is unlikely to exist
            self.get_database_path().clone(), // Use the root database path for this check
            None,                             // No specific consistency required
            0,                                // No retries needed for a ping
        )
        .await
        .map(|_| ()) // If it's Ok(None) or Ok(Some(_)), it's a success for ping
        .or_else(|err| {
            // If the error is DataNotFoundError, it's still a successful ping.
            // Other errors (network, auth) are real failures.
            if matches!(err, FirestoreError::DataNotFoundError(_)) {
                Ok(())
            } else {
                Err(err)
            }
        })
    }

    /// Returns the full database path string (e.g., "projects/my-project/databases/(default)").
    #[inline]
    pub fn get_database_path(&self) -> &String {
        &self.inner.database_path
    }

    /// Returns the base path for documents within this database
    /// (e.g., "projects/my-project/databases/(default)/documents").
    #[inline]
    pub fn get_documents_path(&self) -> &String {
        &self.inner.doc_path
    }

    /// Constructs a [`ParentPathBuilder`] for creating paths to sub-collections
    /// under a specified document.
    ///
    /// # Arguments
    /// * `collection_name`: The name of the collection containing the parent document.
    /// * `document_id`: The ID of the parent document.
    ///
    /// # Errors
    /// Returns [`FirestoreError::InvalidParametersError`] if the `document_id` is invalid.
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

    /// Returns a reference to the [`FirestoreDbOptions`] used to configure this client.
    #[inline]
    pub fn get_options(&self) -> &FirestoreDbOptions {
        &self.inner.options
    }

    /// Returns a reference to the current [`FirestoreDbSessionParams`] for this client instance.
    /// Session parameters can control aspects like consistency and caching for operations
    /// performed with this specific `FirestoreDb` instance.
    #[inline]
    pub fn get_session_params(&self) -> &FirestoreDbSessionParams {
        &self.session_params
    }

    /// Returns a reference to the underlying gRPC client.
    ///
    /// This provides access to the raw `FirestoreClient` from the `gcloud-sdk`
    /// if direct interaction with the gRPC layer is needed.
    #[inline]
    pub fn client(&self) -> &GoogleApi<FirestoreClient<GoogleAuthMiddleware>> {
        &self.inner.client
    }

    /// Clones the `FirestoreDb` instance, replacing its session parameters.
    ///
    /// This is useful for creating a new client instance that shares the same
    /// underlying connection and configuration but has different session-level
    /// settings (e.g., for a specific transaction or consistency requirement).
    ///
    /// # Arguments
    /// * `session_params`: The new [`FirestoreDbSessionParams`] to use.
    #[inline]
    pub fn clone_with_session_params(&self, session_params: FirestoreDbSessionParams) -> Self {
        Self {
            session_params: session_params.into(),
            ..self.clone()
        }
    }

    /// Consumes the `FirestoreDb` instance and returns a new one with replaced session parameters.
    ///
    /// Similar to [`clone_with_session_params`](FirestoreDb::clone_with_session_params)
    /// but takes ownership of `self`.
    ///
    /// # Arguments
    /// * `session_params`: The new [`FirestoreDbSessionParams`] to use.
    #[inline]
    pub fn with_session_params(self, session_params: FirestoreDbSessionParams) -> Self {
        Self {
            session_params: session_params.into(),
            ..self
        }
    }

    /// Clones the `FirestoreDb` instance with a specific consistency selector.
    ///
    /// This creates a new `FirestoreDb` instance configured to use the provided
    /// [`FirestoreConsistencySelector`] for subsequent operations.
    ///
    /// # Arguments
    /// * `consistency_selector`: The consistency mode to apply (e.g., read at a specific time).
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

    /// Clones the `FirestoreDb` instance with a specific cache mode.
    ///
    /// This method is only available if the `caching` feature is enabled.
    ///
    /// # Arguments
    /// * `cache_mode`: The [`FirestoreDbSessionCacheMode`] to apply.
    #[cfg(feature = "caching")]
    pub fn with_cache(&self, cache_mode: crate::FirestoreDbSessionCacheMode) -> Self {
        let existing_session_params = (*self.session_params).clone();

        self.clone_with_session_params(existing_session_params.with_cache_mode(cache_mode))
    }

    /// Clones the `FirestoreDb` instance to enable read-through caching with the provided cache.
    ///
    /// Operations using the returned `FirestoreDb` instance will first attempt to read
    /// from the cache. If data is not found, it will be fetched from Firestore and
    /// then stored in the cache.
    ///
    /// This method is only available if the `caching` feature is enabled.
    ///
    /// # Arguments
    /// * `cache`: A reference to the [`FirestoreCache`](crate::FirestoreCache) to use.
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

    /// Clones the `FirestoreDb` instance to read exclusively from the cache.
    ///
    /// Operations using the returned `FirestoreDb` instance will only attempt to read
    /// from the cache and will not fetch data from Firestore if it's not found in the cache.
    ///
    /// This method is only available if the `caching` feature is enabled.
    ///
    /// # Arguments
    /// * `cache`: A reference to the [`FirestoreCache`](crate::FirestoreCache) to use.
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

/// Ensures that a URL string has a scheme (e.g., "http://").
/// If no scheme is present, "http://" is prepended.
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
