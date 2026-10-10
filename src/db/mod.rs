// Linter allowance for functions that might have many arguments,
// often seen in builder patterns or comprehensive configuration methods.
#![allow(clippy::too_many_arguments)]

mod path_ids;
pub use path_ids::*;

#[cfg(feature = "admin")]
mod admin;
#[cfg(feature = "admin")]
pub use admin::*;

mod get;

mod create;

mod update;

mod delete;

mod query_models;
pub use query_models::*;

mod precondition_models;
pub use precondition_models::*;

mod query;

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
// Re-export serde for convenience as it's often used with Firestore documents.
use serde::{Deserialize, Serialize};
use tracing::*;

mod options;
pub use options::*;

mod transaction;
pub use transaction::*;

#[cfg(test)]
mod transaction_read_tests;

mod transaction_models;
pub use transaction_models::*;

mod support;
pub(crate) use support::*;

mod transaction_ops;
use transaction_ops::*;

mod retry;

mod endpoint;
use endpoint::{FirestoreDbEndpoint, FirestoreEmulatorCredentials};

#[cfg(test)]
mod fake_firestore;

mod session_params;
pub use session_params::*;

mod consistency_selector;
pub use consistency_selector::*;

mod request_options;
pub use request_options::*;

mod parent_path_builder;
pub use parent_path_builder::*;

mod batch_writer;
pub use batch_writer::*;

mod batch_streaming_writer;
pub use batch_streaming_writer::*;

mod batch_simple_writer;
pub use batch_simple_writer::*;

use crate::errors::FirestoreError;
use std::fmt::Formatter;
use std::sync::Arc;

mod transform_models;
pub use transform_models::*;

/// Internal struct holding the core components of the Firestore database client.
/// This includes the database path, document path prefix, options, and the gRPC client.
struct FirestoreDbInner {
    database_path: String,
    doc_path: String,
    options: FirestoreDbOptions,
    client: GoogleApi<FirestoreClient<GoogleAuthMiddleware>>,
    /// Whether this client sends its requests to the `FIRESTORE_EMULATOR_HOST` emulator rather
    /// than the real service (see [`FirestoreDbEndpoint::is_emulator`]). The emulator does not implement the admin API's index management RPCs,
    /// so [`FirestoreIndexSupport`](crate::db::support::FirestoreIndexSupport) reads this to skip
    /// rather than fail every call a caller's startup code makes unconditionally. Only read under
    /// the `admin` feature, so the field only exists there.
    #[cfg(feature = "admin")]
    is_emulator: bool,
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

impl FirestoreDb {
    /// Creates a new `FirestoreDb` instance with the specified Google Project ID.
    ///
    /// This is a convenience method that uses default [`FirestoreDbOptions`].
    /// For more control over configuration, use [`FirestoreDb::with_options`].
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
    /// It authenticates with the Application Default Credentials, for the `cloud-platform`
    /// scope. When the `FIRESTORE_EMULATOR_HOST` environment variable is set, it looks up no
    /// credentials and sends the emulator a stub token instead.
    ///
    /// # Errors
    /// Returns a [`FirestoreError::SystemError`] if no credentials are found or they cannot be
    /// built, for example a service account key with the `auth-default-crypto` feature off and
    /// no rustls `CryptoProvider` installed.
    pub async fn with_options(options: FirestoreDbOptions) -> FirestoreResult<Self> {
        let endpoint = FirestoreDbEndpoint::from_env(&options);
        Self::with_default_auth(options, endpoint).await
    }

    /// [`with_options`](Self::with_options) for `endpoint`, which tests point at a fake
    /// emulator without setting `FIRESTORE_EMULATOR_HOST`.
    async fn with_default_auth(
        options: FirestoreDbOptions,
        endpoint: FirestoreDbEndpoint,
    ) -> FirestoreResult<Self> {
        let client = if endpoint.emulator_host.is_some() {
            debug!("Firestore emulator detected, sending a stub token instead of looking up credentials.");
            endpoint
                .connect(FirestoreEmulatorCredentials::new().into())
                .await?
        } else {
            endpoint.connect_with_adc().await?
        };
        Ok(Self::with_client(options, endpoint, client))
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
            _ => Err(FirestoreError::invalid_parameters(
                "google_project_id",
                "Unable to retrieve google_project_id",
            )),
        }
    }

    /// Creates a new `FirestoreDb` instance with the given options, authenticating with the
    /// service account key file at `service_account_key_path`, for the `cloud-platform` scope.
    ///
    /// The key signs its tokens with the rustls crypto provider of the `auth-default-crypto`
    /// feature. With that feature off, install a rustls `CryptoProvider` before calling this:
    /// without one, every token request panics.
    ///
    /// # Errors
    /// Returns a [`FirestoreError::InvalidParametersError`] if the file cannot be read or is not
    /// JSON, and a [`FirestoreError::SystemError`] if it is not a service account key.
    pub async fn with_options_service_account_key_file(
        options: FirestoreDbOptions,
        service_account_key_path: std::path::PathBuf,
    ) -> FirestoreResult<Self> {
        let unreadable_key = |reason: String| {
            FirestoreError::invalid_parameters(
                "service_account_key_path",
                format!("{}: {reason}", service_account_key_path.display()),
            )
        };
        let key = std::fs::read(&service_account_key_path)
            .map_err(|error| unreadable_key(error.to_string()))?;
        let key =
            serde_json::from_slice(&key).map_err(|error| unreadable_key(error.to_string()))?;
        let credentials =
            gcloud_sdk::google_cloud_auth::credentials::service_account::Builder::new(key)
                .build()
                .map_err(gcloud_sdk::error::Error::from)?;
        Self::with_options_auth(options, credentials).await
    }

    /// Creates a new `FirestoreDb` instance with the given options, authenticating every request
    /// with the headers `auth` serves.
    ///
    /// `auth` is any of:
    /// - a google-cloud-auth `Credentials`, built with the builders of
    ///   `gcloud_sdk::google_cloud_auth::credentials`: a service account key, user credentials,
    ///   impersonation, workload identity federation, the metadata server, custom scopes, or a
    ///   `CredentialsProvider` of your own passed to `Credentials::from`;
    /// - a [`gcloud_sdk::GoogleAuthHeaders`], such as
    ///   [`GoogleAuthHeaders::from_adc_with_scopes`](gcloud_sdk::GoogleAuthHeaders::from_adc_with_scopes)
    ///   builds.
    ///
    /// These credentials are used as given, also when `FIRESTORE_EMULATOR_HOST` is set.
    ///
    /// Credentials built from a service account key sign with a rustls crypto provider. With the
    /// `auth-default-crypto` feature off, install a rustls `CryptoProvider` before building them.
    pub async fn with_options_auth(
        options: FirestoreDbOptions,
        auth: impl Into<GoogleAuthHeaders>,
    ) -> FirestoreResult<Self> {
        let endpoint = FirestoreDbEndpoint::from_env(&options);
        let client = endpoint.connect(auth.into()).await?;
        Ok(Self::with_client(options, endpoint, client))
    }

    fn with_client(
        options: FirestoreDbOptions,
        endpoint: FirestoreDbEndpoint,
        client: GoogleApi<FirestoreClient<GoogleAuthMiddleware>>,
    ) -> Self {
        info!(
            database_path = endpoint.database_path,
            api_url = endpoint.api_url,
            "Created a new database client.",
        );

        let inner = FirestoreDbInner {
            doc_path: format!("{}/documents", endpoint.database_path),
            #[cfg(feature = "admin")]
            is_emulator: endpoint.is_emulator(),
            database_path: endpoint.database_path,
            client,
            options,
        };

        Self {
            inner: Arc::new(inner),
            session_params: Arc::new(FirestoreDbSessionParams::new()),
        }
    }

    /// Deserializes a Firestore [`Document`] into a Rust type `T`.
    ///
    /// This function uses the custom Serde deserializer provided by this crate
    /// to map Firestore's native data types to Rust structs.
    ///
    /// # Errors
    /// Returns a [`FirestoreError::DeserializeError`] if deserialization fails.
    pub fn deserialize_doc_to<T>(doc: &Document) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
    {
        crate::firestore_serde::firestore_document_to_serializable(doc)
    }

    /// Serializes a Rust type `T` into a Firestore [`Document`], setting the document's `name`
    /// field to `document_path`.
    ///
    /// This function uses the custom Serde serializer to convert Rust structs
    /// into Firestore's native data format.
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

    /// Serializes an iterator of field name/[`FirestoreValue`] pairs into a Firestore
    /// [`Document`] at `document_path`.
    ///
    /// Use this for constructing documents dynamically or when working with
    /// partially structured data, rather than a typed `T`.
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
    /// A single attempt, deliberately not retried: the only thing worth knowing from a ping is
    /// whether the database answers right now, and a backoff retry would hide that behind a
    /// delay instead of reporting it.
    ///
    /// # Errors
    /// May return network or authentication errors if the database is unreachable.
    pub async fn ping(&self) -> FirestoreResult<()> {
        // A resource name under `{database}/documents/`, built directly rather than through
        // `get_doc_by_path`: that helper answers a cache hit, or a `ReadCachedOnly` cache miss,
        // without ever reaching the server, which would let ping report a stale or misconfigured
        // cache as a reachable database.
        let document_path = safe_document_path(self.get_documents_path(), "-ping-", "-ping-")?;

        let request = GetDocumentRequest {
            name: document_path,
            consistency_selector: None,
            request_options: self.resolve_request_options(None),
            mask: None,
        };

        match self.client().get().get_document(request).await {
            Ok(_) => Ok(()),
            // NOT_FOUND on a document that is never written is exactly what a reachable,
            // authenticated database returns; any other error is a real connectivity failure.
            Err(status) => match FirestoreError::from(status) {
                FirestoreError::DataNotFoundError(_) => Ok(()),
                err => Err(err),
            },
        }
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

    /// Constructs a [`ParentPathBuilder`] for the path to `document_id` in `collection_name`, for
    /// building paths to its sub-collections.
    ///
    /// # Errors
    /// Returns [`FirestoreError::InvalidParametersError`] if the `document_id` is invalid.
    #[inline]
    pub fn parent_path<C, S>(
        &self,
        collection_name: C,
        document_id: S,
    ) -> FirestoreResult<ParentPathBuilder>
    where
        C: AsRef<str>,
        S: AsRef<str>,
    {
        Ok(ParentPathBuilder::new(safe_document_path(
            self.inner.doc_path.as_str(),
            collection_name.as_ref(),
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

    /// Resolves the effective request options for an operation.
    ///
    /// A per operation override takes precedence over the session wide default
    /// configured with
    /// [`clone_with_request_options`](FirestoreDb::clone_with_request_options).
    #[inline]
    pub(crate) fn resolve_request_options(
        &self,
        request_options: Option<&FirestoreRequestOptions>,
    ) -> Option<gcloud_sdk::google::firestore::v1::RequestOptions> {
        FirestoreRequestOptions::resolve(
            request_options,
            self.session_params.request_options.as_ref(),
        )
    }

    /// Returns a reference to the underlying `gcloud-sdk` gRPC client.
    ///
    /// **Unsupported escape hatch.** The [Fluent API](Self::fluent) is the supported way to use
    /// this crate; this method exists only for the rare case where it does not yet cover
    /// something you need.
    ///
    /// It exposes types from `gcloud-sdk`, whose version is *not* part of this crate's semver
    /// contract: this signature and the types behind it may change in any release, including a
    /// patch release. You also need to depend on `gcloud-sdk` yourself, at a matching version,
    /// since this crate does not re-export it.
    #[inline]
    pub fn client(&self) -> &GoogleApi<FirestoreClient<GoogleAuthMiddleware>> {
        &self.inner.client
    }

    /// Clones the `FirestoreDb` instance, replacing its session parameters.
    ///
    /// This is useful for creating a new client instance that shares the same
    /// underlying connection and configuration but has different session-level
    /// settings (e.g., for a specific transaction or consistency requirement).
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

    /// Clones the `FirestoreDb` instance with default request options.
    ///
    /// Every request issued through the returned instance carries these options,
    /// unless an operation overrides them explicitly.
    #[inline]
    pub fn clone_with_request_options(&self, request_options: FirestoreRequestOptions) -> Self {
        let existing_session_params = (*self.session_params).clone();

        self.clone_with_session_params(
            existing_session_params.with_request_options(request_options),
        )
    }

    /// Clones the `FirestoreDb` instance with default request tags.
    ///
    /// A convenience shortcut for
    /// [`clone_with_request_options`](FirestoreDb::clone_with_request_options).
    /// This is also the way to attach request tags to the CRUD operations
    /// (get/create/update/delete), which have no per operation options.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use firestore::*;
    /// # async fn run() -> FirestoreResult<()> {
    /// let db = FirestoreDb::new("my-gcp-project-id").await?;
    /// let tagged_db = db.clone_with_request_tags(["nightly-report"]);
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn clone_with_request_tags<I>(&self, request_tags: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<FirestoreRequestTag>,
    {
        self.clone_with_request_options(FirestoreRequestOptions::from_tags(request_tags))
    }

    /// Consumes the `FirestoreDb` instance and returns a new one with default
    /// request options.
    #[inline]
    pub fn with_request_options(self, request_options: FirestoreRequestOptions) -> Self {
        let session_params = (*self.session_params)
            .clone()
            .with_request_options(request_options);

        self.with_session_params(session_params)
    }

    /// Consumes the `FirestoreDb` instance and returns a new one with default
    /// request tags.
    ///
    /// A convenience shortcut for
    /// [`with_request_options`](FirestoreDb::with_request_options).
    #[inline]
    pub fn with_request_tags<I>(self, request_tags: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<FirestoreRequestTag>,
    {
        self.with_request_options(FirestoreRequestOptions::from_tags(request_tags))
    }

    /// Clones the `FirestoreDb` instance with a specific cache mode.
    ///
    /// This method is only available if the `caching` feature is enabled.
    #[cfg(feature = "caching")]
    pub fn with_cache(&self, cache_mode: crate::FirestoreDbSessionCacheMode) -> Self {
        let existing_session_params = (*self.session_params).clone();

        self.clone_with_session_params(existing_session_params.with_cache_mode(cache_mode))
    }

    /// Clones this `FirestoreDb` so that reads go through the given cache, falling back to
    /// Firestore.
    ///
    /// This is the mode to reach for by default: anything the cache can answer is served
    /// locally, and anything it cannot is fetched from Firestore as usual.
    ///
    /// Reads by ID are served from the cache for any cached collection, and populate it on a
    /// miss. `list` and `query` are served from the cache only for collections configured to be
    /// preloaded - for others they transparently go to Firestore, because a lazily filled
    /// collection could only answer them with partial results.
    ///
    /// Results are eventually consistent; see [`FirestoreCache`](crate::FirestoreCache) for what
    /// that means in practice.
    ///
    /// This method is only available if the `caching` feature is enabled.
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

    /// Clones this `FirestoreDb` so that reads come exclusively from the given cache, never from
    /// Firestore.
    ///
    /// Reads by ID return `None` when the document is not cached. Requests that the cache cannot
    /// answer *completely* - a `list` or `query` on a collection that is not preloaded, on a
    /// collection the cache does not know about, or a collection group query - return a
    /// [`FirestoreError::CacheError`](crate::errors::FirestoreError::CacheError) rather than a
    /// partial result that would look complete.
    ///
    /// Use [`read_through_cache`](Self::read_through_cache) if you would rather fall back to
    /// Firestore, or [`FirestoreCacheIncompleteCollectionPolicy::PartialResults`](crate::FirestoreCacheIncompleteCollectionPolicy::PartialResults)
    /// if partial results are genuinely acceptable for your use case.
    ///
    /// This method is only available if the `caching` feature is enabled.
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

impl std::fmt::Debug for FirestoreDb {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirestoreDb")
            .field("options", &self.inner.options)
            .field("database_path", &self.inner.database_path)
            .field("doc_path", &self.inner.doc_path)
            .finish()
    }
}

/// Builds the absolute path of a document, validating both `collection_id` and `document_id`.
///
/// `parent` is not validated: a malformed parent is not an injection risk (it is not
/// attacker-controlled the way a collection or document ID can be) and is rejected server-side.
pub(crate) fn safe_document_path<S>(
    parent: &str,
    collection_id: &str,
    document_id: S,
) -> FirestoreResult<String>
where
    S: AsRef<str>,
{
    validate_path_segment(collection_id, "collection_id")?;
    let document_id_ref = document_id.as_ref();
    validate_path_segment(document_id_ref, "document_id")?;
    Ok(format!("{parent}/{collection_id}/{document_id_ref}"))
}

/// Splits a document path into the path of its parent and the document ID.
///
/// Returns `(parent_path, document_id)`, where the parent path is empty when the path has no
/// separator at all.
pub(crate) fn split_document_path(path: &str) -> (&str, &str) {
    match path.rfind('/') {
        Some(split_pos) => (&path[..split_pos], &path[split_pos + 1..]),
        None => ("", path),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::fake_firestore::{FakeFirestore, FakeResponse};
    use gcloud_sdk::tonic::Code;

    /// A 2048-bit RSA key in PKCS#8, generated for these tests and used nowhere else.
    const TEST_SERVICE_ACCOUNT_PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCiNMJDZM3rHRuj\nIxTKvSHs/IGI1oEh3NYbNaAxYOoRSVNbxcvR/ixaB/Um2KzLuLpFQzg+PrMJ6EVg\n9sMXPBetw42BWtE4Q9vMtjYia01RPo0jbhf9N9EAKtytbBmgBgsh+5EeYHpARr27\n+wg2CZb3pwRemiKOJjPIXzc8zT5QKCa/W0/sYHkt55Q4BUYTzPajvY2zMJgFIEcp\nkS4Vf3I/06fl6l2rAs+qrSE3VYXGIbNihQ4C+aCuPJeA6oXAHRHuGWEI3azqqKn/\nr81JsykNVElHfdzsoYl6qWu9IuhnJ7cIF/XvG6UHVum+1+Fh54r5+44ky/ixmlaz\nfSq5BjnvAgMBAAECggEAGFkeDfq8ND4uz1qtPM+OH6I5mX5FbP1WwEfY74CSMh0V\nH7H9qdxi8PK/2GBu87ebclkoQKOtwV91xpvT5hF1pnYzsAafYDhDbqOtVZZQyVC/\n4+EbRb3SqBlG/ds7r3sowaWe/3XQ9AQKaATDE0V2PV97Nu4hIMBYRowQYRaX83UN\n5PDdRzpVbEglBD4yCcuYLmYiRdA8Wvvitf625VNPS2aCz9ibH/4cWDeC8u3Z35rL\n1LwLLHfJ9SPh59KHfXcg8A4rTJ5VHBGkNB92y57RdAcwJ+Q2xRwkiYLUeHIph19b\nCu9sWo466yz9vOf9j7oqez6BeCuqoi6tLr6eG4eOwQKBgQDkWvtMdDmCbrrpGeKP\nDZp2drRHXPGTDT2NyoXmIffq2KhcNVW6EXeVOHQyiCwncKZawwfxRveIPXvU2zTc\nApYi1SnZRdwWmkHiy2GCO/vVYO404c2JXu2n3tLixh5ir39v7yauWRjZ/qQ5/qvO\nU0kKz/tBhik9fK9BJdCqJ013rwKBgQC117pLUOt4vbPSzja+x33gZUobNJKFvJy+\nWEEIvZ4gbM7iBxHco6xzY4Kcn2Wd2Ai+PdQBR6yFbRMscB4BsqIoCy4HLN0i3bz4\nyLW8eNlti06W2Qnd6/Wzyp4a2t7XhPfA982iQA4gcYiSHQC4nzjqOP2dBpfv14bz\noninztGxwQKBgQCw7RERbmd0eIiWvHh978NCj7wkIo4FKlgLuOM/qAfmzFC9iJFQ\nJeJqGiBlWn4jXLN3VO6dcSeuRjzgcaql39clS9Utw2O/m2r65is5dXIsI/rLvDu8\neHFYBFuOWoQGYAUz264znVKU7Cefy4KfzIWmO/hnDyR6wFUk+8CNZQAvfwKBgFZF\nvGABS0Zkkk1Agt6unPz6cVdI8P88Rg1Up74y4DO4C8tW2VWZ3bZ9DrmqMjbaCQPh\nJ5VX4PUIk+EwbDwX+TEQZM0Irv3cv8w0xWxe1aFQR3/wButgCJk9Vxecob8UmcrW\nhpwk0c74rnfMBMyS1hjh4wk92JX05lTuz1mmGPzBAoGAGdf4NYGrmfzlqTwHjgaA\n7K9n8YCGy9M1v17viLNiuvlOUQFVLSyJIgkXoY4ZSfi0G/JKbTXEIGiGtCCN40EC\nOqUNKtjNbnYHV7IiLhS5q1oHZlGow2tSAvT7JhWhHILm+RF5hxJY0m8sWbSkMyR1\nKaJ9dM6o2MEo9MlgXCigO+4=\n-----END PRIVATE KEY-----\n";

    #[tokio::test]
    async fn a_service_account_key_file_signs_the_bearer_token() {
        #[cfg(not(feature = "auth-default-crypto"))]
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let server = FakeFirestore::start(|_, _| {
            (
                "GetDocument".to_string(),
                FakeResponse::Status(Code::NotFound),
            )
        })
        .await;
        let key_file = tempfile::NamedTempFile::new().unwrap();
        let key = serde_json::json!({
            "type": "service_account",
            "project_id": "keyed-project",
            "private_key_id": "keyed-project-key",
            "private_key": TEST_SERVICE_ACCOUNT_PRIVATE_KEY,
            "client_email": "firestore-client@keyed-project.iam.gserviceaccount.com",
        });
        std::fs::write(key_file.path(), key.to_string()).unwrap();
        let mut options = FirestoreDbOptions::new("keyed-project".to_string());
        options.firebase_api_url = Some(format!("http://{}", server.address()));
        let db =
            FirestoreDb::with_options_service_account_key_file(options, key_file.path().into())
                .await
                .unwrap();

        db.ping().await.unwrap();

        let authorizations = server.authorizations();
        assert_eq!(authorizations.len(), 1);
        let authorization = authorizations[0]
            .as_ref()
            .expect("the request must carry an authorization header");
        assert!(
            authorization.as_bytes().starts_with(b"Bearer ey"),
            "expected a signed JWT bearer token, got {authorization:?}"
        );
    }

    #[tokio::test]
    async fn test_ping_reads_a_document_under_the_documents_path() {
        use gcloud_sdk::prost::Message as _;

        // The request name is logged as the "call", rather than asserted on inside the handler:
        // a failed assertion there would panic the server's task and leave the client waiting
        // on a response that never comes, instead of failing the test.
        let server = FakeFirestore::start(|_, request_bytes| {
            let request = GetDocumentRequest::decode(request_bytes).unwrap();
            (request.name, FakeResponse::Status(Code::NotFound))
        })
        .await;

        server.db.ping().await.expect(
            "NOT_FOUND on a document that was never written means the database is reachable",
        );

        let calls = server.calls();
        assert_eq!(calls.len(), 1);
        let (parent, _) = split_document_path(&calls[0]);
        let (documents_path, _) = split_document_path(parent);
        assert!(
            calls[0].starts_with(&format!("{documents_path}/"))
                && documents_path.ends_with("/documents"),
            "ping must read a document under the database's /documents/ path, got {:?}",
            calls[0]
        );
    }

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

        // Exactly 1500 bytes is still accepted; the limit is a byte count, not a char count.
        let at_limit = "e".repeat(1500);
        assert!(safe_document_path(
            "projects/test-project/databases/(default)/documents",
            "test",
            at_limit
        )
        .is_ok());

        for bad_document_id in ["", ".", ".."] {
            assert!(
                safe_document_path(
                    "projects/test-project/databases/(default)/documents",
                    "test",
                    bad_document_id
                )
                .is_err(),
                "expected document_id {bad_document_id:?} to be rejected"
            );
        }

        // A `/` in the collection id re-targets the write at a different collection; this is the
        // one real injection vector `safe_document_path` exists to close.
        for bad_collection_id in ["", ".", "..", "test/other"] {
            assert!(
                safe_document_path(
                    "projects/test-project/databases/(default)/documents",
                    bad_collection_id,
                    "test1"
                )
                .is_err(),
                "expected collection_id {bad_collection_id:?} to be rejected"
            );
        }
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
        assert_eq!(split_document_path("test1"), ("", "test1"));
    }
}
