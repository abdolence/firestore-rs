use gcloud_sdk::GoogleEnvironment;
use rsb_derive::Builder;
use std::time::Duration;

/// Configuration options for the [`FirestoreDb`](crate::FirestoreDb) client.
///
/// This struct allows customization of various aspects of the Firestore client,
/// such as the project ID, database ID, retry behavior, and API endpoint.
/// It uses the `rsb_derive::Builder` to provide a convenient builder pattern
/// for constructing options.
///
/// # Examples
///
/// ```rust
/// use firestore::FirestoreDbOptions;
///
/// let options = FirestoreDbOptions::new("my-gcp-project-id".to_string())
///     .with_database_id("my-custom-db".to_string())
///     .with_max_retries(5);
///
/// // To use the default database ID:
/// let default_db_options = FirestoreDbOptions::new("my-gcp-project-id".to_string());
/// assert_eq!(default_db_options.database_id, firestore::FIREBASE_DEFAULT_DATABASE_ID);
/// ```
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreDbOptions {
    /// The Google Cloud Project ID that owns the Firestore database.
    pub google_project_id: String,

    /// The ID of the Firestore database. Defaults to `"(default)"`.
    /// Use [`FIREBASE_DEFAULT_DATABASE_ID`](crate::FIREBASE_DEFAULT_DATABASE_ID) for the default.
    #[default = "FIREBASE_DEFAULT_DATABASE_ID.to_string()"]
    pub database_id: String,

    /// The maximum number of times to retry a failed operation. Defaults to `3`.
    /// Retries are typically applied to transient errors.
    #[default = "3"]
    pub max_retries: usize,

    /// An optional custom URL for the Firestore API.
    /// If `None`, the default Google Firestore API endpoint is used.
    /// This can be useful for targeting a Firestore emulator.
    /// If the `FIRESTORE_EMULATOR_HOST` environment variable is set, it will
    /// typically override this and the default URL.
    pub firebase_api_url: Option<String>,

    /// The longest one request waits for Firestore, per attempt. `None`, the default, sets no
    /// limit.
    ///
    /// - **Unary requests** (gets, writes, `BatchWrite`, `Commit`, list pages and the admin API
    ///   calls): the whole call. It is also sent to Firestore as the request's deadline.
    /// - **Streamed reads** (queries, aggregations and batch gets): the wait for the stream to
    ///   start and for each next response, counted only while the caller is waiting for one.
    /// - **Not limited:** listeners and the streaming batch writer, whose streams stay quiet for
    ///   as long as nothing changes. A listener resets a silent stream with its own
    ///   [`idle_timeout`](crate::FirestoreListenerParams::idle_timeout).
    ///
    /// A request that runs out of it fails with a
    /// [`DatabaseError`](crate::errors::FirestoreError::DatabaseError) with the code
    /// `DeadlineExceeded` and `retry_possible` set, and is retried wherever a dropped connection
    /// is, each retry with the full limit again. A transaction's `Commit` is the exception: its
    /// writes may already be applied, so it is never retried.
    ///
    /// [`FirestoreDb::clone_with_request_timeout`](crate::FirestoreDb::clone_with_request_timeout)
    /// replaces it for the requests of a clone.
    pub request_timeout: Option<Duration>,
}

impl FirestoreDbOptions {
    /// Attempts to create `FirestoreDbOptions` by detecting the Google Project ID
    /// from the environment (e.g., Application Default Credentials or GCE metadata server).
    ///
    /// If the project ID can be detected, it returns `Some(FirestoreDbOptions)` with
    /// default values for other fields. Otherwise, it returns `None`.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # async fn run() {
    /// use firestore::FirestoreDbOptions;
    ///
    /// if let Some(options) = FirestoreDbOptions::for_default_project_id().await {
    ///     // Use options to create a FirestoreDb client
    ///     println!("Detected project ID: {}", options.google_project_id);
    /// } else {
    ///     println!("Could not detect default project ID.");
    /// }
    /// # }
    /// ```
    pub async fn for_default_project_id() -> Option<FirestoreDbOptions> {
        let google_project_id = GoogleEnvironment::detect_google_project_id().await;

        google_project_id.map(FirestoreDbOptions::new)
    }
}

/// The default database ID for Firestore, which is `"(default)"`.
pub const FIREBASE_DEFAULT_DATABASE_ID: &str = "(default)";
