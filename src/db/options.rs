use gcloud_sdk::GoogleEnvironment;
use rsb_derive::Builder;

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
