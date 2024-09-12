use gcloud_sdk::GoogleEnvironment;
use rsb_derive::Builder;

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreDbOptions {
    pub google_project_id: String,

    #[default = "FIREBASE_DEFAULT_DATABASE_ID.to_string()"]
    pub database_id: String,

    #[default = "3"]
    pub max_retries: usize,

    pub firebase_api_url: Option<String>,
}

impl FirestoreDbOptions {
    pub async fn for_default_project_id() -> Option<FirestoreDbOptions> {
        let google_project_id = GoogleEnvironment::detect_google_project_id().await;

        google_project_id.map(FirestoreDbOptions::new)
    }
}

pub const FIREBASE_DEFAULT_DATABASE_ID: &str = "(default)";
