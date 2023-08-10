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

pub const FIREBASE_DEFAULT_DATABASE_ID: &str = "(default)";
