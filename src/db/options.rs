use rsb_derive::Builder;

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreDbOptions {
    pub google_project_id: String,

    #[default = "3"]
    pub max_retries: usize,
}
