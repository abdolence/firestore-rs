use crate::FirestoreQueryOrder;
use gcloud_sdk::google::firestore::v1::Document;

use rsb_derive::*;

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreListDocParams {
    pub collection_id: String,

    pub parent: Option<String>,

    #[default = "100"]
    pub page_size: usize,

    pub page_token: Option<String>,
    pub order_by: Option<Vec<FirestoreQueryOrder>>,
    pub return_only_fields: Option<Vec<String>>,
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreListDocResult {
    pub documents: Vec<Document>,
    pub page_token: Option<String>,
}
