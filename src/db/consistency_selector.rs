use crate::errors::*;
use crate::timestamp_utils::to_timestamp;
use crate::{FirestoreError, FirestoreTransactionId};
use chrono::prelude::*;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreConsistencySelector {
    Transaction(FirestoreTransactionId),
    ReadTime(DateTime<Utc>),
}

impl TryFrom<&FirestoreConsistencySelector>
    for gcloud_sdk::google::firestore::v1::get_document_request::ConsistencySelector
{
    type Error = FirestoreError;

    fn try_from(selector: &FirestoreConsistencySelector) -> Result<Self, Self::Error> {
        match selector {
            FirestoreConsistencySelector::Transaction(tid) => Ok(gcloud_sdk::google::firestore::v1::get_document_request::ConsistencySelector::Transaction(tid.clone())),
            FirestoreConsistencySelector::ReadTime(ts) => Ok(gcloud_sdk::google::firestore::v1::get_document_request::ConsistencySelector::ReadTime(to_timestamp(*ts)))
        }
    }
}

impl TryFrom<&FirestoreConsistencySelector>
    for gcloud_sdk::google::firestore::v1::batch_get_documents_request::ConsistencySelector
{
    type Error = FirestoreError;

    fn try_from(selector: &FirestoreConsistencySelector) -> Result<Self, Self::Error> {
        match selector {
            FirestoreConsistencySelector::Transaction(tid) => Ok(gcloud_sdk::google::firestore::v1::batch_get_documents_request::ConsistencySelector::Transaction(tid.clone())),
            FirestoreConsistencySelector::ReadTime(ts) => Ok(gcloud_sdk::google::firestore::v1::batch_get_documents_request::ConsistencySelector::ReadTime(to_timestamp(*ts)))
        }
    }
}

impl TryFrom<&FirestoreConsistencySelector>
    for gcloud_sdk::google::firestore::v1::list_documents_request::ConsistencySelector
{
    type Error = FirestoreError;

    fn try_from(selector: &FirestoreConsistencySelector) -> Result<Self, Self::Error> {
        match selector {
            FirestoreConsistencySelector::Transaction(tid) => Ok(gcloud_sdk::google::firestore::v1::list_documents_request::ConsistencySelector::Transaction(tid.clone())),
            FirestoreConsistencySelector::ReadTime(ts) => Ok(gcloud_sdk::google::firestore::v1::list_documents_request::ConsistencySelector::ReadTime(to_timestamp(*ts)))
        }
    }
}

impl TryFrom<&FirestoreConsistencySelector>
    for gcloud_sdk::google::firestore::v1::run_query_request::ConsistencySelector
{
    type Error = FirestoreError;

    fn try_from(selector: &FirestoreConsistencySelector) -> Result<Self, Self::Error> {
        match selector {
            FirestoreConsistencySelector::Transaction(tid) => Ok(gcloud_sdk::google::firestore::v1::run_query_request::ConsistencySelector::Transaction(tid.clone())),
            FirestoreConsistencySelector::ReadTime(ts) => Ok(gcloud_sdk::google::firestore::v1::run_query_request::ConsistencySelector::ReadTime(to_timestamp(*ts)))
        }
    }
}

impl TryFrom<&FirestoreConsistencySelector>
    for gcloud_sdk::google::firestore::v1::partition_query_request::ConsistencySelector
{
    type Error = FirestoreError;

    fn try_from(selector: &FirestoreConsistencySelector) -> Result<Self, Self::Error> {
        match selector {
            FirestoreConsistencySelector::Transaction(_) => Err(FirestoreError::DatabaseError(
                FirestoreDatabaseError::new(FirestoreErrorPublicGenericDetails::new("Unsupported consistency selector".into()),"Unsupported consistency selector".into(), false)
            )),
            FirestoreConsistencySelector::ReadTime(ts) => Ok(gcloud_sdk::google::firestore::v1::partition_query_request::ConsistencySelector::ReadTime(to_timestamp(*ts)))
        }
    }
}

impl TryFrom<&FirestoreConsistencySelector>
    for gcloud_sdk::google::firestore::v1::run_aggregation_query_request::ConsistencySelector
{
    type Error = FirestoreError;

    fn try_from(selector: &FirestoreConsistencySelector) -> Result<Self, Self::Error> {
        match selector {
            FirestoreConsistencySelector::Transaction(tid) => Ok(gcloud_sdk::google::firestore::v1::run_aggregation_query_request::ConsistencySelector::Transaction(tid.clone())),
            FirestoreConsistencySelector::ReadTime(ts) => Ok(gcloud_sdk::google::firestore::v1::run_aggregation_query_request::ConsistencySelector::ReadTime(to_timestamp(*ts)))
        }
    }
}

impl TryFrom<&FirestoreConsistencySelector>
    for gcloud_sdk::google::firestore::v1::transaction_options::read_only::ConsistencySelector
{
    type Error = FirestoreError;

    fn try_from(selector: &FirestoreConsistencySelector) -> Result<Self, Self::Error> {
        match selector {
            FirestoreConsistencySelector::Transaction(_) => Err(FirestoreError::DatabaseError(
                FirestoreDatabaseError::new(FirestoreErrorPublicGenericDetails::new("Unsupported consistency selector".into()),"Unsupported consistency selector".into(), false)
            )),
            FirestoreConsistencySelector::ReadTime(ts) => Ok(gcloud_sdk::google::firestore::v1::transaction_options::read_only::ConsistencySelector::ReadTime(to_timestamp(*ts)))
        }
    }
}

impl TryFrom<&FirestoreConsistencySelector>
    for gcloud_sdk::google::firestore::v1::list_collection_ids_request::ConsistencySelector
{
    type Error = FirestoreError;

    fn try_from(selector: &FirestoreConsistencySelector) -> Result<Self, Self::Error> {
        match selector {
            FirestoreConsistencySelector::Transaction(_) => Err(FirestoreError::DatabaseError(
                FirestoreDatabaseError::new(FirestoreErrorPublicGenericDetails::new("Unsupported consistency selector".into()),"Unsupported consistency selector".into(), false)
            )),
            FirestoreConsistencySelector::ReadTime(ts) => Ok(gcloud_sdk::google::firestore::v1::list_collection_ids_request::ConsistencySelector::ReadTime(to_timestamp(*ts)))
        }
    }
}
