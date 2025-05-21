use crate::errors::*;
use crate::timestamp_utils::to_timestamp;
use crate::{FirestoreError, FirestoreTransactionId};
use chrono::prelude::*;

/// Specifies the consistency guarantee for Firestore read operations.
///
/// When performing reads, Firestore offers different consistency models. This enum
/// allows selecting the desired consistency for an operation, typically by associating
/// it with [`FirestoreDbSessionParams`](crate::FirestoreDbSessionParams).
///
/// See Google Cloud documentation for more details on Firestore consistency:
/// [Data consistency](https://cloud.google.com/firestore/docs/concepts/transaction-options#data_consistency)
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreConsistencySelector {
    /// Reads documents within an existing transaction.
    ///
    /// When this variant is used, the read operation will be part of the transaction
    /// identified by the provided [`FirestoreTransactionId`]. This ensures that all reads
    /// within the transaction see a consistent snapshot of the data.
    ///
    /// Note: Not all operations support being part of a transaction (e.g., `PartitionQuery`).
    Transaction(FirestoreTransactionId),

    /// Reads documents at a specific point in time.
    ///
    /// This ensures that the read operation sees the version of the documents as they
    /// existed at or before the given `read_time`. This is useful for reading historical
    /// data or ensuring that a sequence of reads sees a consistent snapshot without
    /// the overhead of a full transaction. The timestamp must not be older than
    /// one hour, with some exceptions for Point-in-Time Recovery enabled databases.
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
