use crate::errors::FirestoreError;
use crate::{FirestoreConsistencySelector, FirestoreWriteResult};
use chrono::prelude::*;
use chrono::Duration;
use rsb_derive::Builder;

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreTransactionOptions {
    #[default = "FirestoreTransactionMode::ReadWrite"]
    pub mode: FirestoreTransactionMode,
    pub max_elapsed_time: Option<Duration>,
}

impl Default for FirestoreTransactionOptions {
    fn default() -> Self {
        Self {
            mode: FirestoreTransactionMode::ReadWrite,
            max_elapsed_time: None,
        }
    }
}

impl TryFrom<FirestoreTransactionOptions>
    for gcloud_sdk::google::firestore::v1::TransactionOptions
{
    type Error = FirestoreError;

    fn try_from(options: FirestoreTransactionOptions) -> Result<Self, Self::Error> {
        match options.mode {
            FirestoreTransactionMode::ReadOnly => {
                Ok(gcloud_sdk::google::firestore::v1::TransactionOptions {
                    mode: Some(
                        gcloud_sdk::google::firestore::v1::transaction_options::Mode::ReadOnly(
                            gcloud_sdk::google::firestore::v1::transaction_options::ReadOnly {
                                consistency_selector: None,
                            },
                        ),
                    ),
                })
            }
            FirestoreTransactionMode::ReadOnlyWithConsistency(ref selector) => {
                Ok(gcloud_sdk::google::firestore::v1::TransactionOptions {
                    mode: Some(
                        gcloud_sdk::google::firestore::v1::transaction_options::Mode::ReadOnly(
                            gcloud_sdk::google::firestore::v1::transaction_options::ReadOnly {
                                consistency_selector: Some(selector.try_into()?),
                            },
                        ),
                    ),
                })
            }
            FirestoreTransactionMode::ReadWrite => {
                Ok(gcloud_sdk::google::firestore::v1::TransactionOptions {
                    mode: Some(
                        gcloud_sdk::google::firestore::v1::transaction_options::Mode::ReadWrite(
                            gcloud_sdk::google::firestore::v1::transaction_options::ReadWrite {
                                retry_transaction: vec![],
                            },
                        ),
                    ),
                })
            }
            FirestoreTransactionMode::ReadWriteRetry(tid) => {
                Ok(gcloud_sdk::google::firestore::v1::TransactionOptions {
                    mode: Some(
                        gcloud_sdk::google::firestore::v1::transaction_options::Mode::ReadWrite(
                            gcloud_sdk::google::firestore::v1::transaction_options::ReadWrite {
                                retry_transaction: tid,
                            },
                        ),
                    ),
                })
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreTransactionMode {
    ReadOnly,
    ReadWrite,
    ReadOnlyWithConsistency(FirestoreConsistencySelector),
    ReadWriteRetry(FirestoreTransactionId),
}

pub type FirestoreTransactionId = Vec<u8>;

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreTransactionResponse {
    pub write_results: Vec<FirestoreWriteResult>,
    pub commit_time: Option<DateTime<Utc>>,
}
