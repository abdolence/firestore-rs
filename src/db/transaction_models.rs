use crate::errors::FirestoreError;
use crate::{FirestoreConsistencySelector, FirestoreWriteResult};
use chrono::prelude::*;
use chrono::Duration;
use rsb_derive::Builder;

/// Options for configuring a Firestore transaction.
///
/// These options control the behavior of a transaction, such as its mode (read-only or read-write)
/// and consistency requirements for read-only transactions.
///
/// # Examples
///
/// ```rust
/// use firestore::*;
/// use chrono::{Utc, Duration};
///
/// // Default options (ReadWrite mode)
/// let default_options = FirestoreTransactionOptions::default();
/// assert_eq!(default_options.mode, FirestoreTransactionMode::ReadWrite);
///
/// // ReadOnly transaction
/// let readonly_options = FirestoreTransactionOptions::new()
///     .with_mode(FirestoreTransactionMode::ReadOnly);
///
/// // ReadOnly transaction with a specific read time
/// let read_time = Utc::now() - Duration::seconds(60);
/// let pit_readonly_options = FirestoreTransactionOptions::new()
///     .with_mode(FirestoreTransactionMode::ReadOnlyWithConsistency(
///         FirestoreConsistencySelector::ReadTime(read_time)
///     ));
///
/// // Transaction with a maximum elapsed time for retries
/// let timed_options = FirestoreTransactionOptions::new()
///     .with_max_elapsed_time(Some(Duration::seconds(30)));
/// ```
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreTransactionOptions {
    /// The mode of the transaction (e.g., read-only, read-write).
    /// Defaults to [`FirestoreTransactionMode::ReadWrite`].
    #[default = "FirestoreTransactionMode::ReadWrite"]
    pub mode: FirestoreTransactionMode,
    /// An optional maximum duration for the entire transaction, including retries.
    /// If set, the transaction will attempt to complete within this duration.
    /// If `None`, default retry policies of the underlying gRPC client or Firestore service apply.
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

/// Defines the mode of a Firestore transaction.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreTransactionMode {
    /// A read-only transaction.
    ///
    /// In this mode, only read operations are allowed. The transaction will use
    /// strong consistency by default, reading the latest version of data.
    ReadOnly,
    /// A read-write transaction.
    ///
    /// This is the default mode. Both read and write operations are allowed.
    /// Firestore ensures atomicity for all operations within the transaction.
    ReadWrite,
    /// A read-only transaction with a specific consistency requirement.
    ///
    /// Allows specifying how data should be read, for example, at a particular
    /// point in time using [`FirestoreConsistencySelector::ReadTime`].
    ReadOnlyWithConsistency(FirestoreConsistencySelector),
    /// A read-write transaction that attempts to retry a previous transaction.
    ///
    /// This is used internally by the client when retrying a transaction that
    /// failed due to contention or other transient issues. The `FirestoreTransactionId`
    /// is the ID of the transaction to retry.
    ReadWriteRetry(FirestoreTransactionId),
}

/// A type alias for Firestore transaction IDs.
/// Transaction IDs are represented as a vector of bytes.
pub type FirestoreTransactionId = Vec<u8>;

/// Represents the response from committing a Firestore transaction.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreTransactionResponse {
    /// A list of results for each write operation performed within the transaction.
    /// Each [`FirestoreWriteResult`] provides information about a specific write,
    /// such as its update time.
    pub write_results: Vec<FirestoreWriteResult>,
    /// The time at which the transaction was committed.
    /// This is `None` if the transaction was read-only or did not involve writes.
    pub commit_time: Option<DateTime<Utc>>,
}
