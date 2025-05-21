use crate::errors::FirestoreError;
use crate::timestamp_utils::to_timestamp;
use chrono::prelude::*;
use gcloud_sdk::google::firestore::v1::Precondition;

/// A precondition on a document, used for conditional write operations in Firestore.
///
/// Preconditions allow you to specify conditions that must be met for a write
/// operation (create, update, delete) to succeed. If the precondition is not met,
/// the operation will fail, typically with a `DataConflictError` or similar.
/// ```
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreWritePrecondition {
    /// The target document must exist (if `true`) or must not exist (if `false`).
    ///
    /// - `Exists(true)`: The operation will only succeed if the document already exists.
    ///   Useful for conditional updates or deletes.
    /// - `Exists(false)`: The operation will only succeed if the document does not already exist.
    ///   Useful for conditional creates (to prevent overwriting).
    Exists(bool),

    /// The target document must exist and its `update_time` must match the provided timestamp.
    ///
    /// This is used for optimistic concurrency control. The operation will only succeed
    /// if the document has not been modified since the specified `update_time`.
    /// The `DateTime<Utc>` must be microsecond-aligned, as Firestore timestamps have
    /// microsecond precision.
    UpdateTime(DateTime<Utc>),
}

impl TryInto<gcloud_sdk::google::firestore::v1::Precondition> for FirestoreWritePrecondition {
    type Error = FirestoreError;

    fn try_into(self) -> Result<Precondition, Self::Error> {
        match self {
            FirestoreWritePrecondition::Exists(value) => {
                Ok(gcloud_sdk::google::firestore::v1::Precondition {
                    condition_type: Some(
                        gcloud_sdk::google::firestore::v1::precondition::ConditionType::Exists(
                            value,
                        ),
                    ),
                })
            }
            FirestoreWritePrecondition::UpdateTime(value) => {
                Ok(gcloud_sdk::google::firestore::v1::Precondition {
                    condition_type: Some(
                        gcloud_sdk::google::firestore::v1::precondition::ConditionType::UpdateTime(
                            to_timestamp(value),
                        ),
                    ),
                })
            }
        }
    }
}
