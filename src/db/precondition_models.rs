use crate::errors::FirestoreError;
use crate::timestamp_utils::to_timestamp;
use chrono::prelude::*;
use gcloud_sdk::google::firestore::v1::Precondition;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreWritePrecondition {
    Exists(bool),
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
