use crate::errors::*;
use crate::FirestoreValue;

use crate::timestamp_utils::from_timestamp;
use chrono::prelude::*;
use rsb_derive::Builder;

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreWriteResult {
    pub update_time: Option<DateTime<Utc>>,
    pub transform_results: Vec<FirestoreValue>,
}

impl TryInto<FirestoreWriteResult> for gcloud_sdk::google::firestore::v1::WriteResult {
    type Error = FirestoreError;

    fn try_into(self) -> Result<FirestoreWriteResult, Self::Error> {
        Ok(FirestoreWriteResult::new(
            self.transform_results
                .into_iter()
                .map(|r| FirestoreValue::from(r))
                .collect(),
        )
        .opt_update_time(self.update_time.map(from_timestamp).transpose()?))
    }
}
