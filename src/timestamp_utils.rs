use crate::errors::*;
use crate::FirestoreResult;
use chrono::prelude::*;

pub fn from_timestamp(ts: gcloud_sdk::prost_types::Timestamp) -> FirestoreResult<DateTime<Utc>> {
    if let Some(dt) = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32) {
        Ok(dt)
    } else {
        Err(FirestoreError::DeserializeError(
            FirestoreSerializationError::from_message(format!(
                "Invalid or out-of-range datetime: {ts}"
            )),
        ))
    }
}

pub fn to_timestamp(dt: DateTime<Utc>) -> gcloud_sdk::prost_types::Timestamp {
    gcloud_sdk::prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.nanosecond() as i32,
    }
}

pub fn from_duration(duration: gcloud_sdk::prost_types::Duration) -> chrono::Duration {
    chrono::Duration::seconds(duration.seconds)
        + chrono::Duration::nanoseconds(duration.nanos.into())
}
