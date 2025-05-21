use crate::errors::*;
use crate::FirestoreResult;
use chrono::prelude::*;

/// Converts a Google `prost_types::Timestamp` to a `chrono::DateTime<Utc>`.
///
/// Firestore uses Google's `Timestamp` protobuf message to represent timestamps.
/// This function facilitates conversion to the more commonly used `chrono::DateTime<Utc>`
/// in Rust applications.
///
/// # Arguments
/// * `ts`: The Google `Timestamp` to convert.
///
/// # Returns
/// A `FirestoreResult` containing the `DateTime<Utc>` on success, or a
/// `FirestoreError::DeserializeError` if the timestamp is invalid or out of range.
///
/// # Examples
/// ```rust
/// use firestore::timestamp_utils::from_timestamp;
/// use chrono::{Utc, TimeZone};
///
/// let prost_timestamp = gcloud_sdk::prost_types::Timestamp { seconds: 1670000000, nanos: 0 };
/// let chrono_datetime = from_timestamp(prost_timestamp).unwrap();
///
/// assert_eq!(chrono_datetime, Utc.with_ymd_and_hms(2022, 12, 2, 16, 53, 20).unwrap());
/// ```
pub fn from_timestamp(ts: gcloud_sdk::prost_types::Timestamp) -> FirestoreResult<DateTime<Utc>> {
    if let Some(dt) = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32) {
        Ok(dt)
    } else {
        Err(FirestoreError::DeserializeError(
            FirestoreSerializationError::from_message(format!(
                "Invalid or out-of-range datetime: {ts:?}" // Added :? for better debug output
            )),
        ))
    }
}

/// Converts a `chrono::DateTime<Utc>` to a Google `prost_types::Timestamp`.
///
/// This is the reverse of [`from_timestamp`], used when sending timestamp data
/// to Firestore.
///
/// # Arguments
/// * `dt`: The `chrono::DateTime<Utc>` to convert.
///
/// # Returns
/// The corresponding Google `Timestamp`.
///
/// # Examples
/// ```rust
/// use firestore::timestamp_utils::to_timestamp;
/// use chrono::{Utc, TimeZone};
///
/// let chrono_datetime = Utc.with_ymd_and_hms(2022, 12, 2, 16, 53, 20).unwrap();
/// let prost_timestamp = to_timestamp(chrono_datetime);
///
/// assert_eq!(prost_timestamp.seconds, 1670000000);
/// assert_eq!(prost_timestamp.nanos, 0);
/// ```
pub fn to_timestamp(dt: DateTime<Utc>) -> gcloud_sdk::prost_types::Timestamp {
    gcloud_sdk::prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.nanosecond() as i32,
    }
}

/// Converts a Google `prost_types::Duration` to a `chrono::Duration`.
///
/// Google's `Duration` protobuf message is used in some Firestore contexts,
/// for example, in query execution statistics.
///
/// # Arguments
/// * `duration`: The Google `Duration` to convert.
///
/// # Returns
/// The corresponding `chrono::Duration`.
///
/// # Examples
/// ```rust
/// use firestore::timestamp_utils::from_duration;
///
/// let prost_duration = gcloud_sdk::prost_types::Duration { seconds: 5, nanos: 500_000_000 };
/// let chrono_duration = from_duration(prost_duration);
///
/// assert_eq!(chrono_duration, chrono::Duration::milliseconds(5500));
/// ```
pub fn from_duration(duration: gcloud_sdk::prost_types::Duration) -> chrono::Duration {
    chrono::Duration::seconds(duration.seconds)
        + chrono::Duration::nanoseconds(duration.nanos.into())
}
