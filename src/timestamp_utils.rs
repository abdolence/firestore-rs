use crate::errors::*;
use crate::FirestoreResult;
use crate::{FirestoreDateTime, FirestoreDuration};

/// Converts a Google `prost_types::Timestamp` to a [`FirestoreDateTime`].
///
/// Firestore uses Google's `Timestamp` protobuf message to represent timestamps.
/// This function facilitates conversion to [`FirestoreDateTime`], the date/time type
/// used throughout this library.
///
/// # Arguments
/// * `ts`: The Google `Timestamp` to convert.
///
/// # Returns
/// A `FirestoreResult` containing the `Timestamp` on success, or a
/// `FirestoreError::DeserializeError` if the timestamp is invalid or out of range.
///
/// Note that [`FirestoreDateTime`] covers the whole Firestore timestamp range except
/// for the last two days of the year 9999, which lie beyond
/// [`jiff::Timestamp::MAX`]. Such values are reported as an error rather than
/// being silently truncated.
///
/// # Examples
/// ```rust
/// use firestore::timestamp_utils::from_timestamp;
///
/// let prost_timestamp = gcloud_sdk::prost_types::Timestamp { seconds: 1670000000, nanos: 0 };
/// let timestamp = from_timestamp(prost_timestamp).unwrap();
///
/// assert_eq!(timestamp.to_string(), "2022-12-02T16:53:20Z");
/// ```
pub fn from_timestamp(
    ts: gcloud_sdk::prost_types::Timestamp,
) -> FirestoreResult<FirestoreDateTime> {
    FirestoreDateTime::new(ts.seconds, ts.nanos).map_err(|err| {
        FirestoreError::DeserializeError(FirestoreSerializationError::from_message(format!(
            "Invalid or out-of-range datetime: {ts:?}. {err}"
        )))
    })
}

/// Converts a [`FirestoreDateTime`] to a Google `prost_types::Timestamp`.
///
/// This is the reverse of [`from_timestamp`], used when sending timestamp data
/// to Firestore.
///
/// # Arguments
/// * `ts`: The [`FirestoreDateTime`] to convert.
///
/// # Returns
/// The corresponding Google `Timestamp`.
///
/// # Examples
/// ```rust
/// use firestore::timestamp_utils::to_timestamp;
/// use firestore::FirestoreDateTime;
///
/// let timestamp: FirestoreDateTime = "2022-12-02T16:53:20Z".parse().unwrap();
/// let prost_timestamp = to_timestamp(timestamp);
///
/// assert_eq!(prost_timestamp.seconds, 1670000000);
/// assert_eq!(prost_timestamp.nanos, 0);
/// ```
pub fn to_timestamp(ts: FirestoreDateTime) -> gcloud_sdk::prost_types::Timestamp {
    let seconds = ts.as_second();
    let nanos = ts.subsec_nanosecond();

    // Protobuf requires `nanos` to be in the range 0..=999_999_999, while jiff
    // truncates towards zero and so reports negative subsecond nanoseconds for
    // instants before the Unix epoch. Normalise those back into the protobuf range.
    if nanos < 0 {
        gcloud_sdk::prost_types::Timestamp {
            seconds: seconds - 1,
            nanos: nanos + 1_000_000_000,
        }
    } else {
        gcloud_sdk::prost_types::Timestamp { seconds, nanos }
    }
}

/// Converts a Google `prost_types::Duration` to a `FirestoreDuration`.
///
/// Google's `Duration` protobuf message is used in some Firestore contexts,
/// for example, in query execution statistics.
///
/// # Arguments
/// * `duration`: The Google `Duration` to convert.
///
/// # Returns
/// The corresponding `FirestoreDuration`.
///
/// # Examples
/// ```rust
/// use firestore::timestamp_utils::from_duration;
/// use firestore::FirestoreDuration;
///
/// let prost_duration = gcloud_sdk::prost_types::Duration { seconds: 5, nanos: 500_000_000 };
/// let duration = from_duration(prost_duration);
///
/// assert_eq!(duration, FirestoreDuration::from_millis(5500));
/// ```
pub fn from_duration(duration: gcloud_sdk::prost_types::Duration) -> FirestoreDuration {
    FirestoreDuration::new(duration.seconds, duration.nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parses_legacy_offset_formats() {
        // Data written by earlier versions used chrono's to_rfc3339(), which emits
        // a numeric +00:00 offset rather than Z.
        let with_offset: FirestoreDateTime = "2022-12-02T16:53:20+00:00".parse().unwrap();
        let with_zulu: FirestoreDateTime = "2022-12-02T16:53:20Z".parse().unwrap();
        assert_eq!(with_offset, with_zulu);

        let fractional: FirestoreDateTime = "2022-12-02T16:53:20.123456789+00:00".parse().unwrap();
        assert_eq!(fractional.subsec_nanosecond(), 123_456_789);

        // Non-UTC offsets must normalise to the same instant
        let shifted: FirestoreDateTime = "2022-12-02T17:53:20+01:00".parse().unwrap();
        assert_eq!(shifted, with_zulu);
    }

    #[test]
    fn test_timestamp_roundtrip_including_pre_epoch() {
        for seconds in [0i64, 1670000000, -1670000000] {
            let proto = gcloud_sdk::prost_types::Timestamp {
                seconds,
                nanos: 123_456_789,
            };
            let ts = from_timestamp(proto).unwrap();
            let back = to_timestamp(ts);
            assert_eq!(back.seconds, proto.seconds, "seconds for {seconds}");
            assert_eq!(back.nanos, proto.nanos, "nanos for {seconds}");
        }
    }

    #[test]
    fn test_firestore_timestamp_range_bounds() {
        // Firestore documents a range of 0001-01-01 to 9999-12-31, all of which is
        // representable apart from the last two days of year 9999, which fall outside
        // of `jiff::Timestamp::MAX`. Out-of-range values produce an error rather than
        // a silently wrong value.
        let earliest = gcloud_sdk::prost_types::Timestamp {
            seconds: -62_135_596_800,
            nanos: 0,
        };
        let latest_supported = gcloud_sdk::prost_types::Timestamp {
            seconds: FirestoreDateTime::MAX.as_second(),
            nanos: 0,
        };
        let out_of_range = gcloud_sdk::prost_types::Timestamp {
            seconds: 253_402_300_799,
            nanos: 999_999_999,
        };

        assert!(from_timestamp(earliest).is_ok(), "0001-01-01 must parse");
        assert!(from_timestamp(latest_supported).is_ok());
        assert!(from_timestamp(out_of_range).is_err());
    }
}
