use chrono::prelude::*;

pub fn from_timestamp(ts: prost_types::Timestamp) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(
        chrono::NaiveDateTime::from_timestamp(ts.seconds, ts.nanos as u32),
        Utc,
    )
}

pub fn to_timestamp(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.nanosecond() as i32,
    }
}
