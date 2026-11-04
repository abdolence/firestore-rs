use crate::FirestoreExpr;
use serde::Serialize;

/// The unit of the amount in `timestamp_add`, `timestamp_subtract` and `timestamp_diff`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FirestoreTimestampUnit {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
}

/// The unit a timestamp is cut down to by `timestamp_trunc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FirestoreTimestampGranularity {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    #[serde(rename = "week(monday)")]
    WeekStartingMonday,
    #[serde(rename = "week(tuesday)")]
    WeekStartingTuesday,
    #[serde(rename = "week(wednesday)")]
    WeekStartingWednesday,
    #[serde(rename = "week(thursday)")]
    WeekStartingThursday,
    #[serde(rename = "week(friday)")]
    WeekStartingFriday,
    #[serde(rename = "week(saturday)")]
    WeekStartingSaturday,
    #[serde(rename = "week(sunday)")]
    WeekStartingSunday,
    IsoWeek,
    Month,
    Quarter,
    Year,
    IsoYear,
}

/// The part of a timestamp that `timestamp_extract` returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FirestoreTimestampPart {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    DayOfWeek,
    Day,
    DayOfYear,
    Week,
    #[serde(rename = "week(monday)")]
    WeekStartingMonday,
    #[serde(rename = "week(tuesday)")]
    WeekStartingTuesday,
    #[serde(rename = "week(wednesday)")]
    WeekStartingWednesday,
    #[serde(rename = "week(thursday)")]
    WeekStartingThursday,
    #[serde(rename = "week(friday)")]
    WeekStartingFriday,
    #[serde(rename = "week(saturday)")]
    WeekStartingSaturday,
    #[serde(rename = "week(sunday)")]
    WeekStartingSunday,
    IsoWeek,
    Month,
    Quarter,
    Year,
    IsoYear,
}

impl FirestoreExpr {
    /// This timestamp plus `amount` of `unit`.
    pub fn timestamp_add(
        self,
        unit: FirestoreTimestampUnit,
        amount: impl Into<FirestoreExpr>,
    ) -> FirestoreExpr {
        Self::raw("timestamp_add", [self, unit.into(), amount.into()])
    }

    /// This timestamp minus `amount` of `unit`.
    pub fn timestamp_subtract(
        self,
        unit: FirestoreTimestampUnit,
        amount: impl Into<FirestoreExpr>,
    ) -> FirestoreExpr {
        Self::raw("timestamp_subtract", [self, unit.into(), amount.into()])
    }

    /// The number of whole `unit`s from `start` to this timestamp.
    pub fn timestamp_diff(
        self,
        start: impl Into<FirestoreExpr>,
        unit: FirestoreTimestampUnit,
    ) -> FirestoreExpr {
        Self::raw("timestamp_diff", [self, start.into(), unit.into()])
    }

    /// This timestamp cut down to the start of its `granularity`, in UTC.
    pub fn timestamp_trunc(self, granularity: FirestoreTimestampGranularity) -> FirestoreExpr {
        Self::raw("timestamp_trunc", [self, granularity.into()])
    }

    /// As [`timestamp_trunc`](Self::timestamp_trunc), in the time zone `time_zone`, such as
    /// `Asia/Tokyo`.
    pub fn timestamp_trunc_with_time_zone(
        self,
        granularity: FirestoreTimestampGranularity,
        time_zone: impl Into<FirestoreExpr>,
    ) -> FirestoreExpr {
        Self::raw(
            "timestamp_trunc",
            [self, granularity.into(), time_zone.into()],
        )
    }

    /// The `part` of this timestamp, as an integer, in UTC.
    pub fn timestamp_extract(self, part: FirestoreTimestampPart) -> FirestoreExpr {
        Self::raw("timestamp_extract", [self, part.into()])
    }

    /// As [`timestamp_extract`](Self::timestamp_extract), in the time zone `time_zone`, such as
    /// `Asia/Tokyo`.
    pub fn timestamp_extract_with_time_zone(
        self,
        part: FirestoreTimestampPart,
        time_zone: impl Into<FirestoreExpr>,
    ) -> FirestoreExpr {
        Self::raw("timestamp_extract", [self, part.into(), time_zone.into()])
    }

    /// This timestamp as microseconds since the Unix epoch.
    pub fn timestamp_to_unix_micros(self) -> FirestoreExpr {
        Self::raw("timestamp_to_unix_micros", [self])
    }

    /// This timestamp as milliseconds since the Unix epoch.
    pub fn timestamp_to_unix_millis(self) -> FirestoreExpr {
        Self::raw("timestamp_to_unix_millis", [self])
    }

    /// This timestamp as seconds since the Unix epoch.
    pub fn timestamp_to_unix_seconds(self) -> FirestoreExpr {
        Self::raw("timestamp_to_unix_seconds", [self])
    }

    /// The timestamp at this many microseconds since the Unix epoch.
    pub fn unix_micros_to_timestamp(self) -> FirestoreExpr {
        Self::raw("unix_micros_to_timestamp", [self])
    }

    /// The timestamp at this many milliseconds since the Unix epoch.
    pub fn unix_millis_to_timestamp(self) -> FirestoreExpr {
        Self::raw("unix_millis_to_timestamp", [self])
    }

    /// The timestamp at this many seconds since the Unix epoch.
    pub fn unix_seconds_to_timestamp(self) -> FirestoreExpr {
        Self::raw("unix_seconds_to_timestamp", [self])
    }
}
