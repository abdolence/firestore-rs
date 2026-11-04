use crate::FirestoreExpr;

/// The unit of the amount in `timestamp_add`, `timestamp_subtract` and `timestamp_diff`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirestoreTimestampUnit {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
}

/// The unit a timestamp is cut down to by `timestamp_trunc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirestoreTimestampGranularity {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    WeekStartingMonday,
    WeekStartingTuesday,
    WeekStartingWednesday,
    WeekStartingThursday,
    WeekStartingFriday,
    WeekStartingSaturday,
    WeekStartingSunday,
    IsoWeek,
    Month,
    Quarter,
    Year,
    IsoYear,
}

/// The part of a timestamp that `timestamp_extract` returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    WeekStartingMonday,
    WeekStartingTuesday,
    WeekStartingWednesday,
    WeekStartingThursday,
    WeekStartingFriday,
    WeekStartingSaturday,
    WeekStartingSunday,
    IsoWeek,
    Month,
    Quarter,
    Year,
    IsoYear,
}

impl From<FirestoreTimestampUnit> for FirestoreExpr {
    fn from(value: FirestoreTimestampUnit) -> Self {
        FirestoreExpr::constant(match value {
            FirestoreTimestampUnit::Microsecond => "microsecond",
            FirestoreTimestampUnit::Millisecond => "millisecond",
            FirestoreTimestampUnit::Second => "second",
            FirestoreTimestampUnit::Minute => "minute",
            FirestoreTimestampUnit::Hour => "hour",
            FirestoreTimestampUnit::Day => "day",
        })
    }
}

impl From<FirestoreTimestampGranularity> for FirestoreExpr {
    fn from(value: FirestoreTimestampGranularity) -> Self {
        FirestoreExpr::constant(match value {
            FirestoreTimestampGranularity::Microsecond => "microsecond",
            FirestoreTimestampGranularity::Millisecond => "millisecond",
            FirestoreTimestampGranularity::Second => "second",
            FirestoreTimestampGranularity::Minute => "minute",
            FirestoreTimestampGranularity::Hour => "hour",
            FirestoreTimestampGranularity::Day => "day",
            FirestoreTimestampGranularity::Week => "week",
            FirestoreTimestampGranularity::WeekStartingMonday => "week(monday)",
            FirestoreTimestampGranularity::WeekStartingTuesday => "week(tuesday)",
            FirestoreTimestampGranularity::WeekStartingWednesday => "week(wednesday)",
            FirestoreTimestampGranularity::WeekStartingThursday => "week(thursday)",
            FirestoreTimestampGranularity::WeekStartingFriday => "week(friday)",
            FirestoreTimestampGranularity::WeekStartingSaturday => "week(saturday)",
            FirestoreTimestampGranularity::WeekStartingSunday => "week(sunday)",
            FirestoreTimestampGranularity::IsoWeek => "isoweek",
            FirestoreTimestampGranularity::Month => "month",
            FirestoreTimestampGranularity::Quarter => "quarter",
            FirestoreTimestampGranularity::Year => "year",
            FirestoreTimestampGranularity::IsoYear => "isoyear",
        })
    }
}

impl From<FirestoreTimestampPart> for FirestoreExpr {
    fn from(value: FirestoreTimestampPart) -> Self {
        FirestoreExpr::constant(match value {
            FirestoreTimestampPart::Microsecond => "microsecond",
            FirestoreTimestampPart::Millisecond => "millisecond",
            FirestoreTimestampPart::Second => "second",
            FirestoreTimestampPart::Minute => "minute",
            FirestoreTimestampPart::Hour => "hour",
            FirestoreTimestampPart::DayOfWeek => "dayofweek",
            FirestoreTimestampPart::Day => "day",
            FirestoreTimestampPart::DayOfYear => "dayofyear",
            FirestoreTimestampPart::Week => "week",
            FirestoreTimestampPart::WeekStartingMonday => "week(monday)",
            FirestoreTimestampPart::WeekStartingTuesday => "week(tuesday)",
            FirestoreTimestampPart::WeekStartingWednesday => "week(wednesday)",
            FirestoreTimestampPart::WeekStartingThursday => "week(thursday)",
            FirestoreTimestampPart::WeekStartingFriday => "week(friday)",
            FirestoreTimestampPart::WeekStartingSaturday => "week(saturday)",
            FirestoreTimestampPart::WeekStartingSunday => "week(sunday)",
            FirestoreTimestampPart::IsoWeek => "isoweek",
            FirestoreTimestampPart::Month => "month",
            FirestoreTimestampPart::Quarter => "quarter",
            FirestoreTimestampPart::Year => "year",
            FirestoreTimestampPart::IsoYear => "isoyear",
        })
    }
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
