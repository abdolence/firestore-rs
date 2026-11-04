use crate::{FirestoreBoolExpr, FirestoreExpr};

impl FirestoreExpr {
    /// The number of characters of this string.
    pub fn char_length(self) -> FirestoreExpr {
        Self::raw("char_length", [self])
    }

    /// The number of bytes of this string or bytes value.
    pub fn byte_length(self) -> FirestoreExpr {
        Self::raw("byte_length", [self])
    }

    /// True when this string starts with `prefix`.
    pub fn starts_with(self, prefix: impl Into<FirestoreExpr>) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("starts_with", [self, prefix.into()]))
    }

    /// True when this string ends with `postfix`.
    pub fn ends_with(self, postfix: impl Into<FirestoreExpr>) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("ends_with", [self, postfix.into()]))
    }

    /// True when this string contains `substring`.
    pub fn string_contains(self, substring: impl Into<FirestoreExpr>) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("string_contains", [self, substring.into()]))
    }

    /// True when this string matches the `like` `pattern`, where `%` matches any run of
    /// characters.
    pub fn like(self, pattern: impl Into<FirestoreExpr>) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("like", [self, pattern.into()]))
    }

    /// True when any part of this string matches the regular expression `regexp`.
    pub fn regex_contains(self, regexp: impl Into<FirestoreExpr>) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("regex_contains", [self, regexp.into()]))
    }

    /// True when the whole of this string matches the regular expression `regexp`.
    pub fn regex_match(self, regexp: impl Into<FirestoreExpr>) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("regex_match", [self, regexp.into()]))
    }

    /// The first part of this string that matches `regexp`, or null when none does.
    pub fn regex_find(self, regexp: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("regex_find", [self, regexp.into()])
    }

    /// Every part of this string that matches `regexp`, as an array.
    pub fn regex_find_all(self, regexp: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("regex_find_all", [self, regexp.into()])
    }

    /// This string followed by `others`, joined into one string.
    pub fn string_concat<I>(self, others: I) -> FirestoreExpr
    where
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        Self::raw("string_concat", self.followed_by(others))
    }

    /// The zero-based position of the first `search` in this string or bytes value, or -1 when it
    /// is not there.
    pub fn string_index_of(self, search: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("string_index_of", [self, search.into()])
    }

    /// This string or bytes value in upper case.
    pub fn to_upper(self) -> FirestoreExpr {
        Self::raw("to_upper", [self])
    }

    /// This string or bytes value in lower case.
    pub fn to_lower(self) -> FirestoreExpr {
        Self::raw("to_lower", [self])
    }

    /// This string or bytes value in reverse order.
    pub fn string_reverse(self) -> FirestoreExpr {
        Self::raw("string_reverse", [self])
    }

    /// This string or bytes value repeated `repetitions` times.
    pub fn string_repeat(self, repetitions: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("string_repeat", [self, repetitions.into()])
    }

    /// This string with every `find` replaced by `replacement`.
    pub fn string_replace_all(
        self,
        find: impl Into<FirestoreExpr>,
        replacement: impl Into<FirestoreExpr>,
    ) -> FirestoreExpr {
        Self::raw(
            "string_replace_all",
            [self, find.into(), replacement.into()],
        )
    }

    /// This string with the first `find` replaced by `replacement`.
    pub fn string_replace_one(
        self,
        find: impl Into<FirestoreExpr>,
        replacement: impl Into<FirestoreExpr>,
    ) -> FirestoreExpr {
        Self::raw(
            "string_replace_one",
            [self, find.into(), replacement.into()],
        )
    }

    /// The rest of this string from `position`.
    pub fn substring(self, position: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("substring", [self, position.into()])
    }

    /// `length` characters of this string from `position`.
    pub fn substring_with_length(
        self,
        position: impl Into<FirestoreExpr>,
        length: impl Into<FirestoreExpr>,
    ) -> FirestoreExpr {
        Self::raw("substring", [self, position.into(), length.into()])
    }

    /// This string without leading and trailing whitespace.
    pub fn trim(self) -> FirestoreExpr {
        Self::raw("trim", [self])
    }

    /// This string without the leading and trailing `values_to_trim`.
    pub fn trim_with(self, values_to_trim: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("trim", [self, values_to_trim.into()])
    }

    /// This string without leading whitespace.
    pub fn ltrim(self) -> FirestoreExpr {
        Self::raw("ltrim", [self])
    }

    /// This string without the leading `to_trim`.
    pub fn ltrim_with(self, to_trim: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("ltrim", [self, to_trim.into()])
    }

    /// This string without trailing whitespace.
    pub fn rtrim(self) -> FirestoreExpr {
        Self::raw("rtrim", [self])
    }

    /// This string without the trailing `to_trim`.
    pub fn rtrim_with(self, to_trim: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("rtrim", [self, to_trim.into()])
    }

    /// The pieces of this string, split at commas.
    pub fn split(self) -> FirestoreExpr {
        Self::raw("split", [self])
    }

    /// The pieces of this string or bytes value, split at `delimiter`.
    pub fn split_with_delimiter(self, delimiter: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("split", [self, delimiter.into()])
    }

    /// The elements of this array joined into one string with `delimiter` between them.
    pub fn join(self, delimiter: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("join", [self, delimiter.into()])
    }

    /// As [`join`](Self::join), writing a null element as `null_text`.
    pub fn join_with_null_text(
        self,
        delimiter: impl Into<FirestoreExpr>,
        null_text: impl Into<FirestoreExpr>,
    ) -> FirestoreExpr {
        Self::raw("join", [self, delimiter.into(), null_text.into()])
    }
}
