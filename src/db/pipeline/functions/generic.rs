use crate::FirestoreExpr;

impl FirestoreExpr {
    /// This string, bytes value or array followed by the `others`, joined into one.
    pub fn concat<I>(self, others: I) -> FirestoreExpr
    where
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        Self::raw("concat", self.followed_by(others))
    }

    /// The length of this string, bytes value, array, map or vector.
    pub fn length(self) -> FirestoreExpr {
        Self::raw("length", [self])
    }

    /// This string, bytes value or array in reverse order.
    pub fn reverse(self) -> FirestoreExpr {
        Self::raw("reverse", [self])
    }

    /// This expression followed by `rest`: the arguments of a function that takes the receiver
    /// first and then any number more.
    pub(super) fn followed_by<I>(self, rest: I) -> impl Iterator<Item = FirestoreExpr>
    where
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        std::iter::once(self).chain(rest.into_iter().map(Into::into))
    }
}
