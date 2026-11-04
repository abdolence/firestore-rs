use crate::{FirestoreBoolExpr, FirestoreExpr};

impl FirestoreExpr {
    /// True when this expression equals `other`. NaN equals NaN.
    pub fn equal<V: Into<FirestoreExpr>>(self, other: V) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("equal", [self, other.into()]))
    }

    /// Core's name for [`equal`](Self::equal).
    pub fn eq<V: Into<FirestoreExpr>>(self, other: V) -> FirestoreBoolExpr {
        self.equal(other)
    }

    /// True when this expression does not equal `other`, which includes an absent field: unlike
    /// a Core `!=` filter, it keeps the documents that lack the field.
    pub fn not_equal<V: Into<FirestoreExpr>>(self, other: V) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("not_equal", [self, other.into()]))
    }

    /// Core's name for [`not_equal`](Self::not_equal).
    pub fn neq<V: Into<FirestoreExpr>>(self, other: V) -> FirestoreBoolExpr {
        self.not_equal(other)
    }

    pub fn less_than<V: Into<FirestoreExpr>>(self, other: V) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("less_than", [self, other.into()]))
    }

    pub fn less_than_or_equal<V: Into<FirestoreExpr>>(self, other: V) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("less_than_or_equal", [self, other.into()]))
    }

    pub fn greater_than<V: Into<FirestoreExpr>>(self, other: V) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("greater_than", [self, other.into()]))
    }

    pub fn greater_than_or_equal<V: Into<FirestoreExpr>>(self, other: V) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("greater_than_or_equal", [self, other.into()]))
    }
}
