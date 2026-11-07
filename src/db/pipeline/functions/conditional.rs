use crate::{FirestoreBoolExpr, FirestoreExpr};

impl FirestoreExpr {
    /// -1, 0 or 1 as this value is less than, equal to or greater than `other`.
    pub fn cmp(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("cmp", [self, other.into()])
    }

    /// True when this value equals any element of the array `search_space`.
    pub fn equal_any(self, search_space: impl Into<FirestoreExpr>) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("equal_any", [self, search_space.into()]))
    }

    /// Core's name for [`equal_any`](Self::equal_any).
    pub fn is_in(self, search_space: impl Into<FirestoreExpr>) -> FirestoreBoolExpr {
        self.equal_any(search_space)
    }

    /// True when this value equals no element of the array `search_space`.
    pub fn not_equal_any(self, search_space: impl Into<FirestoreExpr>) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("not_equal_any", [self, search_space.into()]))
    }

    /// Core's name for [`not_equal_any`](Self::not_equal_any).
    pub fn is_not_in(self, search_space: impl Into<FirestoreExpr>) -> FirestoreBoolExpr {
        self.not_equal_any(search_space)
    }

    /// True when this value is null. An absent field is not null.
    pub fn is_null(self) -> FirestoreBoolExpr {
        self.equal(None::<bool>)
    }

    /// True when this value exists and is not null. `exists` takes only a field, so any other
    /// expression is compared with null alone.
    pub fn is_not_null(self) -> FirestoreBoolExpr {
        match self {
            Self::Field(_) => self
                .clone()
                .exists()
                .and_maybe(Some(self.not_equal(None::<bool>))),
            _ => self.not_equal(None::<bool>),
        }
    }

    /// True when this value is NaN.
    pub fn is_nan(self) -> FirestoreBoolExpr {
        self.equal(f64::NAN)
    }

    /// True when this field exists and is not NaN. The service accepts only a field here.
    pub fn is_not_nan(self) -> FirestoreBoolExpr {
        self.clone()
            .exists()
            .and_maybe(Some(self.not_equal(f64::NAN)))
    }

    /// This value, or `replacement` when it is null.
    pub fn if_null(self, replacement: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("if_null", [self, replacement.into()])
    }

    /// This value, or `replacement` when the field is absent.
    pub fn if_absent(self, replacement: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("if_absent", [self, replacement.into()])
    }

    /// This value, or `replacement` when evaluating it fails.
    pub fn if_error(self, replacement: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("if_error", [self, replacement.into()])
    }

    /// True when this field is absent. A null field is not absent.
    pub fn is_absent(self) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("is_absent", [self]))
    }

    /// True when evaluating this expression fails.
    pub fn is_error(self) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("is_error", [self]))
    }

    /// True when this field exists. A null field exists. The service accepts only a field here.
    pub fn exists(self) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("exists", [self]))
    }
}
