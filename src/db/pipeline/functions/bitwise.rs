use crate::FirestoreExpr;

impl FirestoreExpr {
    /// The bitwise complement of this integer. Google does not document the bitwise functions,
    /// and the service has failed calls to them with an internal error.
    pub fn bit_not(self) -> FirestoreExpr {
        Self::raw("bit_not", [self])
    }

    /// This integer shifted left by `bits` bits. Undocumented by Google; see
    /// [`bit_not`](Self::bit_not).
    pub fn bit_left_shift(self, bits: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("bit_left_shift", [self, bits.into()])
    }

    /// This integer shifted right by `bits` bits. Undocumented by Google; see
    /// [`bit_not`](Self::bit_not).
    pub fn bit_right_shift(self, bits: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("bit_right_shift", [self, bits.into()])
    }
}
