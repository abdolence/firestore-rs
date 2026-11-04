use crate::FirestoreExpr;

impl FirestoreExpr {
    /// The sum of this expression and `other`.
    // Every method carries its service function's name, which is also `std::ops::Add::add`.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("add", [self, other.into()])
    }

    /// This expression minus `other`.
    pub fn subtract(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("subtract", [self, other.into()])
    }

    /// The product of this expression and `other`.
    pub fn multiply(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("multiply", [self, other.into()])
    }

    /// This expression divided by `other`. Two integers divide as integers: 7 divided by 2 is 3.
    pub fn divide(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("divide", [self, other.into()])
    }

    /// The remainder of this expression divided by `other`. The service's `mod`, which is a Rust
    /// keyword.
    pub fn modulo(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("mod", [self, other.into()])
    }

    /// The absolute value.
    pub fn abs(self) -> FirestoreExpr {
        Self::raw("abs", [self])
    }

    /// The smallest integer not below this number.
    pub fn ceil(self) -> FirestoreExpr {
        Self::raw("ceil", [self])
    }

    /// The largest integer not above this number.
    pub fn floor(self) -> FirestoreExpr {
        Self::raw("floor", [self])
    }

    /// This number rounded to a whole number.
    pub fn round(self) -> FirestoreExpr {
        Self::raw("round", [self])
    }

    /// This number rounded to `places` decimal places.
    pub fn round_with_places(self, places: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("round", [self, places.into()])
    }

    /// This number with its fractional part cut off.
    pub fn trunc(self) -> FirestoreExpr {
        Self::raw("trunc", [self])
    }

    /// This number cut off after `places` decimal places.
    pub fn trunc_with_places(self, places: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("trunc", [self, places.into()])
    }

    /// This number raised to the power `exponent`, as a float.
    pub fn pow(self, exponent: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("pow", [self, exponent.into()])
    }

    /// The square root.
    pub fn sqrt(self) -> FirestoreExpr {
        Self::raw("sqrt", [self])
    }

    /// `e` raised to the power of this number.
    pub fn exp(self) -> FirestoreExpr {
        Self::raw("exp", [self])
    }

    /// The natural logarithm.
    pub fn ln(self) -> FirestoreExpr {
        Self::raw("ln", [self])
    }

    /// The base 10 logarithm.
    pub fn log10(self) -> FirestoreExpr {
        Self::raw("log10", [self])
    }

    /// The natural logarithm; the same as [`ln`](Self::ln).
    pub fn log(self) -> FirestoreExpr {
        Self::raw("log", [self])
    }

    /// The logarithm to `base`.
    pub fn log_with_base(self, base: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("log", [self, base.into()])
    }
}
