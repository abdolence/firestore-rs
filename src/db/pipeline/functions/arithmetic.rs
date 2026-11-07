use crate::FirestoreExpr;

/// The [arithmetic functions](https://docs.cloud.google.com/firestore/native/docs/pipeline/functions/arithmetic_functions) of the service.
impl FirestoreExpr {
    // Every method carries its service function's name, which is also `std::ops::Add::add`.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("add", [self, other.into()])
    }

    pub fn subtract(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("subtract", [self, other.into()])
    }

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

    pub fn abs(self) -> FirestoreExpr {
        Self::raw("abs", [self])
    }

    pub fn ceil(self) -> FirestoreExpr {
        Self::raw("ceil", [self])
    }

    pub fn floor(self) -> FirestoreExpr {
        Self::raw("floor", [self])
    }

    pub fn round(self) -> FirestoreExpr {
        Self::raw("round", [self])
    }

    pub fn round_with_places(self, places: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("round", [self, places.into()])
    }

    pub fn trunc(self) -> FirestoreExpr {
        Self::raw("trunc", [self])
    }

    pub fn trunc_with_places(self, places: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("trunc", [self, places.into()])
    }

    /// This number raised to the power `exponent`, as a float.
    pub fn pow(self, exponent: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("pow", [self, exponent.into()])
    }

    pub fn sqrt(self) -> FirestoreExpr {
        Self::raw("sqrt", [self])
    }

    pub fn exp(self) -> FirestoreExpr {
        Self::raw("exp", [self])
    }

    pub fn ln(self) -> FirestoreExpr {
        Self::raw("ln", [self])
    }

    pub fn log10(self) -> FirestoreExpr {
        Self::raw("log10", [self])
    }

    /// The natural logarithm; the same as [`ln`](Self::ln).
    pub fn log(self) -> FirestoreExpr {
        Self::raw("log", [self])
    }

    pub fn log_with_base(self, base: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("log", [self, base.into()])
    }
}
