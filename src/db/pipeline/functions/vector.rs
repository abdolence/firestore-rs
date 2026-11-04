use crate::FirestoreExpr;

impl FirestoreExpr {
    /// The cosine distance between this vector and `other`.
    pub fn cosine_distance(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("cosine_distance", [self, other.into()])
    }

    /// The dot product of this vector and `other`.
    pub fn dot_product(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("dot_product", [self, other.into()])
    }

    /// The Euclidean distance between this vector and `other`.
    pub fn euclidean_distance(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("euclidean_distance", [self, other.into()])
    }

    /// The number of dimensions of this vector.
    pub fn vector_length(self) -> FirestoreExpr {
        Self::raw("vector_length", [self])
    }
}
