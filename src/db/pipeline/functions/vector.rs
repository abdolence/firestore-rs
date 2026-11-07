use crate::FirestoreExpr;

/// The [vector functions](https://docs.cloud.google.com/firestore/native/docs/pipeline/functions/vector_functions) of the service.
impl FirestoreExpr {
    pub fn cosine_distance(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("cosine_distance", [self, other.into()])
    }

    pub fn dot_product(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("dot_product", [self, other.into()])
    }

    pub fn euclidean_distance(self, other: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("euclidean_distance", [self, other.into()])
    }

    pub fn vector_length(self) -> FirestoreExpr {
        Self::raw("vector_length", [self])
    }
}
