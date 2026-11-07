use crate::FirestoreExpr;

/// The [reference functions](https://docs.cloud.google.com/firestore/native/docs/pipeline/functions/reference_functions) of the service.
impl FirestoreExpr {
    pub fn collection_id(self) -> FirestoreExpr {
        Self::raw("collection_id", [self])
    }

    pub fn document_id(self) -> FirestoreExpr {
        Self::raw("document_id", [self])
    }

    pub fn parent(self) -> FirestoreExpr {
        Self::raw("parent", [self])
    }

    /// This reference's path sliced from segment `offset`, `length` segments long, as a
    /// reference.
    pub fn reference_slice(
        self,
        offset: impl Into<FirestoreExpr>,
        length: impl Into<FirestoreExpr>,
    ) -> FirestoreExpr {
        Self::raw("reference_slice", [self, offset.into(), length.into()])
    }
}
