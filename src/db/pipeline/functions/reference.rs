use crate::FirestoreExpr;

impl FirestoreExpr {
    /// The ID of the collection this document reference points into.
    pub fn collection_id(self) -> FirestoreExpr {
        Self::raw("collection_id", [self])
    }

    /// The ID of the document this reference points at.
    pub fn document_id(self) -> FirestoreExpr {
        Self::raw("document_id", [self])
    }

    /// The reference to the parent of this one.
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
