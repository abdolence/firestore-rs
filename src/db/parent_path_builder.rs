use crate::db::safe_document_path;
use crate::FirestoreResult;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub struct ParentPathBuilder {
    value: String,
}

impl ParentPathBuilder {
    #[inline]
    pub(crate) fn new(initial: String) -> Self {
        Self { value: initial }
    }

    #[inline]
    pub fn at<S>(self, parent_collection_name: &str, parent_document_id: S) -> FirestoreResult<Self>
    where
        S: AsRef<str>,
    {
        Ok(Self::new(safe_document_path(
            self.value.as_str(),
            parent_collection_name,
            parent_document_id.as_ref(),
        )?))
    }
}

impl Display for ParentPathBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl AsRef<str> for ParentPathBuilder {
    fn as_ref(&self) -> &str {
        self.value.as_str()
    }
}

impl From<ParentPathBuilder> for String {
    fn from(pb: ParentPathBuilder) -> Self {
        pb.value
    }
}
