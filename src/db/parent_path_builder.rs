use crate::db::safe_document_path;
use crate::{FirestoreReference, FirestoreResult};
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
    pub fn at<S>(self, collection_name: &str, document_id: S) -> FirestoreResult<Self>
    where
        S: AsRef<str>,
    {
        Ok(Self::new(safe_document_path(
            self.value.as_str(),
            collection_name,
            document_id.as_ref(),
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

impl<'a> From<&'a ParentPathBuilder> for &'a str {
    fn from(pb: &'a ParentPathBuilder) -> &'a str {
        pb.value.as_str()
    }
}

impl From<ParentPathBuilder> for FirestoreReference {
    fn from(pb: ParentPathBuilder) -> Self {
        FirestoreReference(pb.value)
    }
}
