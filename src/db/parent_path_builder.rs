use std::fmt::{Display, Formatter};

pub struct ParentPathBuilder {
    value: String,
}

impl ParentPathBuilder {
    #[inline]
    pub(crate) fn new(initial: String) -> Self {
        Self { value: initial }
    }

    #[inline]
    pub fn at<S>(self, parent_collection_name: &str, parent_document_id: S) -> Self
    where
        S: AsRef<str>,
    {
        Self::new(format!(
            "{}/{}/{}",
            self.value,
            parent_collection_name,
            parent_document_id.as_ref()
        ))
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
