use crate::is_simple_field_segment;
use sqlparser::keywords::ALL_KEYWORDS;
use std::fmt::{self, Display, Formatter};

/// A field name written as a SQL identifier.
///
/// The [`Display`] output of a name is the name itself when it is a simple identifier that is not
/// a SQL keyword, and otherwise the name between backticks, with each backtick inside doubled.
/// Backslashes are written as they are: the SQL grammar has no escape for them in an identifier,
/// unlike the field path syntax of a pipeline.
///
/// Placed in the text of a statement, the output is read by the translator as a field of exactly
/// that name, whatever characters the name holds. A name taken from a document, such as one
/// offered for completion, can therefore be spliced into SQL as it is. A name is one segment of a
/// field path, so a `.` in it is part of the name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FirestoreSqlFieldName<'a>(&'a str);

impl<'a> From<&'a str> for FirestoreSqlFieldName<'a> {
    fn from(name: &'a str) -> Self {
        Self(name)
    }
}

impl Display for FirestoreSqlFieldName<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let is_keyword = ALL_KEYWORDS
            .binary_search(&self.0.to_ascii_uppercase().as_str())
            .is_ok();
        if is_simple_field_segment(self.0) && !is_keyword {
            f.write_str(self.0)
        } else {
            write!(f, "`{}`", self.0.replace('`', "``"))
        }
    }
}
