//! `ALTER COLLECTION`: one write of a field's single-field indexes, its TTL, or both.

use super::ddl::next_start;
use super::translator::FirestoreSqlTranslator;
use crate::errors::FirestoreError;
use crate::{FirestoreFieldUpdate, FirestoreResult};
use sqlparser::parser::Parser;

impl FirestoreSqlTranslator<'_> {
    /// The field update an `ALTER COLLECTION` makes, read after `COLLECTION`.
    pub(super) fn field_update(
        &self,
        parser: &mut Parser,
    ) -> FirestoreResult<FirestoreFieldUpdate> {
        Err(FirestoreError::invalid_sql(
            "ALTER COLLECTION",
            "ALTER COLLECTION is not translated yet",
            next_start(parser),
        ))
    }
}
