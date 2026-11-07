//! The index statements: `CREATE [UNIQUE | SEARCH | VECTOR] INDEX` and both forms of `DROP INDEX`.

use super::ddl::{next_start, FirestoreSqlIndexKind};
use super::translator::FirestoreSqlTranslator;
use crate::errors::FirestoreError;
use crate::{FirestoreIndexDefinition, FirestoreIndexDeleteTarget, FirestoreResult};
use sqlparser::parser::Parser;

impl FirestoreSqlTranslator<'_> {
    /// The index a `CREATE .. INDEX` defines, read after `INDEX` and any `IF NOT EXISTS`.
    pub(super) fn index_definition(
        &self,
        parser: &mut Parser,
        _kind: Option<FirestoreSqlIndexKind>,
    ) -> FirestoreResult<FirestoreIndexDefinition> {
        Err(FirestoreError::invalid_sql(
            "CREATE INDEX",
            "CREATE INDEX is not translated yet",
            next_start(parser),
        ))
    }

    /// The index a `DROP .. INDEX` deletes, by ID or by definition, read after `INDEX` and any
    /// `IF EXISTS`.
    pub(super) fn index_delete_target(
        &self,
        parser: &mut Parser,
        _kind: Option<FirestoreSqlIndexKind>,
    ) -> FirestoreResult<FirestoreIndexDeleteTarget> {
        Err(FirestoreError::invalid_sql(
            "DROP INDEX",
            "DROP INDEX is not translated yet",
            next_start(parser),
        ))
    }
}
