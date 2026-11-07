//! `ALTER COLLECTION`: one write of a field's single-field indexes, its TTL, or both.

use super::ddl::{next_start, option_string, FirestoreSqlOption};
use super::translator::{
    at, expr_start, field_path, is_parameter, name_start, FirestoreSqlTranslator,
};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::{
    FirestoreFieldOverrideIndex, FirestoreFieldOverrideTarget, FirestoreFieldOverrideUpdate,
    FirestoreFieldTtlUpdate, FirestoreFieldUpdate, FirestoreIndexFieldMode,
    FirestoreIndexQueryScope, FirestoreQueryDirection, FirestoreResult,
};
use sqlparser::ast::{Expr, Ident, Value as SqlValue};
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;

/// The word that scopes an entry of `indexes` to collection-group queries.
const COLLECTION_GROUP_WORD: &str = "COLLECTION_GROUP";

impl FirestoreSqlTranslator<'_> {
    /// The field update an `ALTER COLLECTION` makes, read after `COLLECTION`.
    pub(super) fn field_update(
        &self,
        parser: &mut Parser,
    ) -> FirestoreResult<FirestoreFieldUpdate> {
        let collection_group = self.collection_group_id(parser)?;
        let target = if parser.parse_keyword(Keyword::ALTER) {
            let _ = parser.parse_keyword(Keyword::COLUMN);
            self.field_target(parser)?
        } else {
            FirestoreFieldOverrideTarget::AllFields
        };
        parser
            .expect_keyword(Keyword::SET)
            .map_err(FirestoreError::from_sql_parser)?;
        let start = next_start(parser);
        if !parser.peek_keyword(Keyword::OPTIONS) {
            return Err(FirestoreError::invalid_sql(
                "ALTER COLLECTION",
                "SET is followed by OPTIONS (ttl = .., indexes = ..)",
                start,
            ));
        }

        let mut update = FirestoreFieldUpdate::new(collection_group, target);
        for option in self.statement_options(parser)? {
            match option.key.as_str() {
                "ttl" => update.ttl = Some(Self::ttl_update(&option)?),
                "indexes" => update.index_override = Some(Self::override_update(&option)?),
                key => {
                    return Err(FirestoreError::invalid_sql(
                        key,
                        "ALTER COLLECTION takes the options ttl and indexes",
                        option.start,
                    ))
                }
            }
        }
        update.validate().map_err(|error| error.located(start))?;
        Ok(update)
    }

    /// The field `parser` reads next, named by a field path.
    fn field_target(&self, parser: &mut Parser) -> FirestoreResult<FirestoreFieldOverrideTarget> {
        let name = parser
            .parse_object_name(false)
            .map_err(FirestoreError::from_sql_parser)?;
        let mut parts: Vec<Ident> = Vec::with_capacity(name.0.len());
        for part in &name.0 {
            let ident = part.as_ident().ok_or_else(|| {
                FirestoreError::unsupported_sql("this field name", name_start(&name))
            })?;
            if is_parameter(ident) {
                return Err(FirestoreError::sql_parameter_names_field(
                    &ident.value,
                    at(ident.span),
                ));
            }
            parts.push(ident.clone());
        }
        Ok(FirestoreFieldOverrideTarget::Field(field_path(&parts)))
    }

    fn ttl_update(option: &FirestoreSqlOption) -> FirestoreResult<FirestoreFieldTtlUpdate> {
        match &option.value {
            Expr::Value(value) => match value.value {
                SqlValue::Boolean(true) => Some(FirestoreFieldTtlUpdate::Enable),
                SqlValue::Boolean(false) => Some(FirestoreFieldTtlUpdate::Disable),
                _ => None,
            },
            _ => None,
        }
        .ok_or_else(|| {
            FirestoreError::invalid_sql("ttl", "ttl is TRUE or FALSE", expr_start(&option.value))
        })
    }

    fn override_update(
        option: &FirestoreSqlOption,
    ) -> FirestoreResult<FirestoreFieldOverrideUpdate> {
        match &option.value {
            Expr::Value(value) if value.value == SqlValue::Null => {
                Ok(FirestoreFieldOverrideUpdate::Revert)
            }
            Expr::Array(array) => array
                .elem
                .iter()
                .map(|entry| Self::override_index(&option.key, entry))
                .collect::<FirestoreResult<_>>()
                .map(FirestoreFieldOverrideUpdate::Set),
            value => Err(FirestoreError::invalid_sql(
                "indexes",
                "indexes is a list of strings, or NULL",
                expr_start(value),
            )),
        }
    }

    /// One entry of `indexes`: `ASC`, `DESC` or `CONTAINS`, optionally after `COLLECTION_GROUP`.
    fn override_index(key: &str, entry: &Expr) -> FirestoreResult<FirestoreFieldOverrideIndex> {
        let text = option_string(key, entry)?;
        let start = expr_start(entry);
        let words: Vec<&str> = text.split_whitespace().collect();
        let (query_scope, mode) = match words.as_slice() {
            [mode] => (FirestoreIndexQueryScope::Collection, *mode),
            [scope, mode] if scope.eq_ignore_ascii_case(COLLECTION_GROUP_WORD) => {
                (FirestoreIndexQueryScope::AllDescendants, *mode)
            }
            _ => return Err(Self::unknown_index_entry(&text, start)),
        };
        let mode = match mode.to_ascii_uppercase().as_str() {
            "ASC" => FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending),
            "DESC" => FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Descending),
            "CONTAINS" => FirestoreIndexFieldMode::ArrayContains,
            _ => return Err(Self::unknown_index_entry(&text, start)),
        };
        Ok(FirestoreFieldOverrideIndex { query_scope, mode })
    }

    fn unknown_index_entry(entry: &str, start: Option<FirestoreSqlLocation>) -> FirestoreError {
        FirestoreError::invalid_sql(
            "indexes",
            format!(
                "`{entry}` is not an index; an entry is ASC, DESC or CONTAINS, optionally after \
                 {COLLECTION_GROUP_WORD}"
            ),
            start,
        )
    }
}
