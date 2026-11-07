//! `ALTER COLLECTION`: one write of a field's single-field indexes, its TTL, or both.

use super::ddl::next_start;
use super::translator::{at, field_path, is_parameter, name_start, FirestoreSqlTranslator};
use crate::errors::FirestoreError;
use crate::{FirestoreFieldOverrideTarget, FirestoreFieldUpdate, FirestoreResult};
use sqlparser::ast::Ident;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;

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
                "ttl" => update.ttl = Some(option.ttl_update()?),
                "indexes" => update.index_override = Some(option.override_update()?),
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
}
