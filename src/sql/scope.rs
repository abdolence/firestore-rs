use super::expr::SQL_DOCUMENT_NAME_FIELD;
use super::translator::{at, field_path, is_parameter};
use crate::errors::FirestoreError;
use crate::FirestoreResult;
use sqlparser::ast::Ident;

/// The source a query reads, as the query's names see it.
pub(super) struct FirestoreSqlRelation {
    /// The name a column may be qualified with: the source's alias, or else its collection ID.
    pub(super) name: Option<String>,
    /// The collection a bare document ID resolves against, relative to the documents root;
    /// `None` unless the source is one collection.
    pub(super) collection_path: Option<String>,
}

/// The relations the names of one query resolve against.
pub(super) struct FirestoreSqlScope {
    relation: FirestoreSqlRelation,
    /// The field that holds the stored document in the clauses of `ON CONFLICT DO UPDATE`. A
    /// column there is a field of the stored document, unless `excluded` qualifies it as a field
    /// of the row being inserted.
    stored: Option<&'static str>,
}

impl FirestoreSqlScope {
    pub(super) fn new(relation: FirestoreSqlRelation) -> Self {
        Self {
            relation,
            stored: None,
        }
    }

    /// This scope, with its columns resolved against the stored document under `field`.
    pub(super) fn with_stored(self, field: &'static str) -> Self {
        Self {
            stored: Some(field),
            ..self
        }
    }

    pub(super) fn relation(&self) -> &FirestoreSqlRelation {
        &self.relation
    }

    /// The names of the field `parts` names: a first part that names the relation qualifies the
    /// rest; any other dotted name is a path into nested fields.
    pub(super) fn field_names<'p>(&self, parts: &'p [Ident]) -> &'p [Ident] {
        match parts {
            [qualifier, rest @ ..]
                if !rest.is_empty()
                    && self.relation.name.as_deref() == Some(qualifier.value.as_str()) =>
            {
                rest
            }
            _ => parts,
        }
    }

    /// The field path of the column `parts` names.
    pub(super) fn column(&self, parts: &[Ident]) -> FirestoreResult<String> {
        if let Some(parameter) = parts.iter().find(|part| is_parameter(part)) {
            return Err(FirestoreError::invalid_sql(
                &parameter.value,
                "a parameter is a value and cannot name a field",
                at(parameter.span),
            ));
        }
        let Some(stored) = self.stored else {
            return Ok(field_path(self.field_names(parts)));
        };
        let (names, excluded) = match parts {
            [qualifier, rest @ ..]
                if !rest.is_empty() && qualifier.value.eq_ignore_ascii_case("excluded") =>
            {
                (rest, true)
            }
            _ => (self.field_names(parts), false),
        };
        let path = field_path(names);
        if path == SQL_DOCUMENT_NAME_FIELD {
            return Err(FirestoreError::invalid_sql(
                SQL_DOCUMENT_NAME_FIELD,
                "the name of a conflicting row is the name of the stored document, so it has one value",
                parts.first().and_then(|part| at(part.span)),
            ));
        }
        Ok(if excluded {
            path
        } else {
            format!("{stored}.{path}")
        })
    }
}
