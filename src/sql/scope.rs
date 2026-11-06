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
}

impl FirestoreSqlScope {
    pub(super) fn new(relation: FirestoreSqlRelation) -> Self {
        Self { relation }
    }

    pub(super) fn relation(&self) -> &FirestoreSqlRelation {
        &self.relation
    }

    /// The field path of the column `parts` names. A first part that names the relation
    /// qualifies the rest; any other dotted name is a path into nested fields.
    pub(super) fn column(&self, parts: &[Ident]) -> FirestoreResult<String> {
        if let Some(parameter) = parts.iter().find(|part| is_parameter(part)) {
            return Err(FirestoreError::invalid_sql(
                &parameter.value,
                "a parameter is a value and cannot name a field",
                at(parameter.span),
            ));
        }
        Ok(match parts {
            [qualifier, rest @ ..]
                if !rest.is_empty()
                    && self.relation.name.as_deref() == Some(qualifier.value.as_str()) =>
            {
                field_path(rest)
            }
            _ => field_path(parts),
        })
    }
}
