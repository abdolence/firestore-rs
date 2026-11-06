use super::expr::SQL_DOCUMENT_NAME_FIELD;
use super::translator::{at, field_path, field_segment, is_parameter};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::FirestoreResult;
use sqlparser::ast::Ident;

/// The source a query reads, as the query's names see it.
#[derive(Clone)]
pub(super) struct FirestoreSqlRelation {
    /// The name a column may be qualified with: the source's alias, or else its collection ID.
    pub(super) name: Option<String>,
    /// The collection a bare document ID resolves against, relative to the documents root;
    /// `None` unless the source is one collection.
    pub(super) collection_path: Option<String>,
}

/// Where a column lives.
pub(super) enum FirestoreSqlColumn {
    /// A field of the documents the query reads.
    Own { path: String },
    /// A field of a document of an enclosing query, which a subquery reads through a variable.
    Outer {
        /// The depth of the scope of the enclosing query.
        depth: usize,
        /// The name the column was qualified with.
        qualifier: String,
        /// The field path in the documents of the enclosing query.
        path: String,
    },
}

/// Where a query sits in its statement.
#[derive(Clone, Copy)]
pub(super) struct FirestoreSqlPlace<'o> {
    /// The scope of the query this one is a subquery of.
    pub(super) outer: Option<&'o FirestoreSqlScope<'o>>,
    /// Only whether the query finds a row matters, so its select list is not translated.
    pub(super) existence: bool,
}

impl<'o> FirestoreSqlPlace<'o> {
    /// A query that is not inside another.
    pub(super) fn top() -> Self {
        Self {
            outer: None,
            existence: false,
        }
    }

    /// A subquery of the query whose scope is `scope`.
    pub(super) fn inside(scope: &'o FirestoreSqlScope<'o>) -> Self {
        Self {
            outer: Some(scope),
            existence: false,
        }
    }

    /// The same place, for a query whose rows are only counted.
    pub(super) fn for_existence(self) -> Self {
        Self {
            existence: true,
            ..self
        }
    }

    /// The same place, for a query whose rows are read.
    pub(super) fn for_rows(self) -> Self {
        Self {
            existence: false,
            ..self
        }
    }
}

/// The relations the names of one query resolve against.
///
/// The base relation's fields are fields of the documents the query reads. Each joined relation
/// is a map in those documents under the relation's name, so that two relations never share a
/// field.
pub(super) struct FirestoreSqlScope<'o> {
    relation: FirestoreSqlRelation,
    joined: Vec<FirestoreSqlRelation>,
    /// The field that holds the stored document in the clauses of `ON CONFLICT DO UPDATE`. A
    /// column there is a field of the stored document, unless `excluded` qualifies it as a field
    /// of the row being inserted.
    stored: Option<&'static str>,
    /// The scope of the query this one is a subquery of, or the left side of a join whose `ON`
    /// this scope resolves.
    outer: Option<&'o FirestoreSqlScope<'o>>,
    depth: usize,
    /// In an `ON` clause each side could hold a column, so every column names its side.
    qualified_only: bool,
}

impl<'o> FirestoreSqlScope<'o> {
    pub(super) fn new(relation: FirestoreSqlRelation) -> Self {
        Self {
            relation,
            joined: Vec::new(),
            stored: None,
            outer: None,
            depth: 0,
            qualified_only: false,
        }
    }

    /// This scope, inside the query whose scope is `outer`.
    pub(super) fn inside(self, outer: Option<&'o FirestoreSqlScope<'o>>) -> Self {
        Self {
            depth: outer.map_or(0, |outer| outer.depth + 1),
            outer,
            ..self
        }
    }

    /// This scope, for the `ON` clause that joins its relation to the relations of `left`.
    pub(super) fn joining(self, left: &'o FirestoreSqlScope<'o>) -> Self {
        Self {
            qualified_only: true,
            ..self.inside(Some(left))
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

    /// How many queries enclose this one.
    pub(super) fn depth(&self) -> usize {
        self.depth
    }

    /// Joins `relation`, whose fields live under its name.
    pub(super) fn join(&mut self, relation: FirestoreSqlRelation) {
        self.joined.push(relation);
    }

    /// The field path of the column `parts` names, for a clause that has no enclosing query.
    pub(super) fn field(&self, parts: &[Ident]) -> FirestoreResult<String> {
        match self.column(parts)? {
            FirestoreSqlColumn::Own { path } => Ok(path),
            FirestoreSqlColumn::Outer { qualifier, .. } => Err(FirestoreError::invalid_sql(
                &qualifier,
                "a column of an enclosing query cannot name a field here",
                parts.first().and_then(|part| at(part.span)),
            )),
        }
    }

    /// Fails when a relation of this scope is already called `name`.
    pub(super) fn ensure_unused(
        &self,
        name: &str,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<()> {
        let used = std::iter::once(&self.relation)
            .chain(&self.joined)
            .any(|relation| relation.name.as_deref() == Some(name));
        if used {
            Err(FirestoreError::invalid_sql(
                name,
                format!("{name} names two tables of this FROM: give one of them an alias"),
                start,
            ))
        } else {
            Ok(())
        }
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

    /// The field path of `rest` as the relation called `qualifier` holds it, or `None` when this
    /// scope has no such relation.
    fn qualified_path(&self, qualifier: &str, rest: &[Ident]) -> Option<String> {
        if self.relation.name.as_deref() == Some(qualifier) {
            return Some(field_path(rest));
        }
        self.joined
            .iter()
            .any(|joined| joined.name.as_deref() == Some(qualifier))
            .then(|| format!("{}.{}", field_segment(qualifier), field_path(rest)))
    }

    /// The name a `SELECT` gives the column at `path` when it does not name it: the path without
    /// the name of the joined relation that holds it.
    pub(super) fn output_name<'p>(&self, path: &'p str) -> &'p str {
        self.joined
            .iter()
            .filter_map(|joined| joined.name.as_deref())
            .find_map(|name| {
                path.strip_prefix(field_segment(name).as_ref())
                    .and_then(|rest| rest.strip_prefix('.'))
            })
            .unwrap_or(path)
    }

    /// Where the column `parts` names lives.
    ///
    /// A first part that names a relation of this scope or of an enclosing one qualifies the rest.
    /// Any other name is a field of the base relation, unless the query joins or sits inside
    /// another. There a name with dots must start with a relation, and in an `ON` clause every
    /// name must: each could belong to either side, and the alternative to an error is a field
    /// that is silently absent.
    pub(super) fn column(&self, parts: &[Ident]) -> FirestoreResult<FirestoreSqlColumn> {
        if let Some(parameter) = parts.iter().find(|part| is_parameter(part)) {
            return Err(FirestoreError::invalid_sql(
                &parameter.value,
                "a parameter is a value and cannot name a field",
                at(parameter.span),
            ));
        }
        if let Some(stored) = self.stored {
            return Ok(FirestoreSqlColumn::Own {
                path: self.stored_path(stored, parts)?,
            });
        }
        if let [qualifier, rest @ ..] = parts {
            if !rest.is_empty() {
                if let Some(path) = self.qualified_path(&qualifier.value, rest) {
                    return Ok(FirestoreSqlColumn::Own { path });
                }
                let mut enclosing = self.outer;
                while let Some(scope) = enclosing {
                    if let Some(path) = scope.qualified_path(&qualifier.value, rest) {
                        return Ok(FirestoreSqlColumn::Outer {
                            depth: scope.depth,
                            qualifier: qualifier.value.clone(),
                            path,
                        });
                    }
                    enclosing = scope.outer;
                }
                if self.is_strict() {
                    return Err(FirestoreError::invalid_sql(
                        &qualifier.value,
                        format!(
                            "{} is not a table or alias here, and with a join or a subquery a name with dots starts with one: write a nested field as `table.field.nested`",
                            qualifier.value
                        ),
                        at(qualifier.span),
                    ));
                }
            }
        }
        if self.qualified_only {
            let first = &parts[0];
            return Err(FirestoreError::invalid_sql(
                &first.value,
                format!(
                    "{} could be a column of either side of the join: write it as `table.{}`",
                    first.value, first.value
                ),
                at(first.span),
            ));
        }
        Ok(FirestoreSqlColumn::Own {
            path: field_path(parts),
        })
    }

    fn is_strict(&self) -> bool {
        !self.joined.is_empty() || self.outer.is_some()
    }

    fn stored_path(&self, stored: &str, parts: &[Ident]) -> FirestoreResult<String> {
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
