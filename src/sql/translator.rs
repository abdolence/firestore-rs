use super::aggregation::FirestoreSqlAggregation;
use super::parameters::FirestoreSqlBinding;
use super::subquery::{FirestoreSqlCapture, FirestoreSqlCte};
use super::write::SQL_UPSERT_FORM;
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::field_path_segment;
use crate::{
    FirestoreCollectionId, FirestoreDocumentId, FirestoreExpr, FirestoreResult,
    FirestoreSearchQuery, FirestoreSqlOptions, FirestoreSqlStatement, FirestoreValue,
};
use gcloud_sdk::google::firestore::v1::{value::ValueType, Value};
use sqlparser::ast::{Expr, Ident, ObjectName, ObjectNamePart, Statement};
use sqlparser::tokenizer::Span;

// How deep the translator nests into a statement. sqlparser's recursion limit does not bound a
// chain of infix operators, which it parses in a loop but nests one level per operator.
const SQL_MAX_TRANSLATION_DEPTH: usize = 64;

// Translates one parsed statement into the pipeline model.
//
// Every AST node is destructured in full: a field with no pipeline form is refused when it is
// set, so that no part of a statement is silently dropped. A field bound to `_` is notation,
// such as whether `AS` was written.
pub(super) struct FirestoreSqlTranslator<'t> {
    pub(super) documents_path: &'t str,
    pub(super) options: &'t FirestoreSqlOptions,
    pub(super) binding: FirestoreSqlBinding<'t>,
    pub(super) aggregation: Option<FirestoreSqlAggregation>,
    pub(super) search: Option<FirestoreSearchQuery>,
    // The columns of enclosing queries that subqueries have read and no stage defines yet.
    pub(super) pending: Vec<FirestoreSqlCapture>,
    pub(super) ctes: Vec<FirestoreSqlCte>,
    pub(super) cte_copies: usize,
    depth: usize,
}

impl<'t> FirestoreSqlTranslator<'t> {
    pub(super) fn new(
        documents_path: &'t str,
        options: &'t FirestoreSqlOptions,
        binding: FirestoreSqlBinding<'t>,
    ) -> Self {
        Self {
            documents_path,
            options,
            binding,
            aggregation: None,
            search: None,
            pending: Vec::new(),
            ctes: Vec::new(),
            cte_copies: 0,
            depth: 0,
        }
    }

    pub(super) fn statement(
        &mut self,
        statement: &Statement,
    ) -> FirestoreResult<FirestoreSqlStatement> {
        let translated = match statement {
            Statement::Query(query) => FirestoreSqlStatement::Query(self.top_level_query(query)?),
            Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::Merge(_)
                if self.options.read_only =>
            {
                return Err(FirestoreError::invalid_sql(
                    "read_only",
                    "a write is refused by the read-only options",
                    None,
                ))
            }
            Statement::Insert(insert) => self.insert(insert)?,
            Statement::Update(update) => self.update(update)?,
            Statement::Delete(delete) => self.delete(delete)?,
            Statement::Merge(merge) => {
                return Err(FirestoreError::invalid_sql(
                    "MERGE",
                    format!("MERGE has no pipeline form in SQL: {SQL_UPSERT_FORM}"),
                    at(merge.merge_token.0.span),
                ))
            }
            _ => {
                return Err(FirestoreError::invalid_sql(
                    "statement",
                    "only a SELECT, INSERT, UPDATE or DELETE statement translates to a pipeline",
                    None,
                ))
            }
        };
        self.ensure_variables_defined()?;
        Ok(translated)
    }

    pub(super) fn nested<T>(
        &mut self,
        start: Option<FirestoreSqlLocation>,
        translate: impl FnOnce(&mut Self) -> FirestoreResult<T>,
    ) -> FirestoreResult<T> {
        if self.depth == SQL_MAX_TRANSLATION_DEPTH {
            return Err(FirestoreError::invalid_sql(
                "nesting",
                format!("the statement nests deeper than {SQL_MAX_TRANSLATION_DEPTH} levels"),
                start,
            ));
        }
        self.depth += 1;
        let translated = translate(self);
        self.depth -= 1;
        translated
    }

    // The path of the document `path` names, relative to the documents root: `path` is that
    // already, or a full path in this database.
    pub(super) fn document_path<'p>(
        &self,
        path: &'p str,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<&'p str> {
        let relative = path
            .strip_prefix(self.documents_path)
            .and_then(|path| path.strip_prefix('/'))
            .unwrap_or(path);
        let segments: Vec<&str> = relative.split('/').collect();
        if !segments.len().is_multiple_of(2) {
            return Err(FirestoreError::invalid_sql(
                "document",
                format!(
                    "`{path}` is not a document path, which alternates collections and documents"
                ),
                start,
            ));
        }
        self.check_segments(&segments, start)?;
        Ok(relative)
    }

    pub(super) fn collection_path<'p>(
        &self,
        path: &'p str,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<(Option<&'p str>, &'p str)> {
        let segments: Vec<&str> = path.split('/').collect();
        if segments.len().is_multiple_of(2) {
            return Err(FirestoreError::invalid_sql(
                "collection",
                format!("`{path}` is not a collection path, which ends with a collection"),
                start,
            ));
        }
        self.check_segments(&segments, start)?;
        Ok(match path.rsplit_once('/') {
            Some((parent, collection_id)) => (Some(parent), collection_id),
            None => (None, path),
        })
    }

    fn check_segments(
        &self,
        segments: &[&str],
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<()> {
        for (index, segment) in segments.iter().enumerate() {
            if index.is_multiple_of(2) {
                self.check_collection(segment, start)?;
            } else {
                FirestoreDocumentId::validate_str(segment).map_err(|error| error.located(start))?;
            }
        }
        Ok(())
    }

    /// The collection ID `collection_id`, validated and allowed by the options.
    pub(super) fn check_collection(
        &self,
        collection_id: &str,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreCollectionId> {
        let collection_id =
            FirestoreCollectionId::new(collection_id).map_err(|error| error.located(start))?;
        match &self.options.collections {
            Some(allowed) if !allowed.contains(&collection_id) => Err(FirestoreError::invalid_sql(
                "collections",
                format!("the options do not allow the collection {collection_id}"),
                start,
            )),
            _ => Ok(collection_id),
        }
    }
}

impl FirestoreExpr {
    pub(super) fn constant_value(&self) -> Option<&ValueType> {
        match self {
            Self::Constant(FirestoreValue {
                value: Value { value_type },
            }) => value_type.as_ref(),
            _ => None,
        }
    }
}

pub(super) fn refuse_set(
    constructs: &[(bool, &str)],
    location: Option<FirestoreSqlLocation>,
) -> FirestoreResult<()> {
    match constructs.iter().find(|(set, _)| *set) {
        Some((_, construct)) => Err(FirestoreError::unsupported_sql(construct, location)),
        None => Ok(()),
    }
}

// Whether `ident` is a parameter, `@name`. A backticked `` `@name` `` is a field.
pub(super) fn is_parameter(ident: &Ident) -> bool {
    ident.quote_style.is_none() && ident.value.starts_with('@')
}

pub(super) fn is_column(mut expr: &Expr) -> bool {
    loop {
        expr = match expr {
            Expr::Nested(inner) => inner,
            Expr::Identifier(ident) => return !is_parameter(ident),
            Expr::CompoundIdentifier(_) => return true,
            _ => return false,
        };
    }
}

// Where `span` starts, unless sqlparser made it up.
pub(super) fn at(span: Span) -> Option<FirestoreSqlLocation> {
    FirestoreSqlLocation::from_sqlparser(span.start)
}

pub(super) fn name_start(name: &ObjectName) -> Option<FirestoreSqlLocation> {
    match name.0.first()? {
        ObjectNamePart::Identifier(ident) => at(ident.span),
        ObjectNamePart::Function(function) => at(function.name.span),
    }
}

// Where `expr` starts, found along its first operands in a loop: a chain of infix operators
// nests as deep as it is long, and sqlparser's own spans recurse through all of it.
pub(super) fn expr_start(mut expr: &Expr) -> Option<FirestoreSqlLocation> {
    loop {
        expr = match expr {
            Expr::Identifier(ident) => return at(ident.span),
            Expr::CompoundIdentifier(idents) => return at(idents.first()?.span),
            Expr::Value(value) => return at(value.span),
            Expr::Function(function) => return name_start(&function.name),
            Expr::Case { case_token, .. } => return at(case_token.0.span),
            Expr::BinaryOp { left: first, .. }
            | Expr::UnaryOp { expr: first, .. }
            | Expr::Nested(first)
            | Expr::IsNull(first)
            | Expr::IsNotNull(first)
            | Expr::IsTrue(first)
            | Expr::IsNotTrue(first)
            | Expr::IsFalse(first)
            | Expr::IsNotFalse(first)
            | Expr::IsUnknown(first)
            | Expr::IsNotUnknown(first)
            | Expr::IsDistinctFrom(first, _)
            | Expr::IsNotDistinctFrom(first, _)
            | Expr::InList { expr: first, .. }
            | Expr::InSubquery { expr: first, .. }
            | Expr::InUnnest { expr: first, .. }
            | Expr::Between { expr: first, .. }
            | Expr::Like { expr: first, .. }
            | Expr::ILike { expr: first, .. }
            | Expr::SimilarTo { expr: first, .. }
            | Expr::RLike { expr: first, .. }
            | Expr::AnyOp { left: first, .. }
            | Expr::AllOp { left: first, .. }
            | Expr::Collate { expr: first, .. }
            | Expr::AtTimeZone {
                timestamp: first, ..
            }
            | Expr::CompoundFieldAccess { root: first, .. }
            | Expr::JsonAccess { value: first, .. }
            | Expr::Cast { expr: first, .. } => first,
            _ => return None,
        };
    }
}

pub(super) fn field_path(parts: &[Ident]) -> String {
    parts
        .iter()
        .map(|part| field_path_segment(&part.value))
        .collect::<Vec<_>>()
        .join(".")
}
