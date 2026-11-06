use super::expr::SQL_DOCUMENT_NAME_FIELD;
use super::query::{table_start, FirestoreSqlBranch};
use super::scope::{FirestoreSqlRelation, FirestoreSqlScope};
use super::translator::{
    at, expr_start, is_parameter, name_start, refuse_set, FirestoreSqlTranslator,
};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::{
    FirestoreExpr, FirestoreFieldsStage, FirestoreInsertWrite, FirestoreLiteralsSource,
    FirestorePipeline, FirestorePipelineSource, FirestorePipelineStage, FirestorePipelineWrite,
    FirestorePipelineWriteParams, FirestorePipelineWriteTransaction, FirestoreResult,
    FirestoreSelection, FirestoreSqlStatement, FirestoreUpdateWrite, FirestoreUpsertWrite,
    FirestoreValue, FirestoreWriteCollection,
};
use gcloud_sdk::google::firestore::v1::{value::ValueType, MapValue, Value};
use sqlparser::ast::{
    Assignment, AssignmentTarget, Delete, Expr, FromTable, Ident, Insert, Merge, ObjectName,
    ObjectNamePart, Parens, Query, SetExpr, SqliteOnConflict, TableObject, TableWithJoins, Update,
    Values,
};
use std::collections::HashMap;

/// Why `MERGE`, `ON CONFLICT` and `REPLACE INTO` are refused, and the form that does what they
/// are most often written for.
const SQL_UPSERT_FORM: &str =
    "an upsert replaces the stored document, which INSERT OR REPLACE INTO writes";

impl FirestoreSqlStatement {
    /// A write as the statement that runs it. Every SQL write is atomic: it applies in full or
    /// not at all, and Firestore requires that of an upsert and of an insert from a query.
    fn atomic(pipeline: FirestorePipeline, write: impl Into<FirestorePipelineWrite>) -> Self {
        Self::Write(Box::new(
            FirestorePipelineWriteParams::new(pipeline, write.into())
                .with_transaction(FirestorePipelineWriteTransaction::Atomic),
        ))
    }
}

impl FirestoreSqlTranslator<'_> {
    pub(super) fn merge(merge: &Merge) -> FirestoreError {
        FirestoreError::invalid_sql(
            "MERGE",
            format!("MERGE has no pipeline form in SQL: {SQL_UPSERT_FORM}"),
            at(merge.merge_token.0.span),
        )
    }

    pub(super) fn insert(&mut self, insert: &Insert) -> FirestoreResult<FirestoreSqlStatement> {
        let Insert {
            insert_token,
            optimizer_hints,
            or,
            ignore,
            into: _,
            table,
            table_alias,
            columns,
            overwrite,
            source,
            assignments,
            partitioned,
            after_columns,
            has_table_keyword,
            on,
            returning,
            output,
            replace_into,
            priority,
            insert_alias,
            settings,
            format_clause,
            multi_table_insert_type,
            multi_table_into_clauses,
            multi_table_when_clauses,
            multi_table_else_clause,
        } = insert;
        let start = at(insert_token.0.span);
        for (construct, set) in [
            ("ON CONFLICT", on.is_some()),
            ("REPLACE INTO", *replace_into),
        ] {
            if set {
                return Err(FirestoreError::invalid_sql(
                    construct,
                    format!("{construct} has no pipeline form in SQL: {SQL_UPSERT_FORM}"),
                    start,
                ));
            }
        }
        refuse_set(
            &[
                (!optimizer_hints.is_empty(), "an optimizer hint"),
                (*ignore, "INSERT IGNORE"),
                (table_alias.is_some(), "an alias for the INSERT target"),
                (*overwrite, "OVERWRITE"),
                (!assignments.is_empty(), "INSERT .. SET"),
                (partitioned.is_some(), "PARTITION"),
                (!after_columns.is_empty(), "columns after PARTITION"),
                (*has_table_keyword, "INSERT TABLE"),
                (returning.is_some(), "RETURNING"),
                (output.is_some(), "OUTPUT"),
                (priority.is_some(), "an INSERT priority"),
                (insert_alias.is_some(), "an INSERT alias"),
                (settings.is_some(), "SETTINGS"),
                (format_clause.is_some(), "FORMAT"),
                (multi_table_insert_type.is_some(), "a multi-table INSERT"),
                (!multi_table_into_clauses.is_empty(), "a multi-table INSERT"),
                (!multi_table_when_clauses.is_empty(), "a multi-table INSERT"),
                (multi_table_else_clause.is_some(), "a multi-table INSERT"),
            ],
            start,
        )?;
        let upsert = match or {
            None => false,
            Some(SqliteOnConflict::Replace) => true,
            Some(other) => {
                return Err(FirestoreError::unsupported_sql(
                    &format!("INSERT {other}"),
                    start,
                ))
            }
        };

        let TableObject::TableName(name) = table else {
            return Err(FirestoreError::unsupported_sql("this INSERT target", start));
        };
        let collection_path = Self::collection_name(name)?;
        self.collection_path(&collection_path, name_start(name))?;
        let Some(query) = source.as_deref() else {
            return Err(FirestoreError::invalid_sql(
                "INSERT",
                "an INSERT gives its rows with VALUES or SELECT",
                start,
            ));
        };
        let pipeline = match query.body.as_ref() {
            SetExpr::Values(values) => {
                self.insert_values(&collection_path, query, values, columns, start)?
            }
            _ => self.insert_select(query, columns, start)?,
        };
        let target = FirestoreWriteCollection::new(collection_path);
        Ok(if upsert {
            FirestoreSqlStatement::atomic(pipeline, FirestoreUpsertWrite::new().with_target(target))
        } else {
            FirestoreSqlStatement::atomic(pipeline, FirestoreInsertWrite::new().with_target(target))
        })
    }

    /// The rows of a `VALUES` as literals, each a map of the listed columns.
    fn insert_values(
        &mut self,
        collection_path: &str,
        query: &Query,
        values: &Values,
        columns: &[ObjectName],
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestorePipeline> {
        Self::refuse_unmapped_clauses(query)?;
        refuse_set(
            &[
                (query.order_by.is_some(), "ORDER BY"),
                (query.limit_clause.is_some(), "LIMIT"),
            ],
            start,
        )?;
        let Values {
            explicit_row: _,
            value_keyword: _,
            rows,
        } = values;
        if columns.is_empty() {
            return Err(FirestoreError::invalid_sql(
                "INSERT",
                "VALUES gives no field names, so the INSERT lists the columns they belong to",
                start,
            ));
        }
        let names = Self::column_names(columns)?;
        let scope = FirestoreSqlScope::new(FirestoreSqlRelation {
            name: None,
            collection_path: Some(collection_path.to_string()),
        });
        let rows = rows
            .iter()
            .map(|Parens { content, .. }| self.row(&scope, collection_path, &names, content))
            .collect::<FirestoreResult<_>>()?;
        Ok(FirestorePipeline::new(FirestorePipelineSource::Literals(
            FirestoreLiteralsSource::new(rows),
        )))
    }

    /// One row of a `VALUES` as a map. A value is a literal or a parameter, since a literal row
    /// is data and not an expression to evaluate on a document.
    fn row(
        &mut self,
        scope: &FirestoreSqlScope,
        collection_path: &str,
        names: &[String],
        values: &[Expr],
    ) -> FirestoreResult<FirestoreValue> {
        if values.len() != names.len() {
            return Err(FirestoreError::invalid_sql(
                "VALUES",
                format!(
                    "a row has {} values for the {} columns listed",
                    values.len(),
                    names.len()
                ),
                values.first().and_then(expr_start),
            ));
        }
        let mut fields = HashMap::with_capacity(names.len());
        for (name, value) in names.iter().zip(values) {
            let start = expr_start(value);
            let mut expr = self.expr(scope, value)?;
            if name == SQL_DOCUMENT_NAME_FIELD {
                expr = self.document_reference(scope, expr, start)?;
                self.check_in_collection(collection_path, &expr, start)?;
            }
            let FirestoreExpr::Constant(constant) = expr else {
                return Err(FirestoreError::invalid_sql(
                    "VALUES",
                    "a value is a literal or a parameter",
                    start,
                ));
            };
            fields.insert(name.clone(), constant.value);
        }
        Ok(FirestoreValue::from(Value {
            value_type: Some(ValueType::MapValue(MapValue { fields })),
        }))
    }

    /// Fails unless `name` is the reference to a document directly in the collection the `INSERT`
    /// writes to. An insert into a collection takes the ID of a row's `__name__` and no more, so
    /// a name from another collection would write somewhere its path does not say.
    fn check_in_collection(
        &self,
        collection_path: &str,
        name: &FirestoreExpr,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<()> {
        let prefix = format!("{}/{collection_path}/", self.documents_path);
        match name {
            FirestoreExpr::Constant(FirestoreValue {
                value:
                    Value {
                        value_type: Some(ValueType::ReferenceValue(path)),
                    },
            }) if path
                .strip_prefix(&prefix)
                .is_some_and(|id| !id.contains('/')) =>
            {
                Ok(())
            }
            _ => Err(FirestoreError::invalid_sql(
                "__name__",
                format!("the name of a row names a document of {collection_path}, which the INSERT writes to"),
                start,
            )),
        }
    }

    /// The rows of a query, with the fields of its columns named by the column list when there
    /// is one: SQL matches them by position, where a pipeline names them.
    fn insert_select(
        &mut self,
        query: &Query,
        columns: &[ObjectName],
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestorePipeline> {
        let FirestoreSqlBranch {
            mut pipeline,
            columns: selected,
            ..
        } = self.query(query)?;
        if columns.is_empty() {
            return Ok(pipeline);
        }
        let names = Self::column_names(columns)?;
        let Some(selected) = selected else {
            return Err(FirestoreError::invalid_sql(
                "INSERT",
                "a column list needs a SELECT that names its columns, which * does not",
                start,
            ));
        };
        if selected.len() != names.len() {
            return Err(FirestoreError::invalid_sql(
                "INSERT",
                format!(
                    "{} columns are listed and the SELECT outputs {}",
                    names.len(),
                    selected.len()
                ),
                start,
            ));
        }
        if selected != names {
            pipeline
                .stages
                .push(FirestorePipelineStage::Fields(FirestoreFieldsStage::new(
                    selected
                        .iter()
                        .zip(names)
                        .map(|(selected, name)| FirestoreExpr::field(selected).alias(name))
                        .collect(),
                )));
        }
        Ok(pipeline)
    }

    /// The field names of an `INSERT` column list, each a top-level field.
    fn column_names(columns: &[ObjectName]) -> FirestoreResult<Vec<String>> {
        let mut names: Vec<String> = Vec::with_capacity(columns.len());
        for column in columns {
            let start = name_start(column);
            let [ObjectNamePart::Identifier(ident)] = column.0.as_slice() else {
                return Err(FirestoreError::invalid_sql(
                    "INSERT",
                    "a column of an INSERT is the name of one top-level field",
                    start,
                ));
            };
            if is_parameter(ident) {
                return Err(FirestoreError::invalid_sql(
                    &ident.value,
                    "a parameter is a value and cannot name a field",
                    start,
                ));
            }
            if names.contains(&ident.value) {
                return Err(FirestoreError::invalid_sql(
                    "INSERT",
                    format!("the column {} is listed twice", ident.value),
                    start,
                ));
            }
            names.push(ident.value.clone());
        }
        Ok(names)
    }

    pub(super) fn update(&mut self, update: &Update) -> FirestoreResult<FirestoreSqlStatement> {
        let Update {
            update_token,
            optimizer_hints,
            table,
            assignments,
            from,
            selection,
            returning,
            output,
            or,
            order_by,
            limit,
        } = update;
        let start = at(update_token.0.span);
        refuse_set(
            &[
                (!optimizer_hints.is_empty(), "an optimizer hint"),
                (from.is_some(), "FROM"),
                (returning.is_some(), "RETURNING"),
                (output.is_some(), "OUTPUT"),
                (or.is_some(), "UPDATE OR"),
                (!order_by.is_empty(), "ORDER BY"),
                (limit.is_some(), "LIMIT"),
            ],
            start,
        )?;
        let (pipeline, scope) =
            self.written_documents(std::slice::from_ref(table), selection.as_ref(), start)?;
        let transforms = self.assignments(&scope, assignments)?;
        Ok(FirestoreSqlStatement::atomic(
            pipeline,
            FirestoreUpdateWrite::new().with_transforms(transforms),
        ))
    }

    /// The fields an `UPDATE` sets, each named by the path of its target.
    fn assignments(
        &mut self,
        scope: &FirestoreSqlScope,
        assignments: &[Assignment],
    ) -> FirestoreResult<Vec<FirestoreSelection>> {
        let mut paths: Vec<String> = Vec::with_capacity(assignments.len());
        let mut transforms = Vec::with_capacity(assignments.len());
        for Assignment { target, value } in assignments {
            let AssignmentTarget::ColumnName(name) = target else {
                return Err(FirestoreError::unsupported_sql(
                    "a tuple assignment",
                    expr_start(value),
                ));
            };
            let start = name_start(name);
            let parts = name
                .0
                .iter()
                .map(|part| match part {
                    ObjectNamePart::Identifier(ident) => Ok(ident.clone()),
                    ObjectNamePart::Function(_) => {
                        Err(FirestoreError::unsupported_sql("this field name", start))
                    }
                })
                .collect::<FirestoreResult<Vec<Ident>>>()?;
            let path = scope.column(&parts)?;
            if path == SQL_DOCUMENT_NAME_FIELD {
                return Err(FirestoreError::invalid_sql(
                    "__name__",
                    "the name of a document cannot be set",
                    start,
                ));
            }
            if paths
                .iter()
                .any(|set| encloses(set, &path) || encloses(&path, set))
            {
                return Err(FirestoreError::invalid_sql(
                    "SET",
                    format!("{path} is set twice, or inside a field that is set"),
                    start,
                ));
            }
            transforms
                .push(FirestoreSelection::new(self.expr(scope, value)?).with_alias(path.clone()));
            paths.push(path);
        }
        Ok(transforms)
    }

    pub(super) fn delete(&mut self, delete: &Delete) -> FirestoreResult<FirestoreSqlStatement> {
        let Delete {
            delete_token,
            optimizer_hints,
            tables,
            from,
            using,
            selection,
            returning,
            output,
            order_by,
            limit,
        } = delete;
        let start = at(delete_token.0.span);
        refuse_set(
            &[
                (!optimizer_hints.is_empty(), "an optimizer hint"),
                (!tables.is_empty(), "a multi-table DELETE"),
                (using.is_some(), "USING"),
                (returning.is_some(), "RETURNING"),
                (output.is_some(), "OUTPUT"),
                (!order_by.is_empty(), "ORDER BY"),
                (limit.is_some(), "LIMIT"),
            ],
            start,
        )?;
        let (FromTable::WithFromKeyword(sources) | FromTable::WithoutKeyword(sources)) = from;
        let (pipeline, _) = self.written_documents(sources, selection.as_ref(), start)?;
        Ok(FirestoreSqlStatement::atomic(
            pipeline,
            FirestorePipelineWrite::Delete,
        ))
    }

    /// The pipeline that selects the documents an `UPDATE` or `DELETE` changes: the one
    /// collection it names, filtered by its `WHERE`, which it must have so that no statement
    /// changes every document by omission. `WHERE TRUE` says it on purpose.
    fn written_documents(
        &mut self,
        sources: &[TableWithJoins],
        selection: Option<&Expr>,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<(FirestorePipeline, FirestoreSqlScope)> {
        let [TableWithJoins { relation, joins }] = sources else {
            return Err(FirestoreError::invalid_sql(
                "FROM",
                "a write names exactly one collection",
                start,
            ));
        };
        if let Some(join) = joins.first() {
            return Err(FirestoreError::unsupported_sql(
                "JOIN",
                table_start(&join.relation),
            ));
        }
        let (source, relation) = self.table(relation)?;
        let FirestorePipelineSource::Collection(_) = source else {
            return Err(FirestoreError::invalid_sql(
                "a write",
                "a write names a collection, not collection_group(..), documents(..) or database()",
                start,
            ));
        };
        let Some(selection) = selection else {
            return Err(FirestoreError::invalid_sql(
                "WHERE",
                "a write needs a WHERE, and WHERE TRUE selects every document",
                start,
            ));
        };
        let scope = FirestoreSqlScope::new(relation);
        let mut pipeline = FirestorePipeline::new(source);
        let condition = self.expr(&scope, selection)?;
        self.filter(&scope, &mut pipeline, condition, true);
        Ok((pipeline, scope))
    }
}

/// Whether `outer` is `inner`, or a field that holds it: `dims` encloses `dims.w`.
fn encloses(outer: &str, inner: &str) -> bool {
    inner == outer
        || inner
            .strip_prefix(outer)
            .is_some_and(|rest| rest.starts_with('.'))
}
