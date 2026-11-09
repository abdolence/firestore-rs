use super::expr::SQL_DOCUMENT_NAME_FIELD;
use super::query::{table_start, FirestoreSqlBranch};
use super::scope::{FirestoreSqlPlace, FirestoreSqlRelation, FirestoreSqlScope};
use super::translator::{
    at, expr_start, is_parameter, name_start, refuse_set, FirestoreSqlTranslator,
};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::pipeline_expr_builder::FirestorePipelineExprBuilder;
use crate::{
    FirestoreAddFieldsStage, FirestoreBoolExpr, FirestoreCollectionSource,
    FirestoreDefineVariablesStage, FirestoreExpr, FirestoreFieldsStage, FirestoreInsertWrite,
    FirestoreLiteralsSource, FirestorePipeline, FirestorePipelineSource, FirestorePipelineStage,
    FirestorePipelineWrite, FirestorePipelineWriteParams, FirestorePipelineWriteTransaction,
    FirestoreRemoveFieldsStage, FirestoreReplaceWithMode, FirestoreReplaceWithStage,
    FirestoreResult, FirestoreSelection, FirestoreSqlStatement, FirestoreUpdateWrite,
    FirestoreUpsertWrite, FirestoreValue, FirestoreValueType, FirestoreWhereStage,
    FirestoreWriteCollection,
};
use gcloud_sdk::google::firestore::v1::{value::ValueType, MapValue, Value};
use sqlparser::ast::{
    Assignment, AssignmentTarget, ConflictTarget, Delete, DoUpdate, Expr, FromTable, Ident, Insert,
    ObjectName, ObjectNamePart, OnConflict, OnConflictAction, OnInsert, Parens, Query, SetExpr,
    SqliteOnConflict, TableObject, TableWithJoins, Update, Values,
};
use std::collections::{BTreeMap, HashMap};

// Why `MERGE` and `REPLACE INTO` are refused, and the forms that do what they are most often
// written for.
pub(super) const SQL_UPSERT_FORM: &str = "INSERT OR REPLACE INTO replaces the stored document, and INSERT .. ON CONFLICT (__name__) DO UPDATE changes some of its fields";

// The field that holds, during an `ON CONFLICT`, the stored document a row conflicts with.
// Firestore refuses a name between double underscores for a field, so the name is an ordinary
// one that a column of the rows must not take.
const SQL_CONFLICT_STORED_FIELD: &str = "_conflict_stored";

const SQL_CONFLICT_KEY: &str = "conflict_key";

// The fields Firestore adds to the map of a document it reads as a whole, which are not fields
// a write may set.
const SQL_DOCUMENT_METADATA: [&str; 2] = ["__create_time__", "__update_time__"];

struct FirestoreSqlRows {
    pipeline: FirestorePipeline,
    // The names of the fields they hold; `None` for the rows of a `SELECT *`.
    columns: Option<Vec<String>>,
    // Whether they are the literal rows of a `VALUES`, whose `__name__` is known to name a
    // document of the collection written to.
    literal: bool,
}

impl FirestoreSqlRows {
    // Whether the `__name__` of each row is the reference of a document that the pipeline
    // carries over from its source, and not a value it computes or renames. Firestore matches a
    // row to its stored document by that reference, and a string or another field is not one.
    //
    // The pipeline is read from its last stage back to the first one that decides it. A stage
    // this does not know to keep the name answers no.
    fn names_documents(&self) -> bool {
        for stage in self.pipeline.stages.iter().rev() {
            match stage {
                FirestorePipelineStage::Where(_)
                | FirestorePipelineStage::Sort(_)
                | FirestorePipelineStage::Limit(_)
                | FirestorePipelineStage::Offset(_)
                | FirestorePipelineStage::DefineVariables(_) => {}
                FirestorePipelineStage::RemoveFields(stage) => {
                    if stage
                        .fields
                        .iter()
                        .any(|field| field == SQL_DOCUMENT_NAME_FIELD)
                    {
                        return false;
                    }
                }
                FirestorePipelineStage::AddFields(stage) => {
                    if stage.fields.iter().any(|selection| {
                        selection.alias.as_deref() == Some(SQL_DOCUMENT_NAME_FIELD)
                    }) {
                        return false;
                    }
                }
                FirestorePipelineStage::Fields(stage) => {
                    return stage.fields.iter().any(|selection| {
                        match (&selection.expr, selection.alias.as_deref()) {
                            (FirestoreExpr::Field(path), None | Some(SQL_DOCUMENT_NAME_FIELD)) => {
                                path == SQL_DOCUMENT_NAME_FIELD
                                    || path
                                        .strip_suffix(SQL_DOCUMENT_NAME_FIELD)
                                        .is_some_and(|rest| rest.ends_with('.'))
                            }
                            _ => false,
                        }
                    });
                }
                _ => return false,
            }
        }
        matches!(
            self.pipeline.source,
            FirestorePipelineSource::Collection(_)
                | FirestorePipelineSource::CollectionGroup(_)
                | FirestorePipelineSource::Documents(_)
        )
    }
}

struct FirestoreSqlAssignment {
    path: String,
    names: Vec<String>,
    value: FirestoreExpr,
}

enum FirestoreSqlSet {
    Value(FirestoreExpr),
    Nested(BTreeMap<String, FirestoreSqlSet>),
}

impl FirestoreSqlStatement {
    // A write as the statement that runs it. Every SQL write is atomic: it applies in full or
    // not at all, and Firestore requires that of an upsert and of an insert from a query.
    fn atomic(pipeline: FirestorePipeline, write: impl Into<FirestorePipelineWrite>) -> Self {
        Self::Write(Box::new(
            FirestorePipelineWriteParams::new(pipeline, write.into())
                .with_transaction(FirestorePipelineWriteTransaction::Atomic),
        ))
    }
}

impl FirestoreSqlTranslator<'_> {
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
            by_name,
            overriding,
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
        if *replace_into {
            return Err(FirestoreError::invalid_sql(
                "REPLACE INTO",
                format!("REPLACE INTO has no pipeline form in SQL: {SQL_UPSERT_FORM}"),
                start,
            ));
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
                (*by_name, "INSERT BY NAME"),
                (overriding.is_some(), "OVERRIDING"),
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
        if upsert && on.is_some() {
            return Err(FirestoreError::invalid_sql(
                "ON CONFLICT",
                "INSERT OR REPLACE replaces the stored document, which leaves no conflict to resolve",
                start,
            ));
        }

        let TableObject::TableName(name) = table else {
            return Err(FirestoreError::unsupported_sql("this INSERT target", start));
        };
        let collection_path = Self::collection_name(name)?;
        let stored_in = self.collection_source(&collection_path, name_start(name))?;
        let Some(query) = source.as_deref() else {
            return Err(FirestoreError::invalid_sql(
                "INSERT",
                "an INSERT gives its rows with VALUES or SELECT",
                start,
            ));
        };
        let rows = match query.body.as_ref() {
            SetExpr::Values(values) => {
                self.insert_values(&collection_path, query, values, columns, start)?
            }
            _ => self.insert_select(query, columns, start)?,
        };
        let (pipeline, upsert) = match on {
            None => (rows.pipeline, upsert),
            Some(on) => self.on_conflict(on, rows, &collection_path, stored_in, start)?,
        };
        let target = FirestoreWriteCollection::new(collection_path);
        Ok(if upsert {
            FirestoreSqlStatement::atomic(pipeline, FirestoreUpsertWrite::new().with_target(target))
        } else {
            FirestoreSqlStatement::atomic(pipeline, FirestoreInsertWrite::new().with_target(target))
        })
    }

    fn insert_values(
        &mut self,
        collection_path: &str,
        query: &Query,
        values: &Values,
        columns: &[ObjectName],
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreSqlRows> {
        Self::refuse_unmapped_clauses(query)?;
        refuse_set(
            &[
                (query.with.is_some(), "WITH"),
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
        Ok(FirestoreSqlRows {
            pipeline: FirestorePipeline::new(FirestorePipelineSource::Literals(
                FirestoreLiteralsSource::new(rows),
            )),
            columns: Some(names),
            literal: true,
        })
    }

    // One row of a `VALUES` as a map. A value is a literal or a parameter, since a literal row
    // is data and not an expression to evaluate on a document.
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
                expr = self.document_reference(
                    scope,
                    &FirestoreExpr::field(SQL_DOCUMENT_NAME_FIELD),
                    expr,
                    start,
                )?;
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

    // Fails unless `name` is the reference to a document directly in the collection the `INSERT`
    // writes to. An insert into a collection takes the ID of a row's `__name__` and no more, so
    // a name from another collection would write somewhere its path does not say.
    fn check_in_collection(
        &self,
        collection_path: &str,
        name: &FirestoreExpr,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<()> {
        let prefix = format!("{}/{collection_path}/", self.documents_path);
        match name.constant_value() {
            Some(ValueType::ReferenceValue(path))
                if path
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

    // The rows of a query, with the fields of its columns named by the column list when there
    // is one: SQL matches them by position, where a pipeline names them.
    fn insert_select(
        &mut self,
        query: &Query,
        columns: &[ObjectName],
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreSqlRows> {
        let FirestoreSqlBranch {
            mut pipeline,
            columns: selected,
            ..
        } = self.query(query, FirestoreSqlPlace::top())?;
        if columns.is_empty() {
            return Ok(FirestoreSqlRows {
                pipeline,
                columns: selected,
                literal: false,
            });
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
                        .zip(names.iter())
                        .map(|(selected, name)| FirestoreExpr::field(selected).alias(name))
                        .collect(),
                )));
        }
        Ok(FirestoreSqlRows {
            pipeline,
            columns: Some(names),
            literal: false,
        })
    }

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
                return Err(FirestoreError::sql_parameter_names_field(
                    &ident.value,
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
            on_cluster,
            assignments,
            from,
            selection,
            returning,
            output,
            or,
            order_by,
            limit,
            settings,
        } = update;
        let start = at(update_token.0.span);
        refuse_set(
            &[
                (!optimizer_hints.is_empty(), "an optimizer hint"),
                (on_cluster.is_some(), "ON CLUSTER"),
                (from.is_some(), "FROM"),
                (returning.is_some(), "RETURNING"),
                (output.is_some(), "OUTPUT"),
                (or.is_some(), "UPDATE OR"),
                (!order_by.is_empty(), "ORDER BY"),
                (limit.is_some(), "LIMIT"),
                (settings.is_some(), "SETTINGS"),
            ],
            start,
        )?;
        let (pipeline, scope) =
            self.written_documents(std::slice::from_ref(table), selection.as_ref(), start)?;
        let transforms = self
            .assignments(&scope, &scope, assignments)?
            .into_iter()
            .map(|FirestoreSqlAssignment { path, value, .. }| {
                FirestoreSelection::new(value).with_alias(path)
            })
            .collect();
        Ok(FirestoreSqlStatement::atomic(
            pipeline,
            FirestoreUpdateWrite::new().with_transforms(transforms),
        ))
    }

    fn assignments(
        &mut self,
        targets: &FirestoreSqlScope,
        values: &FirestoreSqlScope,
        assignments: &[Assignment],
    ) -> FirestoreResult<Vec<FirestoreSqlAssignment>> {
        let mut set: Vec<FirestoreSqlAssignment> = Vec::with_capacity(assignments.len());
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
            let path = targets.field(&parts)?;
            if path == SQL_DOCUMENT_NAME_FIELD {
                return Err(FirestoreError::invalid_sql(
                    "__name__",
                    "the name of a document cannot be set",
                    start,
                ));
            }
            if set
                .iter()
                .any(|set| encloses(&set.path, &path) || encloses(&path, &set.path))
            {
                return Err(FirestoreError::invalid_sql(
                    "SET",
                    format!("{path} is set twice, or inside a field that is set"),
                    start,
                ));
            }
            set.push(FirestoreSqlAssignment {
                names: targets
                    .field_names(&parts)
                    .iter()
                    .map(|part| part.value.clone())
                    .collect(),
                value: self.expr(values, value)?,
                path,
            });
        }
        Ok(set)
    }

    // Fails unless every row of `rows` can be matched with the stored document it conflicts
    // with, so that no row is written over a document it did not find.
    fn check_conflict(
        &self,
        conflict_target: Option<&ConflictTarget>,
        action: &OnConflictAction,
        rows: &FirestoreSqlRows,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<()> {
        if let Some(ConflictTarget::Columns {
            predicate: Some(predicate),
            ..
        }) = conflict_target
        {
            return Err(FirestoreError::unsupported_sql(
                "WHERE in an ON CONFLICT target",
                expr_start(predicate),
            ));
        }
        let on_name = match conflict_target {
            Some(ConflictTarget::Columns { columns, .. }) => {
                matches!(columns.as_slice(), [column] if column.value == SQL_DOCUMENT_NAME_FIELD)
            }
            Some(ConflictTarget::OnConstraint(_)) => false,
            None => matches!(action, OnConflictAction::DoNothing),
        };
        if !on_name {
            return Err(FirestoreError::invalid_sql(
                "ON CONFLICT",
                "a document conflicts on its name, the only unique key it has: write ON CONFLICT (__name__)",
                start,
            ));
        }
        if rows.columns.as_ref().is_some_and(|columns| {
            !columns
                .iter()
                .any(|column| column == SQL_DOCUMENT_NAME_FIELD)
        }) {
            return Err(FirestoreError::invalid_sql(
                "ON CONFLICT",
                "the rows give no __name__, so each gets a new name and none can conflict",
                start,
            ));
        }
        if !rows.literal && !rows.names_documents() {
            return Err(FirestoreError::invalid_sql(
                "ON CONFLICT",
                "the rows of a SELECT must carry the __name__ of a document: select __name__ itself, or * from a collection; a computed or renamed value cannot be told to be a document reference",
                start,
            ));
        }
        if let Some(column) = rows
            .columns
            .iter()
            .flatten()
            .find(|column| *column == SQL_CONFLICT_STORED_FIELD)
        {
            return Err(FirestoreError::invalid_sql(
                column,
                "ON CONFLICT keeps the stored document of a row under this name, so a column cannot take it",
                start,
            ));
        }
        if !rows.literal && !self.options.subqueries {
            return Err(FirestoreError::invalid_sql(
                "ON CONFLICT",
                "finding the stored document of each row of a SELECT scans the collection once per row, which FirestoreSqlOptions::allow_subqueries() allows",
                start,
            ));
        }
        Ok(())
    }

    // The rows of an `INSERT` with the conflicts with stored documents resolved, and whether
    // the pipeline ends in an upsert.
    //
    // Each row looks its stored document up by the name of the document it would write. A row
    // whose document exists is left out for `DO NOTHING`. For `DO UPDATE` it becomes the stored
    // document with the `SET` fields set, and a row without a stored document stays as it is;
    // either is then written by an upsert, since Firestore has no write that merges.
    fn on_conflict(
        &mut self,
        on: &OnInsert,
        rows: FirestoreSqlRows,
        collection_path: &str,
        stored_in: FirestoreCollectionSource,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<(FirestorePipeline, bool)> {
        let OnInsert::OnConflict(OnConflict {
            conflict_target,
            action,
        }) = on
        else {
            return Err(FirestoreError::unsupported_sql(
                "ON DUPLICATE KEY UPDATE",
                start,
            ));
        };
        self.check_conflict(conflict_target.as_ref(), action, &rows, start)?;

        let e = FirestorePipelineExprBuilder;
        let name = FirestoreExpr::field(SQL_DOCUMENT_NAME_FIELD);
        // The point lookup of a name needs the reference itself. A row of a SELECT may name a
        // document of another collection, so it is matched by its ID, which scans.
        let (key, stored_name) = if rows.literal {
            (name.clone(), name)
        } else {
            (name.clone().document_id(), name.document_id())
        };
        let relation = || FirestoreSqlRelation {
            name: Some(stored_in.collection_id.clone()),
            collection_path: Some(collection_path.to_string()),
        };
        let lookup = |found: FirestoreSelection| {
            FirestoreExpr::ScalarSubquery(
                FirestorePipeline::new(FirestorePipelineSource::Collection(stored_in.clone()))
                    .with_stage(FirestorePipelineStage::Where(FirestoreWhereStage::new(
                        stored_name.clone().is_in(
                            e.array([FirestoreExpr::Variable(SQL_CONFLICT_KEY.to_string())]),
                        ),
                    )))
                    .with_stage(FirestorePipelineStage::Fields(FirestoreFieldsStage::new(
                        vec![found],
                    ))),
            )
        };
        let stored = || FirestoreExpr::field(SQL_CONFLICT_STORED_FIELD);

        let mut pipeline = rows.pipeline;
        pipeline
            .stages
            .push(FirestorePipelineStage::DefineVariables(
                FirestoreDefineVariablesStage::new(vec![key.alias(SQL_CONFLICT_KEY)]),
            ));
        match action {
            OnConflictAction::DoNothing => {
                pipeline.stages.extend([
                    FirestorePipelineStage::AddFields(FirestoreAddFieldsStage::new(vec![lookup(
                        FirestoreExpr::field(SQL_DOCUMENT_NAME_FIELD).alias("found"),
                    )
                    .alias(SQL_CONFLICT_STORED_FIELD)])),
                    FirestorePipelineStage::Where(FirestoreWhereStage::new(stored().is_null())),
                    FirestorePipelineStage::RemoveFields(FirestoreRemoveFieldsStage::new(vec![
                        SQL_CONFLICT_STORED_FIELD.to_string(),
                    ])),
                ]);
                Ok((pipeline, false))
            }
            OnConflictAction::DoUpdate(DoUpdate {
                assignments,
                selection,
            }) => {
                let targets = FirestoreSqlScope::new(relation());
                let values =
                    FirestoreSqlScope::new(relation()).with_stored(SQL_CONFLICT_STORED_FIELD);
                let assignments = self.assignments(&targets, &values, assignments)?;
                let condition = selection
                    .as_ref()
                    .map(|selection| self.expr(&values, selection))
                    .transpose()?;

                pipeline.stages.push(FirestorePipelineStage::AddFields(
                    FirestoreAddFieldsStage::new(vec![lookup(
                        e.current_document().alias("document"),
                    )
                    .alias(SQL_CONFLICT_STORED_FIELD)]),
                ));
                if let Some(condition) = condition {
                    pipeline
                        .stages
                        .push(FirestorePipelineStage::Where(FirestoreWhereStage::new(
                            e.or([stored().is_null(), FirestoreBoolExpr::unchecked(condition)]),
                        )));
                }
                let mut set = BTreeMap::new();
                for FirestoreSqlAssignment { names, value, .. } in assignments {
                    FirestoreSqlSet::place(&mut set, &names, value);
                }
                let updated = FirestoreSqlSet::apply(
                    stored().map_remove(SQL_DOCUMENT_METADATA),
                    &stored(),
                    set,
                );
                let fresh = e.current_document().map_remove(
                    std::iter::once(SQL_CONFLICT_STORED_FIELD).chain(SQL_DOCUMENT_METADATA),
                );
                pipeline.stages.push(FirestorePipelineStage::ReplaceWith(
                    FirestoreReplaceWithStage::new(
                        e.conditional(stored().is_null(), fresh, updated),
                        FirestoreReplaceWithMode::FullReplace,
                    ),
                ));
                Ok((pipeline, true))
            }
        }
    }

    pub(super) fn delete(&mut self, delete: &Delete) -> FirestoreResult<FirestoreSqlStatement> {
        let Delete {
            delete_token,
            optimizer_hints,
            tables,
            from,
            on_cluster,
            using,
            selection,
            returning,
            output,
            order_by,
            limit,
            settings,
        } = delete;
        let start = at(delete_token.0.span);
        refuse_set(
            &[
                (!optimizer_hints.is_empty(), "an optimizer hint"),
                (!tables.is_empty(), "a multi-table DELETE"),
                (on_cluster.is_some(), "ON CLUSTER"),
                (using.is_some(), "USING"),
                (returning.is_some(), "RETURNING"),
                (output.is_some(), "OUTPUT"),
                (!order_by.is_empty(), "ORDER BY"),
                (limit.is_some(), "LIMIT"),
                (settings.is_some(), "SETTINGS"),
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

    // The pipeline that selects the documents an `UPDATE` or `DELETE` changes: the one
    // collection it names, filtered by its `WHERE`, which it must have so that no statement
    // changes every document by omission. `WHERE TRUE` says it on purpose.
    fn written_documents(
        &mut self,
        sources: &[TableWithJoins],
        selection: Option<&Expr>,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<(FirestorePipeline, FirestoreSqlScope<'static>)> {
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
        let (mut pipeline, relation) = self.table(relation, FirestoreSqlPlace::top(), false)?;
        if matches!(
            pipeline.stages.first(),
            Some(FirestorePipelineStage::Sample(_))
        ) {
            return Err(FirestoreError::unsupported_sql("TABLESAMPLE", start));
        }
        let (FirestorePipelineSource::Collection(_), true) =
            (&pipeline.source, pipeline.stages.is_empty())
        else {
            return Err(FirestoreError::invalid_sql(
                "a write",
                "a write names a collection, not a derived table, collection_group(..), documents(..) or database()",
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
        let condition = self.expr(&scope, selection)?;
        self.filter(&scope, &mut pipeline, condition);
        Ok((pipeline, scope))
    }
}

fn encloses(outer: &str, inner: &str) -> bool {
    inner == outer
        || inner
            .strip_prefix(outer)
            .is_some_and(|rest| rest.starts_with('.'))
}

impl FirestoreSqlSet {
    // Records that `value` is assigned to the field at `names`, under the maps that hold it.
    // Two assignments never share a field or one inside the other: `assignments` refuses that.
    fn place(entries: &mut BTreeMap<String, Self>, names: &[String], value: FirestoreExpr) {
        match names {
            [name] => {
                entries.insert(name.clone(), Self::Value(value));
            }
            [name, rest @ ..] => {
                if let Self::Nested(nested) = entries
                    .entry(name.clone())
                    .or_insert_with(|| Self::Nested(BTreeMap::new()))
                {
                    Self::place(nested, rest, value);
                }
            }
            [] => {}
        }
    }

    // `map` with the assigned fields set. A map that holds assigned fields starts from the map
    // of the same name in `read`, or from an empty one when `read` has none, as `UPDATE` does
    // when it sets `a.b` and `a` is missing or is not a map.
    fn apply(
        map: FirestoreExpr,
        read: &FirestoreExpr,
        entries: BTreeMap<String, Self>,
    ) -> FirestoreExpr {
        let e = FirestorePipelineExprBuilder;
        map.map_set(entries.into_iter().map(|(name, set)| {
            let value = match set {
                Self::Value(value) => value,
                Self::Nested(nested) => {
                    let current = read.clone().map_get(name.clone());
                    let current = e.conditional(
                        current.clone().is_type(FirestoreValueType::Map),
                        current,
                        e.map(Vec::<(String, FirestoreExpr)>::new()),
                    );
                    Self::apply(current.clone(), &current, nested)
                }
            };
            (name, value)
        }))
    }
}
