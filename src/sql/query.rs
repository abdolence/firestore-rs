use super::aggregation::FirestoreSqlAggregation;
use super::functions::names_its_output;
use super::scope::{FirestoreSqlRelation, FirestoreSqlScope};
use super::translator::{
    at, expr_start, is_column, is_parameter, name_start, refuse_set, FirestoreSqlTranslator,
};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::pipeline_expr_builder::FirestorePipelineExprBuilder;
use crate::{
    FirestoreAddFieldsStage, FirestoreBoolExpr, FirestoreCollectionGroupSource,
    FirestoreCollectionSource, FirestoreDocumentsSource, FirestoreExpr, FirestoreFieldsStage,
    FirestoreFunction, FirestoreLimitStage, FirestoreOffsetStage, FirestorePipeline,
    FirestorePipelineOrdering, FirestorePipelineSource, FirestorePipelineStage,
    FirestoreQueryDirection, FirestoreRemoveFieldsStage, FirestoreResult, FirestoreSelection,
    FirestoreSortStage, FirestoreValue, FirestoreWhereStage,
};
use gcloud_sdk::google::firestore::v1::{value::ValueType, ArrayValue, Value};
use sqlparser::ast::{
    Distinct, ExceptSelectItem, Expr, FunctionArg, LimitClause, ObjectName, ObjectNamePart, Offset,
    OrderBy, OrderByExpr, OrderByKind, OrderByOptions, OrderBySort, Query, Select, SelectFlavor,
    SelectItem, SelectItemQualifiedWildcardKind, SetExpr, TableAlias, TableFactor,
    TableFunctionArgs, TableWithJoins, Value as SqlValue, ValueWithSpan, WildcardAdditionalOptions,
};

/// A `SELECT` as a pipeline.
pub(super) struct FirestoreSqlBranch {
    pub(super) pipeline: FirestorePipeline,
    /// The names of the columns it outputs, or `None` for `*`, which lists none.
    pub(super) columns: Option<Vec<String>>,
    pub(super) start: Option<FirestoreSqlLocation>,
}

/// What a query outputs for each row.
pub(super) enum FirestoreSqlProjection {
    /// Every field, as `*` selects them.
    Wildcard(FirestoreSqlWildcard),
    /// Only the selected fields.
    Fields(Vec<FirestoreSelection>),
}

/// `*` less the fields `EXCEPT` removes, plus the query's other selected items.
pub(super) struct FirestoreSqlWildcard {
    removed: Vec<String>,
    added: Vec<FirestoreSelection>,
}

impl FirestoreSqlProjection {
    fn selections(&self) -> &[FirestoreSelection] {
        match self {
            Self::Wildcard(wildcard) => &wildcard.added,
            Self::Fields(fields) => fields,
        }
    }

    /// The expression of the selected item named `alias`, which `ORDER BY` may order by.
    fn aliased(&self, alias: &str) -> Option<&FirestoreExpr> {
        self.selections()
            .iter()
            .find(|selection| selection.alias.as_deref() == Some(alias))
            .map(|selection| &selection.expr)
    }

    /// The names of the output columns, or `None` for a `*`.
    fn columns(&self) -> Option<Vec<String>> {
        match self {
            Self::Wildcard(_) => None,
            Self::Fields(fields) => fields
                .iter()
                .map(|selection| match (&selection.alias, &selection.expr) {
                    (Some(alias), _) => Some(alias.clone()),
                    (None, FirestoreExpr::Field(path)) => Some(path.clone()),
                    (None, _) => None,
                })
                .collect(),
        }
    }

    fn into_stages(self) -> Vec<FirestorePipelineStage> {
        match self {
            Self::Wildcard(FirestoreSqlWildcard { removed, added }) => {
                let mut stages = Vec::new();
                if !removed.is_empty() {
                    stages.push(FirestorePipelineStage::RemoveFields(
                        FirestoreRemoveFieldsStage::new(removed),
                    ));
                }
                if !added.is_empty() {
                    stages.push(FirestorePipelineStage::AddFields(
                        FirestoreAddFieldsStage::new(
                            added
                                .into_iter()
                                .map(FirestoreSelection::without_redundant_alias)
                                .collect(),
                        ),
                    ));
                }
                stages
            }
            Self::Fields(fields) => {
                vec![FirestorePipelineStage::Fields(FirestoreFieldsStage::new(
                    fields
                        .into_iter()
                        .map(FirestoreSelection::without_redundant_alias)
                        .collect(),
                ))]
            }
        }
    }
}

impl FirestoreSqlTranslator<'_> {
    /// A query as the statement, with the options' row cap applied.
    pub(super) fn top_level_query(&mut self, query: &Query) -> FirestoreResult<FirestorePipeline> {
        let mut pipeline = self.query(query)?;
        if let Some(max_rows) = self.options.max_rows {
            match pipeline.stages.last_mut() {
                Some(FirestorePipelineStage::Limit(limit)) => {
                    limit.limit = limit.limit.min(max_rows)
                }
                _ => pipeline
                    .stages
                    .push(FirestorePipelineStage::Limit(FirestoreLimitStage::new(
                        max_rows,
                    ))),
            }
        }
        Ok(pipeline)
    }

    fn query(&mut self, query: &Query) -> FirestoreResult<FirestorePipeline> {
        let Query {
            with,
            body,
            order_by,
            limit_clause,
            fetch,
            locks,
            for_clause,
            settings,
            format_clause,
            pipe_operators,
        } = query;
        if let Some(with) = with {
            return Err(FirestoreError::unsupported_sql(
                "WITH",
                at(with.with_token.0.span),
            ));
        }
        refuse_set(
            &[
                (!pipe_operators.is_empty(), "pipe syntax"),
                (fetch.is_some(), "FETCH"),
                (!locks.is_empty(), "FOR UPDATE or FOR SHARE"),
                (for_clause.is_some(), "FOR XML or FOR JSON"),
                (settings.is_some(), "SETTINGS"),
                (format_clause.is_some(), "FORMAT"),
            ],
            None,
        )?;
        let mut pipeline = match body.as_ref() {
            SetExpr::Select(select) => self.select(select, order_by.as_ref())?.pipeline,
            SetExpr::SetOperation { .. } => self.set_operation(body, order_by.as_ref())?,
            other => return Err(Self::unsupported_body(other)),
        };
        if let Some(limit_clause) = limit_clause {
            self.limit(limit_clause, &mut pipeline)?;
        }
        Ok(pipeline)
    }

    /// The error for a query body that is neither a `SELECT` nor a `UNION`.
    pub(super) fn unsupported_body(body: &SetExpr) -> FirestoreError {
        let construct = match body {
            SetExpr::Select(_) | SetExpr::SetOperation { .. } => "a nested set operation",
            SetExpr::Query(_) => "a parenthesised query",
            SetExpr::Values(_) => "VALUES",
            SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_) => {
                "a write in a query"
            }
            SetExpr::Table(_) => "TABLE",
        };
        FirestoreError::unsupported_sql(construct, None)
    }

    /// A `SELECT` with the `ORDER BY` of its query, which sorts before the projection, so that
    /// Firestore can read an index in order rather than sort the projected rows. A query that
    /// groups sorts its groups, after the aggregate stage.
    ///
    /// A `SELECT` nested in this one translates with a state of its own.
    pub(super) fn select(
        &mut self,
        select: &Select,
        order_by: Option<&OrderBy>,
    ) -> FirestoreResult<FirestoreSqlBranch> {
        let outer = (self.aggregation.take(), self.search.take());
        let branch = self.select_with_state(select, order_by);
        (self.aggregation, self.search) = outer;
        branch
    }

    fn select_with_state(
        &mut self,
        select: &Select,
        order_by: Option<&OrderBy>,
    ) -> FirestoreResult<FirestoreSqlBranch> {
        let Select {
            select_token,
            optimizer_hints,
            distinct,
            select_modifiers,
            top,
            top_before_distinct: _,
            projection,
            exclude,
            into,
            from,
            lateral_views,
            prewhere,
            selection,
            connect_by,
            group_by,
            cluster_by,
            distribute_by,
            sort_by,
            having,
            named_window,
            qualify,
            window_before_qualify: _,
            value_table_mode,
            flavor,
        } = select;
        let select_start = at(select_token.0.span);
        let distinct = match distinct {
            None | Some(Distinct::All) => false,
            Some(Distinct::Distinct) => true,
            Some(Distinct::On(_)) => {
                return Err(FirestoreError::unsupported_sql("DISTINCT ON", select_start))
            }
        };
        refuse_set(
            &[
                (!optimizer_hints.is_empty(), "an optimizer hint"),
                (select_modifiers.is_some(), "a SELECT modifier"),
                (top.is_some(), "TOP"),
                (exclude.is_some(), "EXCLUDE"),
                (into.is_some(), "INTO"),
                (!lateral_views.is_empty(), "LATERAL VIEW"),
                (prewhere.is_some(), "PREWHERE"),
                (!connect_by.is_empty(), "CONNECT BY"),
                (!cluster_by.is_empty(), "CLUSTER BY"),
                (!distribute_by.is_empty(), "DISTRIBUTE BY"),
                (!sort_by.is_empty(), "SORT BY"),
                (!named_window.is_empty(), "WINDOW"),
                (qualify.is_some(), "QUALIFY"),
                (
                    value_table_mode.is_some(),
                    "SELECT AS STRUCT or SELECT AS VALUE",
                ),
                (
                    *flavor != SelectFlavor::Standard,
                    "a query that starts with FROM",
                ),
            ],
            select_start,
        )?;

        let (source, relation) = self.from(from, select_start)?;
        let scope = FirestoreSqlScope::new(relation);
        let mut pipeline = FirestorePipeline::new(source);
        if let Some(selection) = selection {
            let (search, condition) = self.split_search(&scope, &pipeline.source, selection)?;
            self.search = search;
            if let Some(condition) = condition {
                let paths = match self.search {
                    None => self.document_reads(&scope, &pipeline.source, &condition),
                    Some(_) => None,
                };
                match paths {
                    Some(paths) => {
                        pipeline.source =
                            FirestorePipelineSource::Documents(FirestoreDocumentsSource::new(paths))
                    }
                    None => pipeline.stages.push(FirestorePipelineStage::Where(
                        FirestoreWhereStage::new(FirestoreBoolExpr::unchecked(condition)),
                    )),
                }
            }
        }

        let mut aggregation =
            FirestoreSqlAggregation::new(projection.iter().filter_map(|item| match item {
                SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                _ => None,
            }));
        self.group_keys(&scope, group_by, projection, distinct, &mut aggregation)?;
        self.aggregation = Some(aggregation);
        let projection = self.projection(&scope, projection)?;
        if let Some(aggregation) = self.aggregation.as_mut().filter(|a| a.is_aggregate()) {
            aggregation.set_outputs(projection.selections());
        }
        let having = having
            .as_ref()
            .map(|having| self.expr(&scope, having))
            .transpose()?;
        let ordered_by_search = match order_by {
            Some(order_by) => self.search_order(&scope, order_by)?,
            None => false,
        };
        let orderings = match order_by {
            Some(order_by) if !ordered_by_search => {
                Some(self.orderings(&scope, &projection, order_by)?)
            }
            _ => None,
        };
        if let Some(search) = self.search.take() {
            Self::install_search(&mut pipeline.source, search);
        }

        let columns = projection.columns();
        match self.aggregation.take().filter(|a| a.is_aggregate()) {
            Some(aggregation) => {
                aggregation.ensure_grouped()?;
                let FirestoreSqlProjection::Fields(selections) = projection else {
                    return Err(FirestoreError::invalid_sql(
                        "*",
                        "* selects every field, where a query that groups or aggregates selects only its groups and aggregates",
                        select_start,
                    ));
                };
                pipeline
                    .stages
                    .extend(aggregation.into_stages(having, orderings, selections));
            }
            None => {
                if having.is_some() {
                    return Err(FirestoreError::invalid_sql(
                        "HAVING",
                        "HAVING filters groups, and this query has no GROUP BY or aggregate",
                        select_start,
                    ));
                }
                if let Some(orderings) = orderings {
                    pipeline
                        .stages
                        .push(FirestorePipelineStage::Sort(FirestoreSortStage::new(
                            orderings,
                        )));
                }
                pipeline.stages.extend(projection.into_stages());
            }
        }
        Ok(FirestoreSqlBranch {
            pipeline,
            columns,
            start: select_start,
        })
    }

    /// The source a query reads and the relation its names resolve against.
    fn from(
        &mut self,
        from: &[TableWithJoins],
        select_start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<(FirestorePipelineSource, FirestoreSqlRelation)> {
        let (relation, joins) = match from {
            [TableWithJoins { relation, joins }] => (relation, joins),
            [] => {
                return Err(FirestoreError::invalid_sql(
                    "FROM",
                    "a query reads from a source, and this one has no FROM",
                    select_start,
                ))
            }
            [_, joined, ..] => return Err(self.join_refused(table_start(&joined.relation))),
        };
        if let Some(join) = joins.first() {
            return Err(self.join_refused(table_start(&join.relation)));
        }
        self.table(relation)
    }

    /// The error for a join, which runs one subquery per row of the query it is in.
    fn join_refused(&self, start: Option<FirestoreSqlLocation>) -> FirestoreError {
        if self.options.subqueries {
            FirestoreError::unsupported_sql("JOIN", start)
        } else {
            FirestoreError::invalid_sql(
                "JOIN",
                "a join runs one subquery per row, which FirestoreSqlOptions::allow_subqueries() allows",
                start,
            )
        }
    }

    fn table(
        &mut self,
        factor: &TableFactor,
    ) -> FirestoreResult<(FirestorePipelineSource, FirestoreSqlRelation)> {
        let TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } = factor
        else {
            let construct = match factor {
                TableFactor::Derived { .. } => "a subquery in FROM",
                TableFactor::UNNEST { .. } => "UNNEST",
                TableFactor::NestedJoin { .. } => "JOIN",
                _ => "this source",
            };
            return Err(FirestoreError::unsupported_sql(
                construct,
                table_start(factor),
            ));
        };
        let start = name_start(name);
        refuse_set(
            &[
                (!with_hints.is_empty(), "a table hint"),
                (version.is_some(), "FOR SYSTEM_TIME AS OF"),
                (*with_ordinality, "WITH ORDINALITY"),
                (!partitions.is_empty(), "PARTITION"),
                (json_path.is_some(), "a JSON path"),
                (sample.is_some(), "TABLESAMPLE"),
                (!index_hints.is_empty(), "an index hint"),
            ],
            start,
        )?;
        let alias = alias.as_ref().map(Self::alias).transpose()?;
        match args {
            None => self.collection(name, alias),
            Some(TableFunctionArgs { args, settings }) => {
                if settings.is_some() {
                    return Err(FirestoreError::unsupported_sql("SETTINGS", start));
                }
                self.table_function(name, args, alias)
            }
        }
    }

    fn alias(alias: &TableAlias) -> FirestoreResult<String> {
        let TableAlias {
            explicit: _,
            name,
            columns,
            at: alias_at,
        } = alias;
        if !columns.is_empty() || alias_at.is_some() {
            return Err(FirestoreError::unsupported_sql(
                "an alias with columns",
                at(name.span),
            ));
        }
        Ok(name.value.clone())
    }

    /// A collection by its ID, or by its path as `` `restaurants/r1/reviews` ``.
    fn collection(
        &mut self,
        name: &ObjectName,
        alias: Option<String>,
    ) -> FirestoreResult<(FirestorePipelineSource, FirestoreSqlRelation)> {
        let start = name_start(name);
        let mut parts = Vec::with_capacity(name.0.len());
        for part in &name.0 {
            match part {
                ObjectNamePart::Identifier(ident) if is_parameter(ident) => {
                    return Err(FirestoreError::invalid_sql(
                        &ident.value,
                        "a parameter is a value and cannot name a collection",
                        at(ident.span),
                    ))
                }
                ObjectNamePart::Identifier(ident) => parts.push(ident.value.as_str()),
                ObjectNamePart::Function(_) => {
                    return Err(FirestoreError::unsupported_sql(
                        "this collection name",
                        start,
                    ))
                }
            }
        }
        // sqlparser splits a backticked name at its dots, which a Firestore ID may hold.
        let path = parts.join(".");
        let (parent, collection_id) = self.collection_path(&path, start)?;
        let source = FirestoreCollectionSource::new(collection_id.to_string())
            .opt_parent(parent.map(|parent| format!("{}/{parent}", self.documents_path)));
        let name = alias.unwrap_or_else(|| collection_id.to_string());
        let relation = FirestoreSqlRelation {
            name: Some(name),
            collection_path: Some(path),
        };
        Ok((FirestorePipelineSource::Collection(source), relation))
    }

    /// `collection_group('id')`, `documents('path', ..)` or `database()`.
    fn table_function(
        &mut self,
        name: &ObjectName,
        args: &[FunctionArg],
        alias: Option<String>,
    ) -> FirestoreResult<(FirestorePipelineSource, FirestoreSqlRelation)> {
        let start = name_start(name);
        let function = Self::function_name(name, start)?;
        let arguments = Self::positional_arguments(args, start)?
            .into_iter()
            .map(|argument| {
                string_literal(argument).ok_or_else(|| {
                    FirestoreError::invalid_sql(
                        &function,
                        format!(
                            "{function}(..) takes string literals: a source is never a parameter"
                        ),
                        expr_start(argument),
                    )
                })
            })
            .collect::<FirestoreResult<Vec<_>>>()?;
        let source = match (function.as_str(), arguments.as_slice()) {
            ("collection_group", [collection_id]) => {
                if !self.options.collection_groups {
                    return Err(FirestoreError::invalid_sql(
                        "collection_group",
                        "the options refuse collection groups",
                        start,
                    ));
                }
                self.check_collection(collection_id, start)?;
                let relation = FirestoreSqlRelation {
                    name: alias.or_else(|| Some(collection_id.to_string())),
                    collection_path: None,
                };
                return Ok((
                    FirestorePipelineSource::CollectionGroup(FirestoreCollectionGroupSource::new(
                        collection_id.to_string(),
                    )),
                    relation,
                ));
            }
            ("documents", paths) if !paths.is_empty() => {
                let paths = paths
                    .iter()
                    .map(|path| Ok(self.document_path(path, start)?.to_string()))
                    .collect::<FirestoreResult<_>>()?;
                FirestorePipelineSource::Documents(FirestoreDocumentsSource::new(paths))
            }
            ("database", []) => {
                if self.options.read_only || self.options.collections.is_some() {
                    return Err(FirestoreError::invalid_sql(
                        "database",
                        "database() reads every collection, which the options refuse",
                        start,
                    ));
                }
                FirestorePipelineSource::Database
            }
            ("subcollection", _) => {
                return Err(FirestoreError::unsupported_sql("subcollection(..)", start))
            }
            ("collection_group" | "documents" | "database", _) => {
                return Err(FirestoreError::invalid_sql(
                    &function,
                    "collection_group takes one collection ID, documents one or more document paths, and database none",
                    start,
                ))
            }
            _ => {
                return Err(FirestoreError::invalid_sql(
                    &function,
                    format!("{function}(..) is not a source: a source is a collection, collection_group(..), documents(..) or database()"),
                    start,
                ))
            }
        };
        let relation = FirestoreSqlRelation {
            name: alias,
            collection_path: None,
        };
        Ok((source, relation))
    }

    /// The documents a filter on `__name__` alone names, when each is a document of the source
    /// collection: read by path, each costs a point read where the filter reads the collection.
    fn document_reads(
        &self,
        scope: &FirestoreSqlScope,
        source: &FirestorePipelineSource,
        condition: &FirestoreExpr,
    ) -> Option<Vec<String>> {
        let FirestorePipelineSource::Collection(_) = source else {
            return None;
        };
        let collection_path = scope.relation().collection_path.as_deref()?;
        let FirestoreExpr::Function(FirestoreFunction { name, args }) = condition else {
            return None;
        };
        let references = match (name.as_str(), args.as_slice()) {
            ("equal", [name_field, FirestoreExpr::Constant(reference)])
            | ("equal", [FirestoreExpr::Constant(reference), name_field])
                if Self::is_document_name(name_field) =>
            {
                vec![&reference.value]
            }
            (
                "equal_any",
                [name_field, FirestoreExpr::Constant(FirestoreValue {
                    value:
                        Value {
                            value_type: Some(ValueType::ArrayValue(ArrayValue { values })),
                        },
                })],
            ) if Self::is_document_name(name_field) => values.iter().collect(),
            _ => return None,
        };
        let mut paths: Vec<String> = Vec::with_capacity(references.len());
        for reference in references {
            let Value {
                value_type: Some(ValueType::ReferenceValue(reference)),
            } = reference
            else {
                return None;
            };
            let path = reference
                .strip_prefix(self.documents_path)?
                .strip_prefix('/')?;
            let (parent, _) = path.rsplit_once('/')?;
            if parent != collection_path {
                return None;
            }
            if !paths.iter().any(|known| known == path) {
                paths.push(path.to_string());
            }
        }
        Some(paths)
    }

    fn projection(
        &mut self,
        scope: &FirestoreSqlScope,
        items: &[SelectItem],
    ) -> FirestoreResult<FirestoreSqlProjection> {
        let mut wildcard = None;
        let mut selections = Vec::with_capacity(items.len());
        for item in items {
            match item {
                SelectItem::UnnamedExpr(expr) => match self.expr(scope, expr)? {
                    field @ FirestoreExpr::Field(_) if is_column(expr) => {
                        selections.push(FirestoreSelection::new(field))
                    }
                    _ => {
                        return Err(FirestoreError::invalid_sql(
                            "AS",
                            "a computed column needs a name: add AS and a name",
                            expr_start(expr),
                        ))
                    }
                },
                SelectItem::ExprWithAlias { expr, alias } => {
                    let value = match expr {
                        Expr::Function(function) if names_its_output(function) => {
                            self.named_function(scope, function, Some(&alias.value))?
                        }
                        _ => self.expr(scope, expr)?,
                    };
                    selections.push(value.alias(alias.value.clone()))
                }
                SelectItem::Wildcard(options) => {
                    wildcard = Some(self.wildcard(scope, options, wildcard.is_some())?)
                }
                SelectItem::QualifiedWildcard(kind, options) => {
                    let qualifier = match kind {
                        SelectItemQualifiedWildcardKind::ObjectName(ObjectName(parts)) => {
                            match parts.as_slice() {
                                [ObjectNamePart::Identifier(ident)] => Some(ident),
                                _ => None,
                            }
                        }
                        SelectItemQualifiedWildcardKind::Expr(_) => None,
                    };
                    match qualifier {
                        Some(ident)
                            if scope.relation().name.as_deref() == Some(ident.value.as_str()) =>
                        {
                            wildcard = Some(self.wildcard(scope, options, wildcard.is_some())?)
                        }
                        _ => {
                            return Err(FirestoreError::unsupported_sql(
                                "this qualified *",
                                at(options.wildcard_token.0.span),
                            ))
                        }
                    }
                }
                SelectItem::ExprWithAliases { expr, .. } => {
                    return Err(FirestoreError::unsupported_sql(
                        "several aliases for one item",
                        expr_start(expr),
                    ))
                }
            }
        }
        Ok(match wildcard {
            Some(removed) => FirestoreSqlProjection::Wildcard(FirestoreSqlWildcard {
                removed,
                added: selections,
            }),
            None => FirestoreSqlProjection::Fields(selections),
        })
    }

    /// The fields `* EXCEPT (..)` removes.
    fn wildcard(
        &mut self,
        scope: &FirestoreSqlScope,
        options: &WildcardAdditionalOptions,
        repeated: bool,
    ) -> FirestoreResult<Vec<String>> {
        let WildcardAdditionalOptions {
            wildcard_token,
            opt_ilike,
            opt_exclude,
            opt_except,
            opt_replace,
            opt_rename,
            opt_alias,
        } = options;
        let start = at(wildcard_token.0.span);
        refuse_set(
            &[
                (repeated, "a second *"),
                (opt_ilike.is_some(), "* ILIKE"),
                (opt_exclude.is_some(), "* EXCLUDE"),
                (opt_replace.is_some(), "* REPLACE"),
                (opt_rename.is_some(), "* RENAME"),
                (opt_alias.is_some(), "an alias for *"),
            ],
            start,
        )?;
        match opt_except {
            Some(ExceptSelectItem {
                first_element,
                additional_elements,
            }) => std::iter::once(first_element)
                .chain(additional_elements)
                .map(|ident| scope.column(std::slice::from_ref(ident)))
                .collect(),
            None => Ok(Vec::new()),
        }
    }

    pub(super) fn orderings(
        &mut self,
        scope: &FirestoreSqlScope,
        projection: &FirestoreSqlProjection,
        order_by: &OrderBy,
    ) -> FirestoreResult<Vec<FirestorePipelineOrdering>> {
        let OrderBy { kind, interpolate } = order_by;
        if interpolate.is_some() {
            return Err(FirestoreError::unsupported_sql("INTERPOLATE", None));
        }
        let OrderByKind::Expressions(keys) = kind else {
            return Err(FirestoreError::unsupported_sql("ORDER BY ALL", None));
        };
        let mut orderings = Vec::with_capacity(keys.len());
        for OrderByExpr {
            expr,
            options: OrderByOptions { sort, nulls_first },
            with_fill,
        } in keys
        {
            if with_fill.is_some() {
                return Err(FirestoreError::unsupported_sql(
                    "WITH FILL",
                    expr_start(expr),
                ));
            }
            // An output name orders by the expression it names, as SQL resolves it first.
            let key = match expr {
                Expr::Identifier(ident) => match projection.aliased(&ident.value) {
                    Some(aliased) => aliased.clone(),
                    None => self.expr(scope, expr)?,
                },
                _ => self.expr(scope, expr)?,
            };
            if let FirestoreExpr::Constant(_) = key {
                return Err(FirestoreError::invalid_sql(
                    "ORDER BY",
                    "ORDER BY a constant or a position orders nothing: name a column",
                    expr_start(expr),
                ));
            }
            let direction = match sort {
                None | Some(OrderBySort::Asc) => FirestoreQueryDirection::Ascending,
                Some(OrderBySort::Desc) => FirestoreQueryDirection::Descending,
                Some(OrderBySort::Using(_)) => {
                    return Err(FirestoreError::unsupported_sql("USING", expr_start(expr)))
                }
            };
            // Firestore orders null and absent values first ascending and last descending. A
            // leading key that is true for them moves them to the other end.
            if matches!(
                (&direction, nulls_first),
                (FirestoreQueryDirection::Ascending, Some(false))
                    | (FirestoreQueryDirection::Descending, Some(true))
            ) {
                let missing = FirestorePipelineExprBuilder
                    .or([key.clone().is_absent(), key.clone().is_null()]);
                orderings.push(FirestoreExpr::from(missing).direction(direction.clone()));
            }
            orderings.push(key.direction(direction));
        }
        Ok(orderings)
    }

    fn limit(
        &mut self,
        limit_clause: &LimitClause,
        pipeline: &mut FirestorePipeline,
    ) -> FirestoreResult<()> {
        let (limit, offset) = match limit_clause {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                if let Some(first) = limit_by.first() {
                    return Err(FirestoreError::unsupported_sql(
                        "LIMIT BY",
                        expr_start(first),
                    ));
                }
                (
                    limit.as_ref(),
                    offset.as_ref().map(|Offset { value, rows: _ }| value),
                )
            }
            LimitClause::OffsetCommaLimit { offset, limit } => (Some(limit), Some(offset)),
        };
        if let Some(offset) = offset {
            let offset = self.count(offset, "OFFSET")?;
            pipeline
                .stages
                .push(FirestorePipelineStage::Offset(FirestoreOffsetStage::new(
                    offset,
                )));
        }
        if let Some(limit) = limit {
            let limit = self.count(limit, "LIMIT")?;
            pipeline
                .stages
                .push(FirestorePipelineStage::Limit(FirestoreLimitStage::new(
                    limit,
                )));
        }
        Ok(())
    }

    /// The count `LIMIT` or `OFFSET` takes: a literal or a parameter.
    fn count(&mut self, expr: &Expr, clause: &str) -> FirestoreResult<u32> {
        let value = match expr {
            Expr::Value(value) => Some(self.value(value)?),
            Expr::Identifier(ident) if is_parameter(ident) => Some(self.parameter(ident)?),
            _ => None,
        };
        match value {
            Some(FirestoreExpr::Constant(FirestoreValue {
                value:
                    Value {
                        value_type: Some(ValueType::IntegerValue(count)),
                    },
            })) => u32::try_from(count).ok(),
            _ => None,
        }
        .ok_or_else(|| {
            FirestoreError::invalid_sql(
                clause,
                format!("{clause} takes a count: a non-negative integer literal or parameter"),
                expr_start(expr),
            )
        })
    }
}

/// The string a literal holds, for an argument that names a source.
fn string_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: SqlValue::SingleQuotedString(text) | SqlValue::DoubleQuotedString(text),
            ..
        }) => Some(text),
        _ => None,
    }
}

/// Where a source starts, when it has a name.
fn table_start(factor: &TableFactor) -> Option<FirestoreSqlLocation> {
    match factor {
        TableFactor::Table { name, .. } => name_start(name),
        _ => None,
    }
}
