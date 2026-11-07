use super::aggregation::FirestoreSqlAggregation;
use super::functions::names_its_output;
use super::scope::{FirestoreSqlPlace, FirestoreSqlRelation, FirestoreSqlScope};
use super::translator::{
    at, expr_start, is_column, is_parameter, name_start, refuse_set, FirestoreSqlTranslator,
};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::pipeline_expr_builder::FirestorePipelineExprBuilder;
use crate::{
    FirestoreAddFieldsStage, FirestoreBoolExpr, FirestoreCollectionGroupSource,
    FirestoreCollectionSource, FirestoreDocumentsSource, FirestoreExpr, FirestoreFieldsStage,
    FirestoreLimitStage, FirestoreOffsetStage, FirestorePipeline, FirestorePipelineOrdering,
    FirestorePipelineSource, FirestorePipelineStage, FirestoreQueryDirection,
    FirestoreRemoveFieldsStage, FirestoreResult, FirestoreSelection, FirestoreSortStage,
    FirestoreSubcollectionSource, FirestoreWhereStage,
};
use gcloud_sdk::google::firestore::v1::value::ValueType;
use sqlparser::ast::{
    Distinct, ExceptSelectItem, Expr, FunctionArg, LimitClause, ObjectName, ObjectNamePart, Offset,
    OrderBy, OrderByExpr, OrderByKind, OrderByOptions, OrderBySort, Query, Select, SelectFlavor,
    SelectItem, SelectItemQualifiedWildcardKind, SetExpr, TableAlias, TableFactor,
    TableFunctionArgs, Value as SqlValue, ValueWithSpan, WildcardAdditionalOptions,
};

pub(super) struct FirestoreSqlBranch {
    pub(super) pipeline: FirestorePipeline,
    // The names of the columns it outputs, or `None` for `*`, which lists none.
    pub(super) columns: Option<Vec<String>>,
    pub(super) start: Option<FirestoreSqlLocation>,
}

pub(super) enum FirestoreSqlProjection {
    Wildcard(FirestoreSqlWildcard),
    Fields(Vec<FirestoreSelection>),
}

#[derive(Default)]
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

    fn aliased(&self, alias: &str) -> Option<&FirestoreExpr> {
        self.selections()
            .iter()
            .find(|selection| selection.alias.as_deref() == Some(alias))
            .map(|selection| &selection.expr)
    }

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
                        FirestoreAddFieldsStage::new(added),
                    ));
                }
                stages
            }
            Self::Fields(fields) => {
                vec![FirestorePipelineStage::Fields(FirestoreFieldsStage::new(
                    fields,
                ))]
            }
        }
    }
}

impl FirestoreSqlTranslator<'_> {
    pub(super) fn top_level_query(&mut self, query: &Query) -> FirestoreResult<FirestorePipeline> {
        let mut pipeline = self.query(query, FirestoreSqlPlace::top())?.pipeline;
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

    // A query as the `SELECT` it ends in, with the pipeline that reads its rows. The common
    // table expressions of its `WITH` are in scope for the query and the ones inside it.
    pub(super) fn query(
        &mut self,
        query: &Query,
        place: FirestoreSqlPlace<'_>,
    ) -> FirestoreResult<FirestoreSqlBranch> {
        Self::refuse_unmapped_clauses(query)?;
        let Query {
            with,
            body,
            order_by,
            limit_clause,
            ..
        } = query;
        let defined = self.ctes.len();
        if let Some(with) = with {
            self.define_ctes(with, place)?;
        }
        let mut branch = match body.as_ref() {
            SetExpr::Select(select) => self.select(select, order_by.as_ref(), place)?,
            SetExpr::SetOperation { .. } => {
                self.set_operation(body, order_by.as_ref(), place.for_rows())?
            }
            other => return Err(Self::unsupported_body(other)),
        };
        if let Some(limit_clause) = limit_clause {
            self.limit(limit_clause, &mut branch.pipeline)?;
        }
        self.ctes.truncate(defined);
        Ok(branch)
    }

    pub(super) fn refuse_unmapped_clauses(query: &Query) -> FirestoreResult<()> {
        let Query {
            with: _,
            body: _,
            order_by: _,
            limit_clause: _,
            fetch,
            locks,
            for_clause,
            settings,
            format_clause,
            pipe_operators,
        } = query;
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
        )
    }

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

    // A `SELECT` with the `ORDER BY` of its query, which sorts before the projection, so that
    // Firestore can read an index in order rather than sort the projected rows. A query that
    // groups sorts its groups, after the aggregate stage.
    //
    // A `SELECT` nested in this one translates with a state of its own.
    pub(super) fn select(
        &mut self,
        select: &Select,
        order_by: Option<&OrderBy>,
        place: FirestoreSqlPlace<'_>,
    ) -> FirestoreResult<FirestoreSqlBranch> {
        let outer = (self.aggregation.take(), self.search.take());
        let branch = self.select_with_state(select, order_by, place);
        (self.aggregation, self.search) = outer;
        branch
    }

    fn select_with_state(
        &mut self,
        select: &Select,
        order_by: Option<&OrderBy>,
        place: FirestoreSqlPlace<'_>,
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

        let (mut pipeline, scope) = self.from(from, select_start, place)?;
        if let Some(selection) = selection {
            let (search, condition) = self.split_search(&scope, &pipeline, selection)?;
            self.search = search;
            if let Some(condition) = condition {
                self.filter(&scope, &mut pipeline, condition);
            }
        }

        if place.existence {
            self.ensure_existence_ungrouped(
                &scope,
                group_by,
                having.is_some(),
                projection,
                select_start,
            )?;
        }
        let items: &[SelectItem] = if place.existence { &[] } else { projection };
        let mut aggregation =
            FirestoreSqlAggregation::new(items.iter().filter_map(|item| match item {
                SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                _ => None,
            }));
        self.group_keys(&scope, group_by, items, distinct, &mut aggregation)?;
        self.aggregation = Some(aggregation);
        let projection = if place.existence {
            FirestoreSqlProjection::Wildcard(FirestoreSqlWildcard::default())
        } else {
            self.projection(&scope, items)?
        };
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
                self.ensure_no_variables_pending(scope.depth())?;
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
                self.define_variables(scope.depth(), &mut pipeline);
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

    // A condition that is the constant `true`, as `ON TRUE` is, keeps every row and adds no
    // stage.
    pub(super) fn filter(
        &mut self,
        scope: &FirestoreSqlScope,
        pipeline: &mut FirestorePipeline,
        condition: FirestoreExpr,
    ) {
        self.define_variables(scope.depth(), pipeline);
        if condition != FirestoreExpr::constant(true) {
            pipeline
                .stages
                .push(FirestorePipelineStage::Where(FirestoreWhereStage::new(
                    FirestoreBoolExpr::unchecked(condition),
                )));
        }
    }

    // The pipeline that reads a table and the relation its names resolve against. `children`
    // says whether the table can be the subcollection of a document: it can in a subquery and
    // as a joined table.
    pub(super) fn table(
        &mut self,
        factor: &TableFactor,
        place: FirestoreSqlPlace<'_>,
        children: bool,
    ) -> FirestoreResult<(FirestorePipeline, FirestoreSqlRelation)> {
        if let TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } = factor
        {
            refuse_set(
                &[(*lateral, "LATERAL"), (sample.is_some(), "TABLESAMPLE")],
                None,
            )?;
            let name = alias.as_ref().map(Self::alias).transpose()?;
            let branch = self.query(subquery, place.for_rows())?;
            let relation = FirestoreSqlRelation {
                name,
                collection_path: None,
            };
            return Ok((branch.pipeline, relation));
        }
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
                TableFactor::UNNEST { .. } => "UNNEST",
                TableFactor::NestedJoin { .. } => "a parenthesised JOIN",
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
        let (source, relation) = match args {
            None => match self.referenced_cte(name, alias.as_deref(), start)? {
                Some(table) => return Ok(table),
                None => self.collection(name, alias)?,
            },
            Some(TableFunctionArgs { args, settings }) => {
                if settings.is_some() {
                    return Err(FirestoreError::unsupported_sql("SETTINGS", start));
                }
                self.table_function(name, args, alias, children)?
            }
        };
        Ok((FirestorePipeline::new(source), relation))
    }

    fn referenced_cte(
        &mut self,
        name: &ObjectName,
        alias: Option<&str>,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<Option<(FirestorePipeline, FirestoreSqlRelation)>> {
        let [ObjectNamePart::Identifier(ident)] = name.0.as_slice() else {
            return Ok(None);
        };
        Ok(self.cte_reference(&ident.value, start)?.map(|pipeline| {
            let relation = FirestoreSqlRelation {
                name: Some(alias.unwrap_or(&ident.value).to_string()),
                collection_path: None,
            };
            (pipeline, relation)
        }))
    }

    pub(super) fn alias(alias: &TableAlias) -> FirestoreResult<String> {
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

    fn collection(
        &mut self,
        name: &ObjectName,
        alias: Option<String>,
    ) -> FirestoreResult<(FirestorePipelineSource, FirestoreSqlRelation)> {
        let path = Self::collection_name(name)?;
        let source = self.collection_source(&path, name_start(name))?;
        let name = alias.unwrap_or_else(|| source.collection_id.clone());
        let relation = FirestoreSqlRelation {
            name: Some(name),
            collection_path: Some(path),
        };
        Ok((FirestorePipelineSource::Collection(source), relation))
    }

    pub(super) fn collection_source(
        &self,
        path: &str,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreCollectionSource> {
        let (parent, collection_id) = self.collection_path(path, start)?;
        Ok(FirestoreCollectionSource::new(collection_id.to_string())
            .opt_parent(parent.map(|parent| format!("{}/{parent}", self.documents_path))))
    }

    pub(super) fn collection_name(name: &ObjectName) -> FirestoreResult<String> {
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
                        name_start(name),
                    ))
                }
            }
        }
        // sqlparser splits a backticked name at its dots, which a Firestore ID may hold.
        Ok(parts.join("."))
    }

    fn table_function(
        &mut self,
        name: &ObjectName,
        args: &[FunctionArg],
        mut alias: Option<String>,
        children: bool,
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
                alias.get_or_insert_with(|| collection_id.to_string());
                FirestorePipelineSource::CollectionGroup(FirestoreCollectionGroupSource::new(
                    collection_id.to_string(),
                ))
            }
            ("subcollection", [collection_id]) => {
                if !children {
                    return Err(FirestoreError::invalid_sql(
                        "subcollection",
                        "subcollection(..) reads the collection under the current document, so it belongs in a subquery or a join",
                        start,
                    ));
                }
                self.check_collection(collection_id, start)?;
                alias.get_or_insert_with(|| collection_id.to_string());
                FirestorePipelineSource::Subcollection(FirestoreSubcollectionSource::new(
                    collection_id.to_string(),
                ))
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
            ("collection_group" | "documents" | "database" | "subcollection", _) => {
                return Err(FirestoreError::invalid_sql(
                    &function,
                    "collection_group and subcollection take one collection ID, documents one or more document paths, and database none",
                    start,
                ))
            }
            _ => {
                return Err(FirestoreError::invalid_sql(
                    &function,
                    format!("{function}(..) is not a source: a source is a collection, collection_group(..), documents(..), subcollection(..) or database()"),
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
                    FirestoreExpr::Field(path) if is_column(expr) => {
                        if self
                            .aggregation
                            .as_ref()
                            .is_some_and(|aggregation| aggregation.is_generated_key(&path))
                        {
                            return Err(FirestoreError::invalid_sql(
                                "AS",
                                "a nested field that is grouped needs a name: add AS and a name",
                                expr_start(expr),
                            ));
                        }
                        let name = scope.output_name(&path);
                        let alias = (name != path).then(|| name.to_string());
                        selections.push(
                            FirestoreSelection::new(FirestoreExpr::Field(path)).opt_alias(alias),
                        )
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
                        Some(ident) if scope.relation().is_called(&ident.value) => {
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
        if scope.is_joined() {
            return Err(FirestoreError::invalid_sql(
                "*",
                "* of a query with a join would select the joined tables too, which replace any field of the left side with the name of their alias: name the columns",
                start,
            ));
        }
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
                .map(|ident| scope.field(std::slice::from_ref(ident)))
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
            if matches!(key, FirestoreExpr::Constant(_)) || key.sql_placeholder().is_some() {
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

    fn count(&mut self, expr: &Expr, clause: &str) -> FirestoreResult<u32> {
        let value = match expr {
            Expr::Value(value) => Some(self.value(value)?),
            Expr::Identifier(ident) if is_parameter(ident) => Some(self.parameter(ident)?),
            _ => None,
        };
        if let Some(value) = &value {
            value.require_value(clause, expr_start(expr))?;
        }
        match value.as_ref().and_then(FirestoreExpr::constant_value) {
            Some(ValueType::IntegerValue(count)) => u32::try_from(*count).ok(),
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

pub(super) fn string_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: SqlValue::SingleQuotedString(text) | SqlValue::DoubleQuotedString(text),
            ..
        }) => Some(text),
        _ => None,
    }
}

pub(super) fn table_start(factor: &TableFactor) -> Option<FirestoreSqlLocation> {
    match factor {
        TableFactor::Table { name, .. } => name_start(name),
        _ => None,
    }
}
