use super::functions::FirestoreSqlCall;
use super::scope::FirestoreSqlScope;
use super::translator::{at, expr_start, is_column, is_parameter, FirestoreSqlTranslator};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::{
    FirestoreAggregate, FirestoreAggregateStage, FirestoreBoolExpr, FirestoreDistinctStage,
    FirestoreExpr, FirestoreFieldsStage, FirestorePipelineOrdering, FirestorePipelineStage,
    FirestoreResult, FirestoreSelection, FirestoreSortStage, FirestoreWhereStage,
};
use sqlparser::ast::{
    DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, Ident, SelectItem,
};
use std::collections::BTreeSet;

/// An expression the query groups by, and the field the aggregate stage outputs it under.
struct FirestoreSqlGroupKey {
    selection: FirestoreSelection,
    name: String,
}

struct FirestoreSqlAccumulator {
    aggregate: FirestoreAggregate,
    name: String,
}

/// A column a query read outside an aggregate's arguments.
struct FirestoreSqlColumnRead {
    path: String,
    start: Option<FirestoreSqlLocation>,
}

/// How a query groups its documents, and what it accumulates over each group.
///
/// Every expression the translator returns for the select list, `HAVING` and `ORDER BY` of the
/// query describes a document after the aggregate stage, which holds only the group keys and the
/// accumulators: an aggregate call is the field of its accumulator, and an expression that is a
/// group key is the field of that key. A column read that neither of these covers cannot be
/// evaluated after the stage, and fails the query.
pub(super) struct FirestoreSqlAggregation {
    keys: Vec<FirestoreSqlGroupKey>,
    accumulators: Vec<FirestoreSqlAccumulator>,
    /// The names no generated name may take: the select list's aliases and the keys' names.
    taken: BTreeSet<String>,
    reads: Vec<FirestoreSqlColumnRead>,
    /// The select list's named items, which `HAVING` and `ORDER BY` refer to by name.
    outputs: Vec<(String, FirestoreExpr)>,
}

impl FirestoreSqlAggregation {
    pub(super) fn new(aliases: impl IntoIterator<Item = String>) -> Self {
        Self {
            keys: Vec::new(),
            accumulators: Vec::new(),
            taken: aliases.into_iter().collect(),
            reads: Vec::new(),
            outputs: Vec::new(),
        }
    }

    /// Whether the query groups or accumulates: it does as soon as it has a key or an aggregate.
    pub(super) fn is_aggregate(&self) -> bool {
        !self.keys.is_empty() || !self.accumulators.is_empty()
    }

    /// Groups by `expr`, output under `alias`. A field is output under its own path unless that
    /// path is nested, whose dots a field name cannot hold. A repeated key is kept once.
    pub(super) fn add_key(&mut self, expr: FirestoreExpr, alias: Option<&str>) {
        if self.keys.iter().any(|key| key.selection.expr == expr) {
            return;
        }
        let name = match (alias, &expr) {
            (Some(alias), _) => alias.to_string(),
            (None, FirestoreExpr::Field(path)) if !path.contains(['.', '`']) => path.clone(),
            (None, _) => self.fresh("_group"),
        };
        self.taken.insert(name.clone());
        let selection = match &expr {
            FirestoreExpr::Field(path) if *path == name => FirestoreSelection::new(expr),
            _ => FirestoreSelection::new(expr).with_alias(name.clone()),
        };
        self.keys.push(FirestoreSqlGroupKey { selection, name });
    }

    /// Sets the select list's named items that `HAVING` and `ORDER BY` may refer to.
    pub(super) fn set_outputs(&mut self, selections: &[FirestoreSelection]) {
        self.outputs = selections
            .iter()
            .filter_map(|selection| {
                let alias = selection.alias.clone()?;
                Some((alias, selection.expr.clone()))
            })
            .collect();
    }

    pub(super) fn output(&self, name: &str) -> Option<&FirestoreExpr> {
        self.outputs
            .iter()
            .find(|(output, _)| output == name)
            .map(|(_, expr)| expr)
    }

    pub(super) fn read(&mut self, path: String, start: Option<FirestoreSqlLocation>) {
        self.reads.push(FirestoreSqlColumnRead { path, start });
    }

    /// Fails for a column that was read outside an aggregate and is not a group key.
    pub(super) fn ensure_grouped(&self) -> FirestoreResult<()> {
        match self.reads.first() {
            Some(FirestoreSqlColumnRead { path, start }) => Err(FirestoreError::invalid_sql(
                "GROUP BY",
                format!("{path} is neither grouped nor aggregated"),
                *start,
            )),
            None => Ok(()),
        }
    }

    /// The field of the accumulator for `aggregate`, adding one when the query has none yet. The
    /// accumulator is output under `output`, or under a generated name when the call is not a
    /// select item of its own.
    fn accumulator(&mut self, aggregate: FirestoreAggregate, output: Option<&str>) -> String {
        if let Some(known) = self
            .accumulators
            .iter()
            .find(|known| known.aggregate == aggregate)
        {
            return known.name.clone();
        }
        let name = match output {
            Some(output) => output.to_string(),
            None => self.fresh("_agg"),
        };
        self.taken.insert(name.clone());
        self.accumulators.push(FirestoreSqlAccumulator {
            aggregate,
            name: name.clone(),
        });
        name
    }

    fn fresh(&self, prefix: &str) -> String {
        let mut index = 0;
        loop {
            let candidate = format!("{prefix}{index}");
            if !self.taken.contains(&candidate) {
                return candidate;
            }
            index += 1;
        }
    }

    /// The stages that group the documents and shape the result: the aggregate (or a `distinct`
    /// when nothing accumulates), `HAVING`, the `ORDER BY`, and a projection unless the stage's
    /// own output is already the select list.
    pub(super) fn into_stages(
        self,
        having: Option<FirestoreExpr>,
        orderings: Option<Vec<FirestorePipelineOrdering>>,
        selections: Vec<FirestoreSelection>,
    ) -> Vec<FirestorePipelineStage> {
        let Self {
            keys, accumulators, ..
        } = self;
        let selections: Vec<FirestoreSelection> = selections
            .into_iter()
            .map(FirestoreSelection::without_redundant_alias)
            .collect();
        let outputs: BTreeSet<&str> = keys
            .iter()
            .map(|key| key.name.as_str())
            .chain(accumulators.iter().map(|known| known.name.as_str()))
            .collect();
        let selected: Option<Vec<&str>> = selections
            .iter()
            .map(|selection| match (&selection.expr, &selection.alias) {
                (FirestoreExpr::Field(name), None) => Some(name.as_str()),
                _ => None,
            })
            .collect();
        let selects_only_outputs = selected.is_some_and(|selected| {
            selected.len() == outputs.len()
                && selected.into_iter().collect::<BTreeSet<_>>() == outputs
        });

        let groups = keys.iter().map(|key| key.selection.clone()).collect();
        let mut stages = Vec::new();
        stages.push(if accumulators.is_empty() {
            FirestorePipelineStage::Distinct(FirestoreDistinctStage::new(groups))
        } else {
            let accumulators = accumulators
                .iter()
                .map(|known| known.aggregate.clone().alias(known.name.clone()))
                .collect();
            FirestorePipelineStage::Aggregate(
                FirestoreAggregateStage::new(accumulators).with_groups(groups),
            )
        });
        if let Some(having) = having {
            stages.push(FirestorePipelineStage::Where(FirestoreWhereStage::new(
                FirestoreBoolExpr::unchecked(having),
            )));
        }
        if let Some(orderings) = orderings {
            stages.push(FirestorePipelineStage::Sort(FirestoreSortStage::new(
                orderings,
            )));
        }
        if !selects_only_outputs {
            stages.push(FirestorePipelineStage::Fields(FirestoreFieldsStage::new(
                selections,
            )));
        }
        stages
    }
}

impl FirestoreSqlTranslator<'_> {
    /// Adds the group keys of the query to `aggregation`: the `GROUP BY` expressions, or for
    /// `SELECT DISTINCT` the select list, which keeps one row per distinct combination.
    ///
    /// A key is output under the alias the select list gives the same expression. `GROUP BY`
    /// also accepts a select alias for the expression it names.
    pub(super) fn group_keys(
        &mut self,
        scope: &FirestoreSqlScope,
        group_by: &GroupByExpr,
        items: &[SelectItem],
        distinct: bool,
        aggregation: &mut FirestoreSqlAggregation,
    ) -> FirestoreResult<()> {
        let exprs = match group_by {
            GroupByExpr::All(_) => {
                return Err(FirestoreError::unsupported_sql("GROUP BY ALL", None))
            }
            GroupByExpr::Expressions(_, modifiers) if !modifiers.is_empty() => {
                return Err(FirestoreError::unsupported_sql("a GROUP BY modifier", None))
            }
            GroupByExpr::Expressions(exprs, _) => exprs,
        };
        if !distinct {
            for expr in exprs {
                let aliased = match expr {
                    Expr::Identifier(ident) if !is_parameter(ident) => {
                        items.iter().find_map(|item| match item {
                            SelectItem::ExprWithAlias { expr, alias }
                                if alias.value == ident.value =>
                            {
                                Some(expr)
                            }
                            _ => None,
                        })
                    }
                    _ => None,
                };
                self.group_key(scope, aliased.unwrap_or(expr), items, aggregation)?;
            }
            return Ok(());
        }
        if !exprs.is_empty() {
            return Err(FirestoreError::unsupported_sql(
                "DISTINCT with GROUP BY",
                None,
            ));
        }
        for item in items {
            match item {
                SelectItem::UnnamedExpr(expr) if is_column(expr) => {
                    self.group_key(scope, expr, items, aggregation)?
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    self.group_key(scope, expr, items, aggregation)?
                }
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                    return Err(FirestoreError::invalid_sql(
                        "*",
                        "SELECT DISTINCT keeps one row per combination of the selected columns, and * selects none by name",
                        None,
                    ))
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Groups by `expr`, output under the alias the select list gives that same expression.
    fn group_key(
        &mut self,
        scope: &FirestoreSqlScope,
        expr: &Expr,
        items: &[SelectItem],
        aggregation: &mut FirestoreSqlAggregation,
    ) -> FirestoreResult<()> {
        let construct = match expr {
            Expr::Rollup(_) => Some("ROLLUP"),
            Expr::Cube(_) => Some("CUBE"),
            Expr::GroupingSets(_) => Some("GROUPING SETS"),
            _ => None,
        };
        if let Some(construct) = construct {
            return Err(FirestoreError::unsupported_sql(construct, expr_start(expr)));
        }
        let translated = self.expr(scope, expr)?;
        if let FirestoreExpr::Constant(_) = translated {
            return Err(FirestoreError::invalid_sql(
                "GROUP BY",
                "grouping by a constant or a position groups nothing: name a column",
                expr_start(expr),
            ));
        }
        let alias = items.iter().find_map(|item| match item {
            SelectItem::ExprWithAlias {
                expr: aliased,
                alias,
            } if aliased == expr => Some(alias.value.as_str()),
            _ => None,
        });
        aggregation.add_key(translated, alias);
        Ok(())
    }

    /// An aggregate call: the field of its accumulator in the aggregate stage.
    pub(super) fn aggregate(
        &mut self,
        scope: &FirestoreSqlScope,
        call: &FirestoreSqlCall<'_>,
        output: Option<&str>,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreExpr> {
        let name = &call.name;
        // The arguments read documents rather than the aggregate stage's output, so they
        // translate outside the aggregation. That also refuses an aggregate inside an aggregate.
        let Some(outer) = self.aggregation.take() else {
            return Err(FirestoreError::invalid_sql(
                name,
                format!("{name}(..) is an aggregate, which only a select list, HAVING or ORDER BY can hold"),
                start,
            ));
        };
        let aggregate = self.accumulator_call(scope, call, start);
        let aggregation = self.aggregation.insert(outer);
        Ok(FirestoreExpr::Field(
            aggregation.accumulator(aggregate?, output),
        ))
    }

    fn accumulator_call(
        &mut self,
        scope: &FirestoreSqlScope,
        call: &FirestoreSqlCall<'_>,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreAggregate> {
        let FirestoreSqlCall {
            name,
            arguments,
            duplicate_treatment,
        } = call;
        let distinct = matches!(duplicate_treatment, Some(DuplicateTreatment::Distinct));
        let accumulator = match (name.as_str(), distinct) {
            ("count", false) => "count",
            ("count", true) => "count_distinct",
            ("array_agg", false) => "array_agg",
            ("array_agg", true) => "array_agg_distinct",
            ("sum" | "count_distinct" | "array_agg_distinct" | "first" | "last", false) => name,
            ("avg" | "average", false) => "average",
            ("min", false) => "minimum",
            ("max", false) => "maximum",
            ("countif" | "count_if", false) => "count_if",
            (_, true) => {
                return Err(FirestoreError::unsupported_sql(
                    &format!("DISTINCT in {name}"),
                    start,
                ))
            }
            _ => {
                return Err(FirestoreError::unsupported_sql(
                    &format!("the aggregate function {name}"),
                    start,
                ))
            }
        };
        if name == "count"
            && !distinct
            && matches!(arguments, [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)])
        {
            return Ok(FirestoreAggregate::raw(
                accumulator,
                Vec::<FirestoreExpr>::new(),
            ));
        }
        let [argument] = Self::positional_arguments(arguments, start)?[..] else {
            return Err(FirestoreError::invalid_sql(
                name,
                format!("{name} takes one argument"),
                start,
            ));
        };
        Ok(FirestoreAggregate::raw(
            accumulator,
            [self.expr(scope, argument)?],
        ))
    }

    /// The column `parts` names, noted as read for the aggregation to check.
    pub(super) fn column(
        &mut self,
        scope: &FirestoreSqlScope,
        parts: &[Ident],
    ) -> FirestoreResult<FirestoreExpr> {
        let path = scope.column(parts)?;
        if let Some(aggregation) = self.aggregation.as_mut() {
            aggregation.read(path.clone(), parts.first().and_then(|part| at(part.span)));
        }
        Ok(FirestoreExpr::Field(path))
    }

    /// How many columns the aggregation has seen read so far.
    pub(super) fn read_mark(&self) -> usize {
        self.aggregation
            .as_ref()
            .map_or(0, |aggregation| aggregation.reads.len())
    }

    /// `translated`, which is the translation of an expression whose column reads since `mark`
    /// belong to it: the field of the group key when it is one, and then those reads are covered.
    pub(super) fn grouped(&mut self, translated: FirestoreExpr, mark: usize) -> FirestoreExpr {
        let Some(aggregation) = self.aggregation.as_mut() else {
            return translated;
        };
        let key = aggregation
            .keys
            .iter()
            .find(|key| key.selection.expr == translated)
            .map(|key| key.name.clone());
        match key {
            Some(name) => {
                aggregation.reads.truncate(mark);
                FirestoreExpr::Field(name)
            }
            None => translated,
        }
    }
}
