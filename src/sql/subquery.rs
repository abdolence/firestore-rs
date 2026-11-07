use super::query::FirestoreSqlBranch;
use super::scope::{FirestoreSqlPlace, FirestoreSqlScope};
use super::translator::{at, refuse_set, FirestoreSqlTranslator};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::{
    FirestoreDefineVariablesStage, FirestoreExpr, FirestoreLimitStage, FirestorePipeline,
    FirestorePipelineSource, FirestorePipelineStage, FirestoreResult,
};
use sqlparser::ast::{Cte, Query, SetExpr, With};

/// How many times in all the pipelines of common table expressions may be repeated in one
/// statement. A common table expression that uses another twice doubles the pipeline, so a short
/// statement could otherwise ask for more memory than it has.
const SQL_MAX_CTE_COPIES: usize = 1024;

/// A column of an enclosing query that a subquery reads. The enclosing query defines it as a
/// variable before the stage that holds the subquery, and the subquery reads the variable.
pub(super) struct FirestoreSqlCapture {
    depth: usize,
    path: String,
    variable: String,
    start: Option<FirestoreSqlLocation>,
}

pub(super) struct FirestoreSqlCte {
    name: String,
    pipeline: FirestorePipeline,
    /// How many query bodies the pipeline holds: its own and those of the expressions it uses.
    copies: usize,
}

impl FirestoreSqlTranslator<'_> {
    pub(super) fn capture(
        &mut self,
        depth: usize,
        qualifier: &str,
        path: String,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreExpr {
        if let Some(known) = self
            .pending
            .iter()
            .find(|capture| capture.depth == depth && capture.path == path)
        {
            return FirestoreExpr::Variable(known.variable.clone());
        }
        let readable: String = format!("{qualifier}_{path}")
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let mut variable = readable.clone();
        let mut suffix = 1;
        while self.pending.iter().any(|known| known.variable == variable) {
            suffix += 1;
            variable = format!("{readable}_{suffix}");
        }
        self.pending.push(FirestoreSqlCapture {
            depth,
            path,
            variable: variable.clone(),
            start,
        });
        FirestoreExpr::Variable(variable)
    }

    pub(super) fn define_variables(&mut self, depth: usize, pipeline: &mut FirestorePipeline) {
        let (defined, pending): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|capture| capture.depth == depth);
        self.pending = pending;
        if !defined.is_empty() {
            pipeline
                .stages
                .push(FirestorePipelineStage::DefineVariables(
                    FirestoreDefineVariablesStage::new(
                        defined
                            .into_iter()
                            .map(|capture| {
                                FirestoreExpr::field(capture.path).alias(capture.variable)
                            })
                            .collect(),
                    ),
                ));
        }
    }

    pub(super) fn ensure_no_variables_pending(&self, depth: usize) -> FirestoreResult<()> {
        match self.pending.iter().find(|capture| capture.depth == depth) {
            Some(capture) => Err(FirestoreError::invalid_sql(
                "subquery",
                format!(
                    "a subquery cannot read {} of the row here: it can in the WHERE, ON, ORDER BY or select list of a query that does not group",
                    capture.path
                ),
                capture.start,
            )),
            None => Ok(()),
        }
    }

    pub(super) fn ensure_variables_defined(&self) -> FirestoreResult<()> {
        match self.pending.first() {
            Some(capture) => self.ensure_no_variables_pending(capture.depth),
            None => Ok(()),
        }
    }

    pub(super) fn require_subqueries(
        &self,
        construct: &str,
        description: &str,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<()> {
        if self.options.subqueries {
            Ok(())
        } else {
            Err(FirestoreError::invalid_sql(
                construct,
                format!(
                    "{description} runs one subquery per row of the query, and each reads the whole collection it matches unless the key has an index; FirestoreSqlOptions::allow_subqueries() allows it"
                ),
                start,
            ))
        }
    }

    /// A subquery of the query whose scope is `scope`, as the pipeline that runs for each of its
    /// rows. It runs once per row whether or not it reads a column of the row, so it must opt in
    /// unless it reads only the subcollection of the row.
    fn subquery(
        &mut self,
        query: &Query,
        place: FirestoreSqlPlace<'_>,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreSqlBranch> {
        let branch = self.query(query, place)?;
        if !matches!(
            branch.pipeline.source,
            FirestorePipelineSource::Subcollection(_)
        ) {
            self.require_subqueries("subquery", "a subquery", start)?;
        }
        Ok(branch)
    }

    fn value_subquery(
        &mut self,
        scope: &FirestoreSqlScope<'_>,
        query: &Query,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestorePipeline> {
        let SetExpr::Select(_) = query.body.as_ref() else {
            return Err(FirestoreError::invalid_sql(
                "subquery",
                "a subquery that returns values is one SELECT, not a UNION",
                start,
            ));
        };
        let branch = self.subquery(query, FirestoreSqlPlace::inside(scope), start)?;
        match branch.columns.as_deref() {
            Some([_]) => Ok(branch.pipeline),
            _ => Err(FirestoreError::invalid_sql(
                "subquery",
                "a subquery that returns values selects exactly one named column, where a pipeline would return maps",
                start,
            )),
        }
    }

    pub(super) fn scalar_subquery(
        &mut self,
        scope: &FirestoreSqlScope<'_>,
        query: &Query,
    ) -> FirestoreResult<FirestoreExpr> {
        let pipeline = self.value_subquery(scope, query, query_start(query))?;
        Ok(FirestoreExpr::ScalarSubquery(pipeline))
    }

    pub(super) fn array_subquery(
        &mut self,
        scope: &FirestoreSqlScope<'_>,
        query: &Query,
    ) -> FirestoreResult<FirestoreExpr> {
        let pipeline = self.value_subquery(scope, query, query_start(query))?;
        Ok(FirestoreExpr::ArraySubquery(pipeline))
    }

    pub(super) fn in_subquery(
        &mut self,
        scope: &FirestoreSqlScope<'_>,
        operand: FirestoreExpr,
        query: &Query,
        negated: bool,
    ) -> FirestoreResult<FirestoreExpr> {
        let values = self.array_subquery(scope, query)?;
        Ok(if negated {
            operand.not_equal_any(values)
        } else {
            operand.equal_any(values)
        }
        .into())
    }

    /// `[NOT] EXISTS (SELECT ..)`: whether the subquery finds a row. It looks for one, and
    /// returns none of the select list.
    pub(super) fn exists(
        &mut self,
        scope: &FirestoreSqlScope<'_>,
        query: &Query,
        negated: bool,
    ) -> FirestoreResult<FirestoreExpr> {
        let place = FirestoreSqlPlace::inside(scope).for_existence();
        let mut pipeline = self.subquery(query, place, query_start(query))?.pipeline;
        pipeline
            .stages
            .push(FirestorePipelineStage::Limit(FirestoreLimitStage::new(1)));
        let found = FirestoreExpr::ArraySubquery(pipeline).array_length();
        Ok(if negated {
            found.equal(0)
        } else {
            found.greater_than(0)
        }
        .into())
    }

    /// Translates the common table expressions of a `WITH` and adds them to those in scope. Each
    /// is translated once, here, and a reference to it repeats its pipeline.
    pub(super) fn define_ctes(
        &mut self,
        with: &With,
        place: FirestoreSqlPlace<'_>,
    ) -> FirestoreResult<()> {
        let With {
            with_token,
            recursive,
            cte_tables,
        } = with;
        if *recursive {
            return Err(FirestoreError::unsupported_sql(
                "WITH RECURSIVE",
                at(with_token.0.span),
            ));
        }
        for cte in cte_tables {
            let Cte {
                alias,
                query,
                from,
                materialized,
                closing_paren_token: _,
            } = cte;
            let start = query_start(query);
            refuse_set(
                &[
                    (from.is_some(), "a FROM in a WITH"),
                    (materialized.is_some(), "MATERIALIZED"),
                ],
                start,
            )?;
            let name = Self::alias(alias)?;
            let copies_before = self.cte_copies;
            let pipeline = self.query(query, place.for_rows())?.pipeline;
            self.ctes.push(FirestoreSqlCte {
                name,
                pipeline,
                copies: 1 + self.cte_copies - copies_before,
            });
        }
        Ok(())
    }

    /// The pipeline of the common table expression called `name`, if there is one in scope. A
    /// name matches without regard to case, as SQL names do, and the latest definition wins.
    pub(super) fn cte_reference(
        &mut self,
        name: &str,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<Option<FirestorePipeline>> {
        let Some(index) = self
            .ctes
            .iter()
            .rposition(|cte| cte.name.eq_ignore_ascii_case(name))
        else {
            return Ok(None);
        };
        self.cte_copies += self.ctes[index].copies;
        if self.cte_copies > SQL_MAX_CTE_COPIES {
            return Err(FirestoreError::invalid_sql(
                "WITH",
                format!(
                    "the common table expressions are used so often that their queries would be repeated more than {SQL_MAX_CTE_COPIES} times: a reference to one repeats its query"
                ),
                start,
            ));
        }
        Ok(Some(self.ctes[index].pipeline.clone()))
    }
}

fn query_start(query: &Query) -> Option<FirestoreSqlLocation> {
    match query.body.as_ref() {
        SetExpr::Select(select) => at(select.select_token.0.span),
        _ => None,
    }
}
