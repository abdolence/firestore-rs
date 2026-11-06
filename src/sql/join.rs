use super::query::table_start;
use super::scope::{FirestoreSqlPlace, FirestoreSqlRelation, FirestoreSqlScope};
use super::translator::{expr_start, field_segment, FirestoreSqlTranslator};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::pipeline_expr_builder::FirestorePipelineExprBuilder;
use crate::{
    FirestoreAddFieldsStage, FirestoreBoolExpr, FirestoreExpr, FirestorePipeline,
    FirestorePipelineSource, FirestorePipelineStage, FirestoreResult, FirestoreSelection,
    FirestoreUnnestStage, FirestoreWhereStage,
};
use sqlparser::ast::{Expr, Join, JoinConstraint, JoinOperator, TableFactor, TableWithJoins};

#[derive(Clone, Copy, PartialEq, Eq)]
enum FirestoreSqlJoinKind {
    /// Each row with each match, and no row without one.
    Inner,
    /// As `Inner`, and each row without a match once, with the joined table null.
    Left,
    /// Each row with every row of the table.
    Cross,
}

struct FirestoreSqlJoin<'a> {
    kind: FirestoreSqlJoinKind,
    table: &'a TableFactor,
    on: Option<&'a Expr>,
}

impl FirestoreSqlTranslator<'_> {
    /// The pipeline that reads the tables of a `FROM`, and the scope of their names.
    ///
    /// A table after the first is joined by a subquery that runs for each row before it: its
    /// documents become an array under the table's name, and `unnest` makes one row of each
    /// element. `UNNEST` of an array of the row needs no subquery.
    pub(super) fn from<'o>(
        &mut self,
        from: &[TableWithJoins],
        select_start: Option<FirestoreSqlLocation>,
        place: FirestoreSqlPlace<'o>,
    ) -> FirestoreResult<(FirestorePipeline, FirestoreSqlScope<'o>)> {
        let [first, rest @ ..] = from else {
            return Err(FirestoreError::invalid_sql(
                "FROM",
                "a query reads from a source, and this one has no FROM",
                select_start,
            ));
        };
        let mut joins = Vec::new();
        for join in &first.joins {
            joins.push(Self::join_of(join)?);
        }
        for TableWithJoins {
            relation,
            joins: more,
        } in rest
        {
            joins.push(FirestoreSqlJoin {
                kind: FirestoreSqlJoinKind::Cross,
                table: relation,
                on: None,
            });
            for join in more {
                joins.push(Self::join_of(join)?);
            }
        }

        let (mut pipeline, relation) = self.table(&first.relation, place, place.outer.is_some())?;
        let mut scope = FirestoreSqlScope::new(relation).inside(place.outer);
        for join in joins {
            match join.table {
                TableFactor::UNNEST { .. } => self.unnest_join(&mut pipeline, &mut scope, &join)?,
                _ => self.table_join(&mut pipeline, &mut scope, place, &join)?,
            }
        }
        Ok((pipeline, scope))
    }

    fn join_of(join: &Join) -> FirestoreResult<FirestoreSqlJoin<'_>> {
        let Join {
            relation,
            global,
            join_operator,
        } = join;
        let start = table_start(relation);
        if *global {
            return Err(FirestoreError::unsupported_sql("GLOBAL JOIN", start));
        }
        let (kind, constraint) = match join_operator {
            JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
                (FirestoreSqlJoinKind::Inner, constraint)
            }
            JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                (FirestoreSqlJoinKind::Left, constraint)
            }
            JoinOperator::CrossJoin(constraint) => (FirestoreSqlJoinKind::Cross, constraint),
            JoinOperator::Right(_) | JoinOperator::RightOuter(_) => {
                return Err(FirestoreError::unsupported_sql("RIGHT JOIN", start))
            }
            JoinOperator::FullOuter(_) => {
                return Err(FirestoreError::unsupported_sql("FULL JOIN", start))
            }
            _ => return Err(FirestoreError::unsupported_sql("this kind of join", start)),
        };
        let on = match constraint {
            JoinConstraint::On(on) => Some(on),
            JoinConstraint::None => None,
            JoinConstraint::Using(_) => {
                return Err(FirestoreError::unsupported_sql("JOIN USING", start))
            }
            JoinConstraint::Natural => {
                return Err(FirestoreError::unsupported_sql("NATURAL JOIN", start))
            }
        };
        match (kind, on) {
            (FirestoreSqlJoinKind::Cross, Some(on)) => Err(FirestoreError::unsupported_sql(
                "ON in a CROSS JOIN",
                expr_start(on),
            )),
            (FirestoreSqlJoinKind::Inner | FirestoreSqlJoinKind::Left, None) => {
                Err(FirestoreError::invalid_sql(
                    "JOIN",
                    "a JOIN needs an ON condition; CROSS JOIN joins without one",
                    start,
                ))
            }
            _ => Ok(FirestoreSqlJoin {
                kind,
                table: relation,
                on,
            }),
        }
    }

    /// Joins a table, as a subquery that runs for each row. A table of the row's own
    /// subcollection reads only that row's children, so it needs no opt-in.
    fn table_join<'o>(
        &mut self,
        pipeline: &mut FirestorePipeline,
        scope: &mut FirestoreSqlScope<'o>,
        place: FirestoreSqlPlace<'_>,
        join: &FirestoreSqlJoin<'_>,
    ) -> FirestoreResult<()> {
        let start = table_start(join.table);
        let (mut matches, relation) = self.table(join.table, place, true)?;
        let Some(name) = relation.name.clone() else {
            return Err(FirestoreError::invalid_sql(
                "JOIN",
                "a joined source needs an alias: its fields live under that name",
                start,
            ));
        };
        scope.ensure_unused(&name, start)?;
        if !matches!(matches.source, FirestorePipelineSource::Subcollection(_)) {
            self.require_subqueries("JOIN", "a join", start)?;
        }
        if let Some(on) = join.on {
            let condition = {
                let on_scope = FirestoreSqlScope::new(relation.clone()).joining(scope);
                self.expr(&on_scope, on)?
            };
            keep_matching(&mut matches, condition);
        }

        self.define_variables(scope.depth(), pipeline);
        let element = FirestoreExpr::field(field_segment(&name));
        let mut stages = vec![FirestorePipelineStage::AddFields(
            FirestoreAddFieldsStage::new(vec![
                FirestoreExpr::ArraySubquery(matches).alias(name.clone())
            ]),
        )];
        if join.kind == FirestoreSqlJoinKind::Left {
            // `unnest` drops a row whose array is empty. One null element keeps it.
            let unmatched = element.clone().array_length().equal(0);
            let null_element =
                FirestorePipelineExprBuilder.array([FirestoreExpr::constant(None::<bool>)]);
            stages.push(FirestorePipelineStage::AddFields(
                FirestoreAddFieldsStage::new(vec![FirestorePipelineExprBuilder
                    .conditional(unmatched, null_element, element.clone())
                    .alias(name)]),
            ));
        }
        stages.push(FirestorePipelineStage::Unnest(FirestoreUnnestStage::new(
            FirestoreSelection::new(element),
        )));
        pipeline.stages.extend(stages);
        scope.join(relation);
        Ok(())
    }

    fn unnest_join<'o>(
        &mut self,
        pipeline: &mut FirestorePipeline,
        scope: &mut FirestoreSqlScope<'o>,
        join: &FirestoreSqlJoin<'_>,
    ) -> FirestoreResult<()> {
        let TableFactor::UNNEST {
            alias,
            array_exprs,
            with_offset,
            with_offset_alias,
            with_ordinality,
        } = join.table
        else {
            return Err(FirestoreError::unsupported_sql("this source", None));
        };
        let start = array_exprs.first().and_then(expr_start);
        if *with_ordinality {
            return Err(FirestoreError::unsupported_sql("WITH ORDINALITY", start));
        }
        let [array] = array_exprs.as_slice() else {
            return Err(FirestoreError::unsupported_sql(
                "UNNEST of several arrays",
                start,
            ));
        };
        if join.kind == FirestoreSqlJoinKind::Left {
            return Err(FirestoreError::unsupported_sql("LEFT JOIN UNNEST", start));
        }
        let Some(alias) = alias else {
            return Err(FirestoreError::invalid_sql(
                "UNNEST",
                "UNNEST needs an alias that names its elements: UNNEST(..) AS element",
                start,
            ));
        };
        let name = Self::alias(alias)?;
        scope.ensure_unused(&name, start)?;

        let array = self.expr(scope, array)?;
        self.define_variables(scope.depth(), pipeline);
        let index_field = with_offset.then(|| {
            with_offset_alias
                .as_ref()
                .map_or_else(|| "offset".to_string(), |alias| alias.value.clone())
        });
        pipeline.stages.push(FirestorePipelineStage::Unnest(
            FirestoreUnnestStage::new(array.alias(name.clone())).opt_index_field(index_field),
        ));
        scope.join(FirestoreSqlRelation {
            name: Some(name),
            collection_path: None,
        });
        if let Some(on) = join.on {
            let condition = self.expr(scope, on)?;
            self.define_variables(scope.depth(), pipeline);
            keep_matching(pipeline, condition);
        }
        Ok(())
    }
}

/// Keeps only the rows of `pipeline` for which `condition` holds. A condition that is the
/// constant `true`, as `ON TRUE` is, keeps all of them and adds no stage.
fn keep_matching(pipeline: &mut FirestorePipeline, condition: FirestoreExpr) {
    if condition != FirestoreExpr::constant(true) {
        pipeline
            .stages
            .push(FirestorePipelineStage::Where(FirestoreWhereStage::new(
                FirestoreBoolExpr::unchecked(condition),
            )));
    }
}
