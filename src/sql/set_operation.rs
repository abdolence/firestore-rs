use super::query::{FirestoreSqlBranch, FirestoreSqlProjection};
use super::scope::{FirestoreSqlRelation, FirestoreSqlScope};
use super::translator::FirestoreSqlTranslator;
use crate::errors::FirestoreError;
use crate::{
    FirestoreDistinctStage, FirestoreExpr, FirestorePipelineStage, FirestoreResult,
    FirestoreSelection, FirestoreSortStage, FirestoreUnionStage,
};
use sqlparser::ast::{OrderBy, SetExpr, SetOperator, SetQuantifier};

impl FirestoreSqlTranslator<'_> {
    /// A chain of `UNION`s, each of whose branches is a plain `SELECT`.
    ///
    /// A pipeline's `union` appends the documents of the other pipeline and aligns nothing, so
    /// the branches' columns meet by name where SQL aligns them by position. The branches must
    /// therefore output the same names in the same order, which makes both readings agree. A
    /// `UNION` that deduplicates then does so over those names. The result outputs the names of
    /// its first branch.
    pub(super) fn set_operation(
        &mut self,
        body: &SetExpr,
        order_by: Option<&OrderBy>,
    ) -> FirestoreResult<FirestoreSqlBranch> {
        let mut unions = Vec::new();
        let mut first = body;
        while let SetExpr::SetOperation {
            left,
            op,
            set_quantifier,
            right,
        } = first
        {
            if *op != SetOperator::Union {
                return Err(FirestoreError::unsupported_sql(&op.to_string(), None));
            }
            let deduplicates = match set_quantifier {
                SetQuantifier::All => false,
                SetQuantifier::Distinct | SetQuantifier::None => true,
                SetQuantifier::ByName
                | SetQuantifier::AllByName
                | SetQuantifier::DistinctByName => {
                    return Err(FirestoreError::unsupported_sql("UNION BY NAME", None))
                }
            };
            unions.push((deduplicates, right.as_ref()));
            first = left;
        }
        unions.reverse();

        let FirestoreSqlBranch {
            mut pipeline,
            columns,
            start,
        } = self.branch(first)?;
        for (index, &(deduplicates, right)) in unions.iter().enumerate() {
            let branch = self.branch(right)?;
            match (&columns, &branch.columns) {
                (Some(expected), Some(actual)) if expected == actual => {}
                (None, None) if !deduplicates => {}
                (None, _) if deduplicates => {
                    return Err(FirestoreError::invalid_sql(
                        "UNION",
                        "UNION DISTINCT deduplicates by the output names, which * does not list: use UNION ALL, or name the columns",
                        branch.start,
                    ))
                }
                _ => {
                    return Err(FirestoreError::invalid_sql(
                        "UNION",
                        "the branches of a UNION must output the same names in the same order: a pipeline aligns the branches by name, where SQL aligns them by position",
                        branch.start,
                    ))
                }
            }
            pipeline
                .stages
                .push(FirestorePipelineStage::Union(FirestoreUnionStage::new(
                    branch.pipeline,
                )));
            // A later deduplicating union deduplicates these rows again.
            let next_deduplicates = unions.get(index + 1).is_some_and(|(next, _)| *next);
            if let (true, false, Some(columns)) = (deduplicates, next_deduplicates, &columns) {
                let groups = columns
                    .iter()
                    .map(|column| FirestoreSelection::new(FirestoreExpr::field(column)))
                    .collect();
                pipeline.stages.push(FirestorePipelineStage::Distinct(
                    FirestoreDistinctStage::new(groups),
                ));
            }
        }
        if let Some(order_by) = order_by {
            let scope = FirestoreSqlScope::new(FirestoreSqlRelation {
                name: None,
                collection_path: None,
            });
            let no_aliases = FirestoreSqlProjection::Fields(Vec::new());
            let orderings = self.orderings(&scope, &no_aliases, order_by)?;
            pipeline
                .stages
                .push(FirestorePipelineStage::Sort(FirestoreSortStage::new(
                    orderings,
                )));
        }
        Ok(FirestoreSqlBranch {
            pipeline,
            columns,
            start,
        })
    }

    fn branch(&mut self, set_expr: &SetExpr) -> FirestoreResult<FirestoreSqlBranch> {
        match set_expr {
            SetExpr::Select(select) => self.select(select, None),
            other => Err(Self::unsupported_body(other)),
        }
    }
}
