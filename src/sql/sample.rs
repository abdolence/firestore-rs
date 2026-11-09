use super::translator::{expr_start, FirestoreSqlTranslator};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::{
    FirestorePipeline, FirestorePipelineStage, FirestoreResult, FirestoreSampleSize,
    FirestoreSampleStage,
};
use gcloud_sdk::google::firestore::v1::value::ValueType;
use sqlparser::ast::{
    Expr, TableSample, TableSampleKind, TableSampleMethod, TableSampleModifier,
    TableSampleQuantity, TableSampleSeedModifier, TableSampleUnit,
};

impl FirestoreSqlTranslator<'_> {
    // The `sample` stage of a `TABLESAMPLE` on a table. `children` says the table is a joined
    // one or in a subquery, which a sample is refused on.
    //
    // `BERNOULLI (p PERCENT)` keeps each document with probability p/100. Any other form would
    // sample differently from what its name says, and is refused. A `p` outside 0 to 100 is left
    // for Firestore to refuse.
    pub(super) fn table_sample(
        &mut self,
        sample: &TableSampleKind,
        children: bool,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreSampleStage> {
        let (TableSampleKind::BeforeTableAlias(sample) | TableSampleKind::AfterTableAlias(sample)) =
            sample;
        let TableSample {
            modifier,
            name,
            quantity,
            seed,
            bucket,
            offset,
        } = sample.as_ref();
        if children {
            return Err(Self::sample_per_row(start));
        }
        let refused = match (modifier, name, seed, bucket, offset) {
            (TableSampleModifier::Sample, ..) => Some("SAMPLE"),
            (_, _, Some(seed), ..) => Some(match seed.modifier {
                TableSampleSeedModifier::Repeatable => "REPEATABLE",
                TableSampleSeedModifier::Seed => "SEED",
            }),
            (_, _, _, Some(_), _) => Some("BUCKET"),
            (_, _, _, _, Some(_)) => Some("TABLESAMPLE OFFSET"),
            (_, Some(TableSampleMethod::System), ..) => Some("TABLESAMPLE SYSTEM"),
            (_, Some(TableSampleMethod::Block), ..) => Some("TABLESAMPLE BLOCK"),
            (_, Some(TableSampleMethod::Row), ..) => Some("TABLESAMPLE ROW"),
            _ => None,
        };
        if let Some(construct) = refused {
            return Err(FirestoreError::unsupported_sql(construct, start));
        }
        let (
            Some(TableSampleMethod::Bernoulli),
            Some(TableSampleQuantity {
                value,
                unit: Some(TableSampleUnit::Percent),
                ..
            }),
        ) = (name, quantity)
        else {
            return Err(FirestoreError::unsupported_sql("TABLESAMPLE", start));
        };
        let percent = self.percent(value)?;
        Ok(FirestoreSampleStage::new(FirestoreSampleSize::Fraction(
            percent / 100.0,
        )))
    }

    fn percent(&mut self, expr: &Expr) -> FirestoreResult<f64> {
        let value = self.bound_value(expr, "TABLESAMPLE PERCENT")?;
        match value.as_ref().and_then(|value| value.constant_value()) {
            Some(ValueType::IntegerValue(percent)) => Some(*percent as f64),
            Some(ValueType::DoubleValue(percent)) => Some(*percent),
            _ => None,
        }
        .ok_or_else(|| {
            FirestoreError::invalid_sql(
                "TABLESAMPLE PERCENT",
                "PERCENT takes a number literal or parameter",
                expr_start(expr),
            )
        })
    }

    // Fails for a source that carries a `Sample` stage, which a join or a subquery runs once for
    // each row of the outer query: the pipeline a derived table or a CTE reference stands for.
    pub(super) fn refuse_sampled_source(
        pipeline: &FirestorePipeline,
        children: bool,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<()> {
        match children && Self::holds_sample(pipeline) {
            true => Err(Self::sample_per_row(start)),
            false => Ok(()),
        }
    }

    fn holds_sample(pipeline: &FirestorePipeline) -> bool {
        pipeline.stages.iter().any(|stage| match stage {
            FirestorePipelineStage::Sample(_) => true,
            FirestorePipelineStage::Union(union) => Self::holds_sample(&union.other),
            _ => false,
        })
    }

    fn sample_per_row(start: Option<FirestoreSqlLocation>) -> FirestoreError {
        FirestoreError::invalid_sql(
            "TABLESAMPLE",
            "a joined table and a subquery run once for each row of the outer query, which would draw the sample again every time",
            start,
        )
    }
}
