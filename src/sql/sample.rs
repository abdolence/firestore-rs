use super::translator::{expr_start, FirestoreSqlTranslator};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::{FirestoreResult, FirestoreSampleSize, FirestoreSampleStage};
use gcloud_sdk::google::firestore::v1::value::ValueType;
use sqlparser::ast::{
    Expr, TableSample, TableSampleKind, TableSampleMethod, TableSampleModifier,
    TableSampleSeedModifier, TableSampleUnit,
};
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Token, TokenWithSpan};

// GoogleSQL names fixed-size sampling RESERVOIR, which sqlparser does not know: it stops at the
// name. Renaming the word to ROW, a method it parses, lets the statement through, and the
// translator reads `ROW` as RESERVOIR. Call before parsing.
pub(super) fn rename_reservoir(tokens: &mut [TokenWithSpan]) {
    let mut after_tablesample = false;
    for TokenWithSpan { token, .. } in tokens {
        match token {
            Token::Whitespace(_) => continue,
            Token::Word(word) if word.keyword == Keyword::TABLESAMPLE => {
                after_tablesample = true;
                continue;
            }
            Token::Word(word)
                if after_tablesample
                    && word.quote_style.is_none()
                    && word.value.eq_ignore_ascii_case("RESERVOIR") =>
            {
                *token = Token::make_keyword("ROW");
            }
            _ => {}
        }
        after_tablesample = false;
    }
}

impl FirestoreSqlTranslator<'_> {
    // The `sample` stage of a `TABLESAMPLE` on a table. `children` says the table is a joined
    // one or in a subquery, which a sample is refused on.
    //
    // `BERNOULLI (p PERCENT)` keeps each document with probability p/100, and
    // `RESERVOIR (n ROWS)` keeps n documents, or all of them when there are fewer. Anything else
    // would sample differently from what its name says, and is refused.
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
        let refuse = |construct: &str| Err(FirestoreError::unsupported_sql(construct, start));
        if children {
            return Err(FirestoreError::invalid_sql(
                "TABLESAMPLE",
                "TABLESAMPLE applies to the first table of the outermost FROM, not to a joined table or a subquery",
                start,
            ));
        }
        if *modifier == TableSampleModifier::Sample {
            return refuse("SAMPLE");
        }
        if let Some(seed) = seed {
            return refuse(match seed.modifier {
                TableSampleSeedModifier::Repeatable => "REPEATABLE",
                TableSampleSeedModifier::Seed => "SEED",
            });
        }
        if bucket.is_some() {
            return refuse("BUCKET");
        }
        if offset.is_some() {
            return refuse("TABLESAMPLE OFFSET");
        }
        match name {
            Some(TableSampleMethod::System) => return refuse("TABLESAMPLE SYSTEM"),
            Some(TableSampleMethod::Block) => return refuse("TABLESAMPLE BLOCK"),
            _ => {}
        }
        let Some(quantity) = quantity else {
            return refuse("TABLESAMPLE");
        };
        match (name, quantity.unit) {
            (Some(TableSampleMethod::Bernoulli), Some(TableSampleUnit::Percent)) => {
                let percent = self.percent(&quantity.value)?;
                Ok(FirestoreSampleStage::new(FirestoreSampleSize::Fraction(
                    percent / 100.0,
                )))
            }
            // RESERVOIR, which `rename_reservoir` wrote as ROW.
            (Some(TableSampleMethod::Row), Some(TableSampleUnit::Rows)) => {
                let rows = self.rows(&quantity.value)?;
                Ok(FirestoreSampleStage::new(FirestoreSampleSize::Documents(
                    rows,
                )))
            }
            _ => refuse("TABLESAMPLE"),
        }
    }

    fn rows(&mut self, expr: &Expr) -> FirestoreResult<u32> {
        match self.sample_quantity(expr, "TABLESAMPLE ROWS")? {
            Some(ValueType::IntegerValue(rows)) => {
                u32::try_from(rows).ok().filter(|rows| *rows > 0)
            }
            _ => None,
        }
        .ok_or_else(|| {
            FirestoreError::invalid_sql(
                "TABLESAMPLE ROWS",
                "ROWS takes a positive integer literal or parameter",
                expr_start(expr),
            )
        })
    }

    fn percent(&mut self, expr: &Expr) -> FirestoreResult<f64> {
        match self.sample_quantity(expr, "TABLESAMPLE PERCENT")? {
            Some(ValueType::IntegerValue(percent)) => Some(percent as f64),
            Some(ValueType::DoubleValue(percent)) => Some(percent),
            _ => None,
        }
        .filter(|percent| (0.0..=100.0).contains(percent))
        .ok_or_else(|| {
            FirestoreError::invalid_sql(
                "TABLESAMPLE PERCENT",
                "PERCENT takes a number literal or parameter from 0 to 100",
                expr_start(expr),
            )
        })
    }

    fn sample_quantity(
        &mut self,
        expr: &Expr,
        construct: &str,
    ) -> FirestoreResult<Option<ValueType>> {
        Ok(self
            .bound_value(expr, construct)?
            .and_then(|value| value.constant_value().cloned()))
    }
}
