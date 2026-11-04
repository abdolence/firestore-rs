use super::expr::{selection_map, wire_value};
use super::model::stage;
use crate::{
    FirestoreAggregateStage, FirestoreDefineStage, FirestoreDistinctStage, FirestoreError,
    FirestoreFindNearestDistanceMeasure, FirestoreFindNearestStage, FirestorePipeline,
    FirestoreReplaceWithMode, FirestoreReplaceWithStage, FirestoreSampleSize, FirestoreSampleStage,
    FirestoreSubcollectionSource, FirestoreUnionStage, FirestoreUnnestStage, FirestoreValue,
};
use gcloud_sdk::google::firestore::v1::{pipeline::Stage, value::ValueType, Pipeline, Value};
use std::collections::HashMap;

impl TryFrom<FirestorePipeline> for Value {
    type Error = FirestoreError;

    fn try_from(pipeline: FirestorePipeline) -> Result<Self, Self::Error> {
        Ok(wire_value(ValueType::PipelineValue(Pipeline::try_from(
            pipeline,
        )?)))
    }
}

impl From<FirestoreSubcollectionSource> for Stage {
    fn from(source: FirestoreSubcollectionSource) -> Self {
        stage(
            "subcollection",
            vec![wire_value(ValueType::StringValue(source.collection_id))],
        )
    }
}

impl TryFrom<FirestoreAggregateStage> for Stage {
    type Error = FirestoreError;

    fn try_from(aggregate: FirestoreAggregateStage) -> Result<Self, Self::Error> {
        Ok(stage(
            "aggregate",
            vec![
                selection_map(
                    "aggregate",
                    aggregate.accumulators.into_iter().map(Into::into).collect(),
                )?,
                selection_map("aggregate", aggregate.groups)?,
            ],
        ))
    }
}

impl TryFrom<FirestoreDistinctStage> for Stage {
    type Error = FirestoreError;

    fn try_from(distinct: FirestoreDistinctStage) -> Result<Self, Self::Error> {
        Ok(stage(
            "distinct",
            vec![selection_map("distinct", distinct.groups)?],
        ))
    }
}

impl TryFrom<FirestoreFindNearestStage> for Stage {
    type Error = FirestoreError;

    fn try_from(find_nearest: FirestoreFindNearestStage) -> Result<Self, Self::Error> {
        let mut options = HashMap::from([(
            "limit".to_string(),
            wire_value(ValueType::IntegerValue(find_nearest.neighbors_limit.into())),
        )]);
        if let Some(field) = find_nearest.distance_result_field {
            options.insert(
                "distance_field".to_string(),
                wire_value(ValueType::FieldReferenceValue(field)),
            );
        }
        Ok(Stage {
            options,
            ..stage(
                "find_nearest",
                vec![
                    wire_value(ValueType::FieldReferenceValue(find_nearest.field_name)),
                    Into::<FirestoreValue>::into(find_nearest.query_vector).value,
                    find_nearest.distance_measure.into(),
                ],
            )
        })
    }
}

impl From<FirestoreFindNearestDistanceMeasure> for Value {
    fn from(measure: FirestoreFindNearestDistanceMeasure) -> Self {
        wire_value(ValueType::StringValue(
            match measure {
                FirestoreFindNearestDistanceMeasure::Euclidean => "euclidean",
                FirestoreFindNearestDistanceMeasure::Cosine => "cosine",
                FirestoreFindNearestDistanceMeasure::DotProduct => "dot_product",
            }
            .to_string(),
        ))
    }
}

impl From<FirestoreSampleStage> for Stage {
    fn from(sample: FirestoreSampleStage) -> Self {
        let (size, mode) = match sample.size {
            FirestoreSampleSize::Documents(count) => {
                (ValueType::IntegerValue(count.into()), "documents")
            }
            FirestoreSampleSize::Fraction(fraction) => {
                (ValueType::DoubleValue(fraction), "percent")
            }
        };
        stage(
            "sample",
            vec![
                wire_value(size),
                wire_value(ValueType::StringValue(mode.to_string())),
            ],
        )
    }
}

impl TryFrom<FirestoreUnionStage> for Stage {
    type Error = FirestoreError;

    fn try_from(union: FirestoreUnionStage) -> Result<Self, Self::Error> {
        Ok(stage("union", vec![union.other.try_into()?]))
    }
}

impl TryFrom<FirestoreUnnestStage> for Stage {
    type Error = FirestoreError;

    fn try_from(unnest: FirestoreUnnestStage) -> Result<Self, Self::Error> {
        let (alias, array) = unnest.selection.into_named("unnest")?;
        Ok(Stage {
            options: unnest
                .index_field
                .map(|field| {
                    (
                        "index_field".to_string(),
                        wire_value(ValueType::FieldReferenceValue(field)),
                    )
                })
                .into_iter()
                .collect(),
            ..stage(
                "unnest",
                vec![
                    Value::try_from(array)?,
                    wire_value(ValueType::FieldReferenceValue(alias)),
                ],
            )
        })
    }
}

impl TryFrom<FirestoreReplaceWithStage> for Stage {
    type Error = FirestoreError;

    fn try_from(replace_with: FirestoreReplaceWithStage) -> Result<Self, Self::Error> {
        Ok(stage(
            "replace_with",
            vec![replace_with.expr.try_into()?, replace_with.mode.into()],
        ))
    }
}

impl From<FirestoreReplaceWithMode> for Value {
    fn from(mode: FirestoreReplaceWithMode) -> Self {
        wire_value(ValueType::StringValue(
            match mode {
                FirestoreReplaceWithMode::FullReplace => "full_replace",
                FirestoreReplaceWithMode::MergeOverwriteExisting => "merge_overwrite_existing",
                FirestoreReplaceWithMode::MergeKeepExisting => "merge_keep_existing",
            }
            .to_string(),
        ))
    }
}

impl TryFrom<FirestoreDefineStage> for Stage {
    type Error = FirestoreError;

    fn try_from(define: FirestoreDefineStage) -> Result<Self, Self::Error> {
        Ok(stage("let", vec![selection_map("let", define.variables)?]))
    }
}

#[cfg(test)]
mod tests {
    use crate::db::pipeline::fixtures::{fixture_case, pipeline};
    use crate::pipeline_builder::FirestorePipelineInitialBuilder;
    use crate::{
        FirestoreError, FirestoreFindNearestDistanceMeasure, FirestoreFindNearestStage,
        FirestoreReplaceWithMode, FirestoreSampleSize, FirestoreSelection, FirestoreVector,
    };
    use gcloud_sdk::google::firestore::v1::Pipeline;
    use std::collections::HashMap;

    fn builder() -> FirestorePipelineInitialBuilder<()> {
        FirestorePipelineInitialBuilder::new(())
    }

    #[derive(serde::Serialize)]
    struct Overwrite {
        title: &'static str,
        extra: i64,
    }

    #[test]
    fn stages_convert_to_the_captured_requests() {
        let captured = [
            (
                "stage-aggregate-ungrouped",
                None,
                builder()
                    .from("pipeline-books")
                    .aggregate(|e| {
                        [
                            e.count_all().alias("total"),
                            e.field("stock")
                                .greater_than(0)
                                .count_if()
                                .alias("in_stock"),
                        ]
                    })
                    .into_pipeline(),
            ),
            (
                "stage-aggregate-grouped",
                None,
                builder()
                    .from("pipeline-books")
                    .aggregate_grouped(
                        |e| [e.field("genre")],
                        |e| {
                            [
                                e.field("rating").average().alias("avg_rating"),
                                e.count_all().alias("books"),
                                e.field("pages").maximum().alias("max_pages"),
                            ]
                        },
                    )
                    .into_pipeline(),
            ),
            (
                "stage-distinct",
                None,
                builder()
                    .from("pipeline-books")
                    .distinct(|e| [e.field("genre")])
                    .into_pipeline(),
            ),
            (
                "stage-find-nearest",
                None,
                builder()
                    .from("pipeline-books")
                    .find_nearest_with_options(
                        FirestoreFindNearestStage::new(
                            "embedding".to_string(),
                            FirestoreVector::new(vec![0.9, 0.1, 0.2]),
                            FirestoreFindNearestDistanceMeasure::Euclidean,
                            3,
                        )
                        .with_distance_result_field("distance".to_string()),
                    )
                    .into_pipeline(),
            ),
            (
                "stage-sample-documents",
                None,
                builder()
                    .from("pipeline-books")
                    .sample(FirestoreSampleSize::Documents(3))
                    .into_pipeline(),
            ),
            (
                "stage-sample-percent",
                None,
                builder()
                    .from("pipeline-books")
                    .sample(FirestoreSampleSize::Fraction(0.5))
                    .into_pipeline(),
            ),
            (
                "stage-union",
                None,
                builder()
                    .from("pipeline-books")
                    .select(|e| [e.field("title").alias("name")])
                    .union(|p| p.from("pipeline-restaurants").select(|e| [e.field("name")]))
                    .into_pipeline(),
            ),
            (
                "stage-unnest",
                None,
                builder()
                    .documents(["pipeline-books/book-01"])
                    .unnest_with_index_field(|e| e.field("tags").alias("tag"), "tag_index")
                    .select(|e| [e.field("tag"), e.field("tag_index")])
                    .into_pipeline(),
            ),
            (
                "stage-replace-with",
                Some("full_replace"),
                builder()
                    .documents(["pipeline-books/book-10"])
                    .replace_with(|e| e.field("awards"), FirestoreReplaceWithMode::FullReplace)
                    .into_pipeline(),
            ),
            (
                "stage-replace-with",
                Some("merge_overwrite_existing"),
                builder()
                    .documents(["pipeline-books/book-10"])
                    .replace_with(
                        |e| {
                            e.constant(Overwrite {
                                title: "Overwritten",
                                extra: 1,
                            })
                        },
                        FirestoreReplaceWithMode::MergeOverwriteExisting,
                    )
                    .select(|e| [e.field("title"), e.field("extra"), e.field("author")])
                    .into_pipeline(),
            ),
            (
                "stage-let-variable",
                None,
                builder()
                    .literals([HashMap::from([("a", 1)])])
                    .define(|e| [e.constant(10).alias("threshold")])
                    .add_fields(|e| {
                        [e.field("a")
                            .greater_than(e.variable("threshold"))
                            .alias("over")]
                    })
                    .into_pipeline(),
            ),
            (
                "join-array-subquery",
                None,
                builder()
                    .from("pipeline-restaurants")
                    .filter(|q| q.field("cuisine").eq("breakfast"))
                    .define(|e| [e.field("__name__").alias("rname")])
                    .select(|e| {
                        [
                            FirestoreSelection::from(e.field("name")),
                            e.array_subquery(|p| {
                                p.from("reviews")
                                    .all_descendants()
                                    .filter(|q| q.field("restaurant").eq(q.variable("rname")))
                                    .sort(|e| [e.field("rating").desc()])
                                    .select(|e| [e.field("rating"), e.field("reviewer")])
                            })
                            .alias("reviews"),
                        ]
                    })
                    .into_pipeline(),
            ),
            (
                "join-scalar-subquery",
                None,
                builder()
                    .from("pipeline-restaurants")
                    .filter(|q| q.field("cuisine").eq("breakfast"))
                    .define(|e| [e.field("__name__").alias("rname")])
                    .select(|e| {
                        [
                            FirestoreSelection::from(e.field("name")),
                            e.scalar_subquery(|p| {
                                p.from("reviews")
                                    .all_descendants()
                                    .filter(|q| q.field("restaurant").eq(q.variable("rname")))
                                    .aggregate(|e| [e.field("rating").average().alias("avg")])
                            })
                            .alias("avg_rating"),
                        ]
                    })
                    .into_pipeline(),
            ),
            (
                "join-subcollection",
                None,
                builder()
                    .from("pipeline-restaurants")
                    .filter(|q| q.field("cuisine").eq("pizza"))
                    .select(|e| {
                        [
                            FirestoreSelection::from(e.field("name")),
                            e.array_subquery(|p| {
                                p.subcollection("reviews").select(|e| [e.field("rating")])
                            })
                            .alias("reviews"),
                            e.raw(
                                "array_length",
                                [e.array_subquery(|p| p.subcollection("reviews"))],
                            )
                            .alias("review_count"),
                        ]
                    })
                    .into_pipeline(),
            ),
            (
                "join-rules",
                Some("scalar_in_where"),
                builder()
                    .from("pipeline-restaurants")
                    .define(|e| [e.field("__name__").alias("rname")])
                    .filter(|q| {
                        q.expr(q.values().scalar_subquery(|p| {
                            p.from("reviews")
                                .all_descendants()
                                .filter(|q| q.field("restaurant").eq(q.variable("rname")))
                                .aggregate(|e| [e.count_all().alias("c")])
                        }))
                        .greater_than(1)
                    })
                    .select(|e| [e.field("name")])
                    .into_pipeline(),
            ),
            (
                "join-rules",
                Some("subquery_in_sort"),
                builder()
                    .from("pipeline-restaurants")
                    .filter(|q| q.field("cuisine").eq("breakfast"))
                    .define(|e| [e.field("__name__").alias("rname")])
                    .sort(|e| {
                        [e.scalar_subquery(|p| {
                            p.from("reviews")
                                .all_descendants()
                                .filter(|q| q.field("restaurant").eq(q.variable("rname")))
                                .aggregate(|e| [e.count_all().alias("c")])
                        })
                        .desc()]
                    })
                    .select(|e| [e.field("name")])
                    .into_pipeline(),
            ),
            (
                "join-rules",
                Some("unnest_array_subquery"),
                builder()
                    .documents(["pipeline-restaurants/rest-01"])
                    .define(|e| [e.field("__name__").alias("rname")])
                    .unnest(|e| {
                        e.array_subquery(|p| {
                            p.from("reviews")
                                .all_descendants()
                                .filter(|q| q.field("restaurant").eq(q.variable("rname")))
                                .select(|e| [e.field("rating")])
                        })
                        .alias("review")
                    })
                    .select(|e| [e.field("name"), e.field("review")])
                    .into_pipeline(),
            ),
        ];

        for (fixture, case, model) in captured {
            assert_eq!(
                Pipeline::try_from(model).unwrap(),
                pipeline(&fixture_case(fixture, case)),
                "{fixture} {case:?}"
            );
        }
    }

    #[test]
    fn stages_that_cannot_be_sent_are_rejected_before_sending() {
        let rejected = [
            builder()
                .from("pipeline-books")
                .aggregate(|e| {
                    [
                        e.count_all().alias("books"),
                        e.field("stock").sum().alias("books"),
                    ]
                })
                .into_pipeline(),
            builder()
                .from("pipeline-books")
                .distinct(|e| [e.raw("to_upper", [e.field("genre")])])
                .into_pipeline(),
            builder()
                .from("pipeline-books")
                .unnest(|e| e.raw("split", [e.field("title")]))
                .into_pipeline(),
            builder()
                .from("pipeline-books")
                .select(|e| {
                    [e.array_subquery(|p| {
                        p.subcollection("reviews")
                            .select(|e| [e.raw("to_upper", [e.field("name")])])
                    })
                    .alias("reviews")]
                })
                .into_pipeline(),
        ];

        for model in rejected {
            let result = Pipeline::try_from(model.clone());
            assert!(
                matches!(result, Err(FirestoreError::InvalidParametersError(_))),
                "{model:?}: {result:?}"
            );
        }
    }
}
