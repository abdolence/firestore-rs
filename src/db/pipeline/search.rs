use super::expr::{selection_map, wire_value};
use super::model::stage;
use crate::{
    FirestoreError, FirestoreExpr, FirestoreLatLng, FirestorePipelineOrdering, FirestoreResult,
    FirestoreSelection, FirestoreTextQuery,
};
use gcloud_sdk::google::firestore::v1::{pipeline::Stage, value::ValueType, ArrayValue, Value};
use rsb_derive::Builder;
use std::collections::HashMap;

/// A full-text or geo search of a collection or collection group, run by the `search` stage
/// right after the source. It needs a search index that covers the query.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreSearch {
    pub query: FirestoreSearchQuery,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    /// The number of index entries read before scoring and sorting.
    pub retrieval_depth: Option<u32>,
    /// The language of the query as a BCP 47 tag, such as `en`.
    pub language_code: Option<String>,
}

/// What a search looks for. Firestore accepts these two and cannot combine them.
#[derive(Debug, Clone, PartialEq)]
pub enum FirestoreSearchQuery {
    Text(FirestoreTextSearch),
    Geo(FirestoreGeoSearch),
}

/// A search for documents whose indexed text fields match a query.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreTextSearch {
    pub query: FirestoreTextQuery,
    /// Orders the results by relevance, the most relevant first, which is the only order
    /// Firestore accepts. Without it the order is unspecified.
    #[default = "false"]
    pub sort_by_score: bool,
    /// Outputs each document's relevance score under this field.
    pub score_field: Option<String>,
}

/// A search for documents whose geo point field lies within a distance of a point.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreGeoSearch {
    /// The path of the geo point field.
    pub field: String,
    pub center: FirestoreLatLng,
    pub max_distance_meters: f64,
    /// Orders the results by their distance from `center`, the nearest first, which is the only
    /// order Firestore accepts.
    #[default = "false"]
    pub sort_by_distance: bool,
}

impl From<FirestoreTextSearch> for FirestoreSearchQuery {
    fn from(search: FirestoreTextSearch) -> Self {
        Self::Text(search)
    }
}

impl From<FirestoreGeoSearch> for FirestoreSearchQuery {
    fn from(search: FirestoreGeoSearch) -> Self {
        Self::Geo(search)
    }
}

impl TryFrom<FirestoreSearch> for Stage {
    type Error = FirestoreError;

    fn try_from(search: FirestoreSearch) -> Result<Self, Self::Error> {
        let mut options = HashMap::<String, Value>::try_from(search.query)?;
        let integers = [
            ("limit", search.limit),
            ("offset", search.offset),
            ("retrieval_depth", search.retrieval_depth),
        ];
        options.extend(integers.into_iter().filter_map(|(name, value)| {
            let value = wire_value(ValueType::IntegerValue(value?.into()));
            Some((name.to_string(), value))
        }));
        if let Some(language_code) = search.language_code {
            options.insert(
                "language_code".to_string(),
                wire_value(ValueType::StringValue(language_code)),
            );
        }
        Ok(Stage {
            options,
            ..stage("search", Vec::new())
        })
    }
}

impl TryFrom<FirestoreSearchQuery> for HashMap<String, Value> {
    type Error = FirestoreError;

    fn try_from(query: FirestoreSearchQuery) -> Result<Self, Self::Error> {
        match query {
            FirestoreSearchQuery::Text(search) => search.try_into(),
            FirestoreSearchQuery::Geo(search) => search.try_into(),
        }
    }
}

impl TryFrom<FirestoreTextSearch> for HashMap<String, Value> {
    type Error = FirestoreError;

    fn try_from(search: FirestoreTextSearch) -> Result<Self, Self::Error> {
        search.query.ensure_matchable()?;
        let matches = FirestoreExpr::raw("document_matches", [search.query.to_string()]);
        let mut options = HashMap::from([("query".to_string(), matches.try_into()?)]);
        if search.sort_by_score {
            options.insert("sort".to_string(), sorted_by(score().desc())?);
        }
        if let Some(field) = search.score_field {
            options.insert(
                "add_fields".to_string(),
                selection_map(
                    "search",
                    vec![FirestoreSelection::new(score()).with_alias(field)],
                )?,
            );
        }
        Ok(options)
    }
}

impl TryFrom<FirestoreGeoSearch> for HashMap<String, Value> {
    type Error = FirestoreError;

    fn try_from(search: FirestoreGeoSearch) -> Result<Self, Self::Error> {
        let distance = FirestoreExpr::raw(
            "geo_distance",
            [
                FirestoreExpr::field(search.field),
                FirestoreExpr::constant(search.center),
            ],
        );
        let within = FirestoreExpr::raw(
            "less_than_or_equal",
            [
                distance.clone(),
                FirestoreExpr::constant(search.max_distance_meters),
            ],
        );
        let mut options = HashMap::from([("query".to_string(), within.try_into()?)]);
        if search.sort_by_distance {
            options.insert("sort".to_string(), sorted_by(distance.asc())?);
        }
        Ok(options)
    }
}

/// `score()` is valid only inside the `search` stage, so it is not a public expression.
fn score() -> FirestoreExpr {
    FirestoreExpr::raw("score", Vec::<FirestoreExpr>::new())
}

/// The `sort` option: an array of orderings, even for one.
fn sorted_by(ordering: FirestorePipelineOrdering) -> FirestoreResult<Value> {
    Ok(wire_value(ValueType::ArrayValue(ArrayValue {
        values: vec![ordering.try_into()?],
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pipeline::fixtures::{fixture_case, pipeline};
    use crate::pipeline_builder::FirestorePipelineInitialBuilder;
    use crate::pipeline_search_builder::{FirestoreTextQueryBuilder, FirestoreTextQueryInput};
    use crate::FirestoreTextQueryClause;
    use crate::{FirestoreGeoPoint, FirestorePipeline};
    use gcloud_sdk::google::firestore::v1::Pipeline;

    const RESTAURANTS: &str = "pipeline-restaurants";

    fn builder() -> FirestorePipelineInitialBuilder<()> {
        FirestorePipelineInitialBuilder::new(())
    }

    fn center() -> FirestoreLatLng {
        FirestoreLatLng(FirestoreGeoPoint {
            latitude: 38.989177,
            longitude: -107.065076,
        })
    }

    fn searched_for_text(query: &str) -> FirestorePipeline {
        builder()
            .from(RESTAURANTS)
            .search(|s| s.text(|_| query))
            .fields(|e| [e.field("name")])
            .into_pipeline()
    }

    #[test]
    fn searches_convert_to_the_captured_requests() {
        let captured = [
            (
                "search-options",
                "full",
                builder()
                    .from(RESTAURANTS)
                    .search(|s| {
                        s.text(|t| t.term("waffles"))
                            .sort_by_score()
                            .score_field("score")
                            .limit(2)
                            .offset(1)
                            .retrieval_depth(100)
                            .language_code("en")
                    })
                    .fields(|e| [e.field("name"), e.field("score")])
                    .into_pipeline(),
            ),
            (
                "search-options",
                "default_order",
                searched_for_text("waffles"),
            ),
            (
                "search-options",
                "add_fields_score_alias",
                builder()
                    .from(RESTAURANTS)
                    .search(|s| s.text(|t| t.term("waffles")).score_field("relevance"))
                    .fields(|e| [e.field("name"), e.field("relevance")])
                    .into_pipeline(),
            ),
            (
                "search-options",
                "limit_stage_after_search",
                builder()
                    .from(RESTAURANTS)
                    .search(|s| s.text(|t| t.term("waffles")).sort_by_score())
                    .limit(1)
                    .fields(|e| [e.field("name")])
                    .into_pipeline(),
            ),
            (
                "search-geo",
                "sort_by_distance",
                builder()
                    .from(RESTAURANTS)
                    .search(|s| {
                        s.geo_within("location", center(), 5000.0)
                            .sort_by_distance()
                    })
                    .fields(|e| [e.field("name")])
                    .into_pipeline(),
            ),
            (
                "search-geo",
                "less_than_or_equal",
                builder()
                    .from(RESTAURANTS)
                    .search(|s| s.geo_within("location", center(), 1000.0))
                    .fields(|e| [e.field("name")])
                    .into_pipeline(),
            ),
            (
                "search-missing-index",
                "collection_group_with_collection_scoped_index",
                builder()
                    .from(RESTAURANTS)
                    .all_descendants()
                    .search(|s| s.text(|t| t.term("waffles")))
                    .fields(|e| [e.field("name")])
                    .into_pipeline(),
            ),
        ];

        for (fixture, case, model) in captured {
            assert_eq!(
                Pipeline::try_from(model).unwrap(),
                pipeline(&fixture_case(fixture, Some(case))),
                "{fixture} {case}"
            );
        }
    }

    #[test]
    fn text_queries_built_from_clauses_convert_to_the_captured_query_strings() {
        let t = FirestoreTextQueryBuilder;
        let captured: [(&str, FirestoreTextQuery); 14] = [
            ("term", t.term("waffles").build_query()),
            ("phrase", t.phrase("maple syrup").build_query()),
            ("phrase-belgian", t.phrase("belgian waffles").build_query()),
            (
                "phrase-wrong-order",
                t.phrase("waffles belgian").build_query(),
            ),
            (
                "and",
                t.for_all([t.term("waffles"), t.term("bacon")])
                    .build_query(),
            ),
            (
                "exclude",
                t.for_all([t.term("waffles"), t.exclude(t.term("bacon"))])
                    .build_query(),
            ),
            (
                "exclude-phrase",
                t.for_all([t.term("pizza"), t.exclude(t.phrase("New York"))])
                    .build_query(),
            ),
            ("case", t.term("WAFFLES").build_query()),
            ("diacritic", t.term("cr\u{ea}pes").build_query()),
            ("stem", t.term("waffle").build_query()),
            ("hyphen-term", t.term("gluten-free").build_query()),
            ("hyphen-phrase", t.phrase("gluten-free").build_query()),
            (
                "quoted-phrase-with-colon",
                t.phrase("field:menu").build_query(),
            ),
            ("tilde", t.term("waffles~").build_query()),
        ];

        for (case, query) in captured {
            let model = builder()
                .from(RESTAURANTS)
                .search(|s| s.text(|_| query))
                .fields(|e| [e.field("name")])
                .into_pipeline();
            assert_eq!(
                Pipeline::try_from(model).unwrap(),
                pipeline(&fixture_case("search-text-grammar", Some(case))),
                "{case}"
            );
        }
    }

    #[test]
    fn for_all_renders_the_clauses_that_are_present() {
        let t = FirestoreTextQueryBuilder;
        let excluded_word = Some("bacon");
        let no_excluded_word: Option<&str> = None;

        let mixed = t.for_all([
            t.term("waffles"),
            excluded_word.and_then(|word| t.exclude(t.term(word))),
            no_excluded_word.and_then(|word| t.exclude(t.term(word))),
        ]);

        assert_eq!(mixed.unwrap().to_string(), "waffles -bacon");
    }

    #[test]
    fn raw_query_strings_convert_to_the_captured_query_strings() {
        let captured = [
            ("or", "waffles OR crepes"),
            ("field-scoped", "menu:waffles"),
            ("prefix", "waff*"),
            ("unbalanced-quote", "\"maple syrup"),
            ("empty", ""),
        ];

        for (case, query) in captured {
            assert_eq!(
                Pipeline::try_from(searched_for_text(query)).unwrap(),
                pipeline(&fixture_case("search-text-grammar", Some(case))),
                "{case}"
            );
        }
    }

    #[test]
    fn searches_the_service_would_reject_are_rejected_before_sending() {
        let rejected = [
            builder()
                .from(RESTAURANTS)
                .search(|s| s.text(|t| t.exclude(t.term("bacon"))))
                .into_pipeline(),
            builder()
                .from(RESTAURANTS)
                .search(|s| s.text(|t| t.for_all(Vec::<Option<FirestoreTextQueryClause>>::new())))
                .into_pipeline(),
            builder()
                .from(RESTAURANTS)
                .search(|s| {
                    s.text(|t| t.for_all([None::<FirestoreTextQueryClause>, t.exclude(None)]))
                })
                .into_pipeline(),
            builder()
                .from(RESTAURANTS)
                .all_descendants()
                .search(|s| s.text(|t| t.phrase("")))
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
