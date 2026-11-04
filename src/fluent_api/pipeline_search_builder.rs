use crate::{
    FirestoreGeoSearch, FirestoreLatLng, FirestoreSearch, FirestoreSearchQuery, FirestoreTextQuery,
    FirestoreTextQueryClause, FirestoreTextSearch,
};

/// Picks what a search looks for: text with [`text`](Self::text), or a distance from a point
/// with [`geo_within`](Self::geo_within).
///
/// The argument of the closure of `.search(|s| ..)` on a pipeline builder.
#[derive(Debug, Clone, Copy)]
pub struct FirestorePipelineSearchBuilder;

/// Sets the options of a search. `Q` is the kind of search, which decides the ordering on offer:
/// [`FirestoreTextSearch`] sorts by score and [`FirestoreGeoSearch`] by distance.
#[derive(Debug, Clone)]
pub struct FirestorePipelineSearchQueryBuilder<Q> {
    query: Q,
    limit: Option<u32>,
    offset: Option<u32>,
    retrieval_depth: Option<u32>,
    language_code: Option<String>,
}

/// Builds the text of a text search from terms, phrases and exclusions.
///
/// It mirrors [`FirestorePipelineFilterBuilder`](crate::pipeline_filter_builder::FirestorePipelineFilterBuilder):
/// every clause is an `Option`, so a list may mix plain and optional clauses.
///
/// The argument of the closure of [`text`](FirestorePipelineSearchBuilder::text).
#[derive(Debug, Clone, Copy)]
pub struct FirestoreTextQueryBuilder;

impl FirestorePipelineSearchBuilder {
    /// Searches the indexed text fields for the query `query` builds. The closure may also
    /// return a string, which is sent as written. A search whose query is `None`, or has no
    /// clause that must match, fails when the pipeline is converted, as Firestore would match
    /// nothing.
    pub fn text<FN, Q>(self, query: FN) -> FirestorePipelineSearchQueryBuilder<FirestoreTextSearch>
    where
        FN: FnOnce(FirestoreTextQueryBuilder) -> Q,
        Q: Into<FirestoreTextQuery>,
    {
        FirestorePipelineSearchQueryBuilder::new(FirestoreTextSearch::new(
            query(FirestoreTextQueryBuilder).into(),
        ))
    }

    /// Searches for documents whose geo point field `field` lies at most `max_distance_meters`
    /// from `center`.
    pub fn geo_within<F: AsRef<str>>(
        self,
        field: F,
        center: FirestoreLatLng,
        max_distance_meters: f64,
    ) -> FirestorePipelineSearchQueryBuilder<FirestoreGeoSearch> {
        FirestorePipelineSearchQueryBuilder::new(FirestoreGeoSearch::new(
            field.as_ref().to_string(),
            center,
            max_distance_meters,
        ))
    }
}

impl<Q> FirestorePipelineSearchQueryBuilder<Q> {
    fn new(query: Q) -> Self {
        Self {
            query,
            limit: None,
            offset: None,
            retrieval_depth: None,
            language_code: None,
        }
    }

    /// Keeps the first `limit` results.
    pub fn limit(self, limit: u32) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    /// Skips the first `offset` results. It needs a [`limit`](Self::limit).
    pub fn offset(self, offset: u32) -> Self {
        Self {
            offset: Some(offset),
            ..self
        }
    }

    /// Reads at most `depth` index entries before scoring and sorting. It must be at least 1.
    pub fn retrieval_depth(self, depth: u32) -> Self {
        Self {
            retrieval_depth: Some(depth),
            ..self
        }
    }

    /// The language of the query as a BCP 47 tag, such as `en`.
    pub fn language_code<S: Into<String>>(self, language_code: S) -> Self {
        Self {
            language_code: Some(language_code.into()),
            ..self
        }
    }
}

impl FirestorePipelineSearchQueryBuilder<FirestoreTextSearch> {
    /// Orders the results by relevance, the most relevant first. Firestore accepts no other
    /// order.
    pub fn sort_by_score(self) -> Self {
        Self {
            query: self.query.with_sort_by_score(true),
            ..self
        }
    }

    /// Outputs each document's relevance score under the field `field`, where a later stage can
    /// read it.
    pub fn score_field<S: Into<String>>(self, field: S) -> Self {
        Self {
            query: self.query.with_score_field(field.into()),
            ..self
        }
    }
}

impl FirestorePipelineSearchQueryBuilder<FirestoreGeoSearch> {
    /// Orders the results by their distance from the center, the nearest first. Firestore
    /// accepts no other order.
    pub fn sort_by_distance(self) -> Self {
        Self {
            query: self.query.with_sort_by_distance(true),
            ..self
        }
    }
}

impl<Q> From<FirestorePipelineSearchQueryBuilder<Q>> for FirestoreSearch
where
    Q: Into<FirestoreSearchQuery>,
{
    fn from(builder: FirestorePipelineSearchQueryBuilder<Q>) -> Self {
        FirestoreSearch::new(builder.query.into())
            .opt_limit(builder.limit)
            .opt_offset(builder.offset)
            .opt_retrieval_depth(builder.retrieval_depth)
            .opt_language_code(builder.language_code)
    }
}

impl FirestoreTextQueryBuilder {
    /// A word the document must contain, always `Some`. Matching ignores case and accents and
    /// finds other forms of the word: `waffle` finds "Waffles". A word that the service would
    /// read as more than one term or as an operator is quoted, which matches it as written,
    /// without other forms.
    pub fn term<S: Into<String>>(&self, word: S) -> Option<FirestoreTextQueryClause> {
        Some(FirestoreTextQueryClause::term(word.into()))
    }

    /// Words the document must contain in this order, always `Some`, matched as written,
    /// without other forms of the words. Punctuation is ignored, and a quote character is
    /// replaced by a space.
    pub fn phrase<S: Into<String>>(&self, words: S) -> Option<FirestoreTextQueryClause> {
        Some(FirestoreTextQueryClause::phrase(words.into()))
    }

    /// Requires the term or phrase of `clause` to be absent from the document. A `None` stays
    /// `None`.
    pub fn exclude(
        &self,
        clause: Option<FirestoreTextQueryClause>,
    ) -> Option<FirestoreTextQueryClause> {
        clause.map(FirestoreTextQueryClause::excluded)
    }

    /// Requires every clause present to hold. The items are all clauses or all `Option`s of
    /// one, and a `None` drops out. None present gives `None`, which `text` rejects when the
    /// pipeline is converted, as does a query with no clause that must match.
    pub fn for_all<I>(&self, clauses: I) -> Option<FirestoreTextQuery>
    where
        I: IntoIterator,
        I::Item: FirestoreTextQueryItem,
    {
        let present: Vec<FirestoreTextQueryClause> = clauses
            .into_iter()
            .filter_map(FirestoreTextQueryItem::build_clause)
            .collect();
        (!present.is_empty()).then(|| present.into_iter().collect())
    }
}

/// A clause, or an `Option` of one, as [`for_all`](FirestoreTextQueryBuilder::for_all) takes
/// them.
pub trait FirestoreTextQueryItem {
    /// The clause, or `None` for an item that stands for no clause.
    fn build_clause(self) -> Option<FirestoreTextQueryClause>;
}

impl FirestoreTextQueryItem for FirestoreTextQueryClause {
    fn build_clause(self) -> Option<FirestoreTextQueryClause> {
        Some(self)
    }
}

impl<C: FirestoreTextQueryItem> FirestoreTextQueryItem for Option<C> {
    fn build_clause(self) -> Option<FirestoreTextQueryClause> {
        self.and_then(FirestoreTextQueryItem::build_clause)
    }
}
