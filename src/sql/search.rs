use super::expr::chain;
use super::scope::FirestoreSqlScope;
use super::translator::{expr_start, name_start, FirestoreSqlTranslator};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::{
    FirestoreExpr, FirestoreGeoPoint, FirestoreGeoSearch, FirestoreLatLng, FirestorePipeline,
    FirestorePipelineSource, FirestoreResult, FirestoreSearch, FirestoreSearchQuery,
    FirestoreTextQuery, FirestoreTextSearch,
};
use gcloud_sdk::google::firestore::v1::value::ValueType;
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, OrderBy, OrderByExpr, OrderByKind, OrderByOptions,
    OrderBySort,
};

const SQL_SCORE_FIELD: &str = "_score";

impl FirestoreSqlTranslator<'_> {
    // Splits the `WHERE` of a query into the search it holds, which belongs to the source, and
    // the condition that remains.
    //
    // A search is a top-level `AND` condition: `document_matches(query)`, or
    // `geo_distance(field, point) <= meters`. Firestore accepts it only right after a collection
    // or collection group source, so it cannot sit under `OR` or `NOT`.
    pub(super) fn split_search(
        &mut self,
        scope: &FirestoreSqlScope,
        pipeline: &FirestorePipeline,
        selection: &Expr,
    ) -> FirestoreResult<(Option<FirestoreSearchQuery>, Option<FirestoreExpr>)> {
        let mut searches = Vec::new();
        let mut rest = Vec::new();
        for conjunct in chain(selection, &BinaryOperator::And) {
            let unparenthesised = unparenthesised(conjunct);
            match search_condition(unparenthesised) {
                Some((name, function)) => searches.push((name, function, unparenthesised)),
                None => rest.push(conjunct),
            }
        }
        let Some(&(name, function, condition)) = searches.first() else {
            return Ok((None, Some(self.expr(scope, selection)?)));
        };
        if let Some(&(other, _, other_condition)) = searches.get(1) {
            return Err(FirestoreError::invalid_sql(
                other,
                "a query holds one search, and Firestore cannot combine two",
                expr_start(other_condition),
            ));
        }
        if !pipeline.stages.is_empty()
            || !matches!(
                pipeline.source,
                FirestorePipelineSource::Collection(_)
                    | FirestorePipelineSource::CollectionGroup(_)
            )
        {
            return Err(FirestoreError::invalid_sql(
                name,
                "a search reads a collection or a collection group directly, before any join or other stage",
                expr_start(condition),
            ));
        }
        let search = match name {
            "document_matches" => self.text_search(scope, function)?,
            _ => self.geo_search(scope, condition, function)?,
        };
        let remaining = if rest.is_empty() {
            None
        } else {
            Some(self.conjunction(scope, &rest)?)
        };
        Ok((Some(search), remaining))
    }

    fn text_search(
        &mut self,
        scope: &FirestoreSqlScope,
        function: &Function,
    ) -> FirestoreResult<FirestoreSearchQuery> {
        let (_, arguments) = Self::call(function)?;
        let start = name_start(&function.name);
        let [query] = Self::positional_arguments(arguments, start)?[..] else {
            return Err(FirestoreError::invalid_sql(
                "document_matches",
                "document_matches takes the query to search for",
                start,
            ));
        };
        match self
            .search_value(scope, query, "document_matches")?
            .constant_value()
        {
            Some(ValueType::StringValue(query)) => Ok(FirestoreSearchQuery::Text(
                FirestoreTextSearch::new(FirestoreTextQuery::from(query.clone())),
            )),
            _ => Err(FirestoreError::invalid_sql(
                "document_matches",
                "document_matches takes a string literal or parameter: the query is never computed",
                expr_start(query),
            )),
        }
    }

    fn geo_search(
        &mut self,
        scope: &FirestoreSqlScope,
        condition: &Expr,
        function: &Function,
    ) -> FirestoreResult<FirestoreSearchQuery> {
        let Expr::BinaryOp {
            op: BinaryOperator::LtEq,
            right,
            ..
        } = condition
        else {
            return Err(Self::search_misplaced(
                "geo_distance",
                expr_start(condition),
            ));
        };
        let (field, center) = self.geo_arguments(scope, function)?;
        let max_distance_meters = self.number(scope, right)?;
        Ok(FirestoreSearchQuery::Geo(FirestoreGeoSearch::new(
            field,
            center,
            max_distance_meters,
        )))
    }

    fn geo_arguments(
        &mut self,
        scope: &FirestoreSqlScope,
        function: &Function,
    ) -> FirestoreResult<(String, FirestoreLatLng)> {
        let (_, arguments) = Self::call(function)?;
        let start = name_start(&function.name);
        let [field, point] = Self::positional_arguments(arguments, start)?[..] else {
            return Err(FirestoreError::invalid_sql(
                "geo_distance",
                "geo_distance takes a geo point field and a point",
                start,
            ));
        };
        let FirestoreExpr::Field(field) = self.expr(scope, field)? else {
            return Err(FirestoreError::invalid_sql(
                "geo_distance",
                "geo_distance takes a geo point field as its first argument",
                expr_start(field),
            ));
        };
        let center =
            match point {
                Expr::Function(call) if called(call, "geo_point") => {
                    let (_, coordinates) = Self::call(call)?;
                    let point_start = name_start(&call.name);
                    let [latitude, longitude] =
                        Self::positional_arguments(coordinates, point_start)?[..]
                    else {
                        return Err(FirestoreError::invalid_sql(
                            "geo_point",
                            "geo_point takes a latitude and a longitude",
                            point_start,
                        ));
                    };
                    FirestoreLatLng(FirestoreGeoPoint {
                        latitude: self.number(scope, latitude)?,
                        longitude: self.number(scope, longitude)?,
                    })
                }
                _ => match self
                    .search_value(scope, point, "geo_distance")?
                    .constant_value()
                {
                    Some(ValueType::GeoPointValue(point)) => FirestoreLatLng(FirestoreGeoPoint {
                        latitude: point.latitude,
                        longitude: point.longitude,
                    }),
                    _ => return Err(FirestoreError::invalid_sql(
                        "geo_distance",
                        "geo_distance takes geo_point(latitude, longitude) or a bound geo point",
                        expr_start(point),
                    )),
                },
            };
        Ok((field, center))
    }

    fn number(&mut self, scope: &FirestoreSqlScope, expr: &Expr) -> FirestoreResult<f64> {
        match self
            .search_value(scope, expr, "geo_distance")?
            .constant_value()
        {
            Some(ValueType::DoubleValue(number)) => Ok(*number),
            Some(ValueType::IntegerValue(number)) => Ok(*number as f64),
            _ => Err(FirestoreError::invalid_sql(
                "geo_distance",
                "a distance and a coordinate are number literals or parameters",
                expr_start(expr),
            )),
        }
    }

    // The value of `expr`, which the search is built from rather than computed at run time.
    fn search_value(
        &mut self,
        scope: &FirestoreSqlScope,
        expr: &Expr,
        construct: &str,
    ) -> FirestoreResult<FirestoreExpr> {
        let value = self.expr(scope, expr)?;
        value.require_value(construct, expr_start(expr))?;
        Ok(value)
    }

    pub(super) fn score(
        &mut self,
        arguments: &[FunctionArg],
        output: Option<&str>,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreExpr> {
        if !arguments.is_empty() {
            return Err(FirestoreError::invalid_sql(
                "score",
                "score takes no arguments",
                start,
            ));
        }
        let Some(FirestoreSearchQuery::Text(search)) = self.search.as_mut() else {
            return Err(FirestoreError::invalid_sql(
                "score",
                "score() reads the relevance of a text search, which needs document_matches(..) in the WHERE",
                start,
            ));
        };
        let field = search
            .score_field
            .get_or_insert_with(|| output.unwrap_or(SQL_SCORE_FIELD).to_string());
        Ok(FirestoreExpr::Field(field.clone()))
    }

    // Whether the `ORDER BY` is the order of the search, which the search itself applies:
    // `score() DESC` for a text search, and the distance it filters on for a geo search. It must
    // be the only key, since Firestore keeps neither the score nor the distance outside the
    // search; a score ordered with other keys is selected under a name and ordered by that name.
    pub(super) fn search_order(
        &mut self,
        scope: &FirestoreSqlScope,
        order_by: &OrderBy,
    ) -> FirestoreResult<bool> {
        let OrderByKind::Expressions(keys) = &order_by.kind else {
            return Ok(false);
        };
        let ordering_call = keys.iter().find_map(|key| match &key.expr {
            Expr::Function(call) if called(call, "score") || called(call, "geo_distance") => {
                Some((key, call))
            }
            _ => None,
        });
        let Some((ordering_key, call)) = ordering_call.filter(|_| self.search.is_some()) else {
            return Ok(false);
        };
        let name = if called(call, "score") {
            "score"
        } else {
            "geo_distance"
        };
        let start = expr_start(&ordering_key.expr);
        let [OrderByExpr {
            options: OrderByOptions { sort, nulls_first },
            with_fill,
            ..
        }] = keys.as_slice()
        else {
            return Err(FirestoreError::invalid_sql(
                name,
                format!("{name}() orders a search only as the only ORDER BY key"),
                start,
            ));
        };
        let ascending = matches!(sort, None | Some(OrderBySort::Asc));
        let ordered_as_searched = match name {
            "score" => matches!(sort, Some(OrderBySort::Desc)),
            _ => ascending,
        };
        if nulls_first.is_some() || with_fill.is_some() || !ordered_as_searched {
            return Err(FirestoreError::invalid_sql(
                name,
                "a search orders by score() DESC, the most relevant first, or by the distance it filters on, the nearest first",
                start,
            ));
        }
        let ordered = match name {
            "geo_distance" => Some(self.geo_arguments(scope, call)?),
            _ => None,
        };
        match (self.search.as_mut(), ordered) {
            (Some(FirestoreSearchQuery::Text(search)), None)
                if Self::call(call)?.1.is_empty() =>
            {
                search.sort_by_score = true
            }
            (Some(FirestoreSearchQuery::Geo(search)), Some((field, center)))
                if field == search.field && center == search.center =>
            {
                search.sort_by_distance = true
            }
            _ => {
                return Err(FirestoreError::invalid_sql(
                    name,
                    "a search orders by the score of a text search, or by the distance a geo search filters on",
                    start,
                ))
            }
        }
        Ok(true)
    }

    pub(super) fn install_search(
        source: &mut FirestorePipelineSource,
        search: FirestoreSearchQuery,
    ) {
        let search = FirestoreSearch::new(search);
        match source {
            FirestorePipelineSource::Collection(source) => source.search = Some(search),
            FirestorePipelineSource::CollectionGroup(source) => source.search = Some(search),
            _ => {}
        }
    }

    pub(super) fn search_misplaced(
        name: &str,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreError {
        FirestoreError::invalid_sql(
            name,
            "a search is a top-level AND condition of the WHERE: document_matches(query), or geo_distance(field, point) <= meters",
            start,
        )
    }
}

fn unparenthesised(mut expr: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    expr
}

fn search_condition(condition: &Expr) -> Option<(&'static str, &Function)> {
    match condition {
        Expr::Function(call) if called(call, "document_matches") => {
            Some(("document_matches", call))
        }
        Expr::BinaryOp { left, .. } => match left.as_ref() {
            Expr::Function(call) if called(call, "geo_distance") => Some(("geo_distance", call)),
            _ => None,
        },
        _ => None,
    }
}

fn called(call: &Function, name: &str) -> bool {
    FirestoreSqlTranslator::function_name(&call.name, None).is_ok_and(|called| called == name)
}
