use super::{FirestoreSqlArgument, CODEGEN_DOCUMENTS_PATH};
use crate::db::relative_to_documents;
use crate::errors::FirestoreError;
use crate::{
    FirestoreAggregateSelection, FirestoreAggregateStage, FirestoreCollectionGroupSource,
    FirestoreCollectionSource, FirestoreDistinctStage, FirestoreExpr, FirestoreFieldsStage,
    FirestoreFunction, FirestoreGeoSearch, FirestoreIndexHint, FirestoreLimitStage,
    FirestoreOffsetStage, FirestorePipeline, FirestorePipelineOrdering, FirestorePipelineSource,
    FirestorePipelineStage, FirestoreQueryDirection, FirestoreResult, FirestoreSampleSize,
    FirestoreSampleStage, FirestoreSearch, FirestoreSearchQuery, FirestoreSelection,
    FirestoreSortStage, FirestoreTextSearch, FirestoreValue, FirestoreWhereStage,
};
use gcloud_sdk::google::firestore::v1::value::ValueType;
use gcloud_sdk::google::firestore::v1::{ArrayValue, Value};
use std::collections::BTreeMap;

// Writes the fluent builder calls that build a pipeline model.
//
// Every model enum is matched in full, so a new source, stage or expression does not compile
// until it has an arm. Functions are named by a string, and one without a method of its own is
// written as `raw`, which builds the same node.
pub(super) struct FirestoreSqlEmitter<'a> {
    arguments: &'a [FirestoreSqlArgument],
}

// The closure an expression is written in. A filter's `q` starts values from other builders
// than `e`, since its own methods build `Option` conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FirestoreSqlClosure {
    Expr,
    Filter,
}

// What the code of an expression builds, which decides how it is converted where a call needs
// another kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FirestoreSqlRustKind {
    // A Rust value, such as `30` or an argument, that converts into a constant.
    Literal,
    Value,
    Condition,
}

struct FirestoreSqlRustExpr {
    code: String,
    kind: FirestoreSqlRustKind,
    closure: FirestoreSqlClosure,
}

// A call of a function as a method of its first argument, such as `a.add(b)`.
struct FirestoreSqlMethodCall<'f> {
    method: &'f str,
    kind: FirestoreSqlRustKind,
    receiver: &'f FirestoreExpr,
    arguments: &'f [FirestoreExpr],
}

impl FirestoreSqlClosure {
    // What starts a builder function, such as `and` or `conditional`.
    fn values(self) -> &'static str {
        match self {
            Self::Expr => "e",
            Self::Filter => "q.values()",
        }
    }

    fn field(self, path: &str) -> String {
        match self {
            Self::Expr => format!("e.field({path:?})"),
            Self::Filter => format!("FirestoreExpr::field({path:?})"),
        }
    }

    fn constant(self, literal: &str) -> String {
        match self {
            Self::Expr => format!("e.constant({literal})"),
            Self::Filter => format!("q.constant({literal})"),
        }
    }
}

impl FirestoreSqlRustExpr {
    // As `impl Into<FirestoreExpr>`, which most arguments take.
    fn into_argument(self) -> String {
        self.code
    }

    // As a `FirestoreExpr`, which a receiver and an item of a list need.
    fn into_expr(self) -> String {
        match self.kind {
            FirestoreSqlRustKind::Literal => self.closure.constant(&self.code),
            FirestoreSqlRustKind::Value => self.code,
            FirestoreSqlRustKind::Condition => format!("FirestoreExpr::from({})", self.code),
        }
    }

    // As a `FirestoreBoolExpr`.
    fn into_condition(self) -> String {
        match self.kind {
            FirestoreSqlRustKind::Condition => self.code,
            _ => format!("FirestoreBoolExpr::unchecked({})", self.into_expr()),
        }
    }
}

impl FirestoreFunction {
    // This call as a method of its first argument, when `FirestoreExpr` has one that builds
    // exactly this call. `equal(x, null)` is `is_null`'s definition.
    fn method_call(&self) -> Option<FirestoreSqlMethodCall<'_>> {
        let (receiver, arguments) = self.args.split_first()?;
        let (method, kind, arguments) = match (self.name.as_str(), arguments) {
            ("equal", [null]) if matches!(null.constant_value(), Some(ValueType::NullValue(_))) => {
                ("is_null", FirestoreSqlRustKind::Condition, &[][..])
            }
            (name, arguments) => {
                let (method, kind) = receiver_method(name, self.args.len())?;
                (method, kind, arguments)
            }
        };
        Some(FirestoreSqlMethodCall {
            method,
            kind,
            receiver,
            arguments,
        })
    }
}

impl<'a> FirestoreSqlEmitter<'a> {
    pub(super) fn new(arguments: &'a [FirestoreSqlArgument]) -> Self {
        Self { arguments }
    }

    pub(super) fn pipeline(
        &self,
        start: &str,
        pipeline: &FirestorePipeline,
    ) -> FirestoreResult<String> {
        let FirestorePipeline { source, stages } = pipeline;
        let mut code = format!("{start}{}", Self::source(source)?);
        for stage in stages {
            code.push_str(&self.stage(stage)?);
        }
        Ok(code)
    }

    fn source(source: &FirestorePipelineSource) -> FirestoreResult<String> {
        let untranslated = match source {
            FirestorePipelineSource::Collection(collection) => return Self::collection(collection),
            FirestorePipelineSource::CollectionGroup(group) => {
                return Self::collection_group(group)
            }
            // Codegen shows the sources of a plain `FROM` and leaves these out. No `SELECT`
            // translates to a literals source, which only an `INSERT` reads.
            FirestorePipelineSource::Database => "database()",
            FirestorePipelineSource::Documents(_) => "documents(..)",
            FirestorePipelineSource::Subcollection(_) => "subcollection(..)",
            FirestorePipelineSource::Literals(_) => "a literals source",
        };
        Err(FirestoreError::sql_codegen_refused(untranslated))
    }

    fn collection(source: &FirestoreCollectionSource) -> FirestoreResult<String> {
        let FirestoreCollectionSource {
            collection_id,
            parent,
            force_index,
            search,
        } = source;
        Self::refuse_index_hint(force_index)?;
        let parent = match parent {
            Some(parent) => Self::parent(parent),
            None => String::new(),
        };
        let search = Self::search(search.as_ref())?;
        Ok(format!(".from({collection_id:?}){parent}{search}"))
    }

    fn collection_group(source: &FirestoreCollectionGroupSource) -> FirestoreResult<String> {
        let FirestoreCollectionGroupSource {
            collection_id,
            force_index,
            search,
        } = source;
        Self::refuse_index_hint(force_index)?;
        let search = Self::search(search.as_ref())?;
        Ok(format!(
            ".from({collection_id:?}).all_descendants(){search}"
        ))
    }

    fn refuse_index_hint(hint: &Option<FirestoreIndexHint>) -> FirestoreResult<()> {
        match hint {
            Some(_) => Err(FirestoreError::sql_codegen_refused("an index hint")),
            None => Ok(()),
        }
    }

    // `.search(..)` of a source's search, which Firestore accepts only right after the source.
    fn search(search: Option<&FirestoreSearch>) -> FirestoreResult<String> {
        let Some(FirestoreSearch {
            query,
            limit,
            offset,
            retrieval_depth,
            language_code,
        }) = search
        else {
            return Ok(String::new());
        };
        // No SQL text sets these, so the snippet has no call for them.
        if limit.is_some()
            || offset.is_some()
            || retrieval_depth.is_some()
            || language_code.is_some()
        {
            return Err(FirestoreError::sql_codegen_refused("a search option"));
        }
        let code = match query {
            FirestoreSearchQuery::Text(FirestoreTextSearch {
                query,
                sort_by_score,
                score_field,
            }) => {
                // The query's text, which `s.text` takes as a string, is what the service receives.
                let mut code = format!("s.text(|_| {:?})", query.to_string());
                if *sort_by_score {
                    code.push_str(".sort_by_score()");
                }
                if let Some(field) = score_field {
                    code.push_str(&format!(".score_field({field:?})"));
                }
                code
            }
            FirestoreSearchQuery::Geo(FirestoreGeoSearch {
                field,
                center,
                max_distance_meters,
                sort_by_distance,
            }) => {
                let mut code = format!(
                    "s.geo_within({field:?}, FirestoreLatLng(FirestoreGeoPoint {{ latitude: {}, longitude: {} }}), {})",
                    Self::double(center.0.latitude),
                    Self::double(center.0.longitude),
                    Self::double(*max_distance_meters),
                );
                if *sort_by_distance {
                    code.push_str(".sort_by_distance()");
                }
                code
            }
        };
        Ok(format!(".search(|s| {code})"))
    }

    // `.parent(..)` of the document `parent`, built from the snippet's own `db`. The translator
    // wrote `parent` below the codegen documents path, as a document path.
    fn parent(parent: &str) -> String {
        let relative = relative_to_documents(CODEGEN_DOCUMENTS_PATH, parent).unwrap_or(parent);
        let segments: Vec<&str> = relative.split('/').collect();
        let documents: Vec<String> = segments
            .iter()
            .step_by(2)
            .zip(segments.iter().skip(1).step_by(2))
            .map(|(collection_id, document_id)| format!("({collection_id:?}, {document_id:?})?"))
            .collect();
        format!(".parent(db.parent_path{})", documents.join(".at"))
    }

    fn stage(&self, stage: &FirestorePipelineStage) -> FirestoreResult<String> {
        let untranslated = match stage {
            FirestorePipelineStage::Where(FirestoreWhereStage { condition }) => {
                return Ok(format!(".filter(|q| {})", self.filter(&condition.0)?))
            }
            FirestorePipelineStage::Fields(FirestoreFieldsStage { fields }) => {
                return Ok(format!(".fields(|e| [{}])", self.selections(fields)?))
            }
            FirestorePipelineStage::Sort(FirestoreSortStage { orderings }) => {
                return Ok(format!(".sort(|e| [{}])", self.orderings(orderings)?))
            }
            FirestorePipelineStage::Limit(FirestoreLimitStage { limit }) => {
                return Ok(format!(".limit({limit})"))
            }
            FirestorePipelineStage::Offset(FirestoreOffsetStage { offset }) => {
                return Ok(format!(".offset({offset})"))
            }
            FirestorePipelineStage::Sample(FirestoreSampleStage { size }) => match size {
                FirestoreSampleSize::Fraction(fraction) => {
                    return Ok(format!(
                        ".sample(FirestoreSampleSize::Fraction({}))",
                        Self::double(*fraction)
                    ))
                }
                // Only `TABLESAMPLE BERNOULLI` builds a sample, and it keeps a fraction.
                FirestoreSampleSize::Documents(_) => "a stage that no SELECT translates to",
            },
            FirestorePipelineStage::Aggregate(FirestoreAggregateStage {
                accumulators,
                groups,
            }) => return self.aggregate(accumulators, groups),
            FirestorePipelineStage::Distinct(FirestoreDistinctStage { groups }) => {
                return Ok(format!(".distinct(|e| [{}])", self.selections(groups)?))
            }
            // Codegen shows the stages of a plain `SELECT` and leaves these out. A `SELECT *`
            // with more columns gets an add_fields stage, and a correlated subquery gets
            // define_variables, as a join does.
            FirestorePipelineStage::AddFields(_) => "a JOIN or a column beside `*`",
            FirestorePipelineStage::RemoveFields(_) => "`* EXCEPT`",
            FirestorePipelineStage::Union(_) => "UNION",
            FirestorePipelineStage::Unnest(_) => "UNNEST",
            FirestorePipelineStage::DefineVariables(_) => "a JOIN or a correlated subquery",
            // No `SELECT` translates to these, which only the fluent API or a write builds.
            FirestorePipelineStage::Raw(_)
            | FirestorePipelineStage::FindNearest(_)
            | FirestorePipelineStage::ReplaceWith(_) => "a stage that no SELECT translates to",
        };
        Err(FirestoreError::sql_codegen_refused(untranslated))
    }

    fn aggregate(
        &self,
        accumulators: &[FirestoreAggregateSelection],
        groups: &[FirestoreSelection],
    ) -> FirestoreResult<String> {
        let accumulators = joined(
            accumulators
                .iter()
                .map(|selection| self.accumulator(selection)),
        )?;
        Ok(match groups.is_empty() {
            true => format!(".aggregate(|e| [{accumulators}])"),
            false => format!(
                ".aggregate_grouped(|e| [{}], |e| [{accumulators}])",
                self.selections(groups)?
            ),
        })
    }

    // An accumulator under its alias. Each has a method of `FirestoreExpr` with its wire name,
    // except a `count` of no argument.
    fn accumulator(
        &self,
        FirestoreAggregateSelection { aggregate, alias }: &FirestoreAggregateSelection,
    ) -> FirestoreResult<String> {
        let FirestoreFunction { name, args } = &aggregate.0;
        let closure = FirestoreSqlClosure::Expr;
        let call = match (name.as_str(), args.as_slice()) {
            ("count", []) => "e.count_all()".to_string(),
            ("count_if", [condition]) => format!(
                "{}.count_if()",
                self.expr(condition, closure)?.into_condition()
            ),
            (
                "count" | "count_distinct" | "sum" | "average" | "minimum" | "maximum" | "first"
                | "last" | "array_agg" | "array_agg_distinct",
                [argument],
            ) => format!("{}.{name}()", self.expr(argument, closure)?.into_expr()),
            _ => format!(
                "FirestoreAggregate::raw({name:?}, [{}])",
                self.list(args, closure, FirestoreSqlRustExpr::into_expr)?
            ),
        };
        Ok(format!("{call}.alias({alias:?})"))
    }

    // `condition` in the filter builder's own terms, as the `filter` closure returns it: an
    // `Option<FirestoreBoolExpr>`.
    fn filter(&self, condition: &FirestoreExpr) -> FirestoreResult<String> {
        let FirestoreExpr::Function(function) = condition else {
            return Ok(format!(
                "q.condition({})",
                self.expr(condition, FirestoreSqlClosure::Filter)?
                    .into_condition()
            ));
        };
        let connective = match function.name.as_str() {
            "and" => Some("for_all"),
            "or" => Some("for_any"),
            _ => None,
        };
        if let Some(connective) = connective {
            let conditions = joined(function.args.iter().map(|operand| self.filter(operand)))?;
            return Ok(format!("q.{connective}([{conditions}])"));
        }
        Ok(match function.method_call() {
            Some(FirestoreSqlMethodCall {
                method,
                kind: FirestoreSqlRustKind::Condition,
                receiver,
                arguments,
            }) => {
                let compared = match receiver {
                    FirestoreExpr::Field(path) => format!("q.field({path:?})"),
                    other => format!(
                        "q.expr({})",
                        self.expr(other, FirestoreSqlClosure::Filter)?.into_expr()
                    ),
                };
                let arguments = self.list(
                    arguments,
                    FirestoreSqlClosure::Filter,
                    FirestoreSqlRustExpr::into_argument,
                )?;
                format!("{compared}.{method}({arguments})")
            }
            _ => format!(
                "q.condition({})",
                self.function(function, FirestoreSqlClosure::Filter)?
                    .into_condition()
            ),
        })
    }

    fn selections(&self, selections: &[FirestoreSelection]) -> FirestoreResult<String> {
        // Beside an aliased item, which is a selection already, a plain expression must become
        // one too, so that the list has one type.
        let mixed = selections.iter().any(|selection| selection.alias.is_some())
            && selections.iter().any(|selection| selection.alias.is_none());
        joined(selections.iter().map(|FirestoreSelection { expr, alias }| {
            let expr = self.expr(expr, FirestoreSqlClosure::Expr)?;
            Ok(match alias {
                Some(alias) => {
                    let aliased = match expr.kind {
                        FirestoreSqlRustKind::Condition => expr.code,
                        _ => expr.into_expr(),
                    };
                    format!("{aliased}.alias({alias:?})")
                }
                None if mixed => format!("FirestoreSelection::from({})", expr.into_expr()),
                None => expr.into_expr(),
            })
        }))
    }

    fn orderings(&self, orderings: &[FirestorePipelineOrdering]) -> FirestoreResult<String> {
        joined(
            orderings
                .iter()
                .map(|FirestorePipelineOrdering { expr, direction }| {
                    let method = match direction {
                        FirestoreQueryDirection::Ascending => "asc",
                        FirestoreQueryDirection::Descending => "desc",
                    };
                    Ok(format!(
                        "{}.{method}()",
                        self.expr(expr, FirestoreSqlClosure::Expr)?.into_expr()
                    ))
                }),
        )
    }

    fn expr(
        &self,
        expr: &FirestoreExpr,
        closure: FirestoreSqlClosure,
    ) -> FirestoreResult<FirestoreSqlRustExpr> {
        let (code, kind) = match expr {
            FirestoreExpr::Field(path) => (closure.field(path), FirestoreSqlRustKind::Value),
            FirestoreExpr::Constant(constant) => {
                (Self::literal(constant)?, FirestoreSqlRustKind::Literal)
            }
            FirestoreExpr::Variable(name) => {
                match self
                    .arguments
                    .iter()
                    .find(|argument| &argument.placeholder == name)
                {
                    Some(argument) => (argument.identifier.clone(), FirestoreSqlRustKind::Literal),
                    None => {
                        return Err(FirestoreError::sql_codegen_refused(
                            "a JOIN or a correlated subquery",
                        ))
                    }
                }
            }
            FirestoreExpr::Function(function) => return self.function(function, closure),
            FirestoreExpr::ArraySubquery(_) | FirestoreExpr::ScalarSubquery(_) => {
                return Err(FirestoreError::sql_codegen_refused("a subquery"))
            }
        };
        Ok(FirestoreSqlRustExpr {
            code,
            kind,
            closure,
        })
    }

    fn function(
        &self,
        function: &FirestoreFunction,
        closure: FirestoreSqlClosure,
    ) -> FirestoreResult<FirestoreSqlRustExpr> {
        let values = closure.values();
        let FirestoreFunction { name, args } = function;
        let name = name.as_str();
        let (code, kind) = match (name, args.as_slice()) {
            ("and" | "or", _) => (
                format!(
                    "{values}.{name}([{}])",
                    self.list(args, closure, FirestoreSqlRustExpr::into_condition)?
                ),
                FirestoreSqlRustKind::Condition,
            ),
            ("not", [operand]) => (
                format!(
                    "{values}.not({})",
                    self.expr(operand, closure)?.into_condition()
                ),
                FirestoreSqlRustKind::Condition,
            ),
            ("conditional", [test, if_true, if_false]) => (
                format!(
                    "{values}.conditional({}, {}, {})",
                    self.expr(test, closure)?.into_condition(),
                    self.expr(if_true, closure)?.into_argument(),
                    self.expr(if_false, closure)?.into_argument()
                ),
                FirestoreSqlRustKind::Value,
            ),
            ("coalesce" | "array", _) => (
                format!(
                    "{values}.{name}([{}])",
                    self.list(args, closure, FirestoreSqlRustExpr::into_expr)?
                ),
                FirestoreSqlRustKind::Value,
            ),
            ("switch_on", [_, _, ..]) => {
                let (cases, default) = args.split_at(args.len() / 2 * 2);
                let cases = self.pairs(cases, closure, FirestoreSqlRustExpr::into_condition)?;
                let code = match default.first() {
                    Some(default) => format!(
                        "{values}.switch_on_with_default([{cases}], {})",
                        self.expr(default, closure)?.into_argument()
                    ),
                    None => format!("{values}.switch_on([{cases}])"),
                };
                (code, FirestoreSqlRustKind::Value)
            }
            ("concat", [receiver, others @ ..]) if !others.is_empty() => (
                format!(
                    "{}.concat([{}])",
                    self.expr(receiver, closure)?.into_expr(),
                    self.list(others, closure, FirestoreSqlRustExpr::into_expr)?
                ),
                FirestoreSqlRustKind::Value,
            ),
            _ => match function.method_call() {
                Some(FirestoreSqlMethodCall {
                    method,
                    kind,
                    receiver,
                    arguments,
                }) => (
                    format!(
                        "{}.{method}({})",
                        self.expr(receiver, closure)?.into_expr(),
                        self.list(arguments, closure, FirestoreSqlRustExpr::into_argument)?
                    ),
                    kind,
                ),
                None => {
                    // An empty array literal does not say its item type to `raw`.
                    let arguments = match args.is_empty() {
                        true => "Vec::<FirestoreExpr>::new()".to_string(),
                        false => format!(
                            "[{}]",
                            self.list(args, closure, FirestoreSqlRustExpr::into_expr)?
                        ),
                    };
                    (
                        format!("{values}.raw({name:?}, {arguments})"),
                        FirestoreSqlRustKind::Value,
                    )
                }
            },
        };
        Ok(FirestoreSqlRustExpr {
            code,
            kind,
            closure,
        })
    }

    fn list(
        &self,
        exprs: &[FirestoreExpr],
        closure: FirestoreSqlClosure,
        written: fn(FirestoreSqlRustExpr) -> String,
    ) -> FirestoreResult<String> {
        joined(
            exprs
                .iter()
                .map(|expr| Ok(written(self.expr(expr, closure)?))),
        )
    }

    fn pairs(
        &self,
        exprs: &[FirestoreExpr],
        closure: FirestoreSqlClosure,
        key: fn(FirestoreSqlRustExpr) -> String,
    ) -> FirestoreResult<String> {
        let keys = exprs.iter().step_by(2);
        let values = exprs.iter().skip(1).step_by(2);
        joined(keys.zip(values).map(|(pair_key, pair_value)| {
            Ok(format!(
                "({}, {})",
                key(self.expr(pair_key, closure)?),
                self.expr(pair_value, closure)?.into_expr()
            ))
        }))
    }

    fn literal(constant: &FirestoreValue) -> FirestoreResult<String> {
        let no_literal = || {
            FirestoreError::invalid_parameters(
                "codegen",
                format!("the constant {constant:?} has no Rust literal"),
            )
        };
        Ok(match &constant.value.value_type {
            Some(ValueType::NullValue(_)) => "None::<bool>".to_string(),
            Some(ValueType::BooleanValue(value)) => value.to_string(),
            Some(ValueType::IntegerValue(value)) => Self::integer(*value),
            Some(ValueType::DoubleValue(value)) => Self::double(*value),
            Some(ValueType::StringValue(value)) => format!("{value:?}"),
            Some(ValueType::ReferenceValue(path)) => Self::reference(path),
            Some(value @ ValueType::ArrayValue(array)) => match Self::references(array) {
                Some(paths) if !paths.is_empty() => {
                    let references: Vec<String> = paths.into_iter().map(Self::reference).collect();
                    format!("[{}]", references.join(", "))
                }
                _ => format!(
                    "serde_json::json!({})",
                    Self::json(value).ok_or_else(no_literal)?
                ),
            },
            Some(value @ ValueType::MapValue(_)) => {
                format!(
                    "serde_json::json!({})",
                    Self::json(value).ok_or_else(no_literal)?
                )
            }
            _ => return Err(no_literal()),
        })
    }

    fn double(value: f64) -> String {
        match (
            value.is_nan(),
            value.is_infinite(),
            value.is_sign_positive(),
        ) {
            (true, _, _) => "f64::NAN".to_string(),
            (_, true, true) => "f64::INFINITY".to_string(),
            (_, true, false) => "f64::NEG_INFINITY".to_string(),
            _ => format!("{value:?}"),
        }
    }

    // An unsuffixed integer literal is an `i32`, which does not compile out of that range.
    fn integer(value: i64) -> String {
        match i32::try_from(value) {
            Ok(_) => value.to_string(),
            Err(_) => format!("{value}_i64"),
        }
    }

    // The reference `path`, in the database of the snippet's own `db` when the translator wrote it
    // in the codegen database.
    fn reference(path: &str) -> String {
        match relative_to_documents(CODEGEN_DOCUMENTS_PATH, path) {
            Some(relative) => {
                let template = format!("{{}}/{}", relative.replace('{', "{{").replace('}', "}}"));
                format!("FirestoreReference::new(format!({template:?}, db.get_documents_path()))")
            }
            None => format!("FirestoreReference::new({path:?}.to_string())"),
        }
    }

    // The paths of `array` when every element is a reference.
    fn references(array: &ArrayValue) -> Option<Vec<&str>> {
        array
            .values
            .iter()
            .map(|element| match &element.value_type {
                Some(ValueType::ReferenceValue(path)) => Some(path.as_str()),
                _ => None,
            })
            .collect()
    }

    // `value` in the syntax of `serde_json::json!`, which serialises to the same value. `None` for
    // a value JSON has no form for.
    fn json(value: &ValueType) -> Option<String> {
        let element = |value: &Value| Self::json(value.value_type.as_ref()?);
        Some(match value {
            ValueType::NullValue(_) => "null".to_string(),
            ValueType::BooleanValue(value) => value.to_string(),
            ValueType::IntegerValue(value) => Self::integer(*value),
            ValueType::DoubleValue(value) if value.is_finite() => format!("{value:?}"),
            ValueType::StringValue(value) => format!("{value:?}"),
            ValueType::ArrayValue(array) => format!(
                "[{}]",
                array
                    .values
                    .iter()
                    .map(element)
                    .collect::<Option<Vec<_>>>()?
                    .join(", ")
            ),
            ValueType::MapValue(map) => {
                // A map's fields have no order; sorted, the snippet is the same every time.
                let fields: BTreeMap<&String, _> = map.fields.iter().collect();
                let fields = fields
                    .into_iter()
                    .map(|(key, value)| Some(format!("{key:?}: {}", element(value)?)))
                    .collect::<Option<Vec<_>>>()?;
                format!("{{{}}}", fields.join(", "))
            }
            _ => return None,
        })
    }
}

// The `FirestoreExpr` method that calls the function `name` with its receiver and `arity - 1`
// more arguments, and the kind of its result. Only the functions that SQL syntax itself
// translates to have one, such as operators, `LIKE`, `SUBSTR` and `TRIM`; any other function is
// written as `raw`, which builds the same node.
fn receiver_method(name: &str, arity: usize) -> Option<(&str, FirestoreSqlRustKind)> {
    use FirestoreSqlRustKind::{Condition, Value};
    Some(match (name, arity) {
        ("mod", 2) => ("modulo", Value),
        ("substring", 3) => ("substring_with_length", Value),
        ("trim", 2) => ("trim_with", Value),
        ("ltrim", 2) => ("ltrim_with", Value),
        ("rtrim", 2) => ("rtrim_with", Value),
        ("exists" | "is_absent", 1) => (name, Condition),
        (
            "equal"
            | "not_equal"
            | "less_than"
            | "less_than_or_equal"
            | "greater_than"
            | "greater_than_or_equal"
            | "equal_any"
            | "not_equal_any"
            | "like",
            2,
        ) => (name, Condition),
        ("to_lower" | "to_upper" | "trim" | "ltrim" | "rtrim", 1) => (name, Value),
        (
            "add" | "subtract" | "multiply" | "divide" | "if_null" | "array_get" | "map_get"
            | "substring",
            2,
        ) => (name, Value),
        _ => return None,
    })
}

fn joined(items: impl Iterator<Item = FirestoreResult<String>>) -> FirestoreResult<String> {
    Ok(items.collect::<FirestoreResult<Vec<_>>>()?.join(", "))
}
