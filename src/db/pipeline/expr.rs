use crate::firestore_serde::serializer::FirestoreValueSerializer;
use crate::{
    FirestoreError, FirestorePipeline, FirestoreQueryDirection, FirestoreResult, FirestoreValue,
};
use gcloud_sdk::google::firestore::v1::{value::ValueType, Function, MapValue, Value};
use rsb_derive::Builder;
use std::collections::hash_map::{Entry, HashMap};

/// A pipeline expression: a field, a constant, a function call or a variable.
///
/// The closures of the fluent pipeline API receive a
/// [`FirestorePipelineExprBuilder`](crate::pipeline_expr_builder::FirestorePipelineExprBuilder)
/// to build them with. An operation on one expression is a method on it, such as
/// [`equal`](Self::equal) or [`alias`](Self::alias).
#[derive(Debug, Clone, PartialEq)]
pub enum FirestoreExpr {
    /// A field of the current document, by its path, such as `path!(Book::title)`.
    Field(String),
    /// A constant value. A `None` is sent as null, unlike a `None` field of a struct, which is
    /// left out as it is in a Core document. A value with no type, which is what a constant that
    /// failed to serialise is left as, fails the conversion of the pipeline.
    Constant(FirestoreValue),
    /// A call of a pipeline function.
    Function(FirestoreFunction),
    /// A variable, defined by the `let` stage or bound by a function such as `array_filter`.
    Variable(String),
    /// The results of a nested pipeline, as an array: empty when it finds nothing, and each
    /// result a map, or just the value when the pipeline outputs a single field. Preview.
    ArraySubquery(FirestorePipeline),
    /// The single result of a nested pipeline: null when it finds nothing, and the value of the
    /// field when it outputs one. More than one result fails the request as `INVALID_ARGUMENT`.
    /// Preview.
    ScalarSubquery(FirestorePipeline),
}

/// A call of the pipeline function `name` with `args`.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreFunction {
    /// The function's wire name, such as `equal` or `to_upper`.
    pub name: String,
    pub args: Vec<FirestoreExpr>,
}

/// An expression that yields a boolean, the only kind a `filter` accepts.
///
/// Firestore accepts any expression as a condition and drops, without an error, every document
/// for which it is not `true`, so a condition that is not a boolean silently matches nothing.
/// The comparisons and the logical functions return this type; for a raw function known to
/// yield a boolean, use [`unchecked`](Self::unchecked).
#[derive(Debug, Clone, PartialEq)]
pub struct FirestoreBoolExpr(pub(crate) FirestoreExpr);

/// An expression with the field name it is output under, as `fields` and `add_fields` take it.
///
/// Without an alias the expression must be a field, which keeps its own path as its name.
/// Converting a pipeline that has an unaliased computed expression, or the same name twice in
/// one stage, fails with
/// [`InvalidParametersError`](crate::errors::FirestoreError::InvalidParametersError).
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreSelection {
    pub expr: FirestoreExpr,
    pub alias: Option<String>,
}

/// One ordering of a `sort` stage.
///
/// Unlike a Core query, a pipeline sort keeps the documents that lack the field, ordering them
/// as null, and adds no tie-break on the document name.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestorePipelineOrdering {
    pub expr: FirestoreExpr,
    pub direction: FirestoreQueryDirection,
}

/// An accumulator of an `aggregate` stage, such as `average` or `count_if`.
///
/// Firestore keeps these apart from scalar functions: an accumulator does not exist outside
/// `aggregate`, and a scalar function does not exist inside it. Build one with the methods of
/// [`FirestoreExpr`] and [`FirestoreBoolExpr`], such as
/// [`average`](FirestoreExpr::average), or with [`raw`](Self::raw).
#[derive(Debug, Clone, PartialEq)]
pub struct FirestoreAggregate(pub(crate) FirestoreFunction);

/// An accumulator with the field name its result is output under, which `aggregate` requires
/// for every accumulator.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct FirestoreAggregateSelection {
    pub aggregate: FirestoreAggregate,
    pub alias: String,
}

impl FirestoreAggregate {
    /// A call of the accumulator `name`, for one that has no method of its own.
    pub fn raw<N, I>(name: N, args: I) -> Self
    where
        N: Into<String>,
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        Self(FirestoreFunction::new(
            name.into(),
            args.into_iter().map(Into::into).collect(),
        ))
    }

    /// Outputs this accumulator's result under the field name `alias`.
    pub fn alias<S: Into<String>>(self, alias: S) -> FirestoreAggregateSelection {
        FirestoreAggregateSelection::new(self, alias.into())
    }
}

impl FirestoreSelection {
    /// The field name the selection is output under: the alias, or else the field's own path.
    /// Fails for a computed expression without an alias.
    pub(crate) fn name(&self, stage: &str) -> FirestoreResult<&str> {
        match (&self.alias, &self.expr) {
            (Some(alias), _) => Ok(alias),
            (None, FirestoreExpr::Field(path)) => Ok(path),
            (None, expr) => Err(FirestoreError::invalid_parameters(
                stage,
                format!("a computed expression needs an alias: {expr:?}"),
            )),
        }
    }

    /// The expression and the field name it is output under, as [`name`](Self::name).
    pub(super) fn into_named(self, stage: &str) -> FirestoreResult<(String, FirestoreExpr)> {
        let name = self.name(stage)?.to_string();
        Ok((name, self.expr))
    }
}

/// Whether `name` is a simple identifier, `[A-Za-z_][A-Za-z0-9_]*`, the only kind of field path
/// segment that Firestore accepts without backticks.
#[cfg(any(feature = "admin", feature = "sql", feature = "caching"))]
pub(crate) fn is_simple_field_segment(name: &str) -> bool {
    name.chars()
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `name` as one segment of a field path: bare when it is a simple identifier, and otherwise
/// between backticks, with `\\` and `` \` `` escaping a backslash and a backtick inside them.
#[cfg(any(feature = "admin", feature = "sql"))]
pub(crate) fn field_path_segment(name: &str) -> std::borrow::Cow<'_, str> {
    if is_simple_field_segment(name) {
        std::borrow::Cow::Borrowed(name)
    } else {
        std::borrow::Cow::Owned(format!(
            "`{}`",
            name.replace('\\', "\\\\").replace('`', "\\`")
        ))
    }
}

impl FirestoreExpr {
    /// A field of the current document, by its path.
    pub fn field<S: Into<String>>(path: S) -> Self {
        Self::Field(path.into())
    }

    /// A constant value.
    pub fn constant<V: serde::Serialize>(value: V) -> Self {
        Self::from(value)
    }

    /// A call of the function `name`, for a function that has no method of its own.
    pub fn raw<N, I>(name: N, args: I) -> Self
    where
        N: Into<String>,
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        Self::Function(FirestoreFunction::new(
            name.into(),
            args.into_iter().map(Into::into).collect(),
        ))
    }

    /// Outputs this expression under the field name `alias`.
    pub fn alias<S: Into<String>>(self, alias: S) -> FirestoreSelection {
        FirestoreSelection::new(self).with_alias(alias.into())
    }

    /// Orders by this expression, ascending.
    pub fn asc(self) -> FirestorePipelineOrdering {
        self.direction(FirestoreQueryDirection::Ascending)
    }

    /// Orders by this expression, descending.
    pub fn desc(self) -> FirestorePipelineOrdering {
        self.direction(FirestoreQueryDirection::Descending)
    }

    /// Orders by this expression in a direction chosen at run time.
    pub fn direction(self, direction: FirestoreQueryDirection) -> FirestorePipelineOrdering {
        FirestorePipelineOrdering::new(self, direction)
    }
}

impl FirestoreBoolExpr {
    /// Treats `expr` as a boolean: for a raw function the caller knows yields one.
    pub fn unchecked(expr: FirestoreExpr) -> Self {
        Self(expr)
    }

    /// Outputs this condition's value under the field name `alias`.
    pub fn alias<S: Into<String>>(self, alias: S) -> FirestoreSelection {
        self.0.alias(alias)
    }
}

impl From<FirestoreBoolExpr> for FirestoreExpr {
    fn from(condition: FirestoreBoolExpr) -> Self {
        condition.0
    }
}

impl From<FirestoreValue> for FirestoreExpr {
    fn from(value: FirestoreValue) -> Self {
        Self::Constant(value)
    }
}

impl<T> From<T> for FirestoreExpr
where
    T: serde::Serialize,
{
    fn from(value: T) -> Self {
        let constant = match value.serialize(FirestoreValueSerializer::new()) {
            // A `None` or a unit has no type of its own, and as a pipeline argument it can only
            // mean null.
            Ok(FirestoreValue {
                value: Value { value_type: None },
            }) => FirestoreValue::from(wire_value(ValueType::NullValue(0))),
            Ok(constant) => constant,
            // `From` has no way to return the error; the value stays untyped, and converting the
            // pipeline rejects it.
            Err(_) => FirestoreValue::from(Value { value_type: None }),
        };
        Self::Constant(constant)
    }
}

impl From<FirestoreExpr> for FirestoreSelection {
    fn from(expr: FirestoreExpr) -> Self {
        FirestoreSelection::new(expr)
    }
}

impl From<FirestoreAggregateSelection> for FirestoreSelection {
    fn from(selection: FirestoreAggregateSelection) -> Self {
        FirestoreSelection::new(FirestoreExpr::Function(selection.aggregate.0))
            .with_alias(selection.alias)
    }
}

impl TryFrom<FirestoreExpr> for Value {
    type Error = FirestoreError;

    fn try_from(expr: FirestoreExpr) -> Result<Self, Self::Error> {
        Ok(match expr {
            FirestoreExpr::Field(path) => wire_value(ValueType::FieldReferenceValue(path)),
            FirestoreExpr::Constant(FirestoreValue {
                value: Value { value_type: None },
            }) => {
                return Err(FirestoreError::invalid_parameters(
                    "constant",
                    "the value could not be serialised",
                ))
            }
            FirestoreExpr::Constant(value) => value.value,
            FirestoreExpr::Function(function) => {
                wire_value(ValueType::FunctionValue(function.try_into()?))
            }
            FirestoreExpr::Variable(name) => wire_value(ValueType::VariableReferenceValue(name)),
            FirestoreExpr::ArraySubquery(pipeline) => subquery("array", pipeline)?,
            FirestoreExpr::ScalarSubquery(pipeline) => subquery("scalar", pipeline)?,
        })
    }
}

impl TryFrom<FirestoreFunction> for Function {
    type Error = FirestoreError;

    fn try_from(function: FirestoreFunction) -> Result<Self, Self::Error> {
        Ok(Function {
            name: function.name,
            args: function
                .args
                .into_iter()
                .map(Value::try_from)
                .collect::<FirestoreResult<_>>()?,
            options: HashMap::new(),
        })
    }
}

impl TryFrom<FirestorePipelineOrdering> for Value {
    type Error = FirestoreError;

    fn try_from(ordering: FirestorePipelineOrdering) -> Result<Self, Self::Error> {
        let direction = match ordering.direction {
            FirestoreQueryDirection::Ascending => "ascending",
            FirestoreQueryDirection::Descending => "descending",
        };
        Ok(wire_value(ValueType::MapValue(MapValue {
            fields: HashMap::from([
                ("expression".to_string(), ordering.expr.try_into()?),
                (
                    "direction".to_string(),
                    wire_value(ValueType::StringValue(direction.to_string())),
                ),
            ]),
        })))
    }
}

pub(super) fn wire_value(value_type: ValueType) -> Value {
    Value {
        value_type: Some(value_type),
    }
}

/// The call of `name` on a nested pipeline, which is how the wire spells a subquery. Not a
/// conversion: the function name is the context the pipeline lacks.
fn subquery(name: &str, pipeline: FirestorePipeline) -> FirestoreResult<Value> {
    Ok(wire_value(ValueType::FunctionValue(Function {
        name: name.to_string(),
        args: vec![pipeline.try_into()?],
        options: HashMap::new(),
    })))
}

/// The `{name: expression}` map that the projecting stage `stage` takes as its argument.
pub(super) fn selection_map(
    stage: &str,
    selections: Vec<FirestoreSelection>,
) -> FirestoreResult<Value> {
    let mut fields = HashMap::with_capacity(selections.len());
    for selection in selections {
        let (name, expr) = selection.into_named(stage)?;
        match fields.entry(name) {
            Entry::Occupied(entry) => {
                return Err(FirestoreError::invalid_parameters(
                    stage,
                    format!("the field name {} is given twice", entry.key()),
                ))
            }
            Entry::Vacant(entry) => {
                entry.insert(Value::try_from(expr)?);
            }
        }
    }
    Ok(wire_value(ValueType::MapValue(MapValue { fields })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_constant_that_fails_to_serialise_is_rejected() {
        let unserialisable = HashMap::from([(true, 1)]);

        let comparison = FirestoreExpr::field("flags").equal(unserialisable.clone());
        let aliased = FirestoreExpr::constant(unserialisable).alias("flags");

        for expr in [FirestoreExpr::from(comparison), aliased.expr] {
            assert!(matches!(
                Value::try_from(expr),
                Err(FirestoreError::InvalidParametersError(_))
            ),);
        }
    }

    #[test]
    fn a_none_constant_is_sent_as_null() {
        let comparison = FirestoreExpr::field("title").equal(None::<String>);

        let sent = Value::try_from(FirestoreExpr::from(comparison)).unwrap();

        assert_eq!(
            sent,
            wire_value(ValueType::FunctionValue(Function {
                name: "equal".to_string(),
                args: vec![
                    wire_value(ValueType::FieldReferenceValue("title".to_string())),
                    wire_value(ValueType::NullValue(0)),
                ],
                options: HashMap::new(),
            }))
        );
    }
}
