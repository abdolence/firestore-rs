use crate::{FirestoreError, FirestoreQueryDirection, FirestoreResult, FirestoreValue};
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
    /// A constant value.
    Constant(FirestoreValue),
    /// A call of a pipeline function.
    Function(FirestoreFunction),
    /// A variable, defined by the `let` stage or bound by a function such as `array_filter`.
    Variable(String),
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

/// An expression with the field name it is output under, as `select` and `add_fields` take it.
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

impl FirestoreExpr {
    /// A field of the current document, by its path.
    pub fn field<S: Into<String>>(path: S) -> Self {
        Self::Field(path.into())
    }

    /// A constant value.
    pub fn constant<V: Into<FirestoreValue>>(value: V) -> Self {
        Self::Constant(value.into())
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
        Self::Constant(value.into())
    }
}

impl From<FirestoreExpr> for FirestoreSelection {
    fn from(expr: FirestoreExpr) -> Self {
        FirestoreSelection::new(expr)
    }
}

impl TryFrom<FirestoreExpr> for Value {
    type Error = FirestoreError;

    fn try_from(expr: FirestoreExpr) -> Result<Self, Self::Error> {
        Ok(match expr {
            FirestoreExpr::Field(path) => wire_value(ValueType::FieldReferenceValue(path)),
            FirestoreExpr::Constant(value) => value.value,
            FirestoreExpr::Function(function) => {
                wire_value(ValueType::FunctionValue(function.try_into()?))
            }
            FirestoreExpr::Variable(name) => wire_value(ValueType::VariableReferenceValue(name)),
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

/// The `{name: expression}` map that the projecting stage `stage` takes as its argument.
pub(super) fn selection_map(
    stage: &str,
    selections: Vec<FirestoreSelection>,
) -> FirestoreResult<Value> {
    let mut fields = HashMap::with_capacity(selections.len());
    for selection in selections {
        let name = match (selection.alias, &selection.expr) {
            (Some(alias), _) => alias,
            (None, FirestoreExpr::Field(path)) => path.clone(),
            (None, expr) => {
                return Err(FirestoreError::invalid_parameters(
                    stage,
                    format!("a computed expression needs an alias: {expr:?}"),
                ))
            }
        };
        match fields.entry(name) {
            Entry::Occupied(entry) => {
                return Err(FirestoreError::invalid_parameters(
                    stage,
                    format!("the field name {} is given twice", entry.key()),
                ))
            }
            Entry::Vacant(entry) => {
                entry.insert(Value::try_from(selection.expr)?);
            }
        }
    }
    Ok(wire_value(ValueType::MapValue(MapValue { fields })))
}
