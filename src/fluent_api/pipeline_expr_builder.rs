use crate::{FirestoreBoolExpr, FirestoreExpr};

/// The argument of every expression closure of the fluent pipeline API other than `filter`, such
/// as `.fields(|e| ..)` and `.sort(|e| ..)`. The `filter` closure takes a
/// [`FirestorePipelineFilterBuilder`](crate::pipeline_filter_builder::FirestorePipelineFilterBuilder).
///
/// It starts expressions and combines conditions, which are plain here: only the filter builder
/// has `Option` conditions. An operation on one expression is a method on
/// that expression, such as `e.field(path!(Book::rating)).greater_than(4.0)`.
#[derive(Debug, Clone, Copy)]
pub struct FirestorePipelineExprBuilder;

impl FirestorePipelineExprBuilder {
    /// A field of the current document, by its path, such as `path!(Book::title)`.
    pub fn field<S: AsRef<str>>(&self, path: S) -> FirestoreExpr {
        FirestoreExpr::field(path.as_ref())
    }

    /// A constant value. A value passed to a comparison becomes a constant without this.
    pub fn constant<V: serde::Serialize>(&self, value: V) -> FirestoreExpr {
        FirestoreExpr::constant(value)
    }

    /// The variable `name`, defined by the `let` stage or bound by a function such as
    /// `array_filter`.
    pub fn variable<S: Into<String>>(&self, name: S) -> FirestoreExpr {
        FirestoreExpr::Variable(name.into())
    }

    /// A call of the function `name`, for a function that has no method of its own.
    pub fn raw<N, I>(&self, name: N, args: I) -> FirestoreExpr
    where
        N: Into<String>,
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        FirestoreExpr::raw(name, args)
    }

    /// True when every condition is. A single condition is returned as it is, and no condition
    /// gives the constant `true`, the identity of `and`. For a `filter` built from conditions
    /// that may be absent, use [`for_all`](crate::pipeline_filter_builder::FirestorePipelineFilterBuilder::for_all).
    pub fn and<I>(&self, conditions: I) -> FirestoreBoolExpr
    where
        I: IntoIterator<Item = FirestoreBoolExpr>,
    {
        self.connect("and", conditions)
    }

    /// True when any condition is. A single condition is returned as it is, and no condition
    /// gives the constant `false`, the identity of `or`. For a `filter` built from conditions
    /// that may be absent, use [`for_any`](crate::pipeline_filter_builder::FirestorePipelineFilterBuilder::for_any).
    pub fn or<I>(&self, conditions: I) -> FirestoreBoolExpr
    where
        I: IntoIterator<Item = FirestoreBoolExpr>,
    {
        self.connect("or", conditions)
    }

    /// True when an odd number of the conditions are.
    pub fn xor<I>(&self, conditions: I) -> FirestoreBoolExpr
    where
        I: IntoIterator<Item = FirestoreBoolExpr>,
    {
        FirestoreBoolExpr(FirestoreExpr::raw("xor", conditions))
    }

    /// True when no condition is.
    pub fn nor<I>(&self, conditions: I) -> FirestoreBoolExpr
    where
        I: IntoIterator<Item = FirestoreBoolExpr>,
    {
        FirestoreBoolExpr(FirestoreExpr::raw("nor", conditions))
    }

    /// True when `condition` is false.
    pub fn not(&self, condition: FirestoreBoolExpr) -> FirestoreBoolExpr {
        FirestoreBoolExpr(FirestoreExpr::raw("not", [condition]))
    }

    /// `function` of `conditions`, where a single condition stands for itself and none stands for
    /// the identity of `function`, since Firestore's `and` and `or` take two or more arguments.
    pub(crate) fn connect<I>(&self, function: &str, conditions: I) -> FirestoreBoolExpr
    where
        I: IntoIterator<Item = FirestoreBoolExpr>,
    {
        match <[FirestoreBoolExpr; 1]>::try_from(conditions.into_iter().collect::<Vec<_>>()) {
            Ok([condition]) => condition,
            Err(conditions) if conditions.is_empty() => {
                FirestoreBoolExpr(FirestoreExpr::constant(function == "and"))
            }
            Err(conditions) => FirestoreBoolExpr(FirestoreExpr::raw(function, conditions)),
        }
    }
}
