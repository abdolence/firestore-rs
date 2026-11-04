use crate::fluent_api::pipeline_expr_builder::FirestorePipelineExprBuilder;
use crate::{FirestoreBoolExpr, FirestoreExpr};

impl FirestorePipelineExprBuilder {
    /// `if_true` when `condition` is true, otherwise `if_false`.
    pub fn conditional(
        &self,
        condition: FirestoreBoolExpr,
        if_true: impl Into<FirestoreExpr>,
        if_false: impl Into<FirestoreExpr>,
    ) -> FirestoreExpr {
        FirestoreExpr::raw(
            "conditional",
            [condition.into(), if_true.into(), if_false.into()],
        )
    }

    /// The value of the first `(condition, value)` case whose condition is true. Fails when none
    /// is; [`switch_on_with_default`](Self::switch_on_with_default) covers that.
    pub fn switch_on<I, V>(&self, cases: I) -> FirestoreExpr
    where
        I: IntoIterator<Item = (FirestoreBoolExpr, V)>,
        V: Into<FirestoreExpr>,
    {
        FirestoreExpr::raw("switch_on", FirestoreExpr::flatten_pairs(cases))
    }

    /// As [`switch_on`](Self::switch_on), with `default` when no condition is true.
    pub fn switch_on_with_default<I, V>(
        &self,
        cases: I,
        default: impl Into<FirestoreExpr>,
    ) -> FirestoreExpr
    where
        I: IntoIterator<Item = (FirestoreBoolExpr, V)>,
        V: Into<FirestoreExpr>,
    {
        FirestoreExpr::raw(
            "switch_on",
            FirestoreExpr::flatten_pairs(cases).chain([default.into()]),
        )
    }

    /// The first of `values` that is neither null nor absent. Takes two or more.
    pub fn coalesce<I>(&self, values: I) -> FirestoreExpr
    where
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        FirestoreExpr::raw("coalesce", values)
    }

    /// An array of `values`.
    pub fn array<I>(&self, values: I) -> FirestoreExpr
    where
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        FirestoreExpr::raw("array", values)
    }

    /// A map of the `(key, value)` entries.
    pub fn map<I, K, V>(&self, entries: I) -> FirestoreExpr
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<FirestoreExpr>,
        V: Into<FirestoreExpr>,
    {
        FirestoreExpr::raw("map", FirestoreExpr::flatten_pairs(entries))
    }

    /// The current time, as a timestamp.
    pub fn current_timestamp(&self) -> FirestoreExpr {
        FirestoreExpr::raw("current_timestamp", Vec::<FirestoreExpr>::new())
    }

    /// The current document, as a map.
    pub fn current_document(&self) -> FirestoreExpr {
        FirestoreExpr::raw("current_document", Vec::<FirestoreExpr>::new())
    }

    /// A random float from 0 up to, and not including, 1.
    pub fn rand(&self) -> FirestoreExpr {
        FirestoreExpr::raw("rand", Vec::<FirestoreExpr>::new())
    }

    /// The greatest of `values`. Takes two or more. The service's `maximum` over arguments;
    /// [`array_maximum`](FirestoreExpr::array_maximum) takes an array.
    pub fn greatest<I>(&self, values: I) -> FirestoreExpr
    where
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        FirestoreExpr::raw("maximum", values)
    }

    /// The least of `values`. Takes two or more. The service's `minimum` over arguments;
    /// [`array_minimum`](FirestoreExpr::array_minimum) takes an array.
    pub fn least<I>(&self, values: I) -> FirestoreExpr
    where
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        FirestoreExpr::raw("minimum", values)
    }
}
