use crate::{FirestoreAggregate, FirestoreBoolExpr, FirestoreExpr};

impl FirestoreExpr {
    /// Counts the documents for which this expression is neither null nor absent. To count all
    /// documents, use `count_all` on the expression builder.
    pub fn count(self) -> FirestoreAggregate {
        FirestoreAggregate::raw("count", [self])
    }

    /// Counts the distinct values of this expression, not counting null.
    pub fn count_distinct(self) -> FirestoreAggregate {
        FirestoreAggregate::raw("count_distinct", [self])
    }

    /// Adds up the numeric values of this expression.
    pub fn sum(self) -> FirestoreAggregate {
        FirestoreAggregate::raw("sum", [self])
    }

    /// The mean of the numeric values of this expression.
    pub fn average(self) -> FirestoreAggregate {
        FirestoreAggregate::raw("average", [self])
    }

    /// The least value of this expression.
    pub fn minimum(self) -> FirestoreAggregate {
        FirestoreAggregate::raw("minimum", [self])
    }

    /// The greatest value of this expression.
    pub fn maximum(self) -> FirestoreAggregate {
        FirestoreAggregate::raw("maximum", [self])
    }

    /// The value of this expression in the first document of the group, in the order of the
    /// preceding `sort`.
    pub fn first(self) -> FirestoreAggregate {
        FirestoreAggregate::raw("first", [self])
    }

    /// The value of this expression in the last document of the group, in the order of the
    /// preceding `sort`.
    pub fn last(self) -> FirestoreAggregate {
        FirestoreAggregate::raw("last", [self])
    }

    /// Collects the values of this expression into an array, in the order of the preceding
    /// `sort`.
    pub fn array_agg(self) -> FirestoreAggregate {
        FirestoreAggregate::raw("array_agg", [self])
    }

    /// Collects the distinct values of this expression into an array, in no particular order.
    pub fn array_agg_distinct(self) -> FirestoreAggregate {
        FirestoreAggregate::raw("array_agg_distinct", [self])
    }
}

impl FirestoreBoolExpr {
    /// Counts the documents for which this condition is true.
    pub fn count_if(self) -> FirestoreAggregate {
        FirestoreAggregate::raw("count_if", [self])
    }
}
