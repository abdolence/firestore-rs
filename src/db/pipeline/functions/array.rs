use crate::{FirestoreBoolExpr, FirestoreExpr};

/// Which occurrence of a value `array_index_of` reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirestoreSearchDirection {
    First,
    Last,
}

impl From<FirestoreSearchDirection> for FirestoreExpr {
    fn from(value: FirestoreSearchDirection) -> Self {
        FirestoreExpr::constant(match value {
            FirestoreSearchDirection::First => "first",
            FirestoreSearchDirection::Last => "last",
        })
    }
}

impl FirestoreExpr {
    /// This array followed by the `others`, joined into one array.
    pub fn array_concat<I>(self, others: I) -> FirestoreExpr
    where
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        Self::raw("array_concat", self.followed_by(others))
    }

    /// True when this array holds `value`.
    pub fn array_contains(self, value: impl Into<FirestoreExpr>) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw("array_contains", [self, value.into()]))
    }

    /// True when this array holds every element of the array `search_values`.
    pub fn array_contains_all(self, search_values: impl Into<FirestoreExpr>) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw(
            "array_contains_all",
            [self, search_values.into()],
        ))
    }

    /// True when this array holds any element of the array `search_values`.
    pub fn array_contains_any(self, search_values: impl Into<FirestoreExpr>) -> FirestoreBoolExpr {
        FirestoreBoolExpr(Self::raw(
            "array_contains_any",
            [self, search_values.into()],
        ))
    }

    /// The number of elements of this array.
    pub fn array_length(self) -> FirestoreExpr {
        Self::raw("array_length", [self])
    }

    /// The element at `index`, counting from the end for a negative index.
    pub fn array_get(self, index: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("array_get", [self, index.into()])
    }

    /// The element at `index`; the same as [`array_get`](Self::array_get). Google does not
    /// document `offset`.
    pub fn offset(self, index: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("offset", [self, index.into()])
    }

    /// The first element of this array.
    pub fn array_first(self) -> FirestoreExpr {
        Self::raw("array_first", [self])
    }

    /// The last element of this array.
    pub fn array_last(self) -> FirestoreExpr {
        Self::raw("array_last", [self])
    }

    /// The first `n` elements of this array.
    pub fn array_first_n(self, n: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("array_first_n", [self, n.into()])
    }

    /// The last `n` elements of this array.
    pub fn array_last_n(self, n: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("array_last_n", [self, n.into()])
    }

    /// The index of the first or last `search` in this array, or -1 when it is not there.
    pub fn array_index_of(
        self,
        search: impl Into<FirestoreExpr>,
        direction: FirestoreSearchDirection,
    ) -> FirestoreExpr {
        Self::raw(
            "array_index_of",
            [self, search.into(), FirestoreExpr::from(direction)],
        )
    }

    /// The index of every `search` in this array.
    pub fn array_index_of_all(self, search: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("array_index_of_all", [self, search.into()])
    }

    /// This array in reverse order.
    pub fn array_reverse(self) -> FirestoreExpr {
        Self::raw("array_reverse", [self])
    }

    /// `length` elements of this array from `offset`.
    pub fn array_slice(
        self,
        offset: impl Into<FirestoreExpr>,
        length: impl Into<FirestoreExpr>,
    ) -> FirestoreExpr {
        Self::raw("array_slice", [self, offset.into(), length.into()])
    }

    /// The elements for which `condition` is true. The closure receives the variable `element`,
    /// which stands for the element being tested.
    pub fn array_filter<N, F>(self, element: N, condition: F) -> FirestoreExpr
    where
        N: Into<String>,
        F: FnOnce(FirestoreExpr) -> FirestoreBoolExpr,
    {
        let element = element.into();
        let condition = condition(FirestoreExpr::Variable(element.clone()));
        self.lambda("array_filter", element, condition.into())
    }

    /// The array of the values `body` gives for each element. The closure receives the variable
    /// `element`, which stands for the element being transformed.
    pub fn array_transform<N, F, R>(self, element: N, body: F) -> FirestoreExpr
    where
        N: Into<String>,
        F: FnOnce(FirestoreExpr) -> R,
        R: Into<FirestoreExpr>,
    {
        let element = element.into();
        let body = body(FirestoreExpr::Variable(element.clone())).into();
        self.lambda("array_transform", element, body)
    }

    /// The sum of the elements of this array of numbers. The aggregate `sum` is separate.
    pub fn array_sum(self) -> FirestoreExpr {
        Self::raw("sum", [self])
    }

    /// The greatest element of this array. The aggregate `maximum` is separate.
    pub fn array_maximum(self) -> FirestoreExpr {
        Self::raw("maximum", [self])
    }

    /// The least element of this array. The aggregate `minimum` is separate.
    pub fn array_minimum(self) -> FirestoreExpr {
        Self::raw("minimum", [self])
    }

    /// The `n` greatest elements of this array, greatest first.
    pub fn array_maximum_n(self, n: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("maximum_n", [self, n.into()])
    }

    /// The `n` least elements of this array, least first.
    pub fn array_minimum_n(self, n: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("minimum_n", [self, n.into()])
    }

    /// The call `function(self, element, body)`: the service reads the element name as a string
    /// and the body as an expression over the variable of that name.
    fn lambda(self, function: &str, element: String, body: FirestoreExpr) -> FirestoreExpr {
        Self::raw(function, [self, FirestoreExpr::constant(element), body])
    }
}
