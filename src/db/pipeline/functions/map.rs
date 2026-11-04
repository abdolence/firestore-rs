use crate::FirestoreExpr;

impl FirestoreExpr {
    /// The value under `key` in this map.
    pub fn map_get(self, key: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("map_get", [self, key.into()])
    }

    /// The value under `key` in this map; the same as [`map_get`](Self::map_get).
    pub fn get_field(self, key: impl Into<FirestoreExpr>) -> FirestoreExpr {
        Self::raw("get_field", [self, key.into()])
    }

    /// This map with each `(key, value)` entry set.
    pub fn map_set<I, K, V>(self, entries: I) -> FirestoreExpr
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<FirestoreExpr>,
        V: Into<FirestoreExpr>,
    {
        Self::raw("map_set", self.followed_by(Self::flatten_pairs(entries)))
    }

    /// This map without the entries under `keys`.
    pub fn map_remove<I>(self, keys: I) -> FirestoreExpr
    where
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        Self::raw("map_remove", self.followed_by(keys))
    }

    /// This map merged with the `others`.
    pub fn map_merge<I>(self, others: I) -> FirestoreExpr
    where
        I: IntoIterator,
        I::Item: Into<FirestoreExpr>,
    {
        Self::raw("map_merge", self.followed_by(others))
    }

    /// The keys of this map, as an array.
    pub fn map_keys(self) -> FirestoreExpr {
        Self::raw("map_keys", [self])
    }

    /// The values of this map, as an array.
    pub fn map_values(self) -> FirestoreExpr {
        Self::raw("map_values", [self])
    }

    /// The entries of this map, as an array of maps with a `k` and a `v` field.
    pub fn map_entries(self) -> FirestoreExpr {
        Self::raw("map_entries", [self])
    }

    /// The two halves of each pair in turn, as `map`, `map_set` and `switch_on` take them.
    pub(crate) fn flatten_pairs<I, K, V>(pairs: I) -> impl Iterator<Item = FirestoreExpr>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<FirestoreExpr>,
        V: Into<FirestoreExpr>,
    {
        pairs
            .into_iter()
            .flat_map(|(first, second)| [first.into(), second.into()])
    }
}
