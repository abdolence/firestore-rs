use crate::FirestoreCollectionId;
use std::collections::BTreeSet;

/// What a SQL statement may do, checked when it is translated, before any request is sent.
///
/// [`new`](Self::new) allows every statement the translator supports, except joins and
/// subqueries and the statements that change an index or a field, which need
/// [`allow_subqueries`](Self::allow_subqueries) and
/// [`allow_index_statements`](Self::allow_index_statements). Text from outside the program, such
/// as a statement an LLM agent wrote, starts from [`read_only`](Self::read_only).
///
/// The options bound what a statement may name before it runs. What it can reach is still
/// bounded by the IAM role of the client's credentials: server client libraries are not subject
/// to Security Rules.
#[derive(Debug, Clone)]
pub struct FirestoreSqlOptions {
    pub(crate) read_only: bool,
    pub(crate) collection_groups: bool,
    pub(crate) collections: Option<BTreeSet<FirestoreCollectionId>>,
    pub(crate) max_rows: Option<u32>,
    pub(crate) subqueries: bool,
    pub(crate) index_statements: bool,
}

impl Default for FirestoreSqlOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl FirestoreSqlOptions {
    /// Allows every statement the translator supports, except joins and subqueries and the
    /// statements that change an index or a field.
    pub fn new() -> Self {
        Self {
            read_only: false,
            collection_groups: true,
            collections: None,
            max_rows: None,
            subqueries: false,
            index_statements: false,
        }
    }

    /// As [`new`](Self::new), and refuses writes, the `database()` source and every statement that
    /// changes an index or a field. [`allow_index_statements`](Self::allow_index_statements) does
    /// not lift the last: read-only wins.
    pub fn read_only() -> Self {
        Self {
            read_only: true,
            ..Self::new()
        }
    }

    /// Refuses the `collection_group(..)` source, which reads every collection with the ID,
    /// under any document.
    pub fn refuse_collection_groups(self) -> Self {
        Self {
            collection_groups: false,
            ..self
        }
    }

    /// Allows only these collection IDs, wherever a statement names a collection: a collection,
    /// each collection of a path such as `` `restaurants/r1/reviews` ``, a collection group and
    /// the paths of `documents(..)`. `database()` is refused, since it reads every collection.
    /// So are `SHOW COLLECTIONS` and `SHOW INDEXES` without `FROM`, which list names outside the
    /// set, and a `DROP INDEX` by ID without `ON`, which reaches an index in any group.
    pub fn with_collections<I>(self, collection_ids: I) -> Self
    where
        I: IntoIterator<Item = FirestoreCollectionId>,
    {
        Self {
            collections: Some(collection_ids.into_iter().collect()),
            ..self
        }
    }

    /// Returns at most `max_rows` rows from a query: its `LIMIT` is lowered to this, or one is
    /// added.
    pub fn with_max_rows(self, max_rows: u32) -> Self {
        Self {
            max_rows: Some(max_rows),
            ..self
        }
    }

    /// Allows joins and subqueries. Each runs one subquery per row of the query it is in, whether
    /// or not it reads a column of that row, and without an index on the join key each of those
    /// reads the whole joined collection. A join or subquery into `subcollection(..)` reads only
    /// the children of each row, so it needs no opt-in. A derived table or a common table
    /// expression in the `FROM` of a query needs none either, since it is the query's own source;
    /// a join into one is still a join.
    pub fn allow_subqueries(self) -> Self {
        Self {
            subqueries: true,
            ..self
        }
    }

    /// Allows `CREATE INDEX`, `DROP INDEX` and `ALTER COLLECTION`, which change what the database
    /// indexes and are refused unless this is called. `SHOW INDEXES`, `SHOW COLLECTIONS` and
    /// `EXPLAIN` only read and need no opt-in. The options of [`read_only`](Self::read_only)
    /// still refuse the three statements, whatever order the calls come in.
    pub fn allow_index_statements(self) -> Self {
        Self {
            index_statements: true,
            ..self
        }
    }
}
