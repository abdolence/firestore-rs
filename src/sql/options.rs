use crate::{FirestoreCollectionId, FirestoreError, FirestoreResult};
use std::collections::BTreeSet;

/// The longest statement accepted unless raised, in bytes. sqlparser nests a chain of infix
/// operators one level per operator, and its parsed statement is dropped recursively: a chain of
/// 8,192 operators drops well within a 2 MiB stack.
const SQL_DEFAULT_MAX_LENGTH: usize = 16 * 1024;

/// What a SQL statement may do, checked when it is translated, before any request is sent.
///
/// [`new`](Self::new) allows every statement the translator supports. Text from outside the
/// program, such as a statement an LLM agent wrote, starts from [`read_only`](Self::read_only).
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
    pub(crate) max_length: usize,
    pub(crate) subqueries: bool,
}

impl Default for FirestoreSqlOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl FirestoreSqlOptions {
    /// Allows every statement the translator supports, except joins and correlated subqueries,
    /// and statements longer than 16 KiB.
    pub fn new() -> Self {
        Self {
            read_only: false,
            collection_groups: true,
            collections: None,
            max_rows: None,
            max_length: SQL_DEFAULT_MAX_LENGTH,
            subqueries: false,
        }
    }

    /// As [`new`](Self::new), and refuses writes and the `database()` source.
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
    ///
    /// Fails with
    /// [`InvalidParametersError`](crate::errors::FirestoreError::InvalidParametersError) for an
    /// ID that is not a valid collection ID.
    pub fn with_collections<I>(self, collection_ids: I) -> FirestoreResult<Self>
    where
        I: IntoIterator,
        I::Item: TryInto<FirestoreCollectionId, Error = FirestoreError>,
    {
        let collections = collection_ids
            .into_iter()
            .map(TryInto::try_into)
            .collect::<FirestoreResult<_>>()?;
        Ok(Self {
            collections: Some(collections),
            ..self
        })
    }

    /// Returns at most `max_rows` rows from a query: its `LIMIT` is lowered to this, or one is
    /// added.
    pub fn with_max_rows(self, max_rows: u32) -> Self {
        Self {
            max_rows: Some(max_rows),
            ..self
        }
    }

    /// Refuses a statement longer than `max_length` bytes, 16 KiB unless set. A longer statement
    /// can chain more operators than a 2 MiB stack drops the parsed statement with.
    pub fn with_max_length(self, max_length: usize) -> Self {
        Self { max_length, ..self }
    }

    /// Allows joins and correlated subqueries. Each runs one subquery per row of the query it is
    /// in, and without an index on the join key each of those reads the whole joined
    /// collection. A join or subquery into `subcollection(..)` reads only the children of each
    /// row, and a derived table or a common table expression does not multiply with the rows, so
    /// none of them needs this.
    pub fn allow_subqueries(self) -> Self {
        Self {
            subqueries: true,
            ..self
        }
    }
}
