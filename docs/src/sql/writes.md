# Writes and ON CONFLICT

`INSERT`, `UPDATE` and `DELETE` run as [pipeline writes](../pipeline-writes.md), with `execute()`
or `execute_in_transaction(..)`. [Writes](../sql.md#writes) in the SQL overview lists the
statements and what each runs as. This page holds the rules of each statement.

## Rules per statement

- `INSERT .. VALUES` needs a column list. Each value is a literal or a bound parameter, and each
  column a top-level field name. An `INSERT` fails with `ALREADY_EXISTS` when a document of that
  name exists;
- a `__name__` column gives the document's name. In `VALUES` a bare ID resolves against the target
  collection and a path outside it is refused; the rows of a `SELECT` keep the name of the document
  they were read from. A row without `__name__` gets an ID from Firestore. A column list over a
  `SELECT` renames its columns by position, and is refused over `*`;
- `UPDATE` and `DELETE` require a `WHERE`, as in BigQuery, and `WHERE TRUE` selects every
  document. The target is one collection. `UPDATE .. SET __name__` is refused;
- the same ID twice in one statement fails with `INVALID_ARGUMENT`, "Multiple mutations per key
  are not allowed", and nothing is written.

## ON CONFLICT

- `ON CONFLICT` takes only the target `(__name__)`, since Firestore has no other unique
  constraint, and `DO NOTHING` also accepts no target;
- in `DO UPDATE`, `excluded.col` is a field of the new row, and a bare column is a field of the
  stored document. A `WHERE` on `DO UPDATE` leaves the stored document unchanged, and does not
  count it, when it is false for it;
- the lookup of the stored document by its name is bounded by the rows written. In `ON CONFLICT`,
  rows of a `SELECT` from another collection are matched by their ID, which scans the target
  collection once per row, and so need `.allow_subqueries()`;
- the `__name__` of such a row must be the name of a document that the translation can see:
  `SELECT __name__, ..`, or `*` of a collection. A string or another field renamed to `__name__`,
  `*` of a derived table, and `* EXCEPT (__name__)` are refused before any request, since a row
  without a document reference cannot be matched;
- `ON CONFLICT` keeps the stored document of each row in a field named `_conflict_stored`. A column
  of that name is refused, and a field of that name in the rows of a `SELECT *` is dropped from
  them.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let written = db
    .fluent()
    .sql(
        "INSERT INTO items (__name__, stock) VALUES (@id, @stock) \
         ON CONFLICT (__name__) DO UPDATE SET stock = stock + excluded.stock",
    )
    .param("id", "a-1")
    .param("stock", 5)
    .execute()
    .await?;
println!("Wrote {} documents", written.documents_modified);
# Ok(())
# }
```

## Concurrent writes

Concurrent writes to one document conflict: some commit, and the others fail with `ABORTED`,
"Optimistic transaction was aborted due to a conflict", with nothing of them applied. To have such
writes retried, run them with `execute_in_transaction(..)` inside `run_transaction`, which runs the
whole transaction again on `ABORTED`, see
[Writes in a transaction](../pipeline-writes.md#writes-in-a-transaction). `execute()` on its own
sends a write once.

## Write limits

Firestore refuses an atomic write that is too big. [Write limits](../pipeline-writes.md#write-limits)
lists the limits.
