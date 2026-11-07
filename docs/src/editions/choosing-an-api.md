# Choosing Core queries, pipelines or SQL

The library reads Firestore through three APIs: Core queries (`db.fluent().select()`),
[pipelines](../pipelines.md) (`db.fluent().pipeline()`) and [SQL](../sql.md)
(`db.fluent().sql(..)`), which runs as a pipeline. Which one fits depends on the edition of the
database first, and on what the query needs.

## On a Standard edition database

The Core API is the only one. Pipelines and SQL fail with `FAILED_PRECONDITION`, and the library
does not retry them. In a [cache](../caching.md) session, a pipeline that the cache answers on its
own succeeds on Standard too, and one it does not answer fails as on Firestore. Be aware not to
rely on that: a pipeline that works from the cache fails once it reaches Firestore.

## On an Enterprise edition database

The Core query API stays the choice for what it covers. Reasons to stay on Core there:

- listeners take Core targets only, see [Listening to document changes](../listening-changes.md);
- the cache answers every supported Core query, and only the pipelines and SQL statements that
  filter, sort, page and project plain fields, see [Caching pipelines](../caching.md#pipelines).
  Anything else runs on Firestore, or fails under `read_cached_only`;
- existing code keeps the Core semantics of null and absent fields, see
  [Query and write semantics](./semantics.md#null-and-absent);
- Core writes in a transaction are buffered and sent with the commit, see
  [Transactions](../transactions.md).

A pipeline adds:

- grouped aggregations, computed fields, `distinct`, `unnest` and `union`;
- [joins](../pipeline-aggregations-and-joins.md) through subqueries;
- [text and geo search](../pipeline-search.md);
- [writes by query](../pipeline-writes.md): update, delete, insert and upsert of the documents a
  pipeline selects;
- Query Explain on Enterprise, where the Core `.explain()` is rejected with `INVALID_ARGUMENT`.

## When to use SQL

Application code keeps the fluent API. A field name in `path!(Book::rating)` is checked when the
program compiles, where a misspelled field in SQL text reads as an absent field and matches
nothing, without an error.

SQL is for text that the program does not write:

- statements typed by a person or a script, such as an admin command line;
- statements kept in configuration or in files;
- statements an LLM agent writes, which run with the
  [read-only options](../sql.md#read-only-options);
- queries ported from a SQL database, where `JOIN` and `GROUP BY` read faster than the builder.

## Checking the plan before production

- a pipeline without an index that covers its filter runs as a table scan, and costs in proportion
  to the collection;
- on Enterprise a Core query runs as a scan when no index covers it, where Standard fails with
  `FAILED_PRECONDITION`, and without `order_by` the order of its results is unspecified;
- Core `.explain()` is rejected on Enterprise, so the plan of a query there comes from the same
  query written as a pipeline.

See [Query plans and cost](./query-plans.md).

## Vector search

A Core [vector search](../vector-search.md) needs a vector index on Standard, and answered with and
without one on Enterprise. The pipeline [`find_nearest`](../pipeline-aggregations-and-joins.md#nearest-neighbours)
stage never uses the index, so its cost follows the size of the collection.

## Moving a Core query to a pipeline

The same filter can return other documents. Check these:

- `neq` matches the documents that lack the field;
- `sort` keeps the documents that lack the field, and needs a unique field last for a stable order;
- the stages run in the order they are written, so `limit` before `sort` limits first;
- `_firestore_id` is an `Option` after a projection, since the result has no document name.

The query below runs both ways. On a book without a `genre` field, the Core query leaves the book
out, and the pipeline returns it:

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct Book { title: String, genre: Option<String> }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
// Leaves out the books without a genre
let core: Vec<Book> = db
    .fluent()
    .select()
    .from("books")
    .filter(|q| q.field(path!(Book::genre)).neq("Horror"))
    .obj()
    .query()
    .await?;

// Returns the books without a genre too
let pipeline: Vec<Book> = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .filter(|q| q.field(path!(Book::genre)).neq("Horror"))
    .obj()
    .query()
    .await?;
# let _ = (core, pipeline);
# Ok(())
# }
```

To keep the Core result in the pipeline, add `q.field(path!(Book::genre)).exists()` to a `for_all`
with the `neq`. [Query and write semantics](./semantics.md) lists every difference.
