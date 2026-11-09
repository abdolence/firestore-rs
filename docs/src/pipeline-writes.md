# Pipeline writes

A pipeline can end in a write: it changes the documents that its source and stages select. A
write starts with its verb, then names its source, so the direction reads left to right, as in
Core: `delete().from(..)`, `update(..).in_col(..)`, `insert().into(..)`, `upsert(..).into(..)`.
Every stage of a read, such as `filter`, `sort` and `limit`, works between the source and the run.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let deleted = db
    .fluent()
    .pipeline()
    .delete()
    .from("books")
    .filter(|q| q.field("stock").eq(0))
    .execute()
    .await?;
println!("Deleted {} documents", deleted.documents_modified);
# Ok(())
# }
```

A write returns a count, `documents_modified`, and not the documents. It has no `explain`,
`query` or `obj`, and a write pipeline that calls them does not compile.

Full example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/pipeline-writes.rs).

## When to use a pipeline write

Core has no write by query. To change the documents a query matches, the program reads them,
computes the new values and writes each one back, with `update` or a
[batch writer](./batch-writes.md). A pipeline write selects and writes in one request, and
Firestore runs both on the server:

- no document is sent to the client, and the response is the count `documents_modified`;
- a value can be an expression over the fields of the document it is set on, so the program does
  not read a field to compute from it;
- `insert` and `upsert` take their documents from another collection, or from the output of a
  [join, aggregation](./pipeline-aggregations-and-joins.md) or [search](./pipeline-search.md);
- `.atomic()` makes the write apply in full or not at all, see [Atomic writes](#atomic-writes);
- many literal rows go in one request, each named by a field with `.document_id(..)`.

```rust,no_run
# use firestore::*;
# use serde::Serialize;
# #[derive(Debug, Clone, Serialize)]
# struct Item { sku: String, price: f64, stock: i64 }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
// Raises the price of every item in stock by a tenth, on the server
let repriced = db
    .fluent()
    .pipeline()
    .update(|e| [e.field(path!(Item::price)).multiply(1.1).alias(path!(Item::price))])
    .in_col("items")
    .filter(|q| q.field(path!(Item::stock)).greater_than(0))
    .execute()
    .await?;
# let _ = repriced;
# Ok(())
# }
```

A write to one document whose ID the program knows stays Core: it works on a Standard edition
database too, and it can be buffered in a transaction. What a pipeline write costs:

- it needs an Enterprise edition database, and fails on Standard with `FAILED_PRECONDITION`, see
  [Standard and Enterprise editions](./editions.md);
- it returns a count and not the documents;
- an atomic write has size limits, see [Write limits](#write-limits);
- the library never retries it, see [Retries and listeners](#retries-and-listeners);
- a transaction takes pipeline writes or buffered writes, not both, see
  [Writes in a transaction](#writes-in-a-transaction).

## The four writes

- `delete()` deletes each selected document;
- `update(|e| [..])` sets the fields it returns on each selected document, and leaves its other
  fields. Each item needs an alias unless it is a field. Its source is named with `.in_col(..)`,
  and `.documents(..)`, `.literals(..)` and `.database()` are available too;
- `insert()` creates each document, and fails with `ALREADY_EXISTS` when one exists;
- `upsert(|e| [..])` creates each document or replaces a stored one, with the fields it returns
  set on it. The stored document is replaced, not merged into.

`insert` and `upsert` write to the collection that `.into("archive")` names, relative to the
database's documents root, so it can be a path such as `"restaurants/rest-01/reviews"`.
`.document_id(|e| e.field("key"))` after `.into(..)` names each document by the ID the expression
evaluates to; the field it reads stays in the stored document. Without it, a document keeps the ID
of its `__name__`, and Firestore assigns one to a row that has none. Without `.into(..)`, each
document is written to its own `__name__`.

```rust,no_run
# use firestore::*;
# use serde::Serialize;
# #[derive(Debug, Clone, Serialize)]
# struct Item { sku: String, stock: i64 }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let inserted = db
    .fluent()
    .pipeline()
    .insert()
    .into("items")
    .document_id(|e| e.field(path!(Item::sku)))
    .literals([
        Item { sku: "a-1".to_string(), stock: 3 },
        Item { sku: "a-2".to_string(), stock: 0 },
    ])
    .execute()
    .await?;

let updated = db
    .fluent()
    .pipeline()
    .update(|e| [e.constant("sold_out").alias("status")])
    .in_col("items")
    .filter(|q| q.field(path!(Item::stock)).eq(0))
    .execute()
    .await?;
# let _ = (inserted, updated);
# Ok(())
# }
```

A literal row that names its document by `__name__` holds the full document path in a
`FirestoreReference`, such as `projects/p/databases/d/documents/items/a-1`; a relative path fails
with `INVALID_ARGUMENT`.

## Atomic writes

`.execute()` writes each document on its own. A failure stops the run and can leave part of the
documents written. An `update` of a document that does not exist writes nothing and counts 0.

`.atomic()` runs the write in a transaction of its own that commits as soon as the write is done,
so the write applies in full or not at all. An `update` of a missing document then fails with
`NOT_FOUND`:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let archived = db
    .fluent()
    .pipeline()
    .insert()
    .into("archive")
    .document_id(|e| e.field("key"))
    .from("books")
    .filter(|q| q.field("stock").eq(0))
    .atomic()
    .execute()
    .await?;
# let _ = archived;
# Ok(())
# }
```

Firestore requires an atomic write, or a transaction, for these:

- every `upsert`;
- an `insert` from any source but `literals`;
- a write after `aggregate`, `distinct`, `unnest`, `find_nearest`, `union`, a `search` or a
  subquery.

The library does not set `.atomic()` itself and does not check these rules: Firestore rejects a
write that breaks one with `INVALID_ARGUMENT`.

## Write limits

Firestore rejects an atomic write that is too big with `InvalidArgument`, "Transaction too big.
Decrease transaction size." The limit is not one number of documents: it depends on the
statement and on the size of the documents. For SQL statements, which run as atomic pipeline
writes, on documents of two small fields (an integer and a short string), the limits are about:

| Statement | Succeeds up to | Fails from |
|---|---|---|
| `UPDATE .. SET touched = 1 WHERE TRUE` | 55,000 documents | 60,000 documents |
| `DELETE FROM .. WHERE TRUE` | 5,475 documents | 5,500 documents |

The `DELETE` limit is about a tenth of the `UPDATE` limit. Larger documents lower both. A large
write commits at roughly 1,000 to 3,000 documents per second.

A large write can also fail with `ResourceExhausted` ("This database has exceeded their maximum
bandwidth for writes") or with `Aborted`. Wait, and send the write again in smaller parts.

Split a large write by a `filter` on a range, such as the key or a timestamp, and send each part
after the one before.

A TTL (time-to-live) policy suits retention better than a scheduled `delete`: Firestore deletes
the documents itself once the timestamp in the policy's field has passed, see
[TTL and the Enterprise edition](./index-management/ttl-and-enterprise.md#ttl-policy). The
[bulk delete](./bulk-delete.md) of the `admin` feature empties a whole collection.

## Writes in a transaction

`execute_in_transaction(&mut transaction)` runs the write in an open transaction, whether it comes
from `db.begin_transaction()` or from `run_transaction`. Firestore holds the write until the
transaction commits and drops it when the transaction rolls back. `.atomic()` has no counterpart
there, since an atomic write commits on its own, and `.atomic().execute_in_transaction(..)` does
not compile.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.run_transaction(|db, transaction| {
    Box::pin(async move {
        db.fluent()
            .pipeline()
            .update(|e| [e.constant("shipped").alias("status")])
            .in_col("orders")
            .filter(|q| q.field("status").eq("packed"))
            .execute_in_transaction(transaction)
            .await?;
        Ok(())
    })
})
.await?;
# Ok(())
# }
```

`execute_in_transaction` differs from Core's `add_to_transaction` in when the write happens.
`add_to_transaction` buffers a write in the library and sends it with the commit. A pipeline write
is sent when `execute_in_transaction` runs, with the transaction's ID, because the pipeline has to
run to find the documents it writes. Its count comes back at once, and the documents change at the
commit.

Firestore sets these rules for a transaction that holds a pipeline write:

- every read comes before the first pipeline write; Firestore fails a read after it with
  `FAILED_PRECONDITION`;
- a transaction takes pipeline writes or buffered writes, not both: Firestore rejects a commit
  that mixes them;
- a write cannot run at a read time, and `execute()` or `.atomic().execute()` on a `db` that is
  bound to a transaction or a read time fails with `InvalidParametersError`; use
  `execute_in_transaction` inside the transaction.

## Retries and listeners

The library sends a pipeline write once and never retries it: a run without a transaction can
have written part of its documents before it failed, and an `insert` sent again fails with
`ALREADY_EXISTS`. A `run_transaction` retries the whole transaction on `ABORTED`, as it does for
any other transaction, and a retried attempt starts again from the callback. A failed `.atomic()`
write reports a retry as a failed commit does: only `ABORTED`, which means nothing was applied,
is `retry_possible`.

The changes of a pipeline write reach listeners. A Core listener on the collection receives a
change for each inserted, updated or upserted document, and a delete for each deleted one. See
[Listening to document changes](./listening-changes.md).
