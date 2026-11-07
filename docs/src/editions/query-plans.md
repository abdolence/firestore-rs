# Query plans and cost

What a read costs depends on the edition of the database and on whether an index serves it. This
page holds the billing model of each edition, how to read the plan Firestore chose for a query, and
the reads that are known to scan.

Two terms are used throughout:

- a *table scan* reads every document of the collection from primary storage, instead of reading
  an index;
- a *read unit* is the billing unit of reads on the Enterprise edition: 4 KiB of data, documents or
  index entries, that a read processes.

## Cost per edition

Standard bills each document read, written and deleted; Enterprise bills read units (4 KiB) and
write units (1 KiB, one more per indexed field) instead, and real-time updates as a separate
charge. The rates differ per region, and are on Google's pricing pages for
[Standard](https://cloud.google.com/firestore/pricing) and
[Enterprise](https://firebase.google.com/docs/firestore/enterprise/pricing).

### Standard edition

Google's [pricing page](https://firebase.google.com/docs/firestore/pricing) bills:

- each document read, written and deleted, the index entries a query reads, storage and network
  bandwidth;
- at least one document read for each query, even one that returns nothing;
- one read for each batch of up to 1000 index entries a query reads; a query with at most one range
  field is not charged for index entries;
- an aggregation (count, sum, avg) by the index entries it reads, and at least one document read;
- a listener one read each time a document is added to its results or updated in them, and when a
  document leaves them. A deleted document is not charged as a read.

### Enterprise edition

Google's [pricing page](https://firebase.google.com/docs/firestore/enterprise/pricing) bills:

- read units for the data a read processes, documents or index entries, counted in 4 KiB. Its
  example: a collection scan over 20 documents of 1 KiB each costs 5 read units, and an indexed
  scan of the same data costs 21, one for each document and one for the index read;
- write units counted in 1 KiB, and one more for each indexed field, so a 5 KiB document with 5
  single-field indexes costs 10 write units. A delete is billed as write units;
- real-time updates as a charge of their own: one unit per 4 KiB of an updated document, plus one
  when a document leaves the results.

A Core query that no index covers runs as a table scan on Enterprise, and is billed by the data it
reads: a scan of the whole collection reads every 4 KiB of it. On Standard the same query is either
refused or answered from an index, and billed per returned document.

A SQL statement that reads a document by ID, with `=` or `IN`, costs 1 read unit for each
document on the Enterprise edition, with or without a further condition such as `stock >= 0`.
Firestore's published pricing charges a point read of the Core `get` in 4 KiB tranches, rounded
up, so a document of up to 4 KiB costs 1 read unit through `get`, as it does through SQL. See
[Reads by ID](../sql/reads.md#reads-by-id).

### Both editions

A read that a [cache](../caching.md) session answers sends no request, so Firestore charges
nothing for it. Query Explain is never answered from the cache.

## Explaining a Core query (Standard edition)

Query Explain returns the plan Firestore chose for a Core query, in the metadata of the results:

```rust,no_run
# use firestore::*;
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
    .select()
    .from(TEST_COLLECTION_NAME)
    .explain()
    // or explain_with_options for options such as analyze, which runs the query to gather
    // additional statistics:
    // .explain_with_options(FirestoreExplainOptions::new().with_analyze(true))
    .stream_query_with_metadata()
    .await?;
# Ok(())
# }
```

Full example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/explain-query.rs).

On a Standard edition database, a Core `.explain()` of a query that needs a missing index fails
with `FAILED_PRECONDITION` and the message "no matching index found."

An Enterprise edition database rejects a Core `.explain()` with `INVALID_ARGUMENT`, "Explain
options are not supported in RunQuery API for Enterprise edition. Please use the ExecutePipeline
API instead." There, write the query as a pipeline to see its plan.

## Explaining a pipeline

Query Explain returns the plan Firestore chose for a pipeline. `.explain()` runs the pipeline for
its plan only: no document is read or returned. `.explain_with_options(..)` with `analyze` set
also returns the documents, and adds the execution profile to the plan. The plan arrives as text
in the metadata of the last item:

```rust,no_run
# use firestore::*;
# use futures::TryStreamExt;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let items: Vec<_> = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .filter(|q| q.field("genre").eq("Dystopian"))
    .explain()
    .stream_query_with_metadata()
    .await?
    .try_collect()
    .await?;

for item in items {
    if let Some(stats) = item.metadata.explain_stats {
        println!("{}", stats.text);
    }
}
# Ok(())
# }
```

The text is Firestore's own format. The library does not parse it, since Firestore can change it
at any time.

A SQL `SELECT` returns its plan the same way, through `explain()` and `explain_with_options(..)` of
the [SQL builder](../sql.md#setup).

### Recognising a full scan

The plan names how the pipeline reads its documents:

- a plan that ends in a `TableScan` node reads every document of the collection from primary
  storage;
- a plan that ends in a `SequentialScan` node with an `index:` line reads through that index,
  such as `index: /books (genre ASC, rating DESC)@[id = CICAgJim14AK]`;
- a search ends in a `StIndexSearch` node.

Run `.explain()` on a pipeline before it reaches production data, and add an index when it shows a
`TableScan` over a large collection. Firestore has no setting that rejects a table scan, and the
library adds no check of its own.

### Choosing an index

`.force_index(..)` on a `from(..)` source makes the pipeline read through the index with a given
ID, the one Firestore assigned to it. It is available on a collection source and on a
collection-group source, and nowhere else:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let by_genre = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .force_index(FirestoreIndexHint::Index("CICAgJim14AK".parse()?))
    .filter(|q| q.field("genre").eq("Dystopian"))
    .query()
    .await?;

let scanned = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .force_index(FirestoreIndexHint::TableScan)
    .filter(|q| q.field("genre").eq("Dystopian"))
    .query()
    .await?;
# let _ = (by_genre, scanned);
# Ok(())
# }
```

`FirestoreIndexHint::TableScan` forces a table scan. An ID that names no index of that collection
fails the request at once with `INVALID_ARGUMENT`. An index that cannot serve the filter is still
used, and the pipeline then reads all of it. A vector index cannot be forced.

## Known table scans

- **A pipeline without an index.** Indexes are optional for a pipeline. A pipeline without an index
  that covers its filter still runs, as a table scan. The cost of a pipeline follows the amount of
  data it scans, so a table scan over a large collection costs in proportion to the collection. A
  search is the exception: it needs a search index, and fails without one, see
  [Pipeline search](../pipeline-search.md#search-indexes);
- **A SQL statement without an index.** A statement that filters on an unindexed field scans the
  collection, as a pipeline does. Firestore has no setting that rejects a table scan, and the
  [read-only options](../sql.md#read-only-options) do not check for one;
- **A Core query on Enterprise.** On Enterprise a Core query runs as a scan when no index covers
  it, where Standard fails with `FAILED_PRECONDITION`, and without `order_by` the order of its
  results is unspecified. Its plan cannot be shown, since Core `.explain()` is rejected there;
- **Pipeline `find_nearest`.** The stage does not use a vector index. Its plan is a `TableScan`
  followed by a sort by distance, even when a vector index exists on the field, so its cost follows
  the size of the collection. A Core [vector search](../vector-search.md) uses the index on
  Standard;
- **Joins and subqueries.** A subquery runs once for each row of the query it is in, whether or not
  it is *correlated*, that is, whether or not it reads a value of the row it runs for. An
  unindexed join scans the joined collection once for each left row, see
  [Join cost](../sql/reads.md#join-cost);
- **Null or absent in one filter.** `genre IS NULL OR is_absent(genre)` loses the index range of
  `genre IS NULL`, see [Query and write semantics](./semantics.md#null-and-absent).
