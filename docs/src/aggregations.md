# Aggregations

The library supports the Core aggregation functions `count`, `sum` and `avg` over the documents a
query matches. Each result field is named by `a.field(alias)`, and its operator is chosen with
`.count()`, `.count_up_to(limit)`, `.sum(field)` or `.avg(field)`:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyAggTestStructure { counter: usize }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let objs: Vec<MyAggTestStructure> = db
    .fluent()
    .select()
    .from(TEST_COLLECTION_NAME)
    .aggregate(|a| a.fields([a.field(path!(MyAggTestStructure::counter)).count()]))
    .obj()
    .query()
    .await?;
# let _ = objs;
# Ok(())
# }
```

Several aggregations run in one query, each under its own alias:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct Totals { count: usize, total: i64, average: f64 }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let totals: Vec<Totals> = db
    .fluent()
    .select()
    .from(TEST_COLLECTION_NAME)
    .aggregate(|a| {
        a.fields([
            a.field(path!(Totals::count)).count(),
            a.field(path!(Totals::total)).sum("n"),
            a.field(path!(Totals::average)).avg("n"),
        ])
    })
    .obj()
    .query()
    .await?;
# let _ = totals;
# Ok(())
# }
```

An aggregation result is not a stored document, so it has no document name, see
[Query and write semantics](./editions/semantics.md#results-without-a-document-name). Grouped
aggregations, by a field, are a pipeline feature on the Enterprise edition, see
[Pipeline aggregations and joins](./pipeline-aggregations-and-joins.md).

## On the Enterprise edition

Core aggregations and transactions behave the same on both editions. An aggregation whose filter
needs a composite index fails on Standard with `FAILED_PRECONDITION`, and runs as a scan on
Enterprise, see [Querying](./querying.md#on-the-enterprise-edition).
