# Pipelines

A *pipeline* is a query that Firestore runs as an ordered list of *stages*. The first stage is a
source, such as a collection; every later stage takes the documents of the stage before it and
passes its own output on: `filter`, `sort`, `fields`, `limit`, and so on. The library sends it
through Firestore's `ExecutePipeline` call.

Pipelines need a database of the Firestore *Enterprise edition*, the edition that runs them. A
pipeline sent to a Standard edition database fails at once with `FAILED_PRECONDITION`, and the
library does not retry it.

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct Book { title: String, author: String, rating: Option<f64>, stock: i64 }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let books: Vec<Book> = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .filter(|q| {
        q.for_all([
            q.field(path!(Book::rating)).greater_than_or_equal(4.2),
            q.field(path!(Book::stock)).greater_than(0),
        ])
    })
    .sort(|e| [e.field(path!(Book::rating)).desc()])
    .limit(10)
    .obj()
    .query()
    .await?;
# let _ = books;
# Ok(())
# }
```

Full example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/pipeline-query.rs).

## When to use a pipeline

The Core query API (`db.fluent().select()`) stays the choice for a Standard edition database, and
for what it covers. A pipeline adds:

- grouped aggregations, computed fields, `distinct`, `unnest` and `union`;
- [joins](./pipeline-aggregations-and-joins.md) through subqueries;
- [text and geo search](./pipeline-search.md);
- [writes by query](./pipeline-writes.md): update, delete, insert and upsert of the documents a
  pipeline selects;
- Query Explain on Enterprise, where the Core `.explain()` is rejected with `INVALID_ARGUMENT`.

Indexes are optional for a pipeline. A pipeline without an index that covers its filter still
runs, as a *table scan*: Firestore reads every document of the collection from primary storage
instead of reading an index. The cost of a pipeline follows the amount of data it scans, so a
table scan over a large collection costs in proportion to the collection. See
[Recognising a full scan](#recognising-a-full-scan).

Three behaviours differ from a Core query with the same filter and order:

- `not_equal` is true for a document that lacks the field, while a Core `!=` leaves the document
  out;
- `sort` keeps the documents that lack the sorted field and orders them as null, while Core
  leaves them out;
- `sort` adds no tie-break on the document name; list a unique field last to fix the order of
  documents with equal values.

## Sources

`db.fluent().pipeline().select()` starts a pipeline that reads documents, and the next call picks
the source, in the words of Core's `select().from(..)`:

- `.from("books")` reads a root collection;
- `.from("reviews").parent(db.parent_path("restaurants", "rest-01")?)` reads the collection under
  a document, as `parent` does in Core;
- `.from("reviews").all_descendants()` reads every collection with that ID, anywhere in the
  database, as a Core collection-group query does;
- `.documents(["books/b1", "books/b2"])` reads the documents at those paths, relative to the
  database's documents root; a path that names no document is left out;
- `.literals(rows)` starts from rows given in the request instead of stored documents, and every
  row serialises to a map;
- `.database()` reads every document of the database;
- `.from_pipeline(pipeline)` runs a pipeline built with the model types, such as
  `FirestorePipeline`.

`parent` and `all_descendants` exclude each other: a pipeline that calls both does not compile,
since Firestore ignores a parent on a collection group.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let parent = db.parent_path("restaurants", "rest-01")?;

let reviews = db
    .fluent()
    .pipeline()
    .select()
    .from("reviews")
    .parent(parent)
    .query()
    .await?;

let every_review = db
    .fluent()
    .pipeline()
    .select()
    .from("reviews")
    .all_descendants()
    .query()
    .await?;
# let _ = (reviews, every_review);
# Ok(())
# }
```

## Filters

`.filter()` takes the same filter builder `q` as Core. A condition starts with
`q.field(path!(Book::rating))` and ends in a comparison; `eq`, `neq`, `is_in` and `is_not_in`
keep Core's names. `q.for_all` joins conditions with AND and `q.for_any` with OR; both nest, and
both drop a `None` entry, so an optional condition is written as it is in Core:

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct Book { title: String, genre: Option<String>, rating: Option<f64>, stock: i64 }
# async fn example(db: FirestoreDb, genre: Option<&str>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let books: Vec<Book> = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .filter(|q| {
        q.for_all([
            q.for_any([
                q.field(path!(Book::stock)).greater_than(0),
                q.field(path!(Book::title)).starts_with("The "),
            ]),
            genre.and_then(|genre| q.field(path!(Book::genre)).eq(genre)),
        ])
    })
    .obj()
    .query()
    .await?;
# let _ = books;
# Ok(())
# }
```

A closure that returns `None`, such as a `for_all` of conditions that are all `None`, adds no
stage and keeps every document.

Besides the comparisons, the builder has `is_null`, `is_nan`, `exists`, `is_absent`, `is_type`,
`starts_with`, `ends_with`, `string_contains`, `like`, `regex_contains`, `regex_match`,
`array_contains`, `array_contains_all`, `array_contains_any` and `equal_any`. A condition on a
computed value starts with `q.expr(..)`, and `q.condition(..)` adds a condition built with the
value builder below.

## The expression builder

Every closure of a pipeline except `filter` receives the value builder `e`, whose conditions are
plain values rather than `Option`s. It starts expressions and combines them:

- `e.field(path!(Book::title))` is a field of the current document;
- `e.constant(10)` is a constant; a value passed to a comparison becomes one without it;
- `e.and([..])`, `e.or([..])`, `e.not(..)` combine conditions;
- `e.conditional(condition, if_true, if_false)`, `e.coalesce([..])`, `e.array([..])` and
  `e.map([..])` build values;
- `e.variable("name")` reads a variable set by `define_variables`.

An operation on one expression is a method on it: `e.field("price").multiply(e.field("count"))`,
`e.field("title").to_lower()`, `e.field("tags").array_length()`. A function without a method of
its own is called with `e.raw("name", [args])`.

## Projection and computed fields

`.fields()` replaces each document with the fields it returns; an item that is not a plain field
needs an alias. `.add_fields()` computes fields and keeps the others, and `.remove_fields()`
drops fields by path:

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct Book { title: String, author: String, pages: i64 }
# #[derive(Debug, Clone, Deserialize)]
# struct Summary {
#     #[serde(alias = "_firestore_id")]
#     id: Option<String>,
#     title: String,
#     author_name: String,
#     reading_hours: f64,
# }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let summaries: Vec<Summary> = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .fields(|e| {
        [
            FirestoreSelection::from(e.field(path!(Book::title))),
            e.field(path!(Book::author)).alias("author_name"),
            e.field(path!(Book::pages)).divide(40.0).alias("reading_hours"),
        ]
    })
    .obj()
    .query()
    .await?;
# let _ = summaries;
# Ok(())
# }
```

A computed item without an alias is rejected with `InvalidParametersError` before the request
is sent. The items of one list can have different types, so a list that mixes plain fields and
aliased ones starts its plain field with `FirestoreSelection::from(..)`, as above.

## Sorting and paging

`.sort()` takes a list of orderings, `e.field(..).asc()` or `.desc()`. `.limit(n)` keeps the
first `n` documents and `.offset(n)` skips the first `n`. They are stages, so their position
matters: a `limit` before a `sort` limits the documents first and sorts what is left, which is
not what the same two calls mean in Core.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let page = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .sort(|e| [e.field("rating").desc(), e.field("title").asc()])
    .offset(20)
    .limit(10)
    .query()
    .await?;
# let _ = page;
# Ok(())
# }
```

## Typed results and metadata

`.obj::<T>()` deserialises the results into `T`, and `.query()`, `.stream_query()`,
`.stream_query_with_errors()` and `.stream_query_with_metadata()` have the shapes they have in
Core. Without `.obj()` the results are `FirestoreDocument`s.

A stored document carries its name, so `_firestore_id` and the timestamp fields are filled as
they are in Core. The result of `.fields()`, `.aggregate()` and the other stages that build new
documents has no name, which leaves `_firestore_id` out: a field of `T` for it must be an
`Option`. Selecting `e.field("__name__")` keeps the document name in the result, without the
timestamps.

`.stream_query_with_metadata()` returns each result as a `FirestoreWithPipelineMetadata<T>`: the
`document`, and the `metadata` of the response it arrived in, with its `execution_time`. A
response that carries explain statistics, or no documents at all, adds an item without a
`document`.

`.request_tags(..)` and `.request_options(..)` attach request options to one pipeline, as they do
for a Core query.

A session created with `read_through_cache` or `read_cached_only` answers a pipeline from the
cache when the cache evaluates every part of it, see [Caching pipelines](./caching.md#pipelines).

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
