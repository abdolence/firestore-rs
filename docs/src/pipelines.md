# Pipelines

A *pipeline* is a query that Firestore runs as an ordered list of *stages*. The first stage is a
source, such as a collection; every later stage takes the documents of the stage before it and
passes its own output on: `filter`, `sort`, `fields`, `limit`, and so on. The library sends it
through Firestore's `ExecutePipeline` call.

Pipelines need a database of the Firestore *Enterprise edition*, the edition that runs them, see
[Standard and Enterprise editions](./editions.md). A pipeline sent to a Standard edition database
fails at once with `FAILED_PRECONDITION`, and the library does not retry it, unless a cache session
answers it, see [Caching pipelines](./caching.md#pipelines).

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
for what it covers. A pipeline adds grouped aggregations, computed fields, joins, search, writes by
query and Query Explain on Enterprise, see
[Choosing Core queries, pipelines or SQL](./editions/choosing-an-api.md).

Indexes are optional for a pipeline, except for a search. A pipeline without an index that covers
its filter runs as a table scan, and costs by the data it scans, see
[Query plans and cost](./editions/query-plans.md).

A pipeline with the same filter and order as a Core query can return other documents: `not_equal`
matches a document that lacks the field, `sort` keeps such documents, and `sort` adds no
tie-break on the document name. See [Query and write semantics](./editions/semantics.md).

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
cache when it only filters, sorts and pages cached documents, see
[Caching pipelines](./caching.md#pipelines).

## Query plans and indexes

`.explain()` and `.explain_with_options(..)` return the plan Firestore chose for a pipeline, and
`.force_index(..)` on a `from(..)` source picks the index it reads through. Both are described in
[Query plans and cost](./editions/query-plans.md#explaining-a-pipeline), with how to recognise a
table scan in the plan.
