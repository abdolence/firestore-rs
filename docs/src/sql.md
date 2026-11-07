# SQL

The library translates a SQL statement into a [pipeline](./pipelines.md) and runs it. It is meant
for SQL text that comes from outside the program: command line tools, admin scripts, reports
defined in configuration, LLM agents, and queries ported from a SQL database. Reads return the
documents of a pipeline, and `INSERT`, `UPDATE` and `DELETE` run as [pipeline writes](./pipeline-writes.md).
With the `admin` feature it also runs [index statements](./sql/indexes.md): `CREATE INDEX`,
`DROP INDEX`, `ALTER COLLECTION` and `SHOW INDEXES`.

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct Book { title: String, rating: Option<f64> }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let books: Vec<Book> = db
    .fluent()
    .sql("SELECT title, rating FROM books WHERE genre = @genre ORDER BY rating DESC LIMIT 10")
    .param("genre", "Dystopian")
    .obj()
    .query()
    .await?;
# let _ = books;
# Ok(())
# }
```

Full example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/sql.rs).

SQL needs a database of the Firestore *Enterprise edition*, as pipelines do: a statement sent to a
Standard edition database fails with `FAILED_PRECONDITION`, unless a cache session answers it.
The index statements and `SHOW COLLECTIONS` use the Admin and Core APIs, and run on both editions.
A statement means what the same pipeline means in the fluent API: Firestore's semantics, listed in
[Semantics](#semantics), where a few results differ from those of a SQL database.

A read through a cache session is answered from the cache when the pipeline it translates to only
filters, sorts and pages cached documents, see [Caching pipelines](./caching.md#sql) for the cached
shapes.

## When to use SQL

Application code keeps the fluent API, whose field paths are checked when the program compiles. SQL
is for text that the program does not write: an admin command line, statements kept in
configuration, statements an LLM agent writes, and queries ported from a SQL database. See
[Choosing Core queries, pipelines or SQL](./editions/choosing-an-api.md#when-to-use-sql).

## Setup

SQL is behind the `sql` cargo feature, off by default:

```toml
[dependencies]
firestore = { version = "...", features = ["sql"] }
```

`db.fluent().sql(text)` starts a statement. The rest of the builder is:

- `.param(name, value)` binds `@name`, and `.params(values)` binds the `?` parameters in order;
- `.options(FirestoreSqlOptions)` limits what the statement may do, see
  [Read-only options](#read-only-options);
- `.obj::<T>()` deserialises the rows into `T`, as in a pipeline;
- `query()` returns every row, and `stream_query()`, `stream_query_with_errors()` and
  `stream_query_with_metadata()` stream them;
- `run()` runs any statement, a `SELECT`, a write, `SHOW` or an index statement, and returns a
  `FirestoreSqlOutcome` with one variant for each kind of statement, see
  [Index statements](./sql/indexes.md#running-a-statement);
- `.wait_until_ready(timeout)` and `.wait_until_ready_with_options(..)`, with the `admin` feature,
  make `run()` wait for the operation of a `CREATE INDEX` or an `ALTER COLLECTION`;
- `execute()` and `execute_in_transaction(&mut transaction)` run an `INSERT`, `UPDATE` or
  `DELETE`, see [Writes](#writes);
- `explain()` and `explain_with_options(..)` return the plan of a `SELECT` in the metadata of the
  last item of `stream_query_with_metadata()`, and so does an `EXPLAIN [ANALYZE]` prefix in the
  text, see [Explaining a pipeline](./editions/query-plans.md#explaining-a-pipeline);
- `.request_tags(..)` and `.request_options(..)` attach request options to one statement, as they
  do for a pipeline.

A row holds the columns the statement selects, so it has no document name unless the statement
selects `__name__`. An `_firestore_id` field of the result type must be an `Option`, as described in
[Pipelines](./pipelines.md#typed-results-and-metadata). A field that a document lacks is absent
from its row, and the result type needs an `Option` for it too.

A statement that is a `SELECT` fails from `execute()`, and a write fails from `query()`, with
`InvalidParametersError` and before any request. So does a `SHOW` or an index statement from
either of them: they run with `run()`.

`FirestoreSqlStatement::translate(&db, text, &parameters, &options)` translates a statement without
running it. The `Query` variant holds a `FirestorePipeline`, which `from_pipeline(..)` runs:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let statement = FirestoreSqlStatement::translate(
    &db,
    "SELECT title FROM books WHERE stock > @min",
    &FirestoreSqlParameters::new().param("min", 0),
    &FirestoreSqlOptions::new(),
)?;

if let FirestoreSqlStatement::Query(pipeline) = statement {
    let rows = db.fluent().pipeline().select().from_pipeline(pipeline).query().await?;
    # let _ = rows;
}
# Ok(())
# }
```

## Dialect and parameters

The dialect is BigQuery's (GoogleSQL), and there is no switch. It reads a collection ID with
hyphens or slashes between backticks, `"text"` as a string literal, `@name` as a parameter and
`UNNEST(..) WITH OFFSET`. A statement holds exactly one SQL statement: text that parses to none or
to several is refused.

Parameters are the way to put a value into a statement:

- `@name` is bound with `.param("name", value)`;
- `?` is bound with `.params([..])`, in the order the `?`s appear;
- a statement uses one form, and mixing them is refused, and so is `$1` or `:name`;
- the value is any `Serialize` type, bound as a typed constant of the pipeline, as a value in the
  fluent API is. A string parameter stays a string, and a number stays a number;
- binding is strict: a parameter that the text uses and no value is bound to is an error, and so
  is a bound value that the text does not use;
- a backticked `` `@name` `` is a field name.

A parameter is a value. It can stand where a value, a `LIMIT` or `OFFSET` count, or a search query
can stand, and it cannot name a collection, a field or a source: `collection_group(..)`,
`documents(..)` and `subcollection(..)` take string literals.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let rows = db
    .fluent()
    .sql("SELECT title FROM books WHERE genre = ? AND stock > ? LIMIT ?")
    .params([serde_json::json!("Dystopian"), serde_json::json!(0), serde_json::json!(5)])
    .query()
    .await?;
# let _ = rows;
# Ok(())
# }
```

## Injection

A parameter value never enters the SQL text, so it cannot change what the statement does. This
holds for every value that is bound.

Be aware of what a parameter does not cover:

- **Collection and field names must never come from input.** Building them into the text with
  `format!` gives whoever supplies the name control of the statement. Map the input to a name
  from a fixed list, and refuse anything else. For text that arrives from outside the program,
  the collection allowlist of the [read-only options](#read-only-options) is the second line of
  defence;
- **`%` and `_` in a `LIKE` parameter are still wildcards.** `title LIKE @pattern` with the value
  `%` matches every title. A backslash before `%` makes it literal. To match user text as it is,
  use `starts_with(title, @text)`, `ends_with(..)` or `string_contains(..)`, which have no
  wildcards;
- **A text search parameter keeps the search operators.** The value of `document_matches(@query)`
  goes to the search as written, so `-bacon` excludes a term and a quoted phrase matches as a
  phrase. See [Pipeline search](./pipeline-search.md#what-the-grammar-supports) for the grammar.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb, requested: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let column = match requested {
    "title" => "title",
    "rating" => "rating",
    "pages" => "pages",
    _ => return Err(format!("{requested} is not a column to sort by").into()),
};

let rows = db
    .fluent()
    .sql(format!("SELECT title FROM books ORDER BY {column}"))
    .query()
    .await?;
# let _ = rows;
# Ok(())
# }
```

## Read-only options

`FirestoreSqlOptions` limits a statement before it runs. A statement that breaks a limit fails at
translation with `InvalidParametersError`, and no request is sent. `FirestoreSqlOptions::new()`
allows every statement the translator supports, except [joins and
subqueries](./sql/reads.md#joins-and-subqueries), and
`FirestoreSqlOptions::read_only()` starts the set for text from outside the program:

- `read_only()` refuses `INSERT`, `UPDATE` and `DELETE`, the `database()` source, and `CREATE`,
  `DROP` and `ALTER COLLECTION`. It allows `SELECT`, `SHOW` and `EXPLAIN [ANALYZE] SELECT`, which
  only read;
- `.refuse_collection_groups()` refuses `collection_group(..)`, which reads every collection with
  that ID under any document;
- `.with_collections([BOOKS, REVIEWS])` allows only these `FirestoreCollectionId`s, wherever the
  statement names one: a collection, each collection of a path, a collection group and the paths of
  `documents(..)`. It refuses `database()` too;
- `.with_max_rows(1_000)` returns at most that many rows: the `LIMIT` is lowered to it, or one is
  added. The cap counts the rows returned, not the rows read: an `OFFSET` still reads the rows it
  skips, and `ARRAY_AGG(..)` or `ARRAY(SELECT ..)` puts a whole collection into one row;
- `.allow_subqueries()` allows joins and subqueries, see
  [Joins and subqueries](./sql/reads.md#joins-and-subqueries).

The translator also refuses a statement longer than 16 KiB, the parser one nested deeper than 16
levels, and the translator one nested deeper than 64. These limits are fixed.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb, text: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
const BOOKS: FirestoreCollectionId = FirestoreCollectionId::from_static("books");
const REVIEWS: FirestoreCollectionId = FirestoreCollectionId::from_static("reviews");

let rows = db
    .fluent()
    .sql(text)
    .options(
        FirestoreSqlOptions::read_only()
            .refuse_collection_groups()
            .with_collections([BOOKS, REVIEWS])
            .with_max_rows(1_000),
    )
    .query()
    .await?;
# let _ = rows;
# Ok(())
# }
```

The options guard against what a statement names. What it can reach stays bounded by the IAM roles
of the credentials the client runs with: server client libraries are not subject to Firestore
Security Rules. A client that runs text from outside the program needs credentials whose role can read and cannot
write.

Firestore has no setting that rejects a table scan, and the options do not check for one. A
statement that filters on an unindexed field scans the collection, as a pipeline does, see
[Known table scans](./editions/query-plans.md#known-table-scans).

## Index statements

`CREATE INDEX`, `DROP INDEX`, `ALTER COLLECTION`, `SHOW INDEXES` and `SHOW COLLECTIONS` run with
`run()`. [Index statements](./sql/indexes.md) lists their forms, outcomes, waits and refusals.

## Reads, joins and subqueries

[Reads, joins and subqueries](./sql/reads.md) lists the sources a `FROM` clause names, reads by
ID, the supported and refused constructs, search, and joins and subqueries with their cost.

## Semantics

A SQL statement keeps Firestore's semantics, because it runs as a pipeline. Where that differs
from what a SQL database does, and from a Core query, is listed in one table for pipelines and SQL:
[Query and write semantics](./editions/semantics.md). The main cases:

- `v IS NULL` matches null only, and a document that lacks `v` does not match;
- `v != 1` matches null and absent too;
- a selected field that a document lacks is missing from its row, where a SQL database holds
  `NULL`;
- a comparison matches values of its own type, and a misspelled field is an absent field, with no
  error.

## Writes

`INSERT`, `UPDATE` and `DELETE` run with `execute()`, which returns the count of documents
written in `documents_modified`, or with `execute_in_transaction(&mut transaction)` in a
transaction, which behaves as in [Pipeline writes](./pipeline-writes.md#writes-in-a-transaction).

Every SQL write is atomic: it applies in full or not at all, as a SQL statement does. It is sent
once and never retried, and a failed write has `retry_possible` set only for `ABORTED`, which means
nothing was applied. To have a write retried on `ABORTED`, run it inside `run_transaction`, see
[Concurrent writes](./sql/writes.md#concurrent-writes). A write has no `explain`, and `.explain()`
on one fails with
`InvalidParametersError`.

| Statement | Runs as |
|---|---|
| `INSERT INTO books (title, stock) VALUES ('A', 1), ('B', 2)` | an insert of the rows |
| `INSERT INTO archive SELECT * FROM books WHERE stock = 0` | an insert of the rows of a `SELECT` |
| `INSERT OR REPLACE INTO books (__name__, title) VALUES ('b1', 'A')` | an upsert: it creates the document or replaces the stored one, and fields not listed are gone |
| `INSERT .. ON CONFLICT (__name__) DO NOTHING` | an insert of the rows that have no stored document |
| `INSERT .. ON CONFLICT (__name__) DO UPDATE SET stock = stock + excluded.stock` | an upsert that sets the listed fields of a stored document |
| `UPDATE books SET stock = 0, dims.w = 2 WHERE ..` | an update of the selected documents; a dotted target sets a nested field |
| `DELETE FROM books WHERE ..` | a delete of the selected documents |

The rules of each statement, `ON CONFLICT` in detail, concurrent writes and the size limit of a
write are in [Writes and ON CONFLICT](./sql/writes.md).

## Errors

A text that does not parse, a construct that is refused, a parameter bound on one side only, and a
statement that the options refuse all fail with `InvalidParametersError`, before any request. Its
`field` names the construct: the keyword or function, the `@name` of a parameter, or `sql` for text
that does not parse. `error` says why, and `location` holds the `line` and `column` where the
construct starts, counting from 1, when the position is known.

```rust,no_run
# use firestore::*;
# use firestore::errors::FirestoreError;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let result = db
    .fluent()
    .sql("SELECT * FROM books TABLESAMPLE SYSTEM (10 PERCENT)")
    .query()
    .await;

if let Err(FirestoreError::InvalidParametersError(error)) = result {
    println!("{} at {:?}: {}", error.public.field, error.public.location, error.public.error);
}
# Ok(())
# }
```

Errors from Firestore itself arrive as the other `FirestoreError` variants, as they do for any
pipeline. An unknown function, a type mismatch and a write that is too big are among them.
