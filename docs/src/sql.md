# SQL

The library translates a SQL statement into a [pipeline](./pipelines.md) and runs it. It is meant
for SQL text that comes from outside the program: command line tools, admin scripts, reports
defined in configuration, LLM agents, and queries ported from a SQL database. Reads return the
documents of a pipeline, and `INSERT`, `UPDATE` and `DELETE` run as [pipeline writes](./pipeline-writes.md).

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

SQL needs a database of the Firestore *Enterprise edition*, as pipelines do. A statement means what
the same pipeline means in the fluent API: Firestore's semantics, listed in
[Semantics](#semantics), where a few results differ from those of a SQL database.

## When to use SQL

Application code keeps the fluent API. A field name in `path!(Book::rating)` is checked when the
program compiles, where a misspelled field in SQL text reads as an absent field and matches
nothing, without an error.

SQL is for text that the program does not write:

- statements typed by a person or a script, such as an admin command line;
- statements kept in configuration or in files;
- statements an LLM agent writes, which run with the [read-only options](#read-only-options);
- queries ported from a SQL database, where `JOIN` and `GROUP BY` read faster than the builder.

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
- `execute()` and `execute_in_transaction(&mut transaction)` run an `INSERT`, `UPDATE` or
  `DELETE`, see [Writes](#writes);
- `explain()` and `explain_with_options(..)` return the plan of a `SELECT` in the metadata of the
  last item of `stream_query_with_metadata()`, see
  [Explaining a pipeline](./pipelines.md#explaining-a-pipeline).

A row holds the columns the statement selects, so it has no document name unless the statement
selects `__name__`. An `_firestore_id` field of the result type must be an `Option`, as described in
[Pipelines](./pipelines.md#typed-results-and-metadata). A field that a document lacks is absent
from its row, and the result type needs an `Option` for it too.

A statement that is a `SELECT` fails from `execute()`, and a write fails from `query()`, with
`InvalidParametersError` and before any request.

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

- `@name` is bound with `.param("name", value)`, and `.param("@name", value)` binds the same;
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
subqueries](#joins-and-subqueries) and statements longer than 16 KiB, and
`FirestoreSqlOptions::read_only()` starts the set for text from outside the program:

- `read_only()` refuses `INSERT`, `UPDATE` and `DELETE`, and the `database()` source;
- `.refuse_collection_groups()` refuses `collection_group(..)`, which reads every collection with
  that ID under any document;
- `.with_collections([BOOKS, REVIEWS])` allows only these `FirestoreCollectionId`s, wherever the
  statement names one: a collection, each collection of a path, a collection group and the paths of
  `documents(..)`. It refuses `database()` too;
- `.with_max_rows(1_000)` returns at most that many rows: the `LIMIT` is lowered to it, or one is
  added. The cap counts the rows returned, not the rows read: an `OFFSET` still reads the rows it
  skips, and `ARRAY_AGG(..)` or `ARRAY(SELECT ..)` puts a whole collection into one row;
- `.with_max_length(bytes)` refuses a longer statement, and the default is 16 KiB;
- `.allow_subqueries()` allows joins and subqueries, see
  [Joins and subqueries](#joins-and-subqueries).

The parser also refuses a statement nested deeper than 16 levels, and the translator one nested
deeper than 64. These limits are fixed.

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
[Recognising a full scan](./pipelines.md#recognising-a-full-scan).

## Sources

The `FROM` clause names where the documents come from:

| Source | Reads |
|---|---|
| ``FROM books`` or ``FROM `books` `` | a root collection; backticks hold an ID with hyphens or dots |
| `` FROM `restaurants/rest-01/reviews` `` | the collection under a document |
| `FROM collection_group('reviews')` | every collection with that ID, anywhere in the database |
| `FROM documents('books/b1', 'books/b2')` | the documents at those paths, relative to the documents root; a path that names no document is left out |
| `FROM database()` | every document of the database |
| `FROM subcollection('reviews')` | the collection under the current document, inside a [subquery or join](#joins-and-subqueries) |
| `FROM (SELECT ..) AS t` | a derived table: the rows of another `SELECT` |
| `FROM cte` | a [common table expression](#joins-and-subqueries) |

A table alias is written `FROM books AS b`. A name that is defined by `WITH` matches without regard
to case, as a table alias does, and hides a collection of the same name.

## Reads by ID

A `WHERE` on `__name__`, `__name__ = 'book-01'`, `__name__ IN ('book-01', 'book-02')` or
`__name__ IN UNNEST(@ids)`, reads each document by its name: the plan is a lookup of the named
documents. A bare ID is a document of the `FROM` collection, and a path such as
`books/book-01` is accepted too.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let rows = db
    .fluent()
    .sql("SELECT title FROM books WHERE __name__ IN UNNEST(@ids)")
    .param("ids", ["book-01", "book-02"])
    .query()
    .await?;
# let _ = rows;
# Ok(())
# }
```

Query Explain with `analyze` on a database of the Enterprise edition reported 1 read unit for each
document read by ID, for `=` and for `IN`, with or without a further condition such as
`stock >= 0`. Firestore's published pricing charges a point read of the Core `get` in 4 KiB
tranches, rounded up, so a document of up to 4 KiB costs 1 read unit through `get`, as it does
through SQL.

A name compared with a column, such as the `__name__` of a joined table, is described in
[Join cost](#join-cost).

## Supported constructs

| Construct | Notes |
|---|---|
| `SELECT` list | columns, `AS` aliases, computed columns with an alias, `*` and `* EXCEPT (a, b)`; nested fields as `awards.hugo`, `awards['nebula']` and `tags[OFFSET(0)]` |
| `WHERE` | comparisons, `AND`, `OR`, `NOT`, `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM`, `[NOT] IN (list)`, `IN UNNEST(array)`, `BETWEEN`, `LIKE`, `ILIKE`, `CASE`, `COALESCE`, `IFNULL` |
| `ORDER BY`, `LIMIT`, `OFFSET` | `ASC`, `DESC`, `NULLS FIRST`, `NULLS LAST`; `LIMIT` and `OFFSET` take an integer or a parameter |
| `GROUP BY`, `HAVING` | columns and expressions; `HAVING` may use an aggregate that the select list does not hold |
| Aggregates | `COUNT(*)`, `COUNT(x)`, `COUNT(DISTINCT x)`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNTIF`, `ARRAY_AGG`, `ARRAY_AGG(DISTINCT x)`, `FIRST`, `LAST` |
| `SELECT DISTINCT` | over the selected columns |
| `UNION [ALL]`, `UNION DISTINCT` | each branch a plain `SELECT` with the same column names in the same order, or all `*` with `UNION ALL`; the query's `ORDER BY` and `LIMIT` apply to the union |
| Text and geo search | `document_matches('query')` and `geo_distance(field, geo_point(lat, lng)) <= meters` as top-level `AND` conditions of the `WHERE`, see [Search](#search) |
| Subqueries | scalar `(SELECT ..)`, `[NOT] EXISTS`, `[NOT] IN (SELECT ..)` and `ARRAY(SELECT ..)`, see [Joins and subqueries](#joins-and-subqueries) |
| Derived tables, `WITH` | non-recursive CTEs |
| Joins | `[INNER] JOIN`, `LEFT JOIN`, `CROSS JOIN`, a comma in `FROM`, and `UNNEST(array) AS x [WITH OFFSET AS pos]` |
| Writes | `INSERT .. VALUES`, `INSERT .. SELECT`, `INSERT OR REPLACE`, `INSERT .. ON CONFLICT`, `UPDATE`, `DELETE`, see [Writes](#writes) |
| Operators | `+ - * / %`, `||` |
| Functions | any other call passes to the pipeline function of that name, such as `starts_with(title, @text)` or `char_length(title)`; Firestore rejects a name it does not have with `INVALID_ARGUMENT` |

SQL names that differ from the pipeline function are renamed: `LOWER` to `to_lower`, `UPPER` to
`to_upper`, `POWER` to `pow`, `IFNULL` to `if_null`, `REGEXP_CONTAINS` to `regex_contains`,
`AVG` to `average`, `MIN` to `minimum`, `MAX` to `maximum`, `COUNTIF` to `count_if`.
`SUBSTR`, `SUBSTRING` and `STRPOS` count from 1 as in SQL, and `ILIKE` compares both sides in lower
case. A `SUBSTR` position of 0 or below passes to Firestore as written: 0 is the whole string, and a
negative position counts from the end with -1 the last character, as in SQL. `ORDINAL(0)` reads
the first element, and a negative `ORDINAL` or `OFFSET` counts from the end, where SQL fails or
gives `NULL`. `NULLS FIRST` and `NULLS LAST` add a leading sort key that tests for an absent or null
value, since Firestore sorts both first in an ascending order.

### Search

The search is part of the source, so the translator lifts it out of the `WHERE`: one
`document_matches('query')` or one `geo_distance(..) <= meters` that is a top-level `AND`
condition, on a collection or collection group source and before any join. `score() AS relevance`
selects the relevance, and `ORDER BY score() DESC` or `ORDER BY geo_distance(..)` as the only sort
key becomes the search's own sort. A search needs a search index, see
[Search indexes](./pipeline-search.md#search-indexes).

### Refused constructs

A construct with no pipeline form fails with `InvalidParametersError` that names it, before any
request. Anything in a parsed statement that the translator does not map is refused and never
dropped, so a statement does not run with a clause missing.

| Construct | Reason |
|---|---|
| Pipe syntax, `FROM t \|> WHERE ..` | not supported in this version |
| `MERGE`, `REPLACE INTO` | `MERGE` has no pipeline form, and `REPLACE INTO` does not parse in the BigQuery dialect; use `INSERT OR REPLACE` or `INSERT .. ON CONFLICT` |
| DDL, `BEGIN`, `EXPLAIN` | a statement is a `SELECT`, `INSERT`, `UPDATE` or `DELETE`; transactions come from the API, and `.explain()` returns the plan |
| `WITH RECURSIVE`, a CTE column list, `MATERIALIZED` | pipelines have no iteration |
| Window functions, `QUALIFY`, `WINDOW` | pipelines have no window stage |
| `INTERSECT`, `EXCEPT`, `UNION BY NAME` | pipelines have no set stage for them |
| `RIGHT JOIN`, `FULL JOIN` | no pipeline form keeps the unmatched rows of the joined side |
| `JOIN .. USING`, `NATURAL JOIN`, a join without `ON`, `LATERAL` | write the condition in `ON` |
| `CAST`, `SAFE_CAST`, typed literals such as `TIMESTAMP '..'`, `INTERVAL`, `STRUCT` | no pipeline function converts a type; bind a typed parameter instead |
| `DISTINCT ON`, `GROUP BY ALL`, `ROLLUP`, `CUBE`, `GROUPING SETS` | no pipeline stage keeps one row per key or groups in these ways |
| `ANY_VALUE`, `STRING_AGG`, `LOGICAL_AND`, `LOGICAL_OR` | no accumulator with that meaning |
| `LIKE .. ESCAPE`, `LIKE ANY` | a backslash escapes in `LIKE` without it |
| `TABLESAMPLE`, `FOR UPDATE`, `FETCH`, `SELECT AS STRUCT` and the other clauses of other dialects | no pipeline form |
| Searches inside `OR` or after a join | Firestore accepts a search only right after the source, and fails the request otherwise |
| An unaliased computed column, or an unaliased nested field such as `awards.hugo` in a query that groups, `ORDER BY` or `GROUP BY` a constant or a position | the column needs a name, since a nested path cannot name an output field; a constant orders and groups nothing |

## Semantics

A SQL statement keeps Firestore's semantics, because it runs as a pipeline. The table lists where
that differs from what a SQL database does. Each row was run against a collection of documents
with an integer, a double, strings, a boolean, a null, a NaN and a document without the field.

### Null and absent

A document either has a field, holding a value that can be null, or lacks it. SQL has no absent
field: the column holds `NULL`.

| Statement | Firestore | A SQL database |
|---|---|---|
| `v = NULL` | matches the documents where `v` is null | matches nothing |
| `v IS NULL` | matches null only; a document that lacks `v` does not match | matches both |
| `v IS NOT NULL` | matches every document with a value that is not null | the same |
| `v != 1`, `NOT (v = 1)` | match null and absent too | exclude both |
| `v IS DISTINCT FROM 1` | matches null and absent too | the same |
| two null join keys | match each other | do not match |
| `v \|\| 'x'` with `v` null or absent | null | `NULL` |
| a selected field that the document lacks | the key is missing from the row | the key holds `NULL` |
| the columns of an unmatched `LEFT JOIN` | the key is missing from the row | the keys hold `NULL` |
| `ORDER BY v` ascending | absent and null first | depends on the database |

A row that lacks a key deserialises into an `Option` field as `None`.

`COUNT(x)` counts documents where `x` has a value that is not null. Over 11 documents, one holding
null and one lacking the field, `COUNT(*)` was 11, `COUNT(v)` was 9 and `COUNT(DISTINCT v)` was 8.

The SQL results are written in SQL, using the pipeline function `is_absent`, which passes through:

- `v IS NULL OR is_absent(v)` matches null and absent;
- `v != 1 AND v IS NOT NULL` leaves them out;
- `if_absent(v, NULL)` selects an absent field as null.

The filters differ in cost. `rating != 4.2 AND rating IS NOT NULL` kept the index ranges of
`rating != 4.2`, and `genre IS NULL OR is_absent(genre)` lost the range: it scanned all 11 index
entries, where `genre IS NULL` scanned 1.

### Types

| Statement | Firestore |
|---|---|
| `v > 0`, `v > 'a'`, `v IN (1, 'b')` | a comparison matches values of its own type, and raises no error for the other types |
| `v LIKE '100%'` over documents where `v` is a number | the whole request fails: `INVALID_ARGUMENT`, "The function like(...) requires `String` but got `LONG`." |
| `v + 1` over a document where `v` is a string | the whole request fails: "The function add(...) requires `Number` but got `STRING`." |
| `WHERE title` with a string field | matches nothing, with no error |
| `ORDER BY v` over mixed types | absent, null, booleans, NaN, numbers, then strings |

A condition earlier in the same `AND` that excludes the other types avoids the failure: with
`k IN ('pct', 'pcx') AND v LIKE '100%'`, the `LIKE` ran only on strings.

### Numbers

- `v = 1` and `v = 1.0` match the same documents, and `DISTINCT` and `GROUP BY` collapse an integer
  and a double of the same value. They collapse null and absent into one group too;
- two integers divide as integers: `7 / 2` is 3, `-7 / 2` is -3 and `7 % 3` is 1, where
  `7.0 / 2` is 3.5. A SQL dialect whose `/` returns a float differs from this.

### Names and patterns

- field names are case-sensitive. `SELECT Title` for a field `title` returns rows without a
  title, and `WHERE Genre = 'Dystopian'` matches nothing. A misspelled field is an absent field,
  with no error;
- `LIKE` is case-sensitive, `%` and `_` are wildcards, and `.` and `(` are literal. A backslash
  escapes a wildcard, written `'100\\%'` in the statement since a backslash escapes in a string
  literal too;
- `__name__` compared with a string is read as a document name, and the translator makes it a
  reference. `__name__` in `INSERT` takes an ID or a path, see [Writes](#writes).

## Joins and subqueries

Joins and subqueries are in Preview in Firestore, see
[Pipeline aggregations and joins](./pipeline-aggregations-and-joins.md#joins). Firestore plans a
join as a nested loop: for each row of the left side it runs the right side as a subquery. A
subquery in an expression, `[NOT] EXISTS`, `[NOT] IN (SELECT ..)`, `(SELECT ..)` or
`ARRAY(SELECT ..)`, runs once per row of the query it is in too, whether or not it reads a column of
that row. Query Explain with `analyze` on an Enterprise database, with 11 documents in the outer
collection and 300 in the inner one:

| Statement | Rows scanned | Read units |
|---|---|---|
| `SELECT title FROM outer` | 11 | 1 |
| `SELECT n FROM inner` | 300 | 7 |
| `SELECT title FROM outer WHERE n IN (SELECT n FROM inner)` | 3,311 (11 + 11 × 300) | 77 |
| `SELECT title FROM outer WHERE (SELECT COUNT(*) AS c FROM inner) > 0` | 3,311 | 27 |
| `SELECT title FROM outer WHERE EXISTS (SELECT 1 FROM inner WHERE n > 100000)` | 3,311 | 77 |

The plan of each subquery statement is an `Apply` of the outer scan to the subquery, so the
uncorrelated `COUNT(*)` ran 11 times too.

The cost depends on the index. Without one on the join key, each run reads the whole joined
collection. A join or subquery is therefore refused unless the options say `.allow_subqueries()`,
and the error says it runs one subquery per row. These need no opt-in:

- a join or subquery into `subcollection(..)`, which reads only the children of each parent;
- a derived table or a CTE in the `FROM` of a query, which is the query's own source and does not
  multiply with the rows; a `JOIN` into one is still a join and needs the opt-in;
- `UNNEST` of an array of the row.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let rows = db
    .fluent()
    .sql(
        "SELECT r.name, v.rating, v.reviewer \
         FROM restaurants AS r LEFT JOIN collection_group('reviews') AS v ON v.restaurant = r.__name__ \
         WHERE r.cuisine = @cuisine",
    )
    .param("cuisine", "breakfast")
    .options(FirestoreSqlOptions::new().allow_subqueries())
    .query()
    .await?;
# let _ = rows;
# Ok(())
# }
```

Rules for the joined names:

- a qualified name, `v.rating`, is a column of the table or alias `v`, searched from the nearest
  query outwards, and a table or alias matches without regard to case; an alias used by two tables
  is an error;
- a joined table lives in a field named after its alias, and a document can hold a field of any
  name, so the translation cannot tell whether the alias is the name of a field of the left side.
  It refuses every way to read such a field instead. In `FROM orders AS o JOIN customers AS
  customer`, `o.customer` is refused, and so is a bare `customer` in any place but `customer IS
  NULL`, since the field `customer` of the orders is replaced by the joined table. Give the table
  another alias. A bare alias of an `UNNEST` names its element, and is allowed;
- in a query with a join or in a subquery, an unqualified name in an `ON` clause is an error, since
  either side could hold it, and so is a dotted name that starts with no table;
- `SELECT *` and `b.*` are refused in a query with a join, for the base table as well as for a
  joined one. The joined tables would come with the base document's fields, and replace any of them
  that has the name of an alias. Name the columns;
- the anti-join, the left rows that have no match, is `WHERE v IS NULL` on the alias.
  `WHERE v.rating IS NULL` matches nothing, since the field of an unmatched row is absent and not
  null;
- a correlated subquery cannot sit in a query that groups, or in a clause with no stage before it
  to carry the column, such as the `SET` of an `UPDATE`;
- `EXISTS (SELECT 1 ..)` ignores the select list, so an aggregate, `GROUP BY` or `HAVING` inside
  `EXISTS` is refused: it would change which rows exist, and the select list that holds it is not
  evaluated.

A CTE, a named subquery defined with `WITH`, runs again for each place that uses it: a CTE
referenced twice runs twice. Results are not kept or reused between references. A statement that
repeats CTE bodies more than 1,024 times in all is refused.

### Join cost

These were measured with Query Explain and `analyze` on an Enterprise database. The collections
were small: 9 restaurants with 6 reviews, 11 books, and 1,500 items.

| Join | Rows scanned | Read units | Results |
|---|---|---|---|
| 9 restaurants to the `reviews` collection group by a reference field, no index | 63 (9 + 9 × 6) | 3 | 6 |
| the same, for the 3 restaurants with `cuisine = 'breakfast'` | 27 (9 + 3 × 6) | 2 | 3 |
| 9 restaurants to `subcollection('reviews')` | 15 (9 + 6) | 1 | 6 |
| 11 books to books on `genre`, with the index `(genre ASC, rating DESC)` | 26 (11 + 15) | 17 | 15 |
| 20 of 1,500 items to the same 1,500 items on `n`, no index | 31,500 (1,500 + 20 × 1,500) | 864 | 20 |

The unindexed join scans the joined collection once for each left row: parents × child collection.
The indexed join reads an index range for each left row, and a join into `subcollection(..)` reads
each parent's children only. The last row took 260 ms on the server, and 1.5 s as the client saw
it.

Rules that follow from it:

- index the join key, and run `.explain()` before the join reaches production data. A plan with a
  `TableScan` under the join is the unindexed case;
- a join that compares the joined table's `__name__` with a column of the left row becomes a lookup
  by name for each left row. In a per-row subquery over a collection of 60 documents, run for 3 rows,
  a plain `=` of `__name__` with a variable scanned the collection: 132 to 180 rows and 4 to 5
  read units, where the lookup by name read 2 rows for 3 read units;
- a join over a large collection that is run for analysis belongs in BigQuery. Firestore joins fit
  a left side that a filter has narrowed to a few documents.

## Writes

`INSERT`, `UPDATE` and `DELETE` run with `execute()`, which returns the count of documents
written in `documents_modified`, or with `execute_in_transaction(&mut transaction)` in a
transaction, which behaves as in [Pipeline writes](./pipeline-writes.md#writes-in-a-transaction).

Every SQL write is atomic: it applies in full or not at all, as a SQL statement does. It is sent
once and never retried, and a failed write has `retry_possible` set only for `ABORTED`, which means
nothing was applied. A write has no `explain`, and `.explain()` on one fails with
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

Rules per statement:

- `INSERT .. VALUES` needs a column list. Each value is a literal or a bound parameter, and each
  column a top-level field name. An `INSERT` fails with `ALREADY_EXISTS` when a document of that
  name exists;
- a `__name__` column gives the document's name. In `VALUES` a bare ID resolves against the target
  collection and a path outside it is refused; the rows of a `SELECT` keep the name of the document
  they were read from. A row without `__name__` gets an ID from Firestore. A column list over a
  `SELECT` renames its columns by position, and is refused over `*`;
- `UPDATE` and `DELETE` require a `WHERE`, as in BigQuery, and `WHERE TRUE` selects every
  document. The target is one collection. `UPDATE .. SET __name__` is refused;
- `ON CONFLICT` takes only the target `(__name__)`, since Firestore has no other unique
  constraint, and `DO NOTHING` also accepts no target. In `DO UPDATE`, `excluded.col` is a field of
  the new row, and a bare column is a field of the stored document. A `WHERE` on `DO UPDATE` leaves
  the stored document unchanged, and does not count it, when it is false for it. The lookup of the
  stored document by its name is bounded by the rows written. In `ON CONFLICT`, rows of a `SELECT`
  from another collection are matched by their ID, which scans the target collection once per row,
  and so need `.allow_subqueries()`. The `__name__` of such a row must be the name of a document
  that the translation can see: `SELECT __name__, ..`, or `*` of a collection. A string or another
  field renamed to `__name__`, `*` of a derived table, and `* EXCEPT (__name__)` are refused before
  any request, since a row without a document reference cannot be matched;
- `ON CONFLICT` keeps the stored document of each row in a field named `_conflict_stored`. A column
  of that name is refused, and a field of that name in the rows of a `SELECT *` is dropped from
  them;
- the same ID twice in one statement fails with `INVALID_ARGUMENT`, "Multiple mutations per key
  are not allowed", and nothing is written;
- 8 concurrent `INSERT .. ON CONFLICT (__name__) DO UPDATE SET stock = stock + excluded.stock`
  statements on one document committed 4 times and failed 4 times with `ABORTED`, "Optimistic
  transaction was aborted due to a conflict", and the stored `stock` was 4. A write that returns
  `ABORTED` can be sent again.

Refused: `MERGE`, `REPLACE INTO`, `INSERT OR IGNORE`, `OR ABORT`, `OR FAIL` and `OR ROLLBACK`,
`RETURNING`, `UPDATE .. FROM`, `ORDER BY` and `LIMIT` on a write, `INSERT .. SET`,
`INSERT .. DEFAULT VALUES`, and any `ON CONFLICT` target but `__name__`.

### Write limits

Firestore rejects an atomic write that is too big with `InvalidArgument`, "Transaction too big.
Decrease transaction size." The limit is not one number of documents. It was measured on documents
of two small fields (an integer and a short string), from a developer machine:

| Statement | Largest that succeeded | Smallest that failed |
|---|---|---|
| `UPDATE .. SET touched = 1 WHERE TRUE` | 55,000 documents, 27.5 s | 60,000 documents, in two runs |
| `DELETE FROM .. WHERE TRUE` | 5,475 documents | 5,500 documents |

The `DELETE` limit was about a tenth of the `UPDATE` limit. Throughput was 1,000 to 3,000 documents
per second, and rose with the size of the write. Two runs that came right after inserting the
documents failed once with `ResourceExhausted` ("This database has exceeded their maximum
bandwidth for writes") and passed after a pause, and one `UPDATE` of 67,500 documents failed once
with `Aborted`.

Split a large write by a `WHERE` on a range, such as the key or a timestamp, and send each part
after the one before. The numbers are measurements on tiny documents.

A TTL (time-to-live) policy suits retention better than a scheduled `DELETE`: Firestore deletes
the documents itself once the timestamp in the policy's field has passed, see
[Index management](./index-management.md). The
[bulk delete](./bulk-delete.md) of the `admin` feature empties a whole collection.

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
