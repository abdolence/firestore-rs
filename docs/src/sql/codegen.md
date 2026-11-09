# Fluent code for a statement

With the `codegen` cargo feature, `FirestoreSqlStatement::fluent_rust(..)` turns a SQL `SELECT` into
the Rust code of the equivalent [pipeline](../pipelines.md) in the fluent API. It is meant for
people who port a statement to application code: the snippet shows the builder calls, the row
struct and the function arguments, and the person who pastes it keeps and edits the result. It
sends nothing, so it needs no client and no credentials.

```toml
[dependencies]
firestore = { version = "...", features = ["codegen"] }
serde_json = "1"
serde_bytes = "0.11"
```

The snippet reads values that have no fixed type into `serde_json::Value`, and bytes through
`serde_bytes`, so the project that pastes it needs `serde_json`, and `serde_bytes` when the
statement reads or binds bytes.

## Calling it

```text
FirestoreSqlStatement::fluent_rust(sql, &parameters, &sample) -> FirestoreResult<String>
```

- `sql` is a `SELECT`;
- `parameters` binds every `@name` and `?` of the text to a sample value, as `.param(..)` and
  `.params(..)` of the [SQL builder](../sql.md#setup) do. A parameter without a value fails as it
  does when the statement runs;
- `sample` holds rows that the statement returned, as `Vec<FirestoreDocument>`. A *sample* is
  enough to type the fields of the row struct, and an empty one is accepted.

The options of a statement are not an argument: nothing runs, so there is nothing for them to
guard, and a `with_max_rows(..)` used to fetch the sample would add a `LIMIT` that the snippet must
not show.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let sql = "SELECT title, author, rating, publishedAt, tags, edition FROM books
WHERE genre = @genre AND rating >= @min_rating ORDER BY rating DESC LIMIT 20";
let parameters = FirestoreSqlParameters::new()
    .param("genre", "Dystopian")
    .param("min_rating", 4.0);

let statement =
    FirestoreSqlStatement::translate(&db, sql, &parameters, &FirestoreSqlOptions::read_only())?;
let FirestoreSqlStatement::Query(pipeline) = statement else {
    return Ok(());
};
let sample = db.fluent().pipeline().select().from_pipeline(pipeline).query().await?;

println!("{}", FirestoreSqlStatement::fluent_rust(sql, &parameters, &sample)?);
# Ok(())
# }
```

Without a database, `&[]` is the sample and every field is typed as unknown, see
[Row struct](#row-struct). For a sample where `author` is missing from one row, `rating` is an
integer in some rows and a double in others, and `edition` is an integer in some rows and a string
in others, the text it returns is:

```rust
// SELECT title, author, rating, publishedAt, tags, edition FROM books
// WHERE genre = @genre AND rating >= @min_rating ORDER BY rating DESC LIMIT 20
use firestore::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct BooksRow {
    title: String,
    author: Option<String>,
    rating: f64,
    #[serde(rename = "publishedAt")]
    published_at: FirestoreTimestamp,
    tags: serde_json::Value,
    /// Observed as integer and string in the sample.
    edition: serde_json::Value,
}

async fn query_books(
    db: &FirestoreDb,
    genre: &str,
    min_rating: f64,
) -> FirestoreResult<Vec<BooksRow>> {
    db.fluent()
        .pipeline()
        .select()
        .from("books")
        .filter(|q| {
            q
                .for_all([
                    q.field("genre").equal(genre),
                    q.field("rating").greater_than_or_equal(min_rating),
                ])
        })
        .sort(|e| [e.field("rating").desc()])
        .fields(|e| [
            e.field("title"),
            e.field("author"),
            e.field("rating"),
            e.field("publishedAt"),
            e.field("tags"),
            e.field("edition"),
        ])
        .limit(20)
        .obj::<BooksRow>()
        .query()
        .await
}
```

The snippet follows the pipeline that the statement translates to, stage by stage, so its stages
are in the order of the translation: `sort` before `fields`, as above. The text is formatted by the
crate, not by `rustfmt`, so run `cargo fmt` after pasting it.[^formatting]

[^formatting]: The crate prints the snippet with a library formatter, since `rustfmt` is a separate
    program that a library cannot count on. It breaks some lines differently from `rustfmt`, such as
    `q.for_all` above.

## Row struct

The snippet's struct holds the columns of the select list, in the order of the list. Without a select
list, as in `SELECT *`, it holds the field names of the sample, in alphabetical order, since the
fields of a document have no order. Each field is typed from the values of its column across the
whole sample:

| Values in the sample | Field type |
|---|---|
| integer | `i64` |
| double, or integer and double | `f64` |
| string | `String` |
| boolean | `bool` |
| timestamp | `FirestoreTimestamp` |
| vector | `FirestoreVector` |
| reference | `FirestoreReference` |
| geo point | `FirestoreLatLng` |
| bytes | `Vec<u8>`, with `#[serde(with = "serde_bytes")]` |
| map or array | `serde_json::Value` |
| two kinds other than integer and double | `serde_json::Value`, with a doc comment naming the kinds |
| absent or null in some rows | the type above in an `Option` |
| absent or null in every row | `Option<serde_json::Value>`, with a doc comment that says so |

`serde_json::Value` cannot hold bytes, so a column that mixes bytes with another kind, or holds
them inside a map or an array, fails to read.

A column that is always present in the sample can still be absent from a document that the
statement returns later, and the type of a field is the type of the sample's values. Check the
types against the data before relying on them.

With an empty sample every field is an `Option<serde_json::Value>`, since nothing says which
fields are always present. With an empty sample and no select list there is nothing to type: the
function returns `Vec<FirestoreDocument>` and the snippet has no struct.

A statement that selects `__name__`, and a sample whose rows carry a document name, give the struct
an `id: Option<String>` field that reads `_firestore_id`, as in
[Pipelines](../pipelines.md#typed-results-and-metadata).

## Naming and arguments

The struct and the function are named after the collection, `BooksRow` and `query_books` for
`books`, and a field after its column in snake case, with a `serde` rename when the two differ. A
name that Rust cannot use, such as a keyword or one that starts with a digit, is made valid, and a
name that another one took gets a suffix, as `db_2` does for a parameter named `db`.

The function takes `db: &FirestoreDb`, then one argument for each parameter, in the alphabetical
order of the names, and `param_1`, `param_2` for `?` in the order of the text. An argument has the
type of the value bound to it, as in the table of the row struct, borrowed unless it is `Copy`.
A null, a map or an array is a `&serde_json::Value`.

## What it shows

`fluent_rust(..)` covers:

- reads of one collection, a path to a nested collection, or a collection group, with `WHERE`,
  `ORDER BY`, `LIMIT`, `OFFSET`, `TABLESAMPLE` and a select list;
- aggregates, with and without `GROUP BY`, and `DISTINCT`;
- text and geo search, `document_matches(..)` and `geo_distance(..)`, see
  [Search](./reads.md#search). Vector distance is a function like any other and is shown as one.

A statement outside this list fails with `InvalidParametersError`, before anything is generated,
and its message names the construct:

- `UNION`, `UNNEST`, subqueries, variables, and so a join, which the translator writes as a
  subquery and an `UNNEST`, see [Reads, joins and subqueries](./reads.md#joins-and-subqueries);
- `* EXCEPT`, and a column written beside `*`;
- the sources `documents(..)`, `database()` and `subcollection(..)`;
- `INSERT`, `UPDATE`, `DELETE`, index statements, `EXPLAIN` and `SHOW`: a statement that is not a
  `SELECT` runs with the [SQL builder](../sql.md#setup), and has no fluent code to show;
- a parameter whose value the translation reads when it builds the pipeline, since the snippet has
  no value to put in its place. Such a parameter is `LIMIT @n` and `OFFSET @n`, the percentage of
  `TABLESAMPLE`, `__name__ = @id` and the other comparisons on `__name__`, the key of a subscript as
  in `x[@key]`, and the arguments of search such as `document_matches(@query)`. Write the value in
  the text instead. A parameter anywhere else becomes an argument of the function.
