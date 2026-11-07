# Query and write semantics

A [pipeline](../pipelines.md) runs with Firestore's pipeline semantics, and a [SQL](../sql.md)
statement runs as a pipeline, so it keeps them too. A Core query has semantics of its own. This
page lists where the two differ, and where the pipeline semantics differ from what a SQL database
does.

## Null and absent

A document either has a field, holding a value that can be null, or lacks it. SQL has no absent
field: the column holds `NULL`. A Core filter or `order_by` on a field never returns a document
that lacks the field.

The pipeline column names Firestore's pipeline functions, with the name of the filter builder
method in brackets where it differs:

| Pipeline | SQL | Pipelines and SQL | Core query | A SQL database |
|---|---|---|---|---|
| `equal` (`eq`) to null | `v = NULL` | matches the documents where `v` is null | matches nothing; `is_null()` finds null fields | matches nothing |
| `is_null` | `v IS NULL` | matches null only; a document that lacks `v` does not match | the same | matches null and absent |
| `is_not_null` | `v IS NOT NULL` | matches every document with a value that is not null | the same | the same |
| `not_equal` (`neq`) | `v != 1`, `NOT (v = 1)` | matches null and absent too | leaves out a document that lacks `v` | excludes null and absent |
| no function of its own | `v IS DISTINCT FROM 1` | matches null and absent too | no counterpart | the same |
| `exists`, `is_absent` | `is_absent(v)` | tests whether the document has `v` | no counterpart | no counterpart |
| a join on a key that is null on both sides | `ON a.k = b.k` | the two match each other | no joins | do not match |
| concatenation with `v` null or absent | `v \|\| 'x'` | null | no counterpart | `NULL` |
| a selected field that the document lacks | `SELECT v` | the key is missing from the row | the field is missing from the document | the key holds `NULL` |
| the columns of an unmatched left join | `LEFT JOIN` | the keys are missing from the row | no joins | the keys hold `NULL` |
| `sort` ascending | `ORDER BY v` | keeps the documents that lack `v`, and orders absent and null first | `order_by` leaves out the documents that lack `v` | depends on the database |
| documents with equal sort values | `ORDER BY v` | no tie-break on the document name; list a unique field last to fix their order | ties end on the document name, in the direction of the last field | depends on the database |

A row that lacks a key deserialises into an `Option` field as `None`.

`COUNT(x)` counts documents where `x` has a value that is not null. Over 11 documents, one holding
null and one lacking the field, `COUNT(*)` is 11 and `COUNT(v)` is 9; `COUNT(DISTINCT v)` counts
each distinct value once.

The SQL results are written in SQL, using the pipeline function `is_absent`, which passes through:

- `v IS NULL OR is_absent(v)` matches null and absent;
- `v != 1 AND v IS NOT NULL` leaves them out;
- `if_absent(v, NULL)` selects an absent field as null.

The filters differ in cost. `rating != 4.2 AND rating IS NOT NULL` keeps the index ranges of
`rating != 4.2`, and `genre IS NULL OR is_absent(genre)` loses the index range of `genre IS NULL`,
so it scans every index entry of the field. See [Query plans and cost](./query-plans.md).

Whether a field is stored as null or left absent is decided when it is written, see
[Explicit null value serialisation](../null-serialization.md).

## Types

| SQL | Pipelines and SQL |
|---|---|
| `v > 0`, `v > 'a'`, `v IN (1, 'b')` | a comparison matches values of its own type, and raises no error for the other types |
| `v LIKE '100%'` over documents where `v` is a number | the whole request fails: `INVALID_ARGUMENT`, "The function like(...) requires `String` but got `LONG`." |
| `v + 1` over a document where `v` is a string | the whole request fails: "The function add(...) requires `Number` but got `STRING`." |
| `WHERE title` with a string field | matches nothing, with no error |
| `ORDER BY v` over mixed types | absent, null, booleans, NaN, numbers, then strings |

A condition earlier in the same `AND` that excludes the other types avoids the failure: with
`k IN ('pct', 'pcx') AND v LIKE '100%'`, the `LIKE` runs only on strings.

A Core range filter matches values of its own type only, and never NaN. A Core `order_by` over
mixed types follows Firestore's order of types: null, booleans, numbers, timestamps, strings,
bytes, references, geo points, arrays, vectors and maps.

## Numbers

- `v = 1` and `v = 1.0` match the same documents, and `DISTINCT` and `GROUP BY` collapse an integer
  and a double of the same value. They collapse null and absent into one group too;
- two integers divide as integers: `7 / 2` is 3, `-7 / 2` is -3 and `7 % 3` is 1, where
  `7.0 / 2` is 3.5. A SQL dialect whose `/` returns a float differs from this;
- a Core query compares an integer and a double by value too, so a filter on `1` matches the double
  `1.0`, and `order_by` sorts them together.

## Names and patterns

- field names are case-sensitive. `SELECT Title` for a field `title` returns rows without a
  title, and `WHERE Genre = 'Dystopian'` matches nothing. A misspelled field is an absent field,
  with no error;
- `LIKE` is case-sensitive, `%` and `_` are wildcards, and `.` and `(` are literal. A backslash
  escapes a wildcard, written `'100\\%'` in the statement since a backslash escapes in a string
  literal too;
- `__name__` compared with a string is read as a document name, and the translator makes it a
  reference. `__name__` in `INSERT` takes an ID or a path, see
  [Writes and ON CONFLICT](../sql/writes.md).

## Stage order

The stages of a pipeline run in the order they are written. A `limit` before a `sort` limits the
documents first and sorts what is left, which is not what the same two calls mean in a Core query,
where `limit` always applies to the ordered results.

## Order without a sort

- a pipeline without a `sort` returns its documents in an order of its own;
- on a Standard edition database, a Core query without `order_by` returns its documents in the
  order of the fields it compares with `<`, `<=`, `>`, `>=`, `!=` or `not-in`, and then by
  document name;
- on an Enterprise edition database the order of a Core query without `order_by` is unspecified.

## Results without a document name

A stored document carries its name, so `_firestore_id` is filled for it in Core and pipeline
results. A Core aggregation result, and the result of a pipeline stage that builds new documents,
such as `.fields()` or `.aggregate()`, has no name. A SQL row holds only the columns the statement
selects, so it has no name unless the statement selects `__name__`. A field of the result type
that reads `_firestore_id` must be an `Option`, see
[Typed results and metadata](../pipelines.md#typed-results-and-metadata) and
[Document metadata as struct fields](../document-metadata.md).

## Cached results

A read that a [cache](../caching.md) session answers follows the same semantics, with these
differences:

- a cached Core query follows the order of the Standard edition, and on an Enterprise edition
  database documents with equal values can come back in another order, see
  [What the cache answers](../caching/modes.md#what-the-cache-answers);
- a cached pipeline returns documents that tie on every sort key in document-name order, where
  Firestore's order of them is unspecified, see [Caching pipelines](../caching.md#pipelines);
- both are eventually consistent, see [How the cache is updated](../caching/updates.md).

## Writes

- a Core write in a transaction is buffered in the library and sent with the commit. A pipeline or
  SQL write is sent when it runs, and its documents change at the commit. A transaction takes
  pipeline writes or buffered writes: Firestore rejects a commit that mixes them with
  `INVALID_ARGUMENT`, see [Writes in a transaction](../pipeline-writes.md#writes-in-a-transaction);
- a pipeline or SQL write is sent once and never retried. Simple batch writes and transactions
  retry, see [Request timeouts](../request-timeouts.md#retry-limits);
- a pipeline `upsert` and SQL `INSERT OR REPLACE` replace the stored document, so its fields that
  the write does not set are gone;
- a write that a unique index refuses returns `DataConflictError`, see
  [TTL and the Enterprise edition](../index-management/ttl-and-enterprise.md#unique-indexes);
- Core transactions behave the same on both editions; under contention an Enterprise database,
  which is optimistic by default, aborts and retries transactions more often than a Standard one,
  see [Transactions](../transactions.md#on-the-enterprise-edition).
