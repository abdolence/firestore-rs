# Index statements

With the `admin` cargo feature next to `sql`, SQL text can list, create and drop indexes, and change
the TTL and the single-field indexes of a field. `CREATE INDEX`, `DROP INDEX` and `ALTER COLLECTION`
run only when the caller opts in with `.allow_index_statements()` on the
[options](#options), so they suit command line tools and admin scripts that the program's own
operators write or run. `SHOW INDEXES`, `SHOW COLLECTIONS` and `EXPLAIN` only read and need no
opt-in. Text from users or LLM agents runs with `read_only()`, which refuses the three statements.
The same operations have a fluent API, see
[One index at a time](../index-management.md#one-index-at-a-time).

```toml
[dependencies]
firestore = { version = "...", features = ["sql", "admin"] }
```

`SHOW COLLECTIONS` and `EXPLAIN` need `sql` alone. An index statement in a build without `admin`
fails with `InvalidParametersError`, before any request; its `field` is the statement, such as
`CREATE INDEX`.

## Running a statement

`run()` of the [SQL builder](../sql.md#setup) runs any statement and returns a
`FirestoreSqlOutcome`, one variant for each kind of statement:

| Statement | Outcome |
|---|---|
| `SELECT`, `EXPLAIN [ANALYZE] SELECT` | `Rows(stream)` |
| `INSERT`, `UPDATE`, `DELETE` | `Written(FirestorePipelineWriteResult)` |
| `SHOW COLLECTIONS` | `Collections(Vec<FirestoreCollectionId>)` |
| `SHOW INDEXES` | `Indexes(FirestoreIndexListing)` |
| `CREATE .. INDEX` | `IndexCreated(FirestoreListedCompositeIndex)` |
| `DROP INDEX` | `IndexDropped(Option<FirestoreListedCompositeIndex>)` |
| `ALTER COLLECTION` | `FieldUpdated(FirestoreListedField)` |

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let outcome = db.fluent().sql("SHOW INDEXES FROM orders").run().await?;

if let FirestoreSqlOutcome::Indexes(listing) = outcome {
    for listed in listing.indexes {
        println!("{} {}", listed.id, listed);
    }
}
# Ok(())
# }
```

`query()`, its streaming variants and `obj()` run `SELECT` and `EXPLAIN [ANALYZE] SELECT`, and fail
with `InvalidParametersError`, before any request, for every other new statement, as they do for a
write. `execute()` fails the same way.

## Names, scope and options

- a collection or a collection group is named by its ID. An ID with hyphens or dots goes between
  backticks, as in a `FROM` clause, and so does an index ID, which can hold `-`, such as
  `` `CICAgPj-z4oK` ``. A path such as `users/alice/orders` is refused: an index belongs to every
  collection with the ID, wherever it is;
- a column of an index is a field path, written as in a `WHERE`;
- an index has a *scope*. `ON orders` is an index for queries on one collection, which a query
  reaches with `.from("orders").parent(..)`. `ON collection_group('orders')` is an index for
  queries across every collection with that ID, which a query reaches with `.all_descendants()`. In
  both the index belongs to the collection group `orders`, and `SHOW INDEXES FROM` and
  `DROP INDEX .. ON` name that group;
- keywords and option values are matched without regard to case;
- an option value is a literal. A `@name` parameter is refused there, and in every name.

## Listing

```sql
SHOW INDEXES;
SHOW INDEXES FROM orders;
SHOW COLLECTIONS;
SHOW COLLECTIONS IN `users/alice`;
```

`SHOW INDEXES` returns a `FirestoreIndexListing` for one collection group, or for every group of
the database except the ones Firestore reserves, such as `__default__`:

- `indexes` holds the composite indexes, each with its `collection_group`, `id`, `state` and the
  `index` itself: `query_scope`, `fields`, `unique`, `density` and the search options. A composite
  index is an index over several fields, which a query that filters or sorts on them needs. On a
  Standard edition database `fields` ends with the `__name__` field that Firestore appends;
- `fields` holds the fields that carry a single-field override or a TTL policy, see
  [ALTER COLLECTION](#alter-collection);
- `unrecognised` holds the items the library cannot represent, each with its `name` and `reason`.

`id` is a `FirestoreIndexId`, the type that `force_index(..)` of a
[pipeline](../editions/query-plans.md#choosing-an-index) takes.

`SHOW COLLECTIONS` returns the collection IDs under the documents root, or under the document that
`IN` names, in the order Firestore lists them. A document that does not exist has none, and the
list is empty.

## CREATE INDEX

```sql
CREATE INDEX ON orders (customer_id, placed_at DESC);
CREATE INDEX ON collection_group('orders') (tags CONTAINS, placed_at DESC);
CREATE INDEX ON `order-lines` (`unit-price` DESC, sku);
CREATE UNIQUE INDEX ON users (email) OPTIONS (density = 'SPARSE_ANY');
CREATE INDEX ON events (kind, at DESC) OPTIONS (density = 'DENSE');
CREATE SEARCH INDEX ON restaurants (menu, description, location GEO)
    OPTIONS (text_language = 'en', text_language_override_field_path = 'lang');
CREATE VECTOR INDEX ON books (genre, embedding) OPTIONS (dimension = 768);
CREATE INDEX IF NOT EXISTS ON orders (customer_id, placed_at DESC);
```

The statement starts the build and returns the index as Firestore lists it: in state `CREATING`,
or `READY` after a [wait](#waiting-for-an-operation). With `IF NOT EXISTS`, an index that already
exists is returned instead, as the list below says. Firestore assigns the ID of an index, so an
index cannot be named: `CREATE INDEX by_customer ON ..` is refused.

The kind of index sets what a column can be:

| Statement | Columns | Options |
|---|---|---|
| `CREATE INDEX` | a field with `ASC` or `DESC`, a bare field, which is ascending, or a field with `CONTAINS`, which matches the members of an array field | `density` |
| `CREATE UNIQUE INDEX` | as `CREATE INDEX` | `density` |
| `CREATE SEARCH INDEX` | a bare field, which is a text field, or a field with `GEO` | `text_language`, `text_language_override_field_path` |
| `CREATE VECTOR INDEX` | ordinary columns, and the vector field last, written bare | `dimension`, which is required, and `density` |

- a column takes a direction or a mode, never both;
- the options of a search index are those of
  [Search indexes](../index-management/search.md). The *density* of an index says which documents it
  holds an entry for, see [Density](../index-management/ttl-and-enterprise.md#density), and a
  *unique* index refuses a second document with the same values;
- `IF NOT EXISTS` makes an index that already exists the outcome instead of an error: the statement
  returns the listed index that the definition matches. Without a wait it comes back in the state
  Firestore lists; with a wait, an index another caller is still building is returned once it is
  `READY`, and one that ends in `NEEDS_REPAIR` fails the statement. `CREATE` of an index that
  exists, without `IF NOT EXISTS`, fails with `DataConflictError`. Firestore keeps answering
  `ALREADY_EXISTS` for a while after an index is deleted, when the listing no longer shows it. `IF NOT EXISTS` returns an index only
  when the listing shows it, so the statement fails with `DataConflictError` in that window; run it
  again later.

A fluent `.create()` takes the same definition, see
[One index at a time](../index-management.md#one-index-at-a-time).

## DROP INDEX

```sql
DROP INDEX `CICAgPj-z4oK`;
DROP INDEX CICAgJim14AK ON orders;
DROP INDEX ON orders (customer_id, placed_at DESC);
DROP UNIQUE INDEX ON users (email) OPTIONS (density = 'SPARSE_ANY');
DROP INDEX IF EXISTS `CICAgPj-z4oK`;
```

A drop deletes one index and returns it as it was listed just before. Firestore deletes it at once,
so there is nothing to wait for.

- **By ID.** The index is read first, and `ON orders` makes the drop fail with `DataNotFoundError`,
  deleting nothing, when the index belongs to another collection group. Firestore itself ignores
  the group of a request, and answers `200` for the second delete of an ID, so the check is made by
  the library. An ID that no index has fails with `DataNotFoundError`;
- **By definition.** `DROP [UNIQUE | SEARCH | VECTOR] INDEX ON target (..) [OPTIONS (..)]` takes
  the text that `CREATE` took, and deletes the one listed index that it matches, as `.sync()` matches
  a declaration. On an Enterprise edition database two indexes over the same fields differ by
  `unique` and `density`, and the options pick between them. No match fails with
  `DataNotFoundError`, and several matches fail with `DataConflictError`; nothing is deleted in
  either case;
- `IF EXISTS` makes an ID that no index has, or a definition that nothing matches, delete nothing
  and return `None` in `IndexDropped`. An index in another group than `ON` names, and a definition
  with several matches, stay errors;
- `UNIQUE`, `SEARCH` and `VECTOR` belong to the definition form. A drop takes one index.

## ALTER COLLECTION

```sql
ALTER COLLECTION sessions ALTER COLUMN expires_at SET OPTIONS (ttl = TRUE, indexes = []);
ALTER COLLECTION sessions ALTER COLUMN expires_at SET OPTIONS (ttl = FALSE, indexes = NULL);
ALTER COLLECTION orders ALTER COLUMN tags SET OPTIONS (indexes = ['CONTAINS', 'COLLECTION_GROUP CONTAINS']);
ALTER COLLECTION orders ALTER address.city SET OPTIONS (indexes = NULL);
ALTER COLLECTION logs SET OPTIONS (indexes = []);
```

`ALTER COLLECTION` changes one field of a collection group, or every field that has no setting of its
own. It sends one write, with the parts that the options name, and returns the field as Firestore
reads it after the write.

- `ttl = TRUE` enables *TTL*, time to live: Firestore deletes a document once the timestamp in the
  field has passed. `ttl = FALSE` disables it. TTL applies to a named field, so
  `ALTER COLLECTION c SET OPTIONS (ttl = ..)` is refused, and the value is `TRUE` or `FALSE`;
- `indexes` sets the *single-field override* of the field. Firestore indexes every field of a
  Standard edition database on its own, and an override replaces those indexes, see
  [Single-field overrides](../index-management/composite.md#single-field-overrides-standard-edition).
  `indexes = [..]` replaces the whole set with the entries listed, `[]` exempts the field from
  single-field indexing, and `NULL` removes the override, so that the field inherits the
  configuration of its group, or of the database;
- an entry of `indexes` is `'ASC'`, `'DESC'` or `'CONTAINS'` for queries on one collection, or the
  same word after `COLLECTION_GROUP ` for queries across the collection group;
- `ttl` and `indexes` in one `SET OPTIONS` are one write;
- without `ALTER COLUMN`, `indexes` sets the configuration of every field that has no setting of
  its own.

Firestore refuses a write that names `indexes` on an Enterprise edition database, unless the list is
empty or `NULL`, and then the write changes nothing; see [Editions](#editions-and-the-emulator). A
`ttl` in the same refused statement is not applied either.

Writes to one field are not sequenced: Firestore accepts a second write while the first is still
running. The statement reads the field after its own write, so a second write that overlaps it can
show in the result.

## Waiting for an operation

`CREATE .. INDEX` and `ALTER COLLECTION` start an operation that Firestore runs in the background.
`.wait_until_ready(timeout)` and `.wait_until_ready_with_options(..)` of the SQL builder make `run()`
return after it has finished, and the outcome is the index or field read after that:

```rust,no_run
# use firestore::*;
# use std::time::Duration;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let outcome = db
    .fluent()
    .sql("CREATE INDEX ON orders (customer_id, placed_at DESC)")
    .options(FirestoreSqlOptions::new().allow_index_statements())
    .wait_until_ready(Duration::from_secs(900))
    .run()
    .await?;

if let FirestoreSqlOutcome::IndexCreated(created) = outcome {
    assert_eq!(created.state, FirestoreIndexState::Ready);
}
# Ok(())
# }
```

A build takes minutes even for an empty collection, see
[Waiting for changes to finish](../index-management.md#waiting-for-changes-to-finish); set the
timeout above that. With `IF NOT EXISTS`, an index that another caller is still building is polled
until it is no longer `CREATING`, under the same timeout and with the same retries as the poll of
an operation. A deadline that passes fails with `SystemError`, code `OPERATION_WAIT_TIMEOUT`, and a
failed operation fails with the operation's status, as in `.sync()`. An index that ends in
`NEEDS_REPAIR`, the state of an index whose last build failed and left no operation running, fails
the same way, with `SystemError`, code `INDEX_NEEDS_REPAIR`.

A wait on any other statement fails with `InvalidParametersError`, field `wait`, before any request.

## Editions and the emulator

The library runs no edition rule of its own on these statements. Firestore answers a statement its
edition does not take with `INVALID_ARGUMENT`, which arrives as `DatabaseError` with Firestore's
message.

| Statement | Standard | Enterprise | Emulator |
|---|---|---|---|
| `CREATE INDEX` | two fields or more; `SPARSE_ALL` only | one field or more; `DENSE` by default | `UNIMPLEMENTED` |
| `CREATE UNIQUE INDEX` | refused | yes | `UNIMPLEMENTED` |
| `CREATE SEARCH INDEX` | refused | yes | `UNIMPLEMENTED` |
| `CREATE VECTOR INDEX` | yes | yes | `UNIMPLEMENTED` |
| `DROP INDEX` | yes | yes | `UNIMPLEMENTED` |
| `ALTER COLLECTION .. indexes` | yes | `[]` and `NULL` only[^enterprise-indexes] | `UNIMPLEMENTED` |
| `ALTER COLLECTION .. ttl` | yes | yes | `UNIMPLEMENTED` |
| `SHOW INDEXES` | yes | yes | `UNIMPLEMENTED` |
| `SHOW COLLECTIONS` | yes | yes | yes |
| `EXPLAIN [ANALYZE]` | fails[^explain-standard] | yes | rows without a plan[^explain-emulator] |

[^enterprise-indexes]: `[]` and `NULL` are accepted and write nothing: an Enterprise edition database
    has no automatic single-field indexes to override.
[^explain-standard]: Like every SQL statement on a Standard edition database, `FAILED_PRECONDITION`.
[^explain-emulator]: The Enterprise emulator returns the rows and no explain output. The Standard
    emulator fails every pipeline and SQL statement with `INVALID_ARGUMENT`.

The emulator serves no Firestore Admin API, whatever its edition, and answers each index call with
`UNIMPLEMENTED`; it arrives as `DatabaseError`, with `retry_possible` unset. `.plan()` and `.sync()`
skip the emulator and return, see [The Firestore emulator](../index-management.md#the-firestore-emulator),
but a statement is an explicit command, so it fails. `SHOW COLLECTIONS` works on the emulator.

## Options

`FirestoreSqlOptions` apply to the new statements as to a `SELECT`, see
[Read-only options](../sql.md#read-only-options):

- `FirestoreSqlOptions::new()` refuses `CREATE`, `DROP` and `ALTER COLLECTION`. They run after
  `.allow_index_statements()`; without it they fail with `InvalidParametersError`, field
  `allow_index_statements`, before any request. `SHOW INDEXES`, `SHOW COLLECTIONS` and
  `EXPLAIN [ANALYZE] SELECT` only read, and run without it;
- `read_only()` refuses `CREATE`, `DROP` and `ALTER COLLECTION` even when `.allow_index_statements()`
  follows it: read-only wins. The error `field` is `read_only`. `read_only()` allows the statements
  that only read;
- `.with_collections([..])` applies to every collection ID a statement names. It refuses
  `SHOW INDEXES` without `FROM`, and `SHOW COLLECTIONS`, which list names outside the set, and
  `DROP INDEX id` without `ON`, which reaches an index in any group. `DROP INDEX id ON collection`
  needs a collection in the set;
- `.refuse_collection_groups()` refuses `collection_group('..')` in an index definition;
- `.with_max_rows(..)` caps the rows of the `SELECT` in `EXPLAIN ANALYZE`; a `SHOW` returns every
  entry.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
const ORDERS: FirestoreCollectionId = FirestoreCollectionId::from_static("orders");

let outcome = db
    .fluent()
    .sql("DROP INDEX `CICAgPj-z4oK` ON orders")
    .options(
        FirestoreSqlOptions::new()
            .allow_index_statements()
            .with_collections([ORDERS]),
    )
    .run()
    .await?;
# let _ = outcome;
# Ok(())
# }
```

## Refused forms

A statement of these forms fails with `InvalidParametersError`, before any request. `field` names
the construct, and `location` says where it starts:

```sql
-- an index cannot be named
CREATE INDEX by_customer ON orders (customer_id, placed_at DESC);
-- a path is not a collection group ID
CREATE INDEX ON `users/alice/orders` (placed_at DESC, total);
-- a column is a field path, with a direction or a mode, not both
CREATE INDEX ON orders (tags CONTAINS DESC);
CREATE INDEX ON orders (lower(name));
-- CREATE VECTOR INDEX takes its dimension in OPTIONS
CREATE VECTOR INDEX ON books (genre, embedding);
-- an option the statement does not take
CREATE INDEX ON orders (customer_id, placed_at DESC) OPTIONS (dimension = 3);
-- an option is a literal
CREATE INDEX ON orders (customer_id, placed_at DESC) OPTIONS (density = @density);
-- an index has no WHERE
CREATE INDEX ON orders (customer_id, placed_at DESC) WHERE total > 0;
-- one index for each DROP
DROP INDEX CICAgJim14AK, CICAgOi36pgK;
-- ttl is TRUE or FALSE, and it applies to a named field
ALTER COLLECTION sessions ALTER COLUMN expires_at SET OPTIONS (ttl = 'yes');
ALTER COLLECTION logs SET OPTIONS (ttl = TRUE);
-- an entry is ASC, DESC or CONTAINS, optionally after COLLECTION_GROUP
ALTER COLLECTION orders ALTER COLUMN tags SET OPTIONS (indexes = ['ASCENDING']);
-- FROM names a collection group ID
SHOW INDEXES FROM `users/alice/orders`;
-- only SHOW INDEXES and SHOW COLLECTIONS
SHOW TABLES;
```

## Explaining a SELECT

`EXPLAIN SELECT ..` returns the plan of a `SELECT` without reading any document, and
`EXPLAIN ANALYZE SELECT ..` also returns the rows and the execution profile. The prefix is the SQL
spelling of `.explain()` and `.explain_with_options(..)`, see
[Explaining a pipeline](../editions/query-plans.md#explaining-a-pipeline):

```sql
EXPLAIN SELECT title FROM books WHERE genre = 'Dystopian';
EXPLAIN ANALYZE SELECT title FROM books WHERE genre = @genre ORDER BY rating DESC LIMIT 10;
```

The plan is Firestore's own text, in the metadata of the last item of the rows. `EXPLAIN` takes a
`SELECT`. It fails for a write, which Firestore rejects with Query Explain or, with `ANALYZE`, runs,
and for an option other than `ANALYZE`. `EXPLAIN` in the text together with `.explain()` on the
builder fails with `InvalidParametersError`, field `explain`: the text and the builder would both
set the options. `.explain()` on `SHOW` or on an index statement fails the same way, since they have
no plan, and so does `.request_options(..)`, or `.request_tags(..)`, on `SHOW INDEXES` and the index
statements, field `request_options`: they go to the Admin API, which takes no request options.

## Errors

Every error is an existing `FirestoreError` variant:

| Case | Error |
|---|---|
| text that does not parse, a refused form | `InvalidParametersError`, `field` the construct, with `location` |
| an index statement in a build without `admin` | `InvalidParametersError` |
| `query()` or `execute()` on a statement that needs `run()`; a wait on a statement with no operation; `EXPLAIN` with `.explain()`, or `.explain()` on `SHOW` or an index statement; request options on an Admin API statement | `InvalidParametersError`, `field` `sql`, `wait`, `explain`, `request_options` |
| an index statement without `.allow_index_statements()`, or with `read_only()` | `InvalidParametersError`, `field` `allow_index_statements` or `read_only` |
| a field update with nothing to change, TTL on every field, a reserved group such as `__default__` | `InvalidParametersError` |
| `CREATE` of an index that exists | `DataConflictError` |
| `DROP` of an unknown ID or of a definition nothing matches | `DataNotFoundError`, code `NO_MATCHING_INDEX` |
| `DROP` of an ID in another group than `ON` names | `DataNotFoundError`, code `INDEX_IN_OTHER_GROUP` |
| `DROP` of a definition that several indexes match | `DataConflictError`, code `SEVERAL_MATCHING_INDEXES`, naming the IDs |
| an index or a field Firestore refuses | `DatabaseError` from `INVALID_ARGUMENT`, with Firestore's message |
| any index call on the emulator | `DatabaseError`, code `Unimplemented` |
| a wait that passes its deadline | `SystemError`, code `OPERATION_WAIT_TIMEOUT` |
| an index waited for with `IF NOT EXISTS` that ends in `NEEDS_REPAIR` | `SystemError`, code `INDEX_NEEDS_REPAIR` |

## Permissions

`SHOW INDEXES` and `SHOW COLLECTIONS` need `roles/datastore.viewer`. `CREATE`, `DROP` and
`ALTER COLLECTION` need `roles/datastore.indexAdmin`. Grant the viewer role alone to credentials
that run text from users or LLM agents, with `read_only()`, so that the role refuses a statement
the options let through.

Full example available
[here](https://github.com/abdolence/firestore-rs/blob/master/examples/sql-indexes.rs).
