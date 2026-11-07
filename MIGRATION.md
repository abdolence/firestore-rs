# Migration guide

## 0.63

### Composite indexes gain `unique` and `density`

`FirestoreCompositeIndex` gains the fields `unique` and `density`, and `FirestoreIndexDensity` is new, with the
variants `SparseAll`, `SparseAny` and `Dense`. Adding the fields breaks code that builds `FirestoreCompositeIndex` with
a struct literal: set `unique` to `false` and `density` to `None` for the defaults. `FirestoreCompositeIndex::new`
and the fluent builder are unaffected, and add `.unique()` and `.density(..)` for the new options.

```rust,ignore
let index = FirestoreCompositeIndex {
    fields,
    query_scope: FirestoreIndexQueryScope::Collection,
    search_options: None,
    unique: false,
    density: None,
};
```

Both options take part in matching a declaration with a listed index. An index that is unique is a different index
from one over the same fields that is not, and so are two indexes with different densities, a declaration without a
density meaning the database's default: `SparseAll` on Standard, `Dense` on Enterprise and `SparseAny` for a text or
geo search index there. A listed index that differs from its declaration in either is reported as undeclared, and
`.prune_undeclared()` deletes it. The index in each `FirestoreListedCompositeIndex` now carries the listed `unique`
and `density`.

A write that a unique index refuses returns `DataConflictError`.

### Index management reads the database edition

`.plan()` and `.sync()` call `GetDatabase` once to read the edition of the database. Before, only a sync that had to
write a field override made the call. The caller needs the `datastore.databases.getMetadata` permission, which
`roles/datastore.viewer` and `roles/datastore.indexAdmin` carry. A `.plan()` run with a narrower custom role fails
until the permission is added.

### Edition rules are checked before any write

`.plan()` and `.sync()` fail with `InvalidParametersError` before anything is written when a declaration breaks a rule
of the database's edition. Before, Firestore refused it at the write, after the sync had applied part of the
statement.

- On a Standard edition database, a composite index may not be unique, may not be a text or geo search index, and
  may not have a single field unless it is a vector field;
- on an Enterprise edition database, a field override may not list single-field indexes. An `.exempt()` there is
  already satisfied, so it is never written and no longer fails. A composite index of one field takes its place;
- a density is accepted only where the edition builds it: `SparseAll` alone on Standard; `SparseAny` and `Dense` on
  Enterprise, with `SparseAny` alone for a text or geo search index;
- a composite index may hold at most one `.array_contains()` field, on either edition.

The 0.62 check that refused every field override on Enterprise is narrowed to an override that lists indexes.

### Enterprise indexes keep their `__name__` as declared

An Enterprise edition database appends no `__name__` field to an index. A declaration there matches only a listed
index with exactly its fields, so `[a, b]` and `[a, b, __name__]` are two indexes and one statement can declare both.
The 100 field limit counts the declared fields only; on Standard it counts the appended `__name__` too.

### A second TTL field is refused at plan time

Declaring a `.ttl()` field while the collection group lists another TTL field fails `.plan()` and `.sync()` with
`InvalidParametersError`, unless `.prune_undeclared()` is set and disables the listed field. Before, the sync sent its
other writes and Firestore refused the second enable with `RESOURCE_EXHAUSTED`.
To move the TTL to another field, declare the new field and set `.prune_undeclared()`.

### Multikey and sharded indexes are reported as unrecognised

A listed index that is multikey or sharded has no declaration that can express it. It is now reported in
`unrecognised` with the reason, and is never matched, replaced or deleted, even with `.prune_undeclared()`. Before, such
an index was read as an ordinary one and could be matched against a declaration or pruned as undeclared.

### Invalid parameters errors carry a SQL location

`FirestoreInvalidParametersPublicDetails` gains a public `location` field of type `Option<FirestoreSqlLocation>`. An error
about SQL text, available with the `sql` feature, sets it to the line and column where the refused construct starts; every
other `InvalidParametersError` leaves it `None`. Adding the field breaks code that builds the struct with a literal and
code that destructures it without `..`; `FirestoreInvalidParametersPublicDetails::new` and its builder are unaffected, and
so is code that reads `field` and `error`.

```rust,ignore
// Before
let FirestoreInvalidParametersPublicDetails { field, error } = details;

// After
let FirestoreInvalidParametersPublicDetails { field, error, .. } = details;
```

### Mixed pipeline and buffered writes fail at commit

A transaction that holds a pipeline write and a buffered write, such as `add_to_transaction`, is no longer refused by
the library. Firestore refuses its commit instead, with `INVALID_ARGUMENT`. The related check went too:
`execute_in_transaction` runs the write in the transaction it is given, also on a `db` bound to another transaction.

### Pipeline bitwise functions are removed

`bit_not`, `bit_left_shift` and `bit_right_shift` are gone from `FirestoreExpr`. Google does not document them and the
service has failed calls to them. `FirestoreExpr::raw` sends them by name for the day they work.

```rust,ignore
// Before
let mask = expr.bit_not();

// After
let mask = FirestoreExpr::raw("bit_not", [expr]);
```

### Pipeline list items take `Into`

The traits `FirestorePipelineConditionExpr`, `FirestoreTextQueryItem` and `FirestoreTextQueryInput` are removed, with
their methods `build_condition`, `build_clause` and `build_query`. The builders now take the standard conversions:

- `for_all` and `for_any` of the filter builder take items that are `Into<Option<FirestoreBoolExpr>>`;
- `for_all` of the text query builder takes items that are `Into<Option<FirestoreTextQueryClause>>`;
- the closure of `text` returns anything that is `Into<FirestoreTextQuery>`: a string, a query, a clause, or an
  `Option` of a query or a clause.

Code that only calls the builders compiles as it is. Code that names the traits in a bound uses `Into` instead, and
calls `.into()` where it called `build_query()`. A closure that returned an `Option` of a string returns the string.

### Week variants take a `Weekday`

The seven `WeekStartingMonday` … `WeekStartingSunday` variants of `FirestoreTimestampGranularity` and `FirestoreTimestampPart` are replaced by `WeekStarting(jiff::civil::Weekday)`: `WeekStartingMonday` becomes `WeekStarting(Weekday::Monday)`.

### Read retries use the delays of transactions

A retried read waits like a transaction and a simple batch writer do: 500 ms for the first retry, growing by 1.5 times
for each next one, each delay randomised by up to 50% either way. Before, it waited a random time of up to 1 second,
then up to 2 seconds, then up to 4. Reads still stop after `max_retries`. The `rand` dependency is removed.

### Cache reads in a transaction or at a read time go to Firestore

A session with a consistency selector never reads the cache: `get`, batch get, `query` and `list` inside
`run_transaction`, or on a handle from `clone_with_consistency_selector`, are answered by Firestore. Before, a document
or a preloaded collection the cache held was served from it, so the transaction did not read or lock those documents and
a read time returned the current cached state. A document read this way is not written to the cache either.

Under `read_cached_only` the same reads return `FirestoreError::CacheError` with the code
`CacheConsistencySelectorUnsupported`, because that mode never contacts Firestore. Code that read a transaction through
a cached-only handle needs `read_through_cache` instead.

Inside `run_transaction`, a `?` on that error makes it transient, so the transaction is retried 4 times, with a few
seconds of backoff, before the error is returned wrapped in `FirestoreError::ErrorInTransaction`. Return
`BackoffError::permanent` from the closure to fail on the first attempt.

### A failed cache read fails the batch read by IDs

The default `get_docs_by_paths` of `FirestoreCacheDocsByPathSupport` yields the error of a path it fails to read.
Before, it logged the error and left that document out, so a batch read by IDs under `read_cached_only` returned fewer
documents without an error, and under `read_through_cache` fell back to Firestore. Both modes now return the error, as a
read of a single document already did. A backend that overrides `get_docs_by_paths` is unaffected.

### Batch reads with a field mask do not fill the cache

A batch get with `.fields(..)` under `read_through_cache` no longer offers the masked documents to the cache. Before, a
cached document could hold only the masked fields, and later reads by ID returned it as the whole document.

### Cached queries return what Firestore returns

The cached query engine, used by `query` on preloaded collections, now returns the documents Firestore returns, in the
order Firestore returns them. The rules it follows are in the "What the cache answers" section of the "Choosing a cache
mode" page of the book. Before, these queries returned other documents, in an arbitrary order:

- **Timestamps.** Before, seconds and nanoseconds were compared separately, so `1.9 s < 2.1 s` was false. Now a
  timestamp compares as one point in time, at the precision Firestore uses for the operator, and an integer and a double
  compare by value.
- **Order and range filters.** Before, only numbers and timestamps had an order: `<` and `>` on a string matched
  nothing, an `end_at` cursor on one returned nothing, and `order_by` returned an arbitrary order or panicked. Now every
  type follows Firestore's order of types.
- **Several `order_by` fields.** Before, the last field decided. Now the first field that differs decides, and ties end
  on the document name.
- **Cursors, `offset` and `limit`.** Before, `limit` applied first, so `limit(10).offset(5)` returned 5 documents and
  `start_at` with a `limit` could return none, and each field of a cursor was compared on its own. Now the cursors apply
  first, then `offset`, then `limit`, and a cursor is one position in the order of its fields.
- **`in`, `not-in` and `array-contains`.** Before, `in` and `array-contains` expected the field and the filter value the
  other way round, so they returned no documents, and `not-in` returned only the documents with an array field. Now
  they match as in Firestore.
- **`find_nearest`, `return_only_fields` and `explain_options`.** Before, a query that set one returned the filtered
  documents without the nearest-neighbour search, projection or plan, and a `get` or batch get with `.fields(..)`
  returned the whole cached document. Now such a read goes to Firestore under `read_through_cache`, and fails with
  `CacheIncompleteCollection` or `CacheFieldMaskUnsupported` under `read_cached_only`.
- **Null and NaN.** Before, `is_null` matched documents without the field, and `is_not_nan` matched documents where the
  field is null or absent. Now neither does, and arrays, maps, bytes, references and geo points take part in equality.

### Cache sessions answer pipelines from the cache

A pipeline or SQL read through `read_through_cache` or `read_cached_only` is answered from the cache when the cache
evaluates every part of it, see the "Pipelines" section of the caching chapter. Before, every pipeline ran on Firestore
whatever the session. Under `read_through_cache` such a read is now eventually consistent, as Core reads through the
cache are, and its items carry no `execution_time`. Under `read_cached_only` a pipeline the cache does not answer fails
with a `CacheError`, where it used to run on Firestore. To keep a pipeline on Firestore, run it on a `FirestoreDb` that
has no cache attached.

## 0.62

### Simple batch writes stop retrying after 60 seconds

`FirestoreSimpleBatchWriteOptions::retry_max_elapsed_time` is now 60 seconds by default instead of `None`.
60 seconds is the limit Google's Firestore service config sets for `BatchWrite`. Before, a `write` that kept failing with a retryable error,
such as `UNAVAILABLE` or `RESOURCE_EXHAUSTED`, retried forever; now it stops retrying 60 seconds after the first
attempt and returns that error. The limit is checked between attempts and does not end an attempt in flight.

To keep retrying with no limit, set the field to `None`:

```rust,ignore
let options = FirestoreSimpleBatchWriteOptions::new().opt_retry_max_elapsed_time(None);
```

### Listener idle reset

`FirestoreListenerParams` gains `idle_timeout`, off by default. When set, a listener closes and reopens a listen
stream that received nothing for that long, resuming from the stored tokens; 120 seconds is the recommended value,
see [Listening the document changes](https://firestore-rust.abdolence.dev/listening-changes.html). Adding the field
breaks code that builds `FirestoreListenerParams` with a struct literal; `FirestoreListenerParams::new` and its
builder are unaffected.

### Listen builder `retry_delay` removed

`FirestoreDocChangesListenerInitBuilder::retry_delay` is removed. The setter stored a delay that `add_target`
never read, so it had no effect. The retry delay belongs to the listener, not to a target: set it with
`FirestoreListenerParams::with_retry_delay` and create the listener with `create_listener_with_params`.

```rust,ignore
let listener = db
    .create_listener_with_params(
        storage,
        FirestoreListenerParams::new().with_retry_delay(std::time::Duration::from_secs(5)),
    )
    .await?;
```

### Results without a document name have no ID fields

An aggregation result has no document name. Before, deserialising one set `_firestore_id` and `_firestore_full_id` to an
empty string; now it has neither. A struct field aliased to `_firestore_id` or `_firestore_full_id` that is not an
`Option` fails to deserialise such a result, because the field is missing. Make it `Option<String>`:

```rust,ignore
#[derive(Deserialize)]
struct Totals {
    #[serde(alias = "_firestore_id")]
    id: Option<String>,
    total: i64,
}
```

Documents that have a name are unchanged. See [Reading Firestore document metadata as struct fields](https://firestore-rust.abdolence.dev/document-metadata.html).

### Text and geo search indexes in index management

Index management declares and lists the text and geo indexes of an Enterprise edition database. The public types change
with it:

- `FirestoreIndexFieldMode` gains the variants `Text` and `Geo`, so an exhaustive `match` on it needs two more arms;
- `FirestoreCompositeIndex` gains the field `search_options`, so a struct literal needs it set, to `None` for the
  defaults. Indexes declared with the fluent builder are unaffected.

`.plan()` and `.sync()` fail with `InvalidParametersError` before writing anything when the plan includes a field
override and the database is an Enterprise edition database, which has no single-field index overrides. Before, the
sync created its indexes and then failed on the first override with `INVALID_ARGUMENT`.

## 0.61

v0.61.0 takes retry limits and delays as `std::time::Duration` instead of `FirestoreDuration`
(`jiff::SignedDuration`), so a negative value no longer compiles. The transaction's retry time cap is renamed
to `retry_max_elapsed_time`, the name the simple batch writer already uses.

| Field or argument | 0.60 | 0.61 |
|---|---|---|
| `FirestoreTransactionOptions` retry cap | `max_elapsed_time: Option<FirestoreDuration>` | `retry_max_elapsed_time: Option<std::time::Duration>` |
| `FirestoreSimpleBatchWriteOptions` retry cap | `retry_max_elapsed_time: Option<FirestoreDuration>` | `retry_max_elapsed_time: Option<std::time::Duration>` |
| `FirestoreErrorInTransaction::retry_after` delay argument | `retry_after: FirestoreDuration` | `retry_after: std::time::Duration` |

The builder setters follow: `with_max_elapsed_time` on `FirestoreTransactionOptions` becomes
`with_retry_max_elapsed_time`, and both fields' setters take the new type. Replace
`FirestoreDuration::from_secs(30)` with `std::time::Duration::from_secs(30)`, or convert a `jiff::SignedDuration`
limit you already have with `std::time::Duration::try_from(value)?`. For a `retry_after`, use `value.unsigned_abs()`
instead: it cannot fail, so it works inside the transaction callback, and it keeps the 0.60 delay.

A negative limit used to fail every transaction or `write` with a `DeserializeError`, and a negative `retry_after` was
used as a positive delay of the same length. `FirestoreDuration` stays for durations Firestore reports, such as
`FirestoreExecutionStats::execution_duration`.

`retry_max_elapsed_time` now bounds a transaction retry whose callback named a `retry_after` too: the retry runs
only if the delay ends within the cap. Before, only `max_retries` bounded those retries, so a cap of zero still let
them run.

`FirestoreDbOptions` gains `request_timeout`, off by default, see
[Request timeouts](https://firestore-rust.abdolence.dev/request-timeouts.html). Adding the field breaks code that builds
`FirestoreDbOptions` with a struct literal naming every field; `FirestoreDbOptions::new` and its builder are unaffected.

Gets, lists, list collection IDs and the start of queries and aggregations now retry a `DEADLINE_EXCEEDED` from
Firestore up to `max_retries` times, with or without a request timeout set, and the error they return after the last
retry has `retry_possible` set. Writes, batch gets, partition queries and streams that have already started still
return it at once, with `retry_possible` unset.

## 0.58

v0.58.0 moves to gcloud-sdk 0.33, which mints the tokens with `google-cloud-auth`, the authentication crate of
Google's official Rust SDK. The gcloud-sdk token sources are removed, and so is the constructor that took one.

### Constructors

| 0.57 | 0.58 |
|---|---|
| `FirestoreDb::new`, `with_options`, `for_default_project_id` | unchanged |
| `FirestoreDb::with_options_service_account_key_file(options, path)` | unchanged, for a service account key |
| `with_options_token_source(options, scopes, TokenSourceType::Default)` | `with_options_auth(options, GoogleAuthHeaders::from_adc_with_scopes(scopes).await?)` |
| `with_options_token_source(options, scopes, TokenSourceType::File(path))` | `with_options_service_account_key_file`, or `with_options_auth` with credentials from `credentials::{service_account, user_account, impersonated, external_account}::Builder::new(json)` |
| `with_options_token_source(options, scopes, TokenSourceType::MetadataServer)` | `with_options_auth(options, credentials::mds::Builder::default().build()?)` |
| `with_options_token_source(options, scopes, TokenSourceType::ExternalSource(..))` | implement `CredentialsProvider` and pass `Credentials::from(..)` to `with_options_auth`, see [examples/token_auth.rs](examples/token_auth.rs) |

`with_options_auth` takes anything that converts into `firestore::gcloud_sdk::GoogleAuthHeaders`, such as `Credentials`
or a `GoogleAuthHeaders`. The `credentials` paths above are modules of
`firestore::gcloud_sdk::google_cloud_auth::credentials`. The library re-exports the gcloud-sdk version it compiles
against as `firestore::gcloud_sdk`, so a direct gcloud-sdk dependency is no longer needed for these types.
Scopes are set on the credentials builders with `with_scopes` or `with_access_specifier`. For the rest of the
authentication API see the [gcloud-sdk migration guide](https://github.com/abdolence/gcloud-sdk-rs#migrating-from-032).

### Behaviour changes

- `with_options_service_account_key_file` reads only service account keys. User credentials and workload identity
  federation files go through `with_options_auth` with the builders above.
- Requests carry every header the credentials produce, so a quota project reaches Firestore as `x-goog-user-project`.
- Credentials are built inside a Tokio runtime, since building them spawns their refresh task.
- Without a rustls crypto provider, `with_options_service_account_key_file` returns a `SystemError` with the code
  `CryptoProviderMissing`.

### Features and MSRV

- The MSRV is 1.91, the MSRV of `google-cloud-auth`.
- The new default feature `auth-default-crypto` brings the aws-lc-rs rustls provider of `google-cloud-auth`, which service
  account keys sign with. With `default-features = false`, enable it next to your TLS feature, or install a rustls
  `CryptoProvider` before creating a client. See [Crypto provider](https://firestore-rust.abdolence.dev/auth.html#crypto-provider).

## 0.56

v0.56.0 bounds how long `run_transaction` and `run_transaction_with_options` keep retrying, and
fixes several cases where a transaction retried the wrong thing.

### Retries are bounded

Before this release, a transaction whose callback kept failing transiently retried forever; no
option stopped it. `FirestoreTransactionOptions` gains a `max_retries` field, 4 by default (5
attempts in total, the same default as Google's official clients), so retrying now stops on its
own. Set it with `with_max_retries`. Adding the field breaks code that builds
`FirestoreTransactionOptions` with a struct literal naming every field; `new()` and `default()` are
unaffected.

`max_elapsed_time` still works as an optional time cap on top of `max_retries`, off by default. A
negative `max_elapsed_time` is now rejected before the first attempt rather than after it, and a
`max_elapsed_time` of zero now means no retry at all, since the first retry no longer runs
immediately (see below).

### The first retry now waits too

A retry used to run immediately after the failure that triggered it, and only the *next* retry
waited a backoff delay (or the callback's `retry_after`). Every retry, the first included, now
waits first. Account for the added delay if you relied on the previous behaviour's immediate first
retry.

### Commit and read retries were tightened

- `Commit` is sent once per attempt. Only `ABORTED` retries the whole transaction; any other
  failure, `UNAVAILABLE` or a dropped connection included, is now returned to the caller without
  rerunning the callback, since the writes may already be applied.
- A read inside a transaction that returns `ABORTED` no longer retries on the same transaction ID;
  it now fails the attempt, and the whole transaction reruns instead.
- A failed callback attempt is now rolled back before the transaction retries or gives up,
  releasing its locks instead of leaving them held until Firestore's own expiry.
- A retried `get` keeps its field mask; it previously dropped it and read the whole document.

## 0.55

v0.55.0 adds validated `FirestoreDocumentId` and `FirestoreCollectionId` newtypes and, along with
them, tightens the checks every document and collection ID already went through. Inputs that were
previously accepted now return `FirestoreError::InvalidParametersError`:

| Input | Before | After |
|---|---|---|
| Document ID empty, `"."`, or `".."` | built a path | `Err` (`field: "document_id"`) |
| Collection ID empty, `"."`, `".."`, or containing `/` | built a path (a `/` retargeted the operation at a different collection) | `Err` (`field: "collection_id"`) |
| `insert().document_id("")` | silently auto-generated a document ID, the same as not calling `.document_id(...)` | `Err`; call `.generate_document_id()` instead |

A collection ID containing `/` used to reach the server and come back as
`FirestoreError::DatabaseError`; it is now rejected on the client as
`FirestoreError::InvalidParametersError` before any request is sent. Code that matches on the
error variant for this case needs to match the new one.

The reserved `__*__` namespace (for example `__id7__`) is unaffected - it is still accepted, since
those documents exist and are readable through Datastore-mode integer IDs.

### Ordering

`.order_by([(path!(T::field), FirestoreQueryDirection::Descending)])` is deprecated in favour of a
closure builder, matching `.filter()` and `.transforms()`:

```rust,ignore
// before
.order_by([(path!(MyTestStructure::some_num), FirestoreQueryDirection::Descending)])
// after
.order(|o| o.fields([o.field(path!(MyTestStructure::some_num)).desc()]))
```

`.order_by(...)` keeps working and is not scheduled for removal in 0.x.

One behaviour differs: an empty ordering built through `.order()` (every entry conditional and
every condition false) clears the query's ordering entirely, the same as never calling `.order()`.
`.order_by([])` instead sets the ordering to an empty list, which is not equivalent for a listing
query - an empty list disables the collection cache's fast path, while no ordering at all does not.

## 0.52

v0.52.0 makes the low level API crate private, so the Fluent API is the only public entry point.
If you called methods on `FirestoreDb` that came from the `*Support` traits, the compiler will
report `no method named ... found`. Here is the mapping.

### Create

| Before | After |
|---|---|
| `db.create_obj::<_, (), _>(C, Some(id), &obj, None)` | `db.fluent().insert().into(C).document_id(id).object(&obj).execute::<()>()` |
| `db.create_obj(C, Some(id), &obj, None)` | `db.fluent().insert().into(C).document_id(id).object(&obj).execute()` |
| `db.create_obj(C, None::<&str>, &obj, None)` | `db.fluent().insert().into(C).generate_document_id().object(&obj).execute()` |
| `db.create_obj_at(parent, C, Some(id), &obj, None)` | `db.fluent().insert().into(C).document_id(id).parent(parent).object(&obj).execute()` |
| `db.create_doc(C, Some(id), doc, None)` | `db.fluent().insert().into(C).document_id(id).document(doc).execute()` |

### Read

| Before | After |
|---|---|
| `db.get_obj::<T, _>(C, id)` returning `T` | `db.fluent().select().by_id_in(C).obj::<T>().one(id)` returning `Option<T>` |
| `db.get_doc(C, id, None)` | `db.fluent().select().by_id_in(C).one(id)` |
| `db.batch_stream_get_objects::<T, _, _>(C, ids, None)` | `db.fluent().select().by_id_in(C).obj::<T>().batch(ids)` |
| `db.batch_stream_get_docs(C, ids, None)` | `db.fluent().select().by_id_in(C).batch(ids)` |

Note the return type change on single reads: the fluent `.one()` returns `Option<T>` instead of
erroring when the document does not exist. Add `.expect(...)` or handle the `None` case if you
relied on the old behaviour.

### Update

| Before | After |
|---|---|
| `db.update_obj(C, id, &obj, None, None, None)` | `db.fluent().update().in_col(C).document_id(id).object(&obj).execute()` |
| `db.update_obj(C, id, &obj, Some(paths!(T::{a, b})), None, None)` | `db.fluent().update().fields(paths!(T::{a, b})).in_col(C).document_id(id).object(&obj).execute()` |
| `db.update_obj(C, id, &obj, None, None, Some(precondition))` | `db.fluent().update().in_col(C).document_id(id).object(&obj).precondition(precondition).execute()` |
| `db.update_doc(C, doc, None, None, None)` | `db.fluent().update().in_col(C).document(doc).execute()` |

### Delete

| Before | After |
|---|---|
| `db.delete_by_id(C, id, None)` | `db.fluent().delete().from(C).document_id(id).execute()` |
| `db.delete_by_id_at(parent, C, id, None)` | `db.fluent().delete().from(C).parent(parent).document_id(id).execute()` |

### Query, list, aggregate, listen

| Before | After |
|---|---|
| `db.query_obj::<T>(params)` | `db.fluent().select().from(C).filter(...).obj::<T>().query()` |
| `db.stream_query_obj::<T>(params)` | `db.fluent().select().from(C)...obj::<T>().stream_query()` |
| `db.stream_list_obj::<T>(params)` | `db.fluent().list().from(C).obj::<T>().stream_all()` |
| `db.list_collection_ids(params)` | `db.fluent().list().collections().get_page()` |
| `db.aggregated_query_obj::<T>(params)` | `db.fluent().select().from(C).aggregate(...).obj::<T>().query()` |
| `db.listen_doc_changes(...)` | `db.fluent().select().from(C).listen().add_target(target_id, &mut listener)?` |

Unchanged in 0.52: batch writers, transactions, `db.create_listener()` and the
`FirestoreDb::serialize_*_to_doc` / `deserialize_doc_to` helpers. `FirestoreResumeStateStorage`
gains one required method - see [Listener changes](#listener-changes).

`FirestoreTransactionOps` also stays public. It is implemented by both `FirestoreTransaction` and
`FirestoreTransactionData`, so you can keep writing transaction-agnostic abstractions over it.

If you implemented the `*Support` traits yourself, for example to stub the database in unit
tests, that is no longer possible.

### Caching changes

`FirestoreCache::new()` and `FirestoreCache::with_options()` still work but are deprecated in
favour of the builder:

```rust
// Before
let mut cache = FirestoreCache::new(
    "example-mem-cache".into(),
    &db,
    FirestoreMemoryCacheBackend::new(
        FirestoreCacheConfiguration::new().add_collection_config(
            &db,
            FirestoreCacheCollectionConfiguration::new(
                "test-caching",
                FirestoreListenerTarget::new(1000),
                FirestoreCacheCollectionLoadMode::PreloadAllDocs,
            ),
        ),
    )?,
    FirestoreMemListenStateStorage::new(),
).await?;
cache.load().await?;

// After
let cache = FirestoreCache::memory(&db)
    .name("example-mem-cache")
    .preloaded_collection("test-caching")
    .build()
    .await?;
```

Two behaviour changes to be aware of:

- `list` and `query` are now served from the cache only for preloaded collections.
  `read_through_cache` falls back to Firestore for the rest, and `read_cached_only` returns a
  `FirestoreError::CacheError` instead of silently returning partial results. Opt back into the
  old behaviour with
  `.incomplete_collection_policy(FirestoreCacheIncompleteCollectionPolicy::PartialResults)`.
- `FirestoreCache::load()` and `shutdown()` now take `&self`, so `let mut cache` bindings will
  report an unused `mut`. Drop the `mut`.

The persistent cache also had a bug where document deletions were never committed, so deleted
documents stayed cached indefinitely. If you use `caching-persistent`, upgrading is recommended.

### Listener changes

The listener now acts on the target-lifecycle messages it previously ignored. Three things change
for callers.

`FirestoreResumeStateStorage` gains a required `forget_resume_state`, so a custom implementation
will not compile until it is added. That is deliberate: it is called when Firestore resets or
removes a target, and a durable storage that quietly kept the token would hand it back on the next
process start, where Firestore rejects it with `INVALID_ARGUMENT`. For most storages it is a
one-liner - dropping the entry, as the in-memory one does:

```rust
async fn forget_resume_state(&self, target: &FirestoreListenerTarget) -> AnyBoxedErrResult<()> {
    self.tokens.write().await.remove(target);
    Ok(())
}
```

A storage that holds nothing across restarts may return `Ok(())` and do nothing. Both shipped
storages implement it.

Listener callbacks now receive `TargetChange` events that carried a resume token. Previously the
listener consumed those itself and never passed them on, so a `RESET` or a `REMOVE` was invisible.
Callbacks that only match `DocumentChange` are unaffected; a callback with a catch-all arm will see
more events than before. Match on them with the new `FirestoreListenerTargetChangeType` alias:

```rust
if let FirestoreListenEvent::TargetChange(target_change) = event {
    if FirestoreListenerTargetChangeType::try_from(target_change.target_change_type)
        == Ok(FirestoreListenerTargetChangeType::Current)
    {
        // The target now reflects a consistent snapshot.
    }
}
```

A listen request Firestore rejects as invalid no longer shuts the whole listener down on the first
attempt. Because a stale resume token is a plausible cause, and one bad token used to take down
every target, the listener now discards all stored resume tokens and retries once before treating
the error as permanent.

### Caching changes from the listener work

The cache now removes a document when Firestore reports it as removed rather than deleted
(`DocumentRemove`), which it previously dropped on the floor - such a document stayed cached
indefinitely. It also acts on target resets: it drops what it holds for the affected collection and
lets Firestore replay it. While that replay is in progress the collection does not answer
`list`/`query` from the cache, so `read_through_cache` falls back to Firestore and
`read_cached_only` returns a `FirestoreError::CacheError` for the duration.

### Changing cached collections at runtime

Collections can now be added to and removed from a running cache, so the set of cached collections
no longer has to be decided when the cache is built:

```rust
cache.add_collection(FirestoreCacheCollection::new("currencies").preload_all()).await?;
cache.remove_collection("currencies").await?;
```

Three things changed to make this possible.

`FirestoreListener::add_target` now takes `&self` instead of `&mut self`, and works after `start()`
as well as before it. Calling it through a `&mut` binding still compiles, so the fluent
`.listen().add_target(target_id, &mut listener)` form is unaffected. It now rejects a target ID that
is already registered, where it previously accepted the duplicate and silently collapsed it. There
is also a new `remove_target`, which is `async` because it forgets the target's stored resume state
before returning.

`FirestoreCacheBackend` gains `cache_configuration`, `add_collection` and `remove_collection`. All
three have default implementations - the last two report that the backend does not support the
operation - so existing backends keep compiling. The `config` field on both shipped backends is now
private; use the `config()` accessor instead.

A cache may now be built with no collections at all, where the builder previously rejected that.

### Shutdown releases resources

`FirestoreCache::shutdown` now releases the backend's resources rather than only stopping the
listener. The in-memory backend drops its cached documents; the persistent backend closes its
database, releasing the exclusive lock on the file so another cache can be opened over the same
directory without waiting for the first to be dropped.

Reads through a cache that has been shut down do not fail: they behave as a cache miss, so
`read_through_cache` falls back to Firestore and `read_cached_only` reports the miss as an error.

### Caching named documents

A cached collection can now be limited to a set of document IDs, which makes the listener watch
exactly those documents instead of the whole collection:

```rust
FirestoreCache::memory(&db)
    .collection_with("configs", |c| c.documents(["site", "billing"]).preload_all())
```

`FirestoreCacheCollectionConfiguration` gains a `collection_watch` field for this, so any code
constructing that struct with a literal needs the extra field - use
`FirestoreCacheCollectionConfiguration::new(..)` and the `with_documents` builder instead. Such a
collection is never listable, whatever its load mode.

### Detecting a diverged cache

The cache now acts on Firestore's existence filter, which reports how many documents a target
matches. When that disagrees with what a preloaded collection holds - meaning changes were missed,
typically deletes that happened while the listener was disconnected - the collection is dropped and
replayed rather than left quietly wrong.

The count is only compared when it can be trusted. A collection filled lazily, or held in a memory
cache configured with `time_to_live` / `time_to_idle`, is never checked this way: a shortfall there
is the cache expiring entries rather than a divergence, and treating it as one would re-download
the collection repeatedly.

`FirestoreCacheBackend` gains `authoritative_doc_count` and `begin_collection_resync` for this, both
with default implementations - the first disables the check, the second falls back to invalidating
the whole cache. Custom backends keep compiling; implement them to get the finer behaviour.
