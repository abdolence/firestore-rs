# Index management

Composite indexes, vector indexes, single-field overrides and TTL policy are usually managed
outside your Rust code: through the console, `gcloud` or the Firebase CLI's
`firestore.indexes.json`. The library supports declaring them in Rust instead, next to the
structs and queries that need them, and reconciling Firestore with one explicit call.

## Enabling the `admin` feature

Index management, and the other administrative operations under `db.fluent()` that need the
Firestore Admin API, are behind the `admin` cargo feature, off by default:

```toml
[dependencies]
firestore = { version = "...", features = ["admin"] }
```

Nothing runs at `FirestoreDb` construction. An administrative operation only runs when your code
calls one of its terminals, `.plan()`/`.sync()` for index management.

## Declaring a collection group's indexes

Everything starts from `db.fluent().indexes().collection_group(...)`, then a chain of
declarations ending in `.plan()` or `.sync()`:

```rust,no_run
# use firestore::*;
# #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
# struct Order {
#     customer_id: String,
#     placed_at: FirestoreTimestamp,
#     tags: Vec<String>,
#     embedding: Vec<f64>,
#     bio: String,
#     expires_at: FirestoreTimestamp,
# }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let report = db
    .fluent()
    .indexes()
    .collection_group("orders")
    .composite(|i| {
        i.indexes([
            i.index([
                i.field(path!(Order::customer_id)).asc(),
                i.field(path!(Order::placed_at)).desc(),
            ]),
            i.index([i.field(path!(Order::tags)).array_contains()]).all_descendants(),
            i.index([
                i.field(path!(Order::customer_id)).asc(),
                i.field(path!(Order::embedding)).vector(768),
            ]),
        ])
    })
    .field_overrides(|f| {
        f.fields([f.field(path!(Order::bio)).exempt()])
    })
    .ttl([path!(Order::expires_at)])
    .sync()
    .await?;
println!("{report}");
# Ok(())
# }
```

`.composite()` builds the group's composite indexes: order a field with `.asc()`/`.desc()`, or
mark it `.array_contains()`, and chain `.all_descendants()` onto the whole index for a
collection-group index rather than a single collection. A `.vector(dimension)` field must be the
last field of the index it belongs to.

`.field_overrides()` builds single-field overrides: `.exempt()` removes a field from automatic
single-field indexing entirely, and `.indexes([...])` replaces it with exactly the indexes listed.
See [below](#a-field-override-replaces-the-whole-automatic-set) for what that replacement means.

`.ttl()` names the one field per collection group whose timestamp value enables document TTL.

Each of `.composite()`, `.field_overrides()` and `.ttl()` replaces whatever an earlier call in the
same chain set, so call each at most once. A composite index needs at least two fields, unless it
is a single vector field on its own; a field override or a TTL path may not repeat. These, and the
vector and field-count limits Firestore itself enforces, are checked at `.plan()`/`.sync()` and
return `InvalidParametersError` naming what is wrong.

## plan() versus sync()

`.plan()` is read-only: it lists what Firestore already has for the owned group and reports what
`.sync()` would change, without writing anything.

`.sync()` reconciles Firestore with the declaration: it creates missing composite indexes, writes
field overrides that differ from what is declared, and enables TTL fields that are not yet
configured.

Both return a value with a `Display` impl, so either can be printed or logged directly, with or
without a tracing subscriber:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let plan = db.fluent().indexes().collection_group("orders").plan().await?;
println!("{plan}");
# Ok(())
# }
```

## Ownership: one statement owns one collection group

A fluent chain names exactly one collection group, the same way `.from()` names one collection for
a query, and that group is the unit of ownership: `.prune_undeclared()` never reaches an index,
override or TTL field on any other group. Several groups need several statements; a startup
function typically runs one per group in turn.

There is no name prefix for a statement's own indexes. Firestore's `Index` and `Field` resources
are server-assigned and carry no label or description, so ownership is expressed entirely by
which collection group a statement names, never by anything stored on the index itself.

## Removing what you no longer declare

By default, an index, override or TTL field Firestore lists for the owned group but this
statement does not declare is only reported, never touched. Add `.prune_undeclared()` to also
delete undeclared composite indexes, revert undeclared field overrides to automatic indexing, and
disable undeclared TTL fields:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
    .indexes()
    .collection_group("orders")
    .prune_undeclared()
    .sync()
    .await?;
# Ok(())
# }
```

An undeclared index in state `NEEDS_REPAIR` is deleted like any other undeclared index once
pruning is on; Firestore does not exempt it. A *declared* index matched to a listed
`NEEDS_REPAIR` index is different: `.sync()` only ever reports that one, never deletes or
recreates it automatically.

## Waiting for changes to finish

By default `.sync()` returns as soon as changes are requested; a created index or a newly enabled
TTL field can still be building. `.wait_until_ready(timeout)` polls until every started change
reaches a terminal state, or returns an error naming what is still pending once `timeout` is up:

```rust,no_run
# use firestore::*;
# use std::time::Duration;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
    .indexes()
    .collection_group("orders")
    .wait_until_ready(Duration::from_secs(900))
    .sync()
    .await?;
# Ok(())
# }
```

`.wait_until_ready_with_options(...)` also sets the poll interval, 5 seconds by default. A
composite or vector index build is not instant even on an empty collection; budget minutes, not
seconds.

## Indexing only chosen fields

`f.all_fields()` targets Firestore's special `*` field path, scoped to the owned collection group.
Combined with `.exempt()`, it excludes every field in the group from automatic single-field
indexing; list the fields you do want indexed by name alongside it:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
    .indexes()
    .collection_group("orders")
    .field_overrides(|f| {
        f.fields([
            f.all_fields().exempt(),
            f.field("customer_id").indexes([f.ascending()]),
            f.field("status").indexes([f.ascending(), f.descending()]),
        ])
    })
    .sync()
    .await?;
# Ok(())
# }
```

A named field declared alongside `all_fields()` wins for that field, since it is more specific:
this is how "everything except these fields" is expressed. Composite indexes are unaffected by
`all_fields()`.

There is no database-wide equivalent. A collection-group statement never reaches the
`__default__` group's own `*` field, so a default applied across every collection group at once is
deliberately out of reach from this API.

## A field override replaces the whole automatic set

Declaring only `.array_contains()` on a field Firestore would otherwise also index for equality
and ordering removes that automatic support: Firestore does not merge an override with its
defaults, and neither does `firestore.indexes.json`. `.indexes([...])` always takes the whole set
to maintain for a field, never one index to add to what is already there. An empty list, or
`.exempt()` (the same thing), excludes the field from single-field indexing entirely.

## `__name__` handling

A composite index declared without `__name__` matches a listed index that has it. Firestore
appends `__name__` to every composite index at creation time, in the direction implied by the
index's last field (ascending, unless the last field is descending). Declare `__name__` yourself
only when a query needs a direction other than the implied one; an explicit declaration then
matches only a listed index whose `__name__` direction is exactly the same.

## Unrecognised items

Firestore can list index or field shapes this crate's domain model has no representation for: a
search index, a MongoDB-compat or Datastore-mode API scope, a `COLLECTION_RECURSIVE` query scope,
or an unspecified field order. These are reported in `unrecognised`, with the reason, and never
proposed for deletion or reversion, even with `.prune_undeclared()` set, since the plan cannot
know that removing them is what the declaration intends.

## Logs and spans

Index management logs through `tracing`. One span covers a whole `.plan()` or `.sync()` call
(`Firestore Index Plan` or `Firestore Index Sync`), and nests child spans for listing, diffing,
applying and, when `.wait_until_ready()` is set, waiting. Every span records its own elapsed time.

An excerpt from a real `.plan()` call, against a project with one declared composite index and
one field override, neither yet present in `firestore-rs-index-sync-test`:

```text
DEBUG Firestore Index Plan{/firestore/collection_group="firestore-rs-index-sync-test"}:Firestore Index List{...}: firestore::db::admin::indexes: Listed the collection group's indexes and fields. indexes=0 fields=0 skipped_indexes=7 skipped_fields=0
 INFO Firestore Index Plan{...}: firestore::db::admin::indexes: Existing state: 0 indexes, 0 field overrides, 0 TTL fields collection_group="firestore-rs-index-sync-test" composite_indexes=0 field_overrides=0 ttl_fields=0
 INFO Firestore Index Plan{...}: firestore::db::admin::indexes: Firestore index plan:
  create_indexes: 1
    [COLLECTION] (country ASC, created_at DESC)
  update_field_overrides: 1
    bio EXEMPT
 collection_group="firestore-rs-index-sync-test" create_indexes=1 update_fields=1 enable_ttl=0 unchanged=0 pending=0 undeclared_indexes=0 undeclared_fields=0 undeclared_ttl=0 unrecognised=0 prune=false
 INFO Firestore Index Plan{...}: firestore::db::admin::indexes: plan() reports what sync() would change; nothing was applied. collection_group="firestore-rs-index-sync-test"
```

`skipped_indexes` counts composite indexes `ListIndexes` returned for other collection groups
sharing the same database; the library filters listed items down to the owned group before
computing anything, since a raw `ListIndexes` on one group's parent has been observed to return
every group's indexes.

`.sync()` also logs a `Firestore Index Apply` span with one child span per applied action, and a
`Firestore Index Wait` span when waiting is requested, with one child span per operation polled.

## The Firestore emulator

`.plan()` and `.sync()` both skip when `FIRESTORE_EMULATOR_HOST` is set. The emulator answers
`ListIndexes` with `UNIMPLEMENTED`, so both return an empty, default plan or report and log an
`info` line saying so, instead of failing, so the same startup code runs unmodified against the
emulator.

## IAM roles

`.plan()` needs only `roles/datastore.viewer`. `.sync()` needs `roles/datastore.indexAdmin` too,
to create, update and delete indexes and fields.

## Running the tests against a real project

`tests/admin-indexes-tests.rs` needs the `admin` feature, a real Firestore project through
`GCP_PROJECT`, and local application default credentials. The fast, read-only test runs by
default:

```bash
GCP_PROJECT=your-project cargo test --test admin-indexes-tests --features admin -- --nocapture
```

Every other test in that file creates, updates or deletes real indexes, overrides or TTL policy,
and a composite or vector index build can take 10-15 minutes even on an empty collection, so those
carry `#[ignore]` and only run explicitly:

```bash
GCP_PROJECT=your-project cargo test --test admin-indexes-tests --features admin -- --ignored --nocapture
```

Full examples available
[here](https://github.com/abdolence/firestore-rs/blob/master/examples/index-management.rs) and
[here](https://github.com/abdolence/firestore-rs/blob/master/examples/index-whitelist.rs).
