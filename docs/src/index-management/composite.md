# Composite and vector indexes

`.composite()` builds the composite indexes of the collection group a statement owns, see
[Index management](../index-management.md) for the statement itself. Order a field with
`.asc()`/`.desc()`, or mark it `.array_contains()`, and chain `.all_descendants()` onto the whole
index for a collection-group index rather than a single collection.

```rust,no_run
# use firestore::*;
# #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
# struct Order {
#     customer_id: String,
#     placed_at: FirestoreTimestamp,
#     tags: Vec<String>,
# }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
    .indexes()
    .collection_group("orders")
    .composite(|i| {
        i.indexes([
            i.index([
                i.field(path!(Order::customer_id)).asc(),
                i.field(path!(Order::placed_at)).desc(),
            ]),
            i.index([
                i.field(path!(Order::tags)).array_contains(),
                i.field(path!(Order::placed_at)).desc(),
            ])
            .all_descendants(),
        ])
    })
    .sync()
    .await?;
# Ok(())
# }
```

On a Standard edition database a composite index needs at least two fields, unless it is a single
vector field on its own; an Enterprise edition database takes an index of one field, see
[TTL and the Enterprise edition](./ttl-and-enterprise.md#the-enterprise-edition). An index holds at
most one `.array_contains()` field. `.plan()` and `.sync()` check these rules before anything is
written, see [Declaring a collection group's indexes](../index-management.md#declaring-a-collection-groups-indexes).

## Vector indexes

A `.vector(dimension)` field makes a vector index, which a Core
[vector search](../vector-search.md) needs on a Standard edition database. The dimension is the one
every indexed vector in the field must have. A vector field must be the last field of the index it
belongs to, and ordered fields can come before it:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
    .indexes()
    .collection_group("articles")
    .composite(|i| {
        i.indexes([
            i.index([i.field("embedding").vector(768)]),
            i.index([i.field("category").asc(), i.field("embedding").vector(768)]),
        ])
    })
    .sync()
    .await?;
# Ok(())
# }
```

A vector index cannot be forced on a pipeline, and the pipeline `find_nearest` stage does not use
it, see [Query plans and cost](../editions/query-plans.md#known-table-scans).

## `__name__` handling

On a Standard edition database, a composite index declared without `__name__` matches a listed
index that has it where Firestore puts it: last in an ordinary index, or directly before the
vector field in a vector index. The direction of that `__name__` does not matter for the match. A
`__name__` anywhere else makes the listed index a different one. Declare `__name__` yourself only
when a query needs a specific direction; an explicit declaration then matches only a listed index
with exactly the same fields, `__name__` included.

An Enterprise edition database appends no `__name__`, so there a declaration matches only a
listed index with exactly its fields.

## Single-field overrides (Standard edition)

A Standard edition database indexes every field on its own automatically. `.field_overrides()`
builds single-field overrides of that: `.exempt()` removes a field from automatic single-field
indexing entirely, and `.indexes([...])` replaces it with exactly the indexes listed. An
Enterprise edition database has no automatic indexes to override, see
[TTL and the Enterprise edition](./ttl-and-enterprise.md#the-enterprise-edition).

### Indexing only chosen fields

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

### A field override replaces the whole automatic set

Declaring only `.array_contains()` on a field Firestore would otherwise also index for equality
and ordering removes that automatic support: Firestore does not merge an override with its
defaults, and neither does `firestore.indexes.json`. `.indexes([...])` always takes the whole set
to maintain for a field, never one index to add to what is already there. An empty list, or
`.exempt()` (the same thing), excludes the field from single-field indexing entirely.

Full examples available
[here](https://github.com/abdolence/firestore-rs/blob/master/examples/index-management.rs) and
[here](https://github.com/abdolence/firestore-rs/blob/master/examples/index-whitelist.rs).
