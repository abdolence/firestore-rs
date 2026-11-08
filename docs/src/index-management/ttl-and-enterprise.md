# TTL and the Enterprise edition

## TTL policy

`.ttl()` names the one field per collection group whose timestamp value enables document TTL
(time-to-live): Firestore deletes a document itself once the timestamp in that field has passed. It
works the same on both editions:

```rust,no_run
# use firestore::*;
# #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
# struct Session { expires_at: FirestoreTimestamp }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
    .indexes()
    .collection_group("sessions")
    .ttl([path!(Session::expires_at)])
    .sync()
    .await?;
# Ok(())
# }
```

`.sync()` enables a declared TTL field that is not yet configured, and `.prune_undeclared()`
disables an undeclared one. Firestore allows only one TTL field per collection group. To move the
TTL to another field, declare the new field and set `.prune_undeclared()`;
[The order `.sync()` writes changes in](../index-management.md#the-order-sync-writes-changes-in)
describes how the move is applied, and when a second TTL field is refused.

## The Enterprise edition

An Enterprise edition database builds no index on its own: there are no automatic single-field
indexes, so every index a query needs is one the statement declares. `.plan()` and `.sync()` read
the database's edition once per call to match a declaration with a listed index, and send what the
statement declares. Firestore answers what it refuses with `INVALID_ARGUMENT`. On Enterprise:

- a composite index of one field is how a single field is indexed. Firestore accepts one there,
  ordered, `.array_contains()` or `__name__`, while a Standard edition database refuses it;
- `.field_overrides()` has nothing to override. An `.exempt()` is never planned, since Firestore
  stores nothing for it. Firestore refuses an override that lists indexes;
- `.ttl()` enables a TTL field as on Standard, and `.prune_undeclared()` disables an undeclared
  one the same way;
- Firestore appends no `__name__` field to an index, so `[a, b]` and `[a, b, __name__]` are two
  different indexes, and a statement can declare both;
- an index can be unique, refusing a write that would give two documents the same entry, and
  dense or sparse, holding an entry for every document or only for the ones that have the indexed
  fields. `.unique()` and `.density(..)` set these, as the sections after the example describe.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
    .indexes()
    .collection_group("users")
    .composite(|i| {
        i.indexes([
            i.index([i.field("email").asc()]).unique(),
            i.index([i.field("country").asc()]),
            i.index([i.field("country").asc(), i.field("nickname").asc()])
                .density(FirestoreIndexDensity::SparseAny),
        ])
    })
    .ttl(["expires_at"])
    .sync()
    .await?;
# Ok(())
# }
```

Without an index that covers it, a Core query on Enterprise runs as a table scan, where Standard
fails with `FAILED_PRECONDITION`, see [Query plans and cost](../editions/query-plans.md#known-table-scans).

## Unique indexes

A unique index refuses a write that would give two documents the same entry in it. Firestore
answers a create, an update and a commit alike with `ALREADY_EXISTS` ("Duplicate values are found
for unique index."), and the write returns `DataConflictError`. A unique index of one field is how
a value such as an email address is kept unique across a collection.

## Density

The *density* of an index says which documents it holds an entry for. A dense index holds one for
every document, with a missing field stored as unset; a sparse index leaves out the documents
that lack the indexed fields. Enterprise builds an index dense by default, and a text or geo
search index as `FirestoreIndexDensity::SparseAny`, an entry for each document that has at
least one of the fields. Standard builds every index as `SparseAll`, an entry only for a document
that has all of them. Firestore answers a density its edition does not build with
`INVALID_ARGUMENT`.

A declaration that sets a density matches only a listed index of that density. A declaration
without one matches a listed index over the same fields, scope and `unique` flag whatever its
density, and when several listed indexes match, the library treats it as it does any declaration
that matches several. Two indexes over the same fields are different indexes when one is unique
and the other is not.
