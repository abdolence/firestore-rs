# Bulk delete

Firestore's `BulkDeleteDocuments` operation removes every document in one or more collection
groups in the background, without you reading or writing each document yourself. The library
exposes it through `db.fluent().delete().bulk()`, behind the same `admin` feature [index
management](./index-management.md) uses; see [Enabling the `admin`
feature](./index-management.md#enabling-the-admin-feature) for turning it on. Google's own
documentation for the underlying operation is at
<https://cloud.google.com/firestore/docs/manage-data/bulk-delete>.

## Why the builder takes collection groups

`.bulk()` takes `.collection_groups(...)`, and there is no `.parent()`: Google's
`BulkDeleteDocuments` deletes every document in every collection with the given ID, at any depth,
across the whole database, and Firestore offers no parent-scoped bulk delete. Naming a collection
group called `orders` deletes every `orders` collection under every document in the database,
including ones your code never queries.

## What survives a bulk delete

- documents written after the operation starts processing;
- any collection whose ID does not match a named group, even nested under a deleted one.

## Background and non-transactional

Firestore picks the documents to delete as of one snapshot time, which the result reports as
`snapshot_time`. The deletes themselves are not one transaction, so other writes can interleave
with them while the operation runs. `execute()` returns once the
operation is requested unless you ask it to wait; either way the deletion itself continues on
Firestore's side, independent of your process.

## Declaring a bulk delete

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let result = db
    .fluent()
    .delete()
    .bulk()
    .collection_groups(["orders"])
    .execute()
    .await?;
println!("{result}");
# Ok(())
# }
```

`.collection_groups(...)` replaces whatever an earlier call in the same chain set, so call it
once with every group you want removed.

## The empty-list and duplicate-group rules

An empty list is rejected before anything is sent: Firestore reads an empty `collection_ids` as
"the whole database", and the library refuses to send that on your behalf. Naming the same group
twice is rejected too. Both checks run at `.execute()`, alongside `FirestoreCollectionId`'s own
validation of each name, and return `InvalidParametersError` naming what is wrong, without making
a request.

## Waiting for the operation and reading the result

By default `execute()` returns as soon as Firestore accepts the operation. Add
`.wait_until_done(timeout)` to poll until it reaches a terminal state instead, or
`.wait_until_done_with_options(...)` for control over the poll interval too:

```rust,no_run
# use firestore::*;
# use std::time::Duration;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let result = db
    .fluent()
    .delete()
    .bulk()
    .collection_groups(["orders"])
    .wait_until_done(Duration::from_secs(300))
    .execute()
    .await?;
println!("{result}");
# Ok(())
# }
```

The returned `FirestoreBulkDeleteResult` carries the operation name, the collection groups you
asked for, and the elapsed time, plus, once you wait for it, the snapshot time Firestore used to
decide what to delete and document and byte progress; without `.wait_until_done(...)`, the
snapshot time and progress stay unset and print as "progress not reported", never as `0/0`. It
implements `Display`, so printing or logging it directly works with or without a tracing
subscriber.

A failed wait leaves the operation running on Firestore's side; the library logs the result built
from what was polled so far at `warn` before returning the error, the same as index management's
`.sync()` does for its own wait.

## Logs and spans

A bulk delete logs through `tracing`, one span covering the whole call (`Firestore Bulk Delete`),
nesting a `Firestore Bulk Delete Wait` child span when you wait. An excerpt from a real run
against five documents:

```text
Started a bulk delete. operation=".../operations/CyA0..." action="bulk_delete" collection_groups="firestore-rs-bulk-delete-test"
Firestore reported the bulk delete's snapshot time. snapshot_time=2026-09-26T19:50:00Z
Operation reached a terminal state. action="bulk_delete" polls=3
```

The third line comes from the same wait every long-running admin operation polls through, index
sync included, which is why it names the operation generically rather than as a bulk delete.

That run took about 11 seconds end to end, for 5 documents; budget for it running longer against
more data.

## The Firestore emulator

The emulator answers `BulkDeleteDocuments` with `UNIMPLEMENTED`. Unlike index management, which
skips silently against the emulator, a bulk delete returns an explicit error instead: there is no
safe default result to report for an operation the emulator cannot run at all, so the library
never lets it pass silently.

## IAM roles

`roles/datastore.bulkAdmin` carries `datastore.databases.bulkDelete`, the permission a bulk delete
needs. `roles/datastore.owner` is a superset and also works.

## Running the tests against a real project

`tests/admin-bulk-delete-tests.rs` writes and bulk-deletes real documents in `GCP_PROJECT`, so it
carries `#[ignore]` and only runs explicitly:

```bash
GCP_PROJECT=your-project cargo test --test admin-bulk-delete-tests --features admin -- --ignored --nocapture
```

Full example available
[here](https://github.com/abdolence/firestore-rs/blob/master/examples/bulk-delete.rs).
