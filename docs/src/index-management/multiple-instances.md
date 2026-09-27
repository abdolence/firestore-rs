# Running from several instances

Several instances of the same application, such as the replicas of one Kubernetes deployment,
declaring the same collection group's indexes need one of the approaches below.

## The problems

- **Version skew during a rolling update.** Old and new replicas declare different indexes; with
  `.prune_undeclared()` set, each flips the other's indexes back and forth, and every flip costs
  the minutes a build takes.
- **Two replicas racing.** Two `.sync()` calls for the same group at the same time create,
  update, delete and revert the same resources concurrently.
- **Duplicate work.** Every replica blocks on the same index builds at startup.

## Recommended: one writer per release

Treat index sync like a schema migration, run once per release, never from a replica.

A Kubernetes `Job`, a Helm `pre-install` or `pre-upgrade` hook, or a CI/CD step runs `.sync()`
with `.prune_undeclared()` once, before the replicas that need the new indexes start. Give it a
`--sync-indexes` flag or a dedicated binary in the same crate, so it shares the same index
declarations the application itself uses:

```rust,no_run
# use firestore::*;
# use std::time::Duration;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let report = db
    .fluent()
    .indexes()
    .collection_group("orders")
    .prune_undeclared()
    .wait_until_ready(Duration::from_secs(30 * 60))
    .sync()
    .await?;
println!("{report}");
# Ok(())
# }
```

`.wait_until_ready(...)` makes the job fail on a build that does not finish, instead of exiting
early while it is still running. Set the Job's `backoffLimit` and `activeDeadlineSeconds` above
that timeout, so Kubernetes does not kill or restart it mid build:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: orders-index-sync
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 2100
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: orders-index-sync
      containers:
        - name: sync-indexes
          image: your-app:latest
          args: ["--sync-indexes"]
```

A Helm hook runs the same image at the same point of a release instead of a standalone `Job`
applied by hand:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: orders-index-sync
  annotations:
    "helm.sh/hook": pre-upgrade,pre-install
    "helm.sh/hook-weight": "0"
    "helm.sh/hook-delete-policy": before-hook-creation,hook-succeeded
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 2100
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: orders-index-sync
      containers:
        - name: sync-indexes
          image: your-app:latest
          args: ["--sync-indexes"]
```

The job's service account needs `roles/datastore.indexAdmin`, granted through Workload Identity.
The replicas only read, so `roles/datastore.viewer` is enough for them.

## In replicas: check without applying

A replica can still confirm its own image agrees with what Firestore has, without writing
anything. Run `.plan()` with `.prune_undeclared()` at startup, and inspect the plan's public
fields:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let plan = db
    .fluent()
    .indexes()
    .collection_group("orders")
    .prune_undeclared()
    .plan()
    .await?;
let pending = !plan.create_indexes.is_empty()
    || !plan.update_fields.is_empty()
    || !plan.enable_ttl.is_empty()
    || !plan.delete_indexes.is_empty()
    || !plan.revert_fields.is_empty()
    || !plan.disable_ttl.is_empty();
if pending {
    eprintln!("{plan}");
    return Err("index declaration is out of sync with Firestore".into());
}
# Ok(())
# }
```

Log the plan, and fail the replica's readiness probe when changes are pending. A replica that
just started should not take the whole deployment down for indexes the release job is already
applying; a failed readiness probe holds it out of traffic and alerts, without restarting it into
the same state.

## When the application syncs itself

Sometimes there is no separate release job, and every replica runs `.sync()` on its own. Add
`.generation(..)` and `.lease(..)` for that case.

`.generation(..)` stops an older release from undoing a newer one. Give it a number that only
increases from one release to the next, such as a build number. `.sync()` records it in the
group's coordination document before it changes anything, and skips, changing nothing, when it
finds a higher generation already stored there. `.plan()` only reads it.

`.lease(..)` stops two replicas from applying changes to the same group at the same time. While
one replica's `.sync()` holds the lease, the others' calls skip or wait for it, as `.on_held(...)`
says. `FirestoreIndexLeaseOptions` also sets the lease's `ttl` (35 minutes by default, at least
one minute) and the `owner` label the others see in logs and skip reports (the pod name plus a
random suffix, by default).

Combined, they also handle a newer release that starts while an older one is still syncing:

- the newer `.sync()` records its generation, then waits for the older one to release the lease,
  even with `FirestoreIndexLeaseOnHeld::Skip`. It waits as long as its own deadline allows (the
  `.wait_until_ready(...)` timeout, or 30 minutes), or the lease wait's timeout when that is
  longer;
- the older `.sync()` reads the coordination document again before every admin request that
  changes something, finds the higher generation, stops before that request and releases the
  lease;
- the newer `.sync()` claims the lease and applies its own declaration.

The older sync fails with `FirestoreError::DataConflictError`, whose code is
`IndexLeaseSuperseded`. The same check stops a holder whose lease another caller took or cleared
(`IndexLeaseTaken`), or whose lease expired (`IndexLeaseExpired`). A holder of the same
generation, or one without a generation, is skipped or waited for as `.on_held(...)` says.

```rust,no_run
# use firestore::*;
# use std::time::Duration;
# async fn example(db: FirestoreDb, release: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let report = db
    .fluent()
    .indexes()
    .collection_group("orders")
    .generation(release)
    .lease(
        FirestoreIndexLeaseOptions::new().with_on_held(FirestoreIndexLeaseOnHeld::Wait(
            FirestoreIndexLeaseWait::new(Duration::from_secs(120)),
        )),
    )
    .prune_undeclared()
    .wait_until_ready(Duration::from_secs(20 * 60))
    .sync()
    .await?;
println!("{report}");
# Ok(())
# }
```

Both keep their coordination document in `firestore-rs-index-coordination` by default. Set
`.coordination_collection(...)` to use a different one, such as when several statements for
different groups should keep their documents apart. The collection is checked only when
`.generation(..)` or `.lease(..)` is set.

A skipped sync still returns `Ok`, with `report.skipped` naming why: `Superseded` for a lower
generation, `LeaseHeld` naming the current owner, its generation and when its lease expires, and
`Emulator` for a sync that ran against the emulator. A lease wait that times out returns the same
`LeaseHeld` skip and logs a warning.

When two replicas claim the group at the same moment, Firestore aborts the transaction of one of
them. The library reads the coordination document again, and the replica that lost skips or waits,
as `.on_held(...)` says. Only a claim that keeps losing until the sync's deadline fails the sync.
A claim whose reply is lost on the way back is read back too: when its lease is recorded, the sync
holds it, runs and releases it.

### Why there is no TTL policy on the coordination collection

The library sets none, and `.sync()` never enables one on it. Firestore deletes an expired
document only some time after it expires, typically within a day, so a TTL policy could not
decide whether a lease is still held at the moment `.sync()` needs to know. Deleting the document
would also forget the group's generation. One document per group is reused instead: releasing a
lease clears its fields, leaving nothing to clean up later.

### Limits

- Without a lease, a generation only stops a sync from starting. A sync that is already running
  finishes with its own declaration.
- With a lease, an older sync stops only before its next admin request. A sync already in its
  final wait for started operations sends no more of them, so the newer sync waits until it
  finishes.
- Without a lease, a declared field override write or a TTL enable sent by two racing replicas
  is unverified. Only deletes, reverts and TTL disables are checked for that, see below.
- Losing the lease during the final wait for started operations to finish does not fail the
  sync: the lease gates admin requests, and that wait sends none.
- The holder trusts its lease for nine tenths of the `ttl` after it sent the last claim or renewal
  Firestore confirmed. Past that, it stops before its next admin request, even when the
  coordination document still names it.
- A cancelled sync, such as a process killed mid sync, leaves the lease as it is; it expires on
  its own, 35 minutes after the last renewal by default.

## Racing syncs converge

Independent of `.generation(..)` and `.lease(..)`, a sync counts these cases as done when another
caller's sync got there first:

- `DeleteIndex` answered `NOT_FOUND`: the sync counts the index in `already_deleted_indexes`;
- a revert Firestore refuses is read back with `GetField`. When the field has no override of its
  own, the sync counts it in `already_reverted_fields`. When Firestore reports another caller's
  revert still in progress, the sync reads the field again every poll until that revert finishes,
  within its own deadline;
- a TTL disable Firestore refuses is read back the same way. When TTL is already off for that
  field, the sync counts it in `already_disabled_ttl`. Firestore reports no progress for a TTL
  disable, so a field that still has TTL fails the sync with the refusal.

This runs on every sync, with or without coordination: two replicas racing with no generation and
no lease at all still each finish without an error, even though only one of their writes actually
changed anything.

## Bulk delete

`db.fluent().delete().bulk()` has no coordination options of its own. Run it the same way as the
recommended index sync, as a one-shot job for a release, never from a replica. N replicas each
calling `.execute()` at startup start N bulk-delete operations over the same collection groups,
work Firestore then bills and schedules independently for each one. See
[Bulk delete](../bulk-delete.md) for the operation itself.
