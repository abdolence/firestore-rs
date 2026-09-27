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
            i.index([
                i.field(path!(Order::tags)).array_contains(),
                i.field(path!(Order::placed_at)).desc(),
            ])
            .all_descendants(),
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

## Subcollections

`.collection_group(...)` takes a subcollection's ID the same way it takes a top-level
collection's: `.collection_group("posts")` names every collection anywhere in the database whose
ID is `posts`, at any depth. A slash-delimited path is rejected the same way `FirestoreCollectionId`
rejects one everywhere else in the library, with a message pointing at
`.parent(db.parent_path(...))`.

Collection scope, the default, declares an index for a query scoped to one subcollection through
`.from("posts").parent(parent_path)`. Add `.all_descendants()` to the same index for a query
across every `posts` subcollection at once, through `.from("posts").all_descendants()`. The two
scopes are different query shapes, so a group can declare both at the same time, one index for
each:

```rust,no_run
# use firestore::*;
# #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
# struct Post {
#     published: bool,
#     created_at: FirestoreTimestamp,
#     tags: Vec<String>,
# }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
    .indexes()
    .collection_group("posts")
    .composite(|i| {
        i.indexes([
            i.index([
                i.field(path!(Post::published)).asc(),
                i.field(path!(Post::created_at)).desc(),
            ]),
            i.index([
                i.field(path!(Post::tags)).array_contains(),
                i.field(path!(Post::created_at)).desc(),
            ])
            .all_descendants(),
        ])
    })
    .sync()
    .await?;

let user_path = db.parent_path("users", "alice")?;
let own_posts: Vec<Post> = db
    .fluent()
    .select()
    .from("posts")
    .parent(&user_path)
    .filter(|q| q.for_all([q.field(path!(Post::published)).eq(true)]))
    .order(|o| o.fields([o.field(path!(Post::created_at)).desc()]))
    .obj()
    .query()
    .await?;

let tagged_rust: Vec<Post> = db
    .fluent()
    .select()
    .from("posts")
    .all_descendants()
    .filter(|q| q.for_all([q.field(path!(Post::tags)).array_contains("rust")]))
    .order(|o| o.fields([o.field(path!(Post::created_at)).desc()]))
    .obj()
    .query()
    .await?;
# let _ = (own_posts, tagged_rust);
# Ok(())
# }
```

The ID is shared across the whole database, including subcollections your own code never writes
to. A statement owns the collection group, so `.prune_undeclared()` on `collection_group("posts")`
reaches the indexes of every `posts` collection in the database.

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

A pruning sync sends its deletes last, and only after every index the same sync created has
finished building, so a replaced index keeps serving queries until the new one can. The sync
waits for that even without `.wait_until_ready(...)`, and a composite index build can take
minutes. The wait ends by the sync's deadline: the `.wait_until_ready(timeout)` timeout when you
set one, 30 minutes from the start of the sync otherwise. If a new index fails to build, or is
still building at the deadline, the sync deletes nothing and returns the error.

The sync also deletes nothing when Firestore answers a `CreateIndex` with `ALREADY_EXISTS`. That
means Firestore holds an index it counts as the declared one, but the library did not match it to
any listed index, so it can be one of the indexes planned for deletion. The sync logs a warning
naming the declared index and carries on with everything else.

In both cases `withheld_deletes` on the report lists the indexes left in place and the reason.
Reverts and TTL disables do not wait for new indexes.

`.prune_undeclared().plan()` previews a pruning sync without writing anything: `delete_indexes`,
`revert_fields` and `disable_ttl` list exactly what a pruning `.sync()` would remove, and
`kept_undeclared_indexes`, `kept_undeclared_fields` and `kept_undeclared_ttl` list what it would
leave alone without `.prune_undeclared()`.

## Running from several instances

Several replicas of the same application, such as a Kubernetes deployment's pods, should not each
call `.sync()` on their own. Two replicas racing on the same collection group create, delete and
revert the same resources at the same time, and during a rolling update old and new replicas
declare different indexes, so `.prune_undeclared()` on both flips indexes back and forth for as
long as the rollout takes.

Run `.sync()` once per release instead of from the replicas, but not as one call: a sync that
prunes before the new replicas are up deletes an index the still-running old ones query, and their
queries fail with `FAILED_PRECONDITION` for as long as the rollout takes. Split it in two: create
and update what the release declares before the rollout, without `.prune_undeclared()`, then prune
what it no longer declares once the rollout has finished:

```rust,no_run
# use firestore::*;
# use std::time::Duration;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let report = db
    .fluent()
    .indexes()
    .collection_group("orders")
    .wait_until_ready(Duration::from_secs(30 * 60))
    .sync()
    .await?;
println!("{report}");
# Ok(())
# }
```

Add `.prune_undeclared()` for the second run. `.wait_until_ready(...)` makes either run fail when a
build does not finish in time, so neither reports success while indexes are still building.

From a Kubernetes `Job`, run the first as an early step and the second once the rollout is
serving; from a CI/CD step, the same split before and after the deploy step. From Helm, the
`helm.sh` annotations turn a `Job` into a chart hook; a `pre-install,pre-upgrade` hook runs the
create-only sync, and a `post-upgrade` hook the pruning one:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: orders-index-sync-create
  annotations:
    "helm.sh/hook": pre-install,pre-upgrade
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
          image: your-app:1.4.2
          args: ["--sync-indexes"]
```

The pruning hook is the same manifest under a different name, `"helm.sh/hook": post-upgrade`, and
`--sync-indexes --prune` (or whatever flag your binary uses to turn on `.prune_undeclared()`) in
`args`. Run `helm upgrade` with `--wait` or `--atomic` (which implies `--wait`): Helm's documented
install lifecycle waits for the chart's resources to reach a ready state before running
`post-install`, and `post-upgrade` is documented the same way, "after all resources have been
upgraded"; without `--wait`, nothing holds `post-upgrade` back until the new pods are actually
serving, and pruning at that point can still delete an index one of them still queries. See
[Helm's chart hooks](https://helm.sh/docs/topics/charts_hooks/) and the
[`--wait` and `--atomic` flags on `helm upgrade`](https://helm.sh/docs/v3/helm/helm_upgrade/).

Set the Job's `backoffLimit` to `0` and `activeDeadlineSeconds` above the sync's timeout, so
Kubernetes does not restart or kill it mid build. A Helm hook is only waited for up to
`helm upgrade`'s own `--timeout`, 5 minutes by default, independent of `activeDeadlineSeconds`:
pass a longer one (`helm upgrade --timeout 35m`, or more) alongside an `activeDeadlineSeconds` in
the same range, or Helm gives up on the hook long before Kubernetes does.

A `pre-install` hook runs before Helm creates any of the chart's ordinary resources, so the
`serviceAccountName` the Job names must already exist by then: give the `ServiceAccount` its own
`pre-install,pre-upgrade` hook annotation, at `helm.sh/hook-weight: "-1"`, a lower weight than the
create Job's `0`, so Helm creates it first.

Helm's documented hook ordering also names `pre-rollback` and `post-rollback`: `pre-rollback` runs
"after templates are rendered, but before any resources are rolled back", the same shape as
`pre-install` and `pre-upgrade`. Put the create-only Job on `pre-rollback` too, so the indexes the
restored release needs exist before its pods come back; Helm's docs do not say whether a
rollback's hooks are rendered from the revision being restored or the one currently installed, so
confirm which one runs in your own cluster before relying on it. Leave the pruning Job off
`post-rollback`: a rollback has no record of what the release you are rolling back from declared,
so pruning there can delete an index the restored release still needs.

A plain `Job` outside Helm has no `hook-delete-policy` to remove the previous run, and a completed
Job under the same fixed name fails a later `kubectl apply` with an immutable-field error. Set
`ttlSecondsAfterFinished` on it, or delete the previous Job before applying the next one.

The application's own replicas do not need to sync at all. If you want one to check its own
declaration against Firestore, `.plan()` with `.prune_undeclared()` at startup is read-only and
safe from any number of replicas at once: log a warning, or emit a metric, when `create_indexes`,
`update_fields`, `enable_ttl`, `delete_indexes`, `revert_fields` or `disable_ttl` comes back
non-empty. `unchanged` and `unchanged_ttl` are non-empty in the steady state and say nothing about
drift on their own. Do not wire this into the replica's readiness probe: after a Helm rollback, or
when an old pod restarts once the release job has already run, every replica would see the same
drift and fail its probe at once.

The job's service account needs `roles/datastore.indexAdmin`, the role `.sync()` always needs.
Replicas that only call `.plan()` need `roles/datastore.viewer`.

When two syncs do overlap regardless, index creates and deletes converge on their own: a
`CreateIndex` answered `ALREADY_EXISTS`, and a `DeleteIndex` answered `NOT_FOUND`, both count as
done. A field revert Firestore refuses because another writer already reverted it counts as done
too, and one refused while another writer's revert is still in progress is not simply retried:
this sync polls the field until that revert finishes, within its own deadline, then counts it as
done. A TTL disable does not get that wait: Firestore reports no in-progress state for a TTL
field, so a disable refused while another writer's disable is still running fails, rather than
waiting for it, and only converges when the other writer's disable had already finished. A field
override write or a TTL enable that races another sync's is not checked against the field's
current state either, so it can still fail.

## The order `.sync()` writes changes in

`.sync()` applies one plan in a fixed order, stopping at the first write that fails:

- creates missing composite indexes;
- writes declared field overrides;
- disables undeclared TTL fields;
- enables declared TTL fields;
- reverts undeclared field overrides;
- deletes undeclared composite indexes, once the new ones have finished building.

Every TTL disable finishes before any TTL enable is sent, since Firestore allows only one TTL
field per collection group.

## Waiting for changes to finish

By default `.sync()` returns as soon as the last change is requested; a created index or a newly
enabled TTL field can still be building. `.wait_until_ready(timeout)` polls until every started
change reaches a terminal state, or returns an error naming what is still pending once `timeout`
is up:

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
composite or vector index build is not instant even on an empty collection, so budget minutes
for it.

The timeout covers the whole sync, from the start of the call, and every wait inside it ends by
the same deadline. The last round of polls goes out at the deadline itself. One poll may take up
to 10 seconds to answer (or the whole timeout, if that is shorter), so an error can arrive that
long after the deadline. A poll that takes longer is sent again next round, and so is one that
Firestore answers with `UNAVAILABLE`, `DEADLINE_EXCEEDED` or `NOT_FOUND`.

A sync that fails after it started writing leaves the writes already applied in place, since
`.sync()` never rolls those back. It logs the report built so far at `warn` before returning the
error, so what went through survives in the log even though the returned `Result` is an error.
This holds for any failure: a refused write, a failed operation, or a deadline passed in the
final wait or in a wait between two writes.

## A write to a field waits for its own earlier write in the same sync

Two writes to the same field resource are never sent back to back: overlapping writes to one field
are not known to be safe, so the later one waits for the earlier one to reach a terminal state
first. This applies whenever one sync writes a field override and also enables or disables TTL on
that same field, and to a TTL move in particular: disabling the old TTL field, then enabling the
new one, waits for the disable to settle before the enable is sent. Writes to different fields are
sent back to back, since Firestore accepts that.

These waits happen even without `.wait_until_ready(...)`. They end by the sync's deadline: the
`.wait_until_ready(timeout)` timeout when you set one, 30 minutes from the start of the sync
otherwise, which is long enough for a field override's own single-field index build. There is no
separate way to configure that default today.

A revert can wait the same way for a different sync's write, not this one's own: see
[Running from several instances](#running-from-several-instances).

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

A composite index declared without `__name__` matches a listed index that has it where Firestore
puts it: last in an ordinary index, or directly before the vector field in a vector index. The
direction of that `__name__` does not matter for the match. A `__name__` anywhere else makes the
listed index a different one. Declare `__name__` yourself only when a query needs a specific
direction; an explicit declaration then matches only a listed index with exactly the same fields,
`__name__` included.

Field paths compare in one spelling, so `` `expires_at` `` and `expires_at`, or `` a.`b` `` and
`` `a`.b ``, are the same field. A segment needs backticks only when it is not a plain identifier
(letters, digits and `_`, not starting with a digit).

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

An excerpt from a real `.plan()` call against `firestore-rs-index-sync-test`, currently empty (no
composite indexes, field overrides or TTL fields declared, and none listed for the group):

```text
DEBUG Firestore Index Plan{/firestore/collection_group="firestore-rs-index-sync-test" /firestore/prune=false}:Firestore Index List{...}: firestore::db::admin::indexes: Listed the collection group's indexes and fields. indexes=0 fields=0 skipped_indexes=7 skipped_fields=0
 INFO Firestore Index Plan{...}: firestore::db::admin::indexes: Existing state: 0 indexes, 0 field overrides, 0 TTL fields collection_group="firestore-rs-index-sync-test" composite_indexes=0 field_overrides=0 ttl_fields=0
 INFO Firestore Index Plan{...}: firestore::db::admin::indexes: Firestore index plan: no changes (0 unchanged, 0 pending, 0 ttl unchanged)
 collection_group="firestore-rs-index-sync-test" create_indexes=0 update_fields=0 enable_ttl=0 unchanged=0 pending=0 delete_indexes=0 revert_fields=0 disable_ttl=0 kept_undeclared_indexes=0 kept_undeclared_fields=0 kept_undeclared_ttl=0 unrecognised=0
 INFO Firestore Index Plan{...}: firestore::db::admin::indexes: plan() reports what sync() would change; nothing was applied. collection_group="firestore-rs-index-sync-test"
```

`skipped_indexes` counts composite indexes `ListIndexes` returned for other collection groups
sharing the same database; the library filters listed items down to the owned group before
computing anything, since a raw `ListIndexes` on one group's parent has been observed to return
every group's indexes.

The plan event's fields split undeclared items the same way pruning does: `delete_indexes`,
`revert_fields` and `disable_ttl` are what a pruning sync would remove, and
`kept_undeclared_indexes`, `kept_undeclared_fields` and `kept_undeclared_ttl` are what it would
leave alone. This run has none of either, since the group carries nothing undeclared to begin
with.

`.sync()` also logs a `Firestore Index Apply` span with one child span per applied action, and a
`Firestore Index Wait` span when waiting is requested, with one child span per operation polled.

## Timings, unchanged TTL and the emulator marker on the report

`FirestoreIndexSyncReport` carries a `timings` value alongside its lists: the whole call's
duration, plus how long listing, applying and, when you waited, waiting each took. `Display`
prints it as `timings: total <n> ms`, followed by `, list <n> ms`, `, apply <n> ms` and
`, wait <n> ms` for whichever phases ran.

`unchanged_ttl` reports declared TTL fields that were already active before this sync ran,
separately from `enabled_ttl`, which is what this sync itself enabled.

`skipped` is `Some(FirestoreIndexSyncSkipReason::Emulator)` when the sync ran against the emulator
and did nothing, `None` once it has actually talked to Firestore. `.plan()` carries the same
marker on `FirestoreIndexPlan::skipped`, so a plan made against the emulator prints as skipped
rather than as "no changes needed" on a project the emulator never actually checked.

## The Firestore emulator

`.plan()` and `.sync()` both skip when `FIRESTORE_EMULATOR_HOST` is set. The emulator answers
`ListIndexes` with `UNIMPLEMENTED`, so both return an empty plan or report carrying the emulator
`skipped` marker, and log an `info` line saying so, instead of failing, so the same startup code
runs unmodified against the emulator.

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
[here](https://github.com/abdolence/firestore-rs/blob/master/examples/index-management.rs),
[here](https://github.com/abdolence/firestore-rs/blob/master/examples/index-whitelist.rs) and
[here](https://github.com/abdolence/firestore-rs/blob/master/examples/index-subcollection.rs).
