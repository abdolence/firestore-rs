# Firestore emulator

To work with the Google Firestore emulator you can use the environment variable:

```bash
export FIRESTORE_EMULATOR_HOST="localhost:8080"
```

or specify it as an option using `FirestoreDb::with_options()`.

When `FIRESTORE_EMULATOR_HOST` is set, the library does not look up the Google
credentials and uses a stub token instead, since the emulator does not
authenticate the requests. This means you do not need any credentials
configured to develop against it. Specifying credentials explicitly, with
`FirestoreDb::with_options_auth()` for example, still takes precedence.

The emulator answers [bulk delete](./bulk-delete.md) and [index management](./index-management.md#the-firestore-emulator)
with `UNIMPLEMENTED`: `.plan()`, `.sync()`, the index calls `list()`, `create()`, `delete()` and
`update_field()`, and the [SQL index statements](./sql/indexes.md) fail with it. `SHOW COLLECTIONS`
works on the emulator.

## On the Enterprise edition

The emulator acts as one edition, Standard by default. `gcloud` starts it, and needs a Java runtime
(a JDK) for it:

```bash
gcloud emulators firestore start --host-port=127.0.0.1:8080 --edition=enterprise
```

The emulator prints `Database Edition: STANDARD` or `Database Edition: ENTERPRISE` as it starts.

Start the emulator with `--edition=enterprise` to run pipelines and SQL against it; the default
Standard emulator answers them with `INVALID_ARGUMENT`, and neither edition of the emulator
supports bulk delete. Core inserts and queries run on an emulator of either edition, and Core
aggregations, transactions and listeners run on the Enterprise one as on a live database.

The Enterprise emulator runs SQL and pipelines, and ignores Query Explain: an `EXPLAIN` or
`EXPLAIN ANALYZE` statement, and `.explain()`, return the rows and no plan. Check the plan of a
query against a live database, see [Explaining a pipeline](./editions/query-plans.md#explaining-a-pipeline).
