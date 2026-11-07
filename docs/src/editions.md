# Standard and Enterprise editions

Firestore databases come in two editions, Standard and Enterprise. The library supports both, and
some features need Enterprise. Every page of this book applies to both editions, unless its title,
or the part of the book it sits in, names one edition, or a section "On the Enterprise edition"
lists what differs.

The edition belongs to a database, not to the project: one project can hold databases of both
editions, and the client picks one by its ID, see
[Client instance and lifecycle](./client.md#multiple-databases).

## What each edition runs

| Feature | Standard | Enterprise |
|---|---|---|
| Core reads, queries, writes and batch writes | yes | yes |
| A Core query that no index covers | fails with `FAILED_PRECONDITION`, "The query requires an index" | runs as a *table scan*, reading every document of the collection; without `order_by` the order of its results is unspecified |
| Core aggregations: count, sum, avg | yes | yes, with the same results |
| [Transactions](./transactions.md) | yes | yes; optimistic by default, so under contention more attempts abort and are retried |
| Core `.explain()` | yes | rejected with `INVALID_ARGUMENT` |
| Core [vector search](./vector-search.md) | needs a vector index | answered with and without a vector index |
| [Pipelines](./pipelines.md) and [SQL](./sql.md) | fail with `FAILED_PRECONDITION`, unless a cache session answers them | yes |
| [Pipeline writes](./pipeline-writes.md) and SQL writes | no | yes |
| [Text and geo search](./pipeline-search.md) | no | yes, with a search index |
| [Listeners](./listening-changes.md) | yes | yes, on a database with real-time updates enabled |
| [Caching](./caching.md) | Core reads; pipeline and SQL reads only when the cache answers them in full | Core reads, pipeline and SQL reads; the cache's listener needs real-time updates |
| Automatic single-field indexes | yes | none |
| Single-field overrides | yes | none; an `.exempt()` is already satisfied |
| Composite index of one field | a vector field only | yes |
| Unique indexes | no | yes |
| Index *density*: which documents an index holds an entry for | `SparseAll` only | `Dense` by default, or `SparseAny`; a search index `SparseAny` |
| `__name__` appended to an index | yes | no |
| TTL policy | yes | yes |
| [Bulk delete](./bulk-delete.md) | yes | yes |
| [Firestore emulator](./emulator.md) | the default emulator | an emulator started with `--edition=enterprise` |
| Billing | per document read, written and deleted | read units and write units, with real-time updates billed separately |

## Telling which edition a database is

`gcloud firestore databases describe` prints the settings of a database, among them its edition,
its real-time updates mode and its concurrency mode:

```sh
gcloud firestore databases describe --database=my-database
```

These lines of its output name the three settings:

```text
concurrencyMode: OPTIMISTIC
databaseEdition: ENTERPRISE
realtimeUpdatesMode: REALTIME_UPDATES_MODE_ENABLED
```

The Firestore page of the Google Cloud console lists the databases of a project with the edition
of each.

Two errors also tell the editions apart: a pipeline sent to a Standard edition database fails with
`FAILED_PRECONDITION`, and a Core `.explain()` sent to an Enterprise edition database fails with
`INVALID_ARGUMENT`, "Explain options are not supported in RunQuery API for Enterprise edition".

[Index management](./index-management.md) reads the edition itself, through `GetDatabase`, on
every `.plan()` and `.sync()`, and follows the rules of that edition.

## Settings fixed at creation

The edition and the real-time updates setting are immutable: to change either, create a new
database with the wanted settings and move the data. Both are chosen when the database is created:

```sh
gcloud firestore databases create --database=my-database --edition=enterprise \
  --location=nam5 --enable-firestore-data-access --enable-realtime-updates
```

Google's documentation does not agree on the default of real-time updates for an Enterprise
edition database, so set `--enable-realtime-updates` explicitly when the database needs
[listeners](./listening-changes.md#on-the-enterprise-edition) or a
[cache](./caching.md#on-the-enterprise-edition).

## Limits

- a pipeline or SQL statement sent to a Standard edition database fails at once with
  `FAILED_PRECONDITION`, and the library does not retry it;
- a search without a search index that covers it fails with `FAILED_PRECONDITION`, see
  [Pipeline search](./pipeline-search.md);
- joins and subqueries are in Preview, see
  [Pipeline aggregations and joins](./pipeline-aggregations-and-joins.md#joins);
- Firestore rejects an atomic pipeline or SQL write that is too big, see
  [Write limits](./pipeline-writes.md#write-limits);
- Firestore has no setting that rejects a table scan, see
  [Query plans and cost](./editions/query-plans.md#recognising-a-full-scan);
- a Core `.explain()` is rejected on Enterprise, where only a pipeline returns a plan.

## Query and write semantics

A pipeline and a SQL statement keep Firestore's pipeline semantics, which differ from a Core query
in a few places: `not_equal` matches a document that lacks the field, `sort` keeps such documents,
and the stages run in the order they are written.
[Query and write semantics](./editions/semantics.md) lists every difference, with the SQL spelling
of each.

## Indexes

An Enterprise edition database builds no index on its own, takes indexes of one field, unique and
dense indexes, and search indexes. A Standard edition database indexes every field automatically,
and takes single-field overrides. See
[Composite and vector indexes](./index-management/composite.md),
[Search indexes](./index-management/search.md) and
[TTL and the Enterprise edition](./index-management/ttl-and-enterprise.md) for the rules, and
[Waiting for changes to finish](./index-management.md#waiting-for-changes-to-finish) for the build
times measured on each edition.

## Cost

Standard bills each document read, written and deleted; Enterprise bills read units (4 KiB) and
write units (1 KiB, one more per indexed field) instead, and real-time updates as a separate
charge. A *read unit* is 4 KiB of data, documents or index entries, that a read processes, so an
Enterprise read costs by the data it scans. The rates are on Google's pricing pages, for
[Standard](https://cloud.google.com/firestore/pricing) and
[Enterprise](https://firebase.google.com/docs/firestore/enterprise/pricing).
[Query plans and cost](./editions/query-plans.md) shows how to read what a query costs.

## Which API to use

[Choosing Core queries, pipelines or SQL](./editions/choosing-an-api.md) lists when each API fits,
on each edition.
