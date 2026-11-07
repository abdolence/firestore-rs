# Standard and Enterprise editions

Firestore databases come in two editions, Standard and Enterprise. The library supports both, and
some features need Enterprise. Every page of this book applies to both editions, unless its title,
or the part of the book it sits in, names one edition, or a section "On the Enterprise edition"
lists what differs.

The edition belongs to a database, not to the project: one project can hold databases of both
editions, and the client picks one by its ID, see
[Client instance and lifecycle](./client.md#multiple-databases).

## What each edition runs

The API column names the API a row is about: Core queries, pipelines, SQL, Admin for index
management, TTL and bulk delete, or Any for a row that belongs to no one API. "Pipeline, SQL" marks
a row where both behave the same. A number points to the notes under the table.

| Feature | API | Standard | Enterprise |
|---|---|---|---|
| Reads and queries | Core | yes | yes |
| Reads and queries | [Pipeline](./pipelines.md), [SQL](./sql.md) | fails[^pipeline-standard] | yes |
| Writes and batch writes | Core | yes | yes |
| [Writes](./pipeline-writes.md) | Pipeline, SQL | no | yes |
| Query that no index covers | Core | fails[^core-no-index] | table scan[^core-table-scan] |
| Query that no index covers | Pipeline, SQL | no | table scan[^pipeline-table-scan] |
| Aggregations: count, sum, avg | Core | yes | yes, same results |
| [Transactions](./transactions.md) | Any | yes | yes[^optimistic] |
| Query plans | Core | `.explain()` | rejected[^explain] |
| Query plans | Pipeline, SQL | no | `.explain()`[^explain] |
| [Vector search](./vector-search.md) | Core | needs a vector index[^vector-index] | no index needed[^vector-enterprise] |
| [Vector search](./vector-search.md) | Pipeline | no | table scan[^pipeline-vector] |
| [Text and geo search](./pipeline-search.md) | Pipeline, SQL | no | needs a search index |
| [Listeners](./listening-changes.md) | Core | yes | needs real-time updates |
| [Caching](./caching.md) | Core | yes | needs real-time updates |
| [Caching](./caching.md) | Pipeline, SQL | only if the cache answers in full | needs real-time updates |
| Automatic single-field indexes | Admin | yes | none |
| Single-field overrides | Admin | yes | none[^exempt] |
| Composite index of one field | Admin | a vector field only | yes |
| Unique indexes | Admin | no | yes |
| Index density[^density] | Admin | `SparseAll` only | `Dense` by default, or `SparseAny`[^search-sparse-any] |
| `__name__` appended to an index | Admin | yes | no |
| TTL policy | Admin | yes | yes |
| [Index statements](./sql/indexes.md) | SQL | yes | yes |
| [Bulk delete](./bulk-delete.md) | Admin | yes | yes |
| [Firestore emulator](./emulator.md) | Any | the default emulator | started with `--edition=enterprise` |
| Billing | Any | per document read, written and deleted | read units and write units[^realtime-billing] |

[^pipeline-standard]: A pipeline or SQL statement sent to a Standard edition database fails with
    `FAILED_PRECONDITION`, unless a [cache](./caching.md#pipelines) session answers it.
[^core-no-index]: `FAILED_PRECONDITION`, "The query requires an index".
[^core-table-scan]: The query reads every document of the collection. Without `order_by` the order of its results is
    unspecified.
[^pipeline-table-scan]: A pipeline or SQL statement that no index covers runs as a table scan. A search is the
    exception: it needs a search index and fails without one, see
    [Known table scans](./editions/query-plans.md#known-table-scans).
[^optimistic]: Optimistic by default, so under contention more attempts abort and are retried.
[^explain]: An Enterprise edition database rejects a Core `.explain()` with `INVALID_ARGUMENT`, so explain
    the same query as a pipeline or SQL, see
    [Explaining a pipeline](./editions/query-plans.md#explaining-a-pipeline).
[^vector-index]: Without a vector index the query fails with `FAILED_PRECONDITION`.
[^vector-enterprise]: A vector index can still be created on Enterprise, but whether a Core query uses it cannot be
    seen, since Core `.explain()` is rejected there.
[^pipeline-vector]: The `find_nearest` stage reads every document with or without a vector index, and a filter
    before it does not narrow the read. Google lists this as a known issue of pipelines. Use a Core
    `find_nearest` where the index should be used, see
    [Vector search](./vector-search.md#on-the-enterprise-edition).
[^exempt]: An `.exempt()` is already satisfied.
[^density]: The density of an index is which documents it holds an entry for.
[^search-sparse-any]: A search index is `SparseAny`.
[^realtime-billing]: Real-time updates are billed separately.

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
- a Core `.explain()` is rejected on Enterprise. The same query written as a pipeline or SQL returns its plan through `.explain()`, see [Explaining a pipeline](./editions/query-plans.md#explaining-a-pipeline).

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
times to expect on each edition.

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
