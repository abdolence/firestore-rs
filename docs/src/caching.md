# Caching

The library supports caching for collections and documents. A Firestore listener keeps the cache
up to date when documents change, so updates are propagated across distributed instances
automatically.

This avoids reading, and paying for, the same documents repeatedly. It is particularly useful for
dictionaries, configuration and other data that changes rarely, and can reduce both cost and
latency noticeably.

Caching is opt-in through cargo features:

- `caching-memory` for an in-memory cache, implemented with the
  [moka cache library](https://github.com/moka-rs/moka);
- `caching-persistent` for a persistent, disk backed cache, implemented with
  [redb](https://github.com/cberner/redb) and protobuf.

The cache serves Core reads, and pipeline and SQL reads, on both editions. Which reads it answers
is listed in [Choosing a cache mode](./caching/modes.md), and the Core query rules in
[What the cache answers](./caching/modes.md#what-the-cache-answers).

## Pipelines

[Pipelines](./pipelines.md) and [SQL](./sql.md) reads use the cache in the same sessions,
`read_through_cache` and `read_cached_only`. The cache answers a pipeline on its own when every
part of it is in the lists below, with the semantics Firestore gives a pipeline. Caching is meant
for data such as static dictionaries, or sessions read by ID, so the cache filters, sorts and pages
documents, and leaves computing to Firestore. Anything else runs on Firestore under
`read_through_cache`, and fails with a `CacheError` under `read_cached_only`.

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct Country { name: String, region: String }
# async fn example(db: FirestoreDb, cache: FirestoreMemoryCache) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let european: Vec<Country> = db
    .read_through_cache(&cache)
    .fluent()
    .pipeline()
    .select()
    .from("countries")
    .filter(|q| q.field(path!(Country::region)).eq("Europe"))
    .sort(|e| [e.field(path!(Country::name)).asc()])
    .obj()
    .query()
    .await?;
# let _ = european;
# Ok(())
# }
```

Sources:

- a cached collection, `.from(name)`, with `.parent(..)` for a sub-collection. The collection must
  be preloaded, as for a Core `query`;
- `.documents([..])`, and a collection whose first `filter` pins `__name__` with `eq` or `is_in`,
  read the named documents by ID, from any cached collection.

Stages: `filter`, `sort`, `limit`, `offset`, `fields`, `add_fields` and `remove_fields`. A `limit`
or `offset` needs a `sort` before it, since Firestore returns unsorted documents in an order of its
own. `fields`, `add_fields` and `remove_fields` take plain fields, with or without an alias, as
top-level fields. A computed expression in them runs on Firestore.

Functions:

- comparisons: `equal`, `not_equal`, `less_than`, `less_than_or_equal`, `greater_than`,
  `greater_than_or_equal`, and `is_null`, `is_not_null`, `is_nan`, `is_not_nan`, which are built
  from them;
- `and`, `or`, `not`, `exists`, `is_absent`;
- `equal_any`, `not_equal_any`, `array_contains`, `array_contains_all`, `array_contains_any`.

Everything else runs on Firestore: arithmetic and string functions, aggregates, `distinct`,
search, `find_nearest`, `union`, `unnest`, subqueries and variables, collection groups,
`database()`, `literals`, `force_index`, raw stages, writes, `like` and every other function. So
do Query Explain and a pipeline in a transaction or at a read time. The cache also sends a
pipeline to Firestore when it meets a value Firestore fails the request on, such as `not` of a
string, when a field name is not a simple identifier (letters, digits and underscores, not
starting with a digit), and when it would have to order arrays, maps, geo points or vectors, in a
`sort` or a `less_than`.

### What differs from a pipeline that Firestore runs

- Results are eventually consistent, as for Core reads, see
  [How the cache is updated](./caching/updates.md).
- The cache does not check the edition of the database. On a Standard edition database, where
  Firestore fails every pipeline with `FAILED_PRECONDITION`, a pipeline the cache answers
  succeeds, and one it does not answer fails as on Firestore.
- A result from the cache has no `execution_time` in its metadata, and an empty result has no
  item without a document.
- A read by ID that names a document the cache does not hold runs on Firestore under
  `read_through_cache`. Under `read_cached_only` the missing documents are left out, as a Core batch
  read leaves them. In a preloaded collection, a `__name__` filter that names a missing document
  is answered from the collection, where the document is absent.
- Pipelines never write to the cache: a pipeline that runs on Firestore leaves the cache as it was.
  Reads by ID through `get` fill a lazily loaded collection.
- Documents that tie on every sort key come back in document-name order from the cache, where
  Firestore's order of them is unspecified, so a `limit` that cuts through them can keep other
  documents than Firestore would.

### SQL

A SQL read runs as a pipeline, so the same lists apply. These shapes are cached, over one cached
collection or `documents(..)`:

- `SELECT` of columns, aliases, `*` and `* EXCEPT (..)`;
- `WHERE` with comparisons, `AND`, `OR`, `NOT`, `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM`,
  `[NOT] IN (list)`, `IN UNNEST(array)` and `BETWEEN`;
- `WHERE __name__ = 'id'` and `WHERE __name__ IN ('id', ..)`, read by ID;
- `ORDER BY`, and `LIMIT` and `OFFSET` with an `ORDER BY`.

`GROUP BY`, aggregates, `DISTINCT`, `UNION`, joins and subqueries, `collection_group(..)`,
`database()`, `+ - * / %`, `||`, `LOWER`, `UPPER`, `CHAR_LENGTH`, `LIKE`, `ILIKE`, `CASE`,
`COALESCE`, `IFNULL`, `SUBSTR` and search run on Firestore.

## On the Enterprise edition

The listener that keeps a cache current needs real-time updates on a database of the Enterprise
edition. They are enabled when the database is created:

```sh
gcloud firestore databases create --database=my-database --edition=enterprise \
  --location=nam5 --enable-firestore-data-access --enable-realtime-updates
```

The setting is immutable, and Google's documentation does not agree on its default for an
Enterprise edition database, so set the flag explicitly, see
[Settings fixed at creation](./editions.md#settings-fixed-at-creation).

A cached Core query follows the order of the Standard edition, and documents with equal values can
come back in another order from an Enterprise edition database, see
[What the cache answers](./caching/modes.md#on-the-enterprise-edition).
