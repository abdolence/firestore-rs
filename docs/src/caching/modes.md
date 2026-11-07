# Choosing a cache mode

- `db.read_through_cache(&cache)` serves what it can from the cache and goes to Firestore for the
  rest. This is the mode to reach for by default.
- `db.read_cached_only(&cache)` never contacts Firestore. Reads by ID return `None` on a miss, and
  requests the cache cannot answer completely return an error.

Which operations use the cache:

| Operation | Cached |
|---|---|
| Read by ID, batch read by IDs | yes, for any cached collection |
| Listing all documents in a collection | only for **preloaded** collections |
| Querying a collection (filtering, ordering, cursors) | only for **preloaded** collections, and only for the queries in [What the cache answers](#what-the-cache-answers) |
| Reads in a transaction or at a read time | never: `read_through_cache` reads from Firestore, `read_cached_only` returns an error, see [Transactions](../transactions.md#transactions-and-the-cache) |
| Pipeline and SQL reads | for **preloaded** collections, and by ID for any cached collection, with the stages and functions in [Pipelines](../caching.md#pipelines) |
| Paged listing, queries with metadata, aggregations, writes | never |

## What the cache answers

A Core query on a preloaded collection runs in the library's cached query engine. The engine
returns the documents Firestore returns, in the order Firestore returns them:

- **timestamps and numbers.** Timestamps compare by seconds and then by the fraction of a second,
  at the precision Firestore uses:
  - `==`, the range operators and `array-contains` compare a timestamp on its own at the
    nanosecond;
  - `in`, `not-in` and `array-contains-any` compare at the microsecond that Firestore stores;
  - a timestamp inside an array or a map compares at the microsecond, whichever operator is used.

  An integer and a double compare by value, so a filter on `1` matches the double `1.0`, and
  `order_by` sorts them together;
- **order and range filters on every type.** `<`, `<=`, `>`, `>=`, `order_by` and cursors follow
  Firestore's order of types (null, booleans, numbers, timestamps, strings, bytes, references, geo
  points, arrays, vectors, maps) and its order within each type. A range filter matches values of
  its own type only, and never NaN;
- **several `order_by` fields.** The first field that differs decides. Ties end on the document
  name, in the direction of the last field. A document that lacks an `order_by` field is not
  returned, as in Firestore;
- **no `order_by`.** Results come in the order of the fields compared with `<`, `<=`, `>`, `>=`,
  `!=` or `not-in`, and then by document name;
- **cursors, `offset` and `limit`.** The cursors apply first, then `offset`, then `limit`, so
  `limit(10).offset(5)` skips 5 documents and returns up to 10. A cursor is one position in the
  order of its fields;
- **`in`, `not-in` and `array-contains`.** `in` matches a scalar field equal to any listed value,
  `not-in` one equal to none of them, leaving out documents where the field is null or absent, and
  `array-contains` an array field holding the value;
- **equality on arrays, maps, bytes, references and geo points.** `==`, `!=`, `in`, `not-in`,
  `array-contains` and `array-contains-any` match these values. Arrays are equal when their
  elements are equal in order, and maps when their fields are equal by key; inside both, an
  integer equals the double of the same value, and null and NaN equal themselves. `!=` also matches
  a field of another type;
- **null and NaN in a filter.** A null in a filter value matches nothing: `== null`, `!= null` and
  `array-contains` null return no documents, a null in an `in` or `array-contains-any` list is
  ignored, and a `not-in` list that holds a null returns no documents. NaN equals nothing, itself
  included, and is in no range; `!=` and `not-in` match it. Use `is_null` to find null fields:
  `is_null` does not match a document without the field, and `is_not_nan` does not match a
  document where the field is null or absent.

The engine applies none of `find_nearest`, `return_only_fields` and `explain_options`. A query that
sets one is answered by Firestore under `read_through_cache`, and fails with
`CacheIncompleteCollection` under `read_cached_only`. A `get` or batch get with `.fields(..)`
follows the same rule, and fails with `CacheFieldMaskUnsupported` under `read_cached_only`.

A query answered from the cache is eventually consistent, see
[How the cache is updated](./updates.md).

### On the Enterprise edition

The cached order is that of the Standard edition. Documents with equal values can come back in
another order from an Enterprise edition database than from the cache, see
[Query and write semantics](../editions/semantics.md#cached-results).
