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
| Querying a collection (filtering, ordering, cursors) | only for **preloaded** collections, and only for supported queries |
| Paged listing, queries with metadata, aggregations, transactions, writes | never |
