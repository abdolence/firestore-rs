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
