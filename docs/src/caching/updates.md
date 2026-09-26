# How the cache is updated

- When you read a document by ID through the cache and it is not there, it is fetched from
  Firestore and cached;
- The Firestore listener updates the cache when a document changes, whether the change came from
  your application or from elsewhere;
- Preloading at startup.

Cached results are eventually consistent: they reflect the last state the listener delivered, so a
write may take a moment to show up. Do not cache data that must be read at strong consistency.

Firestore also reports how many documents a target matches. When that disagrees with what a
preloaded collection holds - which means changes were missed, typically deletes that happened while
the listener was disconnected - the cache drops the collection and has Firestore replay it. The
count is only compared when it can be trusted: a collection the cache expires entries from, or
fills lazily, is never checked this way.

When Firestore resets or removes a listener target - after a reconnect, or when a stored resume
token has expired - the cache drops what it holds for that collection and Firestore replays it.
While that replay is in progress the collection stops answering `list` and `query` from the cache:
`read_through_cache` falls back to Firestore, and `read_cached_only` returns a `CacheError` rather
than a listing that looks complete but is not.

Full examples are available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/caching_memory_collections.rs)
and [here](https://github.com/abdolence/firestore-rs/blob/master/examples/caching_persistent_collections.rs).
