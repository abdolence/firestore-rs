# Load modes, and why listings need preloading

- `PreloadNone` (`.collection(name)`): don't preload anything, just fill the cache while working;
- `PreloadAllDocs` (`.collection_with(name, |c| c.preload_all())`): preload all documents in the
  collection;
- `PreloadAllIfEmpty` (`.collection_with(name, |c| c.preload_all_if_empty())`): preload all
  documents only if the cache is empty. This is useful for the persistent cache; for the memory
  cache it is the same as `PreloadAllDocs`, since an in-memory cache always starts empty.

`.preloaded_collection(name)` picks the appropriate preloading mode for the backend.

A lazily filled collection holds only the documents that happened to be read through it.
Answering a `list` or `query` from it would return a subset that looks like a complete answer, so
the library refuses to do so: `read_through_cache` quietly falls back to Firestore, and
`read_cached_only` returns an error naming the collection. If partial results are genuinely
acceptable, opt in with
`.incomplete_collection_policy(FirestoreCacheIncompleteCollectionPolicy::PartialResults)`.
