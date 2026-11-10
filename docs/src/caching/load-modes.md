# Load modes, and why listings need preloading

- `PreloadNone` (`.collection(name)`): don't preload anything, just fill the cache while working;
- `PreloadAllDocs` (`.collection_with(name, |c| c.preload_all())`): preload all documents in the
  collection;
- `PreloadAllIfEmpty` (`.collection_with(name, |c| c.preload_all_if_empty())`): preload all
  documents unless an earlier run finished downloading the collection. This is useful for the
  persistent cache, where the saving applies when the cache loads; for the memory cache it is the
  same as `PreloadAllDocs`, since an in-memory cache always starts empty. The persistent cache
  keeps a marker for each collection whose download has finished, and skips the download only for
  a collection with a marker. A collection whose download was interrupted, for example because the
  process was killed, has no marker: its rows are discarded and it is downloaded again on the next
  start. `invalidate_all` and a resync after Firestore resets the listener target remove the
  marker too, and each costs one more download on the next start.

`.preloaded_collection(name)` picks the appropriate preloading mode for the backend.

A lazily filled collection holds only the documents that happened to be read through it.
Answering a `list` or `query` from it would return a subset that looks like a complete answer, so
the library refuses to do so: `read_through_cache` quietly falls back to Firestore, and
`read_cached_only` returns an error naming the collection. If partial results are genuinely
acceptable, opt in with
`.incomplete_collection_policy(FirestoreCacheIncompleteCollectionPolicy::PartialResults)`.
