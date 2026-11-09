# Load modes, and why listings need preloading

- `PreloadNone` (`.collection(name)`): don't preload anything, just fill the cache while working;
- `PreloadAllDocs` (`.collection_with(name, |c| c.preload_all())`): preload all documents in the
  collection;
- `PreloadAllIfEmpty` (`.collection_with(name, |c| c.preload_all_if_empty())`): preload all
  documents only if the cache is empty. This is useful for the persistent cache, where the saving
  applies when the cache loads; for the memory cache it is the same as `PreloadAllDocs`, since an
  in-memory cache always starts empty. The persistent cache records when the download of a
  collection has finished, and skips the download only for a collection with that record. A
  collection whose download was interrupted, for example because the process was killed, is
  discarded and downloaded again on the next start. A persistent cache written before this record
  existed downloads each such collection once more on its first start.

`.preloaded_collection(name)` picks the appropriate preloading mode for the backend.

A lazily filled collection holds only the documents that happened to be read through it.
Answering a `list` or `query` from it would return a subset that looks like a complete answer, so
the library refuses to do so: `read_through_cache` quietly falls back to Firestore, and
`read_cached_only` returns an error naming the collection. If partial results are genuinely
acceptable, opt in with
`.incomplete_collection_policy(FirestoreCacheIncompleteCollectionPolicy::PartialResults)`.
