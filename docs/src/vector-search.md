# Vector search

The library supports K-nearest neighbour (KNN) vector search in Core queries: `find_nearest` keeps
the documents whose vector field is nearest to a query vector.

A vector is stored in a field of type `FirestoreVector`, which serialises to a Firestore vector
value:

```rust
# use firestore::*;
# use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Deserialize, Serialize)]
struct Article {
    title: String,
    embedding: FirestoreVector,
}

let article = Article {
    title: "Firestore editions".to_string(),
    embedding: FirestoreVector::new(vec![0.1, 0.9, 0.3]),
};
# let _ = article;
```

`vec![..].into()` builds a `FirestoreVector` too, from any iterator of `f64`.

## The vector index

On a Standard edition database a vector query needs a vector index on the field, and uses it. The
index declares the dimension every indexed vector in the field must have. Without one the query
fails with `FAILED_PRECONDITION`, "Missing vector index configuration", and the message carries the
`gcloud` command that creates the index.
[Index management](./index-management/composite.md#vector-indexes) declares one with
`.vector(dimension)`, and the console or `gcloud` create one too.

## Finding the nearest documents

`find_nearest(field, vector, measure, limit)` after the source and filters keeps the `limit`
documents nearest to `vector`, nearest first:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct Article { title: String, embedding: FirestoreVector }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let nearest: Vec<Article> = db
    .fluent()
    .select()
    .from("articles")
    .find_nearest(
        path!(Article::embedding),
        FirestoreVector::new(vec![0.1, 0.8, 0.2]),
        FirestoreFindNearestDistanceMeasure::Cosine,
        5,
    )
    .obj()
    .query()
    .await?;
# let _ = nearest;
# Ok(())
# }
```

`FirestoreFindNearestDistanceMeasure` has three measures:

- `Euclidean`, the straight-line distance between the two vectors;
- `Cosine`, which compares the angle between the vectors and ignores their length;
- `DotProduct`, which is like cosine, and takes the length into account too.

Documents at the same distance come back in no fixed order. A `Cosine` query fails with
`FAILED_PRECONDITION`, "Cannot compute cosine distance against a vector with a magnitude of zero",
when the collection holds a vector of zeros.

## Distance and threshold

`find_nearest_with_options(..)` takes a `FirestoreFindNearestOptions`, with two more settings:

- `.with_distance_result_field(name)` outputs the distance of each document under that field, so
  the result type can read it. It holds the straight-line distance for `Euclidean`, the cosine
  distance for `Cosine` (0 for the same direction), and the dot product itself for `DotProduct`;
- `.with_distance_threshold(value)` returns only the documents within that distance. For
  `Euclidean` and `Cosine` it is the largest distance, and a document at exactly that distance is
  kept. For `DotProduct`, where a larger value means a nearer vector, it is the smallest one.

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct Article {
#     title: String,
#     embedding: FirestoreVector,
#     distance: Option<f64>,
# }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let nearest: Vec<Article> = db
    .fluent()
    .select()
    .from("articles")
    .find_nearest_with_options(
        FirestoreFindNearestOptions::new(
            path!(Article::embedding),
            FirestoreVector::new(vec![0.1, 0.8, 0.2]),
            FirestoreFindNearestDistanceMeasure::Euclidean,
            10,
        )
        .with_distance_result_field(path!(Article::distance))
        .with_distance_threshold(0.5),
    )
    .obj()
    .query()
    .await?;
# let _ = nearest;
# Ok(())
# }
```

Full example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/nearest-vector-query.rs).

A vector query is never answered from a [cache](./caching/modes.md#what-the-cache-answers): under
`read_through_cache` it goes to Firestore, and under `read_cached_only` it fails with
`CacheIncompleteCollection`.

## On the Enterprise edition

A Core vector query runs on an Enterprise edition database without a vector index. Query Explain is refused there, so its plan cannot show
what the query reads.

The pipeline [`find_nearest`](./pipeline-aggregations-and-joins.md#nearest-neighbours) stage takes
the same vector and measures, and has no distance threshold. It does not use a vector index: its
plan is a table scan, so its cost follows the size of the collection, see
[Query plans and cost](./editions/query-plans.md#known-table-scans).
