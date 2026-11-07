# Pipeline aggregations and joins

The stages in this chapter build new documents from the ones before them, so their results have
no document name. A field of the result type for `_firestore_id` must be an `Option`, as
described in [Pipelines](./pipelines.md#typed-results-and-metadata).

## Aggregations

`.aggregate()` computes accumulators over all documents and outputs one document that holds them.
Each accumulator needs an alias, which names its field in the result:

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct Totals { total: i64, in_stock: i64, average_rating: Option<f64> }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let totals: Vec<Totals> = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .aggregate(|e| {
        [
            e.count_all().alias("total"),
            e.field("stock").greater_than(0).count_if().alias("in_stock"),
            e.field("rating").average().alias("average_rating"),
        ]
    })
    .obj()
    .query()
    .await?;
# let _ = totals;
# Ok(())
# }
```

The accumulators are `count_all` on `e`, which counts every document whether or not it has any
field, and these on an expression: `count`, `count_distinct`, `sum`, `average`, `minimum`,
`maximum`, `first`, `last`, `array_agg` and `array_agg_distinct`, plus `count_if` on a condition.
They are separate from the scalar functions of the same names: `sum`, `minimum` and `maximum`
inside `.aggregate()` accumulate over documents, and the array versions are `array_sum`,
`array_minimum` and `array_maximum`.

`.aggregate_grouped(groups, accumulators)` outputs one document per group, with the group's values
and its accumulators. A null value is a group of its own:

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct GenreStats { genre: Option<String>, books: i64, longest: i64 }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let stats: Vec<GenreStats> = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .aggregate_grouped(
        |e| [e.field(path!(GenreStats::genre))],
        |e| {
            [
                e.count_all().alias("books"),
                e.field("pages").maximum().alias("longest"),
            ]
        },
    )
    .sort(|e| [e.field(path!(GenreStats::books)).desc()])
    .obj()
    .query()
    .await?;
# let _ = stats;
# Ok(())
# }
```

Full example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/pipeline-query.rs).

## Distinct values

`.distinct()` outputs one document per distinct combination of the values it selects, and a null
is a value of its own:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let genres = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .distinct(|e| [e.field("genre")])
    .query()
    .await?;
# let _ = genres;
# Ok(())
# }
```

## Sampling

`.sample()` keeps a random sample of the documents: a number of documents, or a fraction from 0.0
to 1.0 of them. Firestore names the second mode `percent`, but it takes a fraction.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let some_books = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .sample(FirestoreSampleSize::Documents(3))
    .query()
    .await?;

let tenth = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .sample(FirestoreSampleSize::Fraction(0.1))
    .query()
    .await?;
# let _ = (some_books, tenth);
# Ok(())
# }
```

## Union

`.union()` appends the documents of another pipeline. The closure receives the same source
picker as a subquery does, so it starts with `p.from(..)`:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let names = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .fields(|e| [e.field("title").alias("name")])
    .union(|p| p.from("restaurants").fields(|e| [e.field("name")]))
    .query()
    .await?;
# let _ = names;
# Ok(())
# }
```

## Unnest

`.unnest()` outputs one document per element of an array field, with the element under the alias
of the selection. `.unnest_with_index_field()` also outputs each element's position in the array.
A selection without an alias replaces the array field with its elements:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let tags = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .unnest_with_index_field(|e| e.field("tags").alias("tag"), "tag_index")
    .fields(|e| [e.field("title"), e.field("tag"), e.field("tag_index")])
    .query()
    .await?;
# let _ = tags;
# Ok(())
# }
```

## Replacing documents

`.replace_with(expr, mode)` replaces each document with the map `expr` evaluates to.
`FirestoreReplaceWithMode::FullReplace` makes the map the document; `MergeOverwriteExisting` adds
the map's fields and replaces the fields of the same name; `MergeKeepExisting` adds them and keeps
the fields of the same name.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let awards = db
    .fluent()
    .pipeline()
    .select()
    .documents(["books/book-10"])
    .replace_with(|e| e.field("awards"), FirestoreReplaceWithMode::FullReplace)
    .query()
    .await?;
# let _ = awards;
# Ok(())
# }
```

## Nearest neighbours

`.find_nearest(field, vector, measure, limit)` keeps the `limit` documents whose vector field is
nearest to the query vector. It takes Core's `FirestoreVector` and
`FirestoreFindNearestDistanceMeasure`, and has no counterpart to Core's `distance_threshold`.
`.find_nearest_with_options()` takes a `FirestoreFindNearestStage`, which can also name a field to
output each document's distance under.

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct Nearest { title: String, distance: f64 }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let nearest: Vec<Nearest> = db
    .fluent()
    .pipeline()
    .select()
    .from("books")
    .find_nearest_with_options(
        FirestoreFindNearestStage::new(
            "embedding".to_string(),
            FirestoreVector::new(vec![0.9, 0.1, 0.2]),
            FirestoreFindNearestDistanceMeasure::Euclidean,
            3,
        )
        .with_distance_result_field("distance".to_string()),
    )
    .fields(|e| [e.field("title"), e.field("distance")])
    .obj()
    .query()
    .await?;
# let _ = nearest;
# Ok(())
# }
```

The stage does not use a vector index, so its cost follows the size of the collection, see
[Known table scans](./editions/query-plans.md#known-table-scans). A Core vector query,
`select().find_nearest(..)`, has a distance threshold and uses a vector index on a Standard edition
database, see [Vector search](./vector-search.md).

## Joins

Joins are in Preview: Firestore announced them without a launch stage, so treat their behaviour
as subject to change.

A join adds to each document the result of another pipeline, a *subquery*. A subquery is
*correlated* when it reads a value of the document it runs for. Pipelines pass that value through
a variable: `.define_variables()` sets variables for the stages after it, and `e.variable("name")`
or `q.variable("name")` reads one, inside a subquery as well.

`e.array_subquery(..)` returns the results of its pipeline as an array, which is empty when the
pipeline finds nothing:

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct Review { rating: i64, reviewer: String }
# #[derive(Debug, Clone, Deserialize)]
# struct RestaurantWithReviews { name: String, reviews: Vec<Review> }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let restaurants: Vec<RestaurantWithReviews> = db
    .fluent()
    .pipeline()
    .select()
    .from("restaurants")
    .filter(|q| q.field("cuisine").eq("breakfast"))
    .define_variables(|e| [e.field("__name__").alias("restaurant")])
    .fields(|e| {
        [
            FirestoreSelection::from(e.field("name")),
            e.array_subquery(|p| {
                p.from("reviews")
                    .all_descendants()
                    .filter(|q| q.field("restaurant").eq(q.variable("restaurant")))
                    .sort(|e| [e.field("rating").desc()])
                    .fields(|e| [e.field("rating"), e.field("reviewer")])
            })
            .alias("reviews"),
        ]
    })
    .obj()
    .query()
    .await?;
# let _ = restaurants;
# Ok(())
# }
```

The subquery closure receives the source picker directly, `p.from("reviews")`, since a nested
pipeline has no verb. The example above matches reviews to restaurants through a `restaurant`
reference field on each review.

`e.scalar_subquery(..)` returns the one result of its pipeline: null when the pipeline finds
nothing, and an `INVALID_ARGUMENT` failure of the whole request, "Subpipeline returned multiple
results", when it finds several. Use it with an aggregation, which always yields one document:

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct RestaurantWithAverage { name: String, average_rating: Option<f64> }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let restaurants: Vec<RestaurantWithAverage> = db
    .fluent()
    .pipeline()
    .select()
    .from("restaurants")
    .define_variables(|e| [e.field("__name__").alias("restaurant")])
    .fields(|e| {
        [
            FirestoreSelection::from(e.field("name")),
            e.scalar_subquery(|p| {
                p.from("reviews")
                    .all_descendants()
                    .filter(|q| q.field("restaurant").eq(q.variable("restaurant")))
                    .aggregate(|e| [e.field("rating").average().alias("average")])
            })
            .alias("average_rating"),
        ]
    })
    .obj()
    .query()
    .await?;
# let _ = restaurants;
# Ok(())
# }
```

A subquery whose last stage outputs a single field returns the bare values of that field, and one
that outputs two or more fields returns maps. The `average_rating` above is a number rather than
a map with an `average` field, and a subquery that selects only `rating` returns an array of
numbers:

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct RestaurantWithRatings { name: String, ratings: Vec<i64> }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let restaurants: Vec<RestaurantWithRatings> = db
    .fluent()
    .pipeline()
    .select()
    .from("restaurants")
    .fields(|e| {
        [
            FirestoreSelection::from(e.field("name")),
            e.array_subquery(|p| p.subcollection("reviews").fields(|e| [e.field("rating")]))
                .alias("ratings"),
        ]
    })
    .obj()
    .query()
    .await?;
# let _ = restaurants;
# Ok(())
# }
```

`p.subcollection("reviews")` reads the collection under the current document of the enclosing
pipeline, so it needs no variable. Only a subquery or a `union` branch can start with it: a
top-level pipeline that does does not compile.

Full example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/pipeline-join.rs).
