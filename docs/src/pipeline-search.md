# Pipeline search

A pipeline can search a collection for text or for places near a point. A search is part of the
source: `.search(..)` follows `.from(..)` directly, and every other stage, such as `filter`,
`fields` or `sort`, comes after it.

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct Hit { name: String, score: f64 }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let hits: Vec<Hit> = db
    .fluent()
    .pipeline()
    .select()
    .from("restaurants")
    .search(|s| {
        s.text(|t| t.for_all([t.term("waffles"), t.exclude(t.term("bacon"))]))
            .sort_by_score()
            .score_field("score")
            .limit(20)
    })
    .fields(|e| [e.field("name"), e.field("score")])
    .obj()
    .query()
    .await?;
# let _ = hits;
# Ok(())
# }
```

A search needs a search index that covers it, see [Search indexes](#search-indexes). Without one
the request fails with `FAILED_PRECONDITION`, and the library does not retry it. A search takes a
collection or a collection group as its source, and nothing else; a collection-group search needs
an index scoped to the collection group.

## Text search

`s.text(..)` searches every indexed text field of the collection. Its closure builds the query
from three kinds of clause, in the style of the filter builder `q`:

- `t.term("waffles")` is a word the document must contain; matching ignores case and accents and
  finds other forms of the word, so `waffle` finds "Waffles";
- `t.phrase("maple syrup")` is a list of words the document must contain in that order; a phrase
  is matched as written, without other forms of the words, so the phrase `"waffle"` does not
  find a document that contains "waffles", where the term `waffle` does;
- `t.exclude(..)` requires the term or phrase to be absent.

`t.for_all([..])` lists the clauses that must all hold. It takes plain clauses and `Option`s of
them, and drops a `None`, so an optional clause is written as an optional filter condition is. A
query with no clause that must match, such as one that only excludes, or a term without text,
matches nothing in Firestore, so the library rejects it with `InvalidParametersError` before the
request is sent.

The closure may also return a string, which is sent as written, for a query the builder does not
build.

### What the builder renders

The service reads the query text with a grammar of its own, and the builder renders each clause so
that the text means what the clause says. `FirestoreTextQuery` implements `Display`, which shows
the text that is sent:

```rust
use firestore::pipeline_search_builder::FirestoreTextQueryBuilder;

let t = FirestoreTextQueryBuilder;

let query = t
    .for_all([t.term("waffles"), t.exclude(t.phrase("maple syrup"))])
    .expect("the query has clauses");
assert_eq!(query.to_string(), r#"waffles -"maple syrup""#);

// A word the service would read as an operator, an exclusion or a field scope is quoted.
let query = t.for_all([t.term("menu:waffles"), t.term("-bacon")]).unwrap();
assert_eq!(query.to_string(), r#""menu:waffles" "-bacon""#);
```

A quoted word is matched as written, without other forms of the word. A quote character inside a
phrase is replaced by a space.

### What the grammar supports

The query grammar is Firestore's, and it is narrow. These are the limits, each measured against
the service:

- terms separated by spaces must all match; there is no OR, and `pancakes OR crepes` matches only
  the documents that contain both words;
- there is no field scoping: `menu:waffles` is read as the two terms `menu` and `waffles`, so the
  search always covers every indexed text field;
- parentheses do not group;
- there is no prefix or wildcard syntax: `waff*` and `w?ffles` match nothing.

`FirestoreTextQuery` leaves OR and field scoping out for that reason.

## Geo search

`s.geo_within(field, center, max_distance_meters)` keeps the documents whose geo point field lies
at most that distance, in metres, from the centre:

```rust,no_run
# use firestore::*;
# use serde::Deserialize;
# #[derive(Debug, Clone, Deserialize)]
# struct Name { name: String }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let center = FirestoreLatLng(FirestoreGeoPoint {
    latitude: 38.989177,
    longitude: -107.065076,
});

let nearby: Vec<Name> = db
    .fluent()
    .pipeline()
    .select()
    .from("restaurants")
    .search(|s| s.geo_within("location", center, 1000.0).sort_by_distance())
    .fields(|e| [e.field("name")])
    .obj()
    .query()
    .await?;
# let _ = nearby;
# Ok(())
# }
```

Firestore accepts only the "at most" comparison, so the search has no "farther than" form. A
single search is a text search or a geo search; combining the two in one search is not supported
by Firestore.

Full example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/pipeline-search.rs).

## Options

Both kinds of search take these options:

- `.limit(n)` keeps the first `n` results, and `.offset(n)` skips the first `n`; an offset needs
  a limit;
- `.retrieval_depth(n)` reads at most `n` index entries before the results are scored and sorted;
  it must be at least 1;
- `.language_code("en")` sets the language of the query as a BCP 47 language tag, the standard
  short code for a language, such as `en`.

A text search also has `.sort_by_score()` and `.score_field("score")`; a geo search has
`.sort_by_distance()`. Each ordering has a single direction, because Firestore accepts no other:
the most relevant result first for a score, and the nearest first for a distance.
`.score_field(..)` outputs the relevance score of each document under a field, where a later stage
can read it. The score and the distance are not general expressions: Firestore rejects them
outside the search.

A `.filter(..)` after the search narrows its matches.

## Search indexes

A search runs only against search indexes:

- a text search needs a *text index* on the fields it searches, and Firestore searches all of the
  indexed text fields of the collection, never one by name;
- a geo search needs a *geo index* on the geo point field;
- one index holds either text fields or geo fields, and mixing search fields with ordinary ones
  is rejected;
- a collection-group search needs indexes scoped to the collection group.

[Index management](./index-management.md#text-and-geo-search-indexes) declares them in Rust,
with `i.field(..).text()` and `i.field(..).geo()`, and creates them with `.sync()`.
