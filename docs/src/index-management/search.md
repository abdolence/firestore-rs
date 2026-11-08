# Search indexes (Enterprise edition)

The pipeline [search stage](../pipeline-search.md) runs against search indexes, which only a
database of the Firestore Enterprise edition has. A text index covers one or more string fields,
and a geo index covers a geo point field. Declare them as composite indexes with `.text()` and
`.geo()` on a field:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
    .indexes()
    .collection_group("restaurants")
    .composite(|i| {
        i.indexes([
            i.index([i.field("menu").text(), i.field("description").text()])
                .search_options(
                    FirestoreSearchIndexOptions::new().with_text_language("en".to_string()),
                ),
            i.index([i.field("location").geo()]),
        ])
    })
    .sync()
    .await?;
# Ok(())
# }
```

- A search index holds text and geo fields only. An index that mixes them with ordered, array or
  vector fields is rejected with `InvalidParametersError` at `.plan()`/`.sync()`;
- A text search covers all the text fields of the index it uses, and Firestore offers no index
  that matches one field by name;
- `.search_options(..)` sets the options of a text index. `text_language` is the BCP 47 language
  tag the fields are indexed in, such as `en`; when it is left unset, Firestore detects each
  document's language and lists the option as `und`, the code for an undetermined language.
  `text_language_override_field_path` names the field that holds a document's own language, and
  Firestore reads `language` when it is unset. An index declared without options matches the
  index Firestore lists with those defaults;
- `.geo_with(config)` takes a `FirestoreGeoIndexConfig` that can leave GeoJSON points out of a geo
  index, which Firestore indexes by default;
- `.all_descendants()` on the index scopes it to a collection group, which a collection-group
  search needs;
- a search index is always built as `FirestoreIndexDensity::SparseAny`, see
  [Density](./ttl-and-enterprise.md#density);
- a Standard edition database answers a search index with `INVALID_ARGUMENT`.
