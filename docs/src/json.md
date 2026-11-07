# JSON conversion

With the `json` cargo feature, `db.json()` converts Firestore values and documents to JSON and back.
It is meant for tools that move documents out of Firestore and into it again, such as an export and
an import, and for tools that show documents to a person. It does not send anything.
`FirestoreJsonConverter::new(documents_path)` builds the same converter from a documents path,
without a client.

```toml
[dependencies]
firestore = { version = "...", features = ["json"] }
serde_json = "1"
```

The converter writes two encodings:

- **tagged**, for export and import. It reads back to the same Firestore value, without loss.
  A type that JSON lacks becomes a one-key object, a *wrapper*, whose key starts with `$`;
- **plain**, for display. It is the most readable JSON for each type, and it has no way back: a
  timestamp, a reference and a string look alike.

| Method | Result |
|---|---|
| `tagged(&value)` | `FirestoreResult<FirestoreTaggedJson>` |
| `plain(&value)` | `FirestorePlainJson` |
| `tagged_document(&document, &options)` | `FirestoreResult<FirestoreTaggedJson>` |
| `plain_document(&document, &options)` | `FirestorePlainJson` |
| `value_from_tagged(json)` | `FirestoreResult<FirestoreValue>` |
| `document_from_tagged(json)` | `FirestoreResult<FirestoreDocument>` |

A value is a `gcloud_sdk::google::firestore::v1::Value`, the type that `FirestoreDocument::fields`
holds, so a field converts without a copy. `FirestoreValue::value` is one.

`FirestoreTaggedJson` and `FirestorePlainJson` wrap a `serde_json::Value`, which `into_inner()`
returns, and implement `Serialize` and `Deserialize`. Write a line of an export with
`serde_json::to_string(&tagged)` and read one with `serde_json::from_str::<FirestoreTaggedJson>(..)`.

> Do not turn either type into a `FirestoreValue` with `.into()`. The conversion serialises the JSON
> as data, so `{"$timestamp": "2023-11-14T22:13:20Z"}` is stored as a map holding a string. A tagged
> value goes back to a value through `db.json().value_from_tagged(..)`.

## Export and import

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let options = FirestoreJsonDocumentOptions::new();

let Some(document) = db.fluent().select().by_id_in("books").one("dune").await? else {
    return Ok(());
};
let line = serde_json::to_string(&db.json().tagged_document(&document, &options)?)?;

let tagged = serde_json::from_str::<FirestoreTaggedJson>(&line)?;
let document = db.json().document_from_tagged(tagged)?;
db.fluent()
    .update()
    .in_col("books")
    .document(document)
    .execute()
    .await?;
# Ok(())
# }
```

An imported document carries its name, so write it with `update()`, or with a batch writer.
`insert().document(..)` refuses a document whose name is set.

## Encodings of each type

| Firestore value | Tagged | Plain |
|---|---|---|
| null | `null` | `null` |
| boolean | `true` | `true` |
| integer, at most 2^53 − 1 in size | `42` | `42` |
| integer, 2^53 and beyond | `{"$int": "9007199254740992"}` | `9007199254740992` |
| double | `1.5`, `3.0`, `-0.0` | `1.5`, `3.0`, `-0.0` |
| NaN, infinities | `{"$double": "NaN"}`, `{"$double": "Infinity"}`, `{"$double": "-Infinity"}` | `"NaN"`, `"Infinity"`, `"-Infinity"` |
| string | `"Dune"` | `"Dune"` |
| bytes | `{"$bytes": "AAEC/w=="}` | `"AAEC/w=="` |
| timestamp | `{"$timestamp": "2023-11-14T22:13:20.123456Z"}` | `"2023-11-14T22:13:20.123456Z"` |
| reference | `{"$ref": "books/dune"}` | `"books/dune"` |
| geo point | `{"$geo": [51.5, -0.12]}` | `{"latitude": 51.5, "longitude": -0.12}` |
| array | `[1, "two", null]` | `[1, "two", null]` |
| map | `{"city": "London"}` | `{"city": "London"}` |
| map with the single key `$timestamp` | `{"$map": {"$timestamp": "x"}}` | `{"$timestamp": "x"}` |
| vector | `{"$vector": [1.0, 2.5]}` | `[1.0, 2.5]` |

- Bytes are base64 with the standard alphabet and padding;
- a map is escaped as `{"$map": {..}}` only in the tagged encoding, and only when its single key
  starts with `$`, so that it is not read as a wrapper. A map with no key or several is never
  escaped;
- map keys are written in byte order. Whether a JSON reader keeps that order depends on the
  `preserve_order` feature of `serde_json`; the format does not depend on it;
- a `Value` with no type set is written as `null` in both encodings.

## Documents

A document is one flat object holding `__name__` and the fields.

```json
{"__name__": "books/dune", "published": {"$timestamp": "2023-11-14T22:13:20Z"}, "title": "Dune"}
```

The keys follow the order of `serde_json`'s map. By default it is sorted, so a field named `Title`
is written before `__name__`. With the `preserve_order` feature of `serde_json`, which the `json`
feature does not enable, the order is `__name__`, the metadata, then the fields in byte order.

`FirestoreJsonDocumentOptions::new().with_metadata(true)` adds `__create_time__` and
`__update_time__` when the document carries them: wrapped as `$timestamp` in the tagged encoding,
as strings in the plain one. `document_from_tagged` ignores both, so the document it returns has
neither.

`__name__` is left out of a document without a name, such as an aggregation or a pipeline `select`
result. A top-level field whose name starts with `$` needs no escape, since the top level of a
document is always read as fields.

Firestore refuses `__name__`, `__create_time__`, `__update_time__` and every other name between
double underscores as a stored field, so these keys cannot collide with a stored document's fields.
The converter does not check for them. A pipeline result with a field of such a name overwrites
the key of the name in the object.

## References and portability

A reference and `__name__` are written relative to the database of the converter when they point
into it, and as a full name otherwise:
`projects/latestbit/databases/other/documents/books/dune`. A relative name resolves against the
database of the converter that reads it, so an export from one database imports into another, and
a full name keeps pointing where it pointed.

A name is read as a full name when it has the form
`projects/{project}/databases/{database}/documents/{path}` with an odd number of segments. A
relative document path has an even number of segments, so a document in a collection called
`projects` is not mistaken for another project's.

## Integers and doubles

Firestore integers are 64-bit. A JSON number that a JavaScript reader parses as an IEEE double
holds integers exactly only up to 2^53 − 1, so the tagged encoding writes anything beyond as
`$int` with the decimal text. 2^53 itself goes to `$int`, since a JavaScript reader cannot tell it
from 2^53 + 1. The plain encoding writes every integer as a number.

A double is written with a `.` or an exponent, `3.0` and `1e+300`, so it reads back as a double and
not as an integer. A coordinate of a geo point follows the same rule.

### Exact doubles and `float_roundtrip`

`serde_json` parses a float one ULP (unit in the last place, the gap between a double and its
neighbour) off for about one in six values with many digits, such as a vector component widened
from `f32`. The text then reads back as the neighbouring double. A float round trip, a double
written to text and parsed again to the same double, needs the `float_roundtrip` feature of
`serde_json`.

The `json` feature turns it on. Cargo builds one `serde_json` per program with the features of every
crate that uses it, so a program that enables `json` parses every float exactly, at the cost of a
slower float parse.

## Timestamps

Timestamps are written as RFC 3339 in UTC with a `Z`, with 0, 3, 6 or 9 fraction digits:
`2023-11-14T22:13:20Z`, `2023-11-14T22:13:20.500Z`, `2023-11-14T22:13:20.123456Z`. The range is
the whole range Firestore accepts, from `0001-01-01T00:00:00Z` to `9999-12-31T23:59:59.999999Z`.
`FirestoreInstant` ends 26 hours earlier, so the converter does not go through it.

Reading accepts an offset (`+01:00`), a lowercase `t` and `z`, and a space for `T`, and keeps
nanoseconds. Firestore stores microseconds and cuts the digits beyond; the converter leaves that to
Firestore. `FirestoreTimestamp::truncated_to_firestore_precision` cuts them before a write.

## Values that exist only in pipelines

Field references, variable references, functions and pipelines are parts of a pipeline request, and
a stored document never holds one.

- The tagged encoding refuses them with `JSON_EXPRESSION_VALUE`, since they cannot be read back;
- the plain encoding shows what the caller holds: the text of a field or variable reference, the
  name of a function, and `"pipeline"`.

## Reading the tagged encoding

| JSON | Firestore value |
|---|---|
| `-0` | the double -0.0 |
| a number without a `.` or an exponent that fits `i64` | integer |
| an integer from 2^63 to 2^64 − 1 | refused, `JSON_NUMBER_OUT_OF_RANGE` |
| any other number up to the range of a double | double |
| an object with one key that starts with `$` | the wrapper |
| any other object | map |

A wrapper takes exactly one form of content:

| Wrapper | Content |
|---|---|
| `$int` | a string that parses as `i64` |
| `$double` | `"NaN"`, `"Infinity"` or `"-Infinity"` |
| `$timestamp` | an RFC 3339 string |
| `$bytes` | a base64 string, standard alphabet |
| `$ref` | a string |
| `$geo` | an array of two numbers, each a JSON number or a `$double` wrapper |
| `$vector` | an array of numbers |
| `$map` | an object, whose keys are taken as they are |

A component of `$geo` and `$vector` may be an integer literal, so `{"$vector": [1, 2]}` reads as
two doubles.

Only the form of a wrapper is checked. An empty `$vector`, a latitude of 91 and a timestamp in the
year 10000 read without an error, and Firestore refuses them on write. A timestamp in the year
10000 reads only in the signed form the writer produces, `+10000-01-01T00:00:00Z`; without the
sign it is a `JSON_INVALID_WRAPPER` error.

A number above `u64` in hand-written input reads as a double.

## Errors

A failed conversion is a `DeserializeError` when it reads JSON and a `SerializeError` when it
writes it. The code is in `public.code`, a constant of `firestore::errors::codes`, and the message names the JSON path of the value, such as
`` `reviews[2].at` ``. A document error carries the full document name in `document_path`.

| Case | Error |
|---|---|
| an object with one key that starts with `$` and is none of the eight wrappers | `DeserializeError`, code `JSON_UNKNOWN_WRAPPER` |
| a wrapper with content that is not its form, such as `{"$timestamp": 17}` | `DeserializeError`, code `JSON_INVALID_WRAPPER` |
| a JSON integer above `i64::MAX` | `DeserializeError`, code `JSON_NUMBER_OUT_OF_RANGE` |
| `document_from_tagged` of JSON that is not an object or has no string `__name__` | `DeserializeError`, code `JSON_INVALID_DOCUMENT` |
| `tagged` or `tagged_document` of a field reference, variable reference, function or pipeline | `SerializeError`, code `JSON_EXPRESSION_VALUE` |
