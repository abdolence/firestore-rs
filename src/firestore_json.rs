//! Conversion of Firestore values and documents to and from JSON, in two encodings: the tagged
//! one, which reads back without loss, and the plain one, which is only for display.

use crate::db::relative_to_documents;
use crate::errors::codes::{
    JSON_EXPRESSION_VALUE, JSON_INVALID_DOCUMENT, JSON_INVALID_WRAPPER, JSON_NUMBER_OUT_OF_RANGE,
    JSON_UNKNOWN_WRAPPER,
};
use crate::errors::{FirestoreError, FirestoreSerializationError};
use crate::firestore_serde::FirestoreParsedReference;
use crate::{
    is_vector, FirestoreDb, FirestoreDocument, FirestoreResult, FirestoreValue, FirestoreVector,
};
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use gcloud_sdk::google::firestore::v1::value::ValueType;
use gcloud_sdk::google::firestore::v1::{ArrayValue, MapValue, Value};
use gcloud_sdk::google::r#type::LatLng;
use gcloud_sdk::prost_types::Timestamp;
use rsb_derive::Builder;
use serde_json::{json, Map, Number, Value as Json};
use std::collections::HashMap;

/// The largest integer a JavaScript number, an IEEE double, holds with its neighbours distinct.
/// A reader in JavaScript cannot tell 2^53 from 2^53 + 1, so both go to `$int`.
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

/// How a value or document is written as JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirestoreJsonEncoding {
    /// Reads back without loss. Plain JSON types stay plain. A type JSON lacks becomes a one-key
    /// object, a wrapper such as `{"$timestamp": "2023-11-14T22:13:20Z"}`. It is the only
    /// encoding the deserialising functions are specified for.
    Tagged,
    /// The most readable form, for display. A timestamp, a reference and a string look alike, so
    /// reading it back with the deserialising functions parses, but a timestamp and a reference
    /// come back as strings.
    Plain,
}

/// What a document conversion writes beside the document's name and fields.
#[derive(Debug, Clone, PartialEq, Eq, Builder)]
pub struct FirestoreJsonOptions {
    /// The encoding of the fields.
    #[default = "FirestoreJsonEncoding::Tagged"]
    pub encoding: FirestoreJsonEncoding,
    /// Adds `__create_time__` and `__update_time__` when the document carries them.
    #[default = "false"]
    pub metadata: bool,
}

/// Converts Firestore values and documents to and from JSON, relative to one database.
///
/// References and document names inside the database are written relative to it, and a relative
/// name read back resolves against it. `documents_path` is the database's documents root.
#[derive(Clone, Debug)]
struct FirestoreJsonConverter<'a> {
    documents_path: &'a str,
}

/// Conversion of values and documents to and from JSON, behind the `json-conversion` feature.
///
/// Like [`serialize_to_doc`](FirestoreDb::serialize_to_doc), these are associated functions that
/// take their context explicitly, so they need no client. `documents_path` is the documents root of
/// the database, `projects/{project}/databases/{database}/documents`, as
/// [`get_documents_path`](FirestoreDb::get_documents_path) returns it. References and document
/// names inside that database are written relative to it, and a relative name read back resolves
/// against it.
impl FirestoreDb {
    /// Writes `value` in `encoding`.
    ///
    /// # Errors
    /// The tagged encoding returns a [`FirestoreError::SerializeError`] with code
    /// `JSON_EXPRESSION_VALUE` for a field reference, variable reference, function or pipeline
    /// value, which a stored document never holds and which cannot be read back. The plain
    /// encoding never fails.
    pub fn serialize_value_to_json(
        documents_path: &str,
        value: &Value,
        encoding: FirestoreJsonEncoding,
    ) -> FirestoreResult<Json> {
        FirestoreJsonConverter::new(documents_path).value_to_json(value, encoding)
    }

    /// Writes `document` as one flat object holding `__name__`, the metadata when `options` ask
    /// for it, and the fields in the encoding of `options`. The keys come out in the order of
    /// serde_json's map: sorted by default, and `__name__`, the metadata, then the fields in
    /// byte order with serde_json's `preserve_order` feature, which `json-conversion` does not
    /// enable.
    ///
    /// # Errors
    /// Fails like [`serialize_value_to_json`](FirestoreDb::serialize_value_to_json).
    pub fn serialize_doc_to_json(
        documents_path: &str,
        document: &FirestoreDocument,
        options: &FirestoreJsonOptions,
    ) -> FirestoreResult<Json> {
        FirestoreJsonConverter::new(documents_path).document_to_json(document, options)
    }

    /// Reads a value written in the tagged encoding.
    ///
    /// The plain encoding parses too, but a timestamp and a reference come back as strings,
    /// since the plain encoding does not tell them from a string.
    ///
    /// # Errors
    /// Returns a [`FirestoreError::DeserializeError`] with code `JSON_UNKNOWN_WRAPPER`,
    /// `JSON_INVALID_WRAPPER` or `JSON_NUMBER_OUT_OF_RANGE`. Only what a wrapper's own form
    /// requires is checked: Firestore refuses an empty vector or a latitude of 91 itself.
    pub fn deserialize_value_from_json(
        documents_path: &str,
        json: Json,
    ) -> FirestoreResult<FirestoreValue> {
        FirestoreJsonConverter::new(documents_path).value_from_json(json)
    }

    /// Reads a document written by [`serialize_doc_to_json`](FirestoreDb::serialize_doc_to_json)
    /// in the tagged encoding.
    ///
    /// `__create_time__` and `__update_time__` are ignored, so the document has neither. It
    /// carries a name, so write it with `update()`: `insert()` refuses a named document. The plain
    /// encoding parses too, but a timestamp and a reference come back as strings.
    ///
    /// # Errors
    /// Returns a [`FirestoreError::DeserializeError`] with code `JSON_INVALID_DOCUMENT` unless
    /// the input is an object with a string `__name__`, and fails like
    /// [`deserialize_value_from_json`](FirestoreDb::deserialize_value_from_json) for a field.
    pub fn deserialize_doc_from_json(
        documents_path: &str,
        json: Json,
    ) -> FirestoreResult<FirestoreDocument> {
        FirestoreJsonConverter::new(documents_path).document_from_json(json)
    }

    /// The name of a document or reference relative to the database whose documents root is
    /// `documents_path`, or the full name when it belongs to another database. It is the form
    /// the JSON functions write names in.
    pub fn relative_document_name<'n>(documents_path: &str, name: &'n str) -> &'n str {
        FirestoreJsonConverter::new(documents_path).relative_name(name)
    }
}

/// A conversion that failed, with the JSON path of the value it failed at, built while the error
/// unwinds so that the success path pays nothing for it.
struct JsonConversionError {
    code: &'static str,
    path: String,
    message: String,
}

impl JsonConversionError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        JsonConversionError {
            code,
            path: String::new(),
            message: message.into(),
        }
    }

    fn invalid_wrapper(key: &str, expected: &str, content: &Json) -> Self {
        Self::new(
            JSON_INVALID_WRAPPER,
            format!("{key} expects {expected}, got {content}"),
        )
    }

    fn expression_refused(kind: &str) -> Self {
        Self::new(
            JSON_EXPRESSION_VALUE,
            format!("{kind} is part of a request, not of stored data, and has no tagged form"),
        )
    }

    fn under(mut self, segment: &str) -> Self {
        let separator = if self.path.is_empty() || self.path.starts_with('[') {
            ""
        } else {
            "."
        };
        self.path = format!("{segment}{separator}{}", self.path);
        self
    }

    fn under_index(self, index: usize) -> Self {
        self.under(&format!("[{index}]"))
    }

    fn describe(&self) -> String {
        if self.path.is_empty() {
            self.message.clone()
        } else {
            format!("`{}`: {}", self.path, self.message)
        }
    }

    fn into_error(self, document_path: Option<&str>) -> FirestoreSerializationError {
        FirestoreSerializationError::json(self.code, self.describe(), document_path)
    }

    fn into_decode_error(self, document_path: Option<&str>) -> FirestoreError {
        FirestoreError::DeserializeError(self.into_error(document_path))
    }

    fn into_encode_error(self, document_path: Option<&str>) -> FirestoreError {
        FirestoreError::SerializeError(self.into_error(document_path))
    }
}

impl<'a> FirestoreJsonConverter<'a> {
    fn new(documents_path: &'a str) -> Self {
        Self { documents_path }
    }

    fn value_to_json(
        &self,
        value: &Value,
        encoding: FirestoreJsonEncoding,
    ) -> FirestoreResult<Json> {
        match encoding {
            FirestoreJsonEncoding::Tagged => self
                .encode_tagged(value)
                .map_err(|error| error.into_encode_error(None)),
            FirestoreJsonEncoding::Plain => Ok(self.encode_plain(value)),
        }
    }

    fn document_to_json(
        &self,
        document: &FirestoreDocument,
        options: &FirestoreJsonOptions,
    ) -> FirestoreResult<Json> {
        let object = match options.encoding {
            FirestoreJsonEncoding::Tagged => {
                let mut object = self.document_header(
                    document,
                    options,
                    |time| json!({"$timestamp": time.to_string()}),
                );
                let fields = self
                    .encode_tagged_fields(&document.fields)
                    .map_err(|error| {
                        let document_path =
                            Some(document.name.as_str()).filter(|name| !name.is_empty());
                        error.into_encode_error(document_path)
                    })?;
                object.extend(fields);
                object
            }
            FirestoreJsonEncoding::Plain => {
                let mut object =
                    self.document_header(document, options, |time| Json::String(time.to_string()));
                object.extend(self.encode_plain_fields(&document.fields));
                object
            }
        };
        Ok(Json::Object(object))
    }

    fn value_from_json(&self, json: Json) -> FirestoreResult<FirestoreValue> {
        self.decode(json)
            .map(FirestoreValue::from)
            .map_err(|error| error.into_decode_error(None))
    }

    fn document_from_json(&self, json: Json) -> FirestoreResult<FirestoreDocument> {
        let Json::Object(mut object) = json else {
            return Err(JsonConversionError::new(
                JSON_INVALID_DOCUMENT,
                "a document is a JSON object",
            )
            .into_decode_error(None));
        };
        let name = match object.remove("__name__") {
            Some(Json::String(name)) => self.resolve_name(&name),
            _ => {
                return Err(JsonConversionError::new(
                    JSON_INVALID_DOCUMENT,
                    "a document has a string `__name__`",
                )
                .into_decode_error(None))
            }
        };
        object.remove("__create_time__");
        object.remove("__update_time__");
        let fields = self
            .decode_entries(object)
            .map_err(|error| error.into_decode_error(Some(&name)))?;
        Ok(FirestoreDocument {
            name,
            fields,
            create_time: None,
            update_time: None,
        })
    }

    /// Fields in byte order of their keys, which gcloud-sdk's `HashMap` does not keep, so that
    /// the output does not depend on the hasher.
    fn sorted(fields: &HashMap<String, Value>) -> Vec<(&String, &Value)> {
        let mut entries: Vec<_> = fields.iter().collect();
        entries.sort_unstable_by_key(|(key, _)| *key);
        entries
    }

    fn relative_name<'n>(&self, name: &'n str) -> &'n str {
        relative_to_documents(self.documents_path, name).unwrap_or(name)
    }

    /// The full name of a reference written by `relative_name`.
    ///
    /// A full name is `projects/{project}/databases/{database}/documents/{path}` with an odd
    /// number of segments. A relative name has an even number, so a document in a collection
    /// called `projects` is not mistaken for another project's.
    fn resolve_name(&self, text: &str) -> String {
        match FirestoreParsedReference::documents_path(text) {
            Some(path) if path.split('/').count() % 2 == 0 => text.to_string(),
            _ => format!("{}/{text}", self.documents_path),
        }
    }

    fn document_header(
        &self,
        document: &FirestoreDocument,
        options: &FirestoreJsonOptions,
        time: impl Fn(&Timestamp) -> Json,
    ) -> Map<String, Json> {
        let mut object = Map::new();
        if !document.name.is_empty() {
            object.insert(
                "__name__".to_string(),
                Json::String(self.relative_name(&document.name).to_string()),
            );
        }
        if options.metadata {
            if let Some(create_time) = &document.create_time {
                object.insert("__create_time__".to_string(), time(create_time));
            }
            if let Some(update_time) = &document.update_time {
                object.insert("__update_time__".to_string(), time(update_time));
            }
        }
        object
    }

    /// The components of a stored vector: a map of exactly the marker and an array of finite
    /// doubles. Any other map, including one Firestore refuses, is written as a map, which reads
    /// back to the same value.
    fn vector_components(map: &MapValue) -> Option<Vec<Json>> {
        if !is_vector(map) || map.fields.len() != 2 {
            return None;
        }
        let Some(ValueType::ArrayValue(components)) = map.fields.get("value")?.value_type.as_ref()
        else {
            return None;
        };
        components
            .values
            .iter()
            .map(|component| match component.value_type {
                Some(ValueType::DoubleValue(double)) => Number::from_f64(double).map(Json::Number),
                _ => None,
            })
            .collect()
    }

    /// The spelling of a double JSON has no number for, the one protobuf's JSON mapping uses.
    fn non_finite_text(double: f64) -> &'static str {
        if double.is_nan() {
            "NaN"
        } else if double.is_sign_negative() {
            "-Infinity"
        } else {
            "Infinity"
        }
    }

    fn tagged_double(double: f64) -> Json {
        Number::from_f64(double).map_or_else(
            || json!({"$double": Self::non_finite_text(double)}),
            Json::Number,
        )
    }

    fn plain_double(double: f64) -> Json {
        Number::from_f64(double).map_or_else(
            || Json::String(Self::non_finite_text(double).to_string()),
            Json::Number,
        )
    }

    fn encode_tagged(&self, value: &Value) -> Result<Json, JsonConversionError> {
        let Some(value_type) = &value.value_type else {
            return Ok(Json::Null);
        };
        Ok(match value_type {
            ValueType::NullValue(_) => Json::Null,
            ValueType::BooleanValue(boolean) => Json::Bool(*boolean),
            ValueType::IntegerValue(integer) if integer.unsigned_abs() <= MAX_SAFE_INTEGER => {
                Json::from(*integer)
            }
            ValueType::IntegerValue(integer) => json!({"$int": integer.to_string()}),
            ValueType::DoubleValue(double) => Self::tagged_double(*double),
            ValueType::StringValue(string) => Json::String(string.clone()),
            ValueType::BytesValue(bytes) => json!({"$bytes": STANDARD.encode(bytes)}),
            ValueType::TimestampValue(timestamp) => json!({"$timestamp": timestamp.to_string()}),
            ValueType::ReferenceValue(name) => json!({"$ref": self.relative_name(name)}),
            ValueType::GeoPointValue(point) => json!({"$geo": [
                Self::tagged_double(point.latitude),
                Self::tagged_double(point.longitude),
            ]}),
            ValueType::ArrayValue(array) => Json::Array(
                array
                    .values
                    .iter()
                    .enumerate()
                    .map(|(index, element)| {
                        self.encode_tagged(element)
                            .map_err(|failure| failure.under_index(index))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            ValueType::MapValue(map) => match Self::vector_components(map) {
                Some(components) => json!({"$vector": components}),
                None => {
                    let object = self.encode_tagged_fields(&map.fields)?;
                    let looks_like_wrapper =
                        object.len() == 1 && object.keys().all(|key| key.starts_with('$'));
                    if looks_like_wrapper {
                        json!({"$map": object})
                    } else {
                        Json::Object(object)
                    }
                }
            },
            ValueType::FieldReferenceValue(_) => {
                return Err(JsonConversionError::expression_refused("a field reference"))
            }
            ValueType::VariableReferenceValue(_) => {
                return Err(JsonConversionError::expression_refused(
                    "a variable reference",
                ))
            }
            ValueType::FunctionValue(_) => {
                return Err(JsonConversionError::expression_refused("a function"))
            }
            ValueType::PipelineValue(_) => {
                return Err(JsonConversionError::expression_refused("a pipeline"))
            }
        })
    }

    fn encode_tagged_fields(
        &self,
        fields: &HashMap<String, Value>,
    ) -> Result<Map<String, Json>, JsonConversionError> {
        let mut object = Map::new();
        for (key, value) in Self::sorted(fields) {
            let encoded = self
                .encode_tagged(value)
                .map_err(|failure| failure.under(key))?;
            object.insert(key.clone(), encoded);
        }
        Ok(object)
    }

    fn encode_plain(&self, value: &Value) -> Json {
        let Some(value_type) = &value.value_type else {
            return Json::Null;
        };
        match value_type {
            ValueType::NullValue(_) => Json::Null,
            ValueType::BooleanValue(boolean) => Json::Bool(*boolean),
            ValueType::IntegerValue(integer) => Json::from(*integer),
            ValueType::DoubleValue(double) => Self::plain_double(*double),
            ValueType::StringValue(string) => Json::String(string.clone()),
            ValueType::BytesValue(bytes) => Json::String(STANDARD.encode(bytes)),
            ValueType::TimestampValue(timestamp) => Json::String(timestamp.to_string()),
            ValueType::ReferenceValue(name) => Json::String(self.relative_name(name).to_string()),
            ValueType::GeoPointValue(point) => {
                let mut object = Map::new();
                object.insert("latitude".to_string(), Self::plain_double(point.latitude));
                object.insert("longitude".to_string(), Self::plain_double(point.longitude));
                Json::Object(object)
            }
            ValueType::ArrayValue(array) => Json::Array(
                array
                    .values
                    .iter()
                    .map(|element| self.encode_plain(element))
                    .collect(),
            ),
            ValueType::MapValue(map) => match Self::vector_components(map) {
                Some(components) => Json::Array(components),
                None => Json::Object(self.encode_plain_fields(&map.fields)),
            },
            ValueType::FieldReferenceValue(reference)
            | ValueType::VariableReferenceValue(reference) => Json::String(reference.clone()),
            ValueType::FunctionValue(function) => Json::String(function.name.clone()),
            ValueType::PipelineValue(_) => Json::String("pipeline".to_string()),
        }
    }

    fn encode_plain_fields(&self, fields: &HashMap<String, Value>) -> Map<String, Json> {
        Self::sorted(fields)
            .into_iter()
            .map(|(key, value)| (key.clone(), self.encode_plain(value)))
            .collect()
    }

    fn decode(&self, json: Json) -> Result<Value, JsonConversionError> {
        let value_type = match json {
            Json::Null => ValueType::NullValue(0),
            Json::Bool(boolean) => ValueType::BooleanValue(boolean),
            Json::Number(number) => Self::decode_number(&number)?,
            Json::String(string) => ValueType::StringValue(string),
            Json::Array(elements) => ValueType::ArrayValue(ArrayValue {
                values: elements
                    .into_iter()
                    .enumerate()
                    .map(|(index, element)| {
                        self.decode(element)
                            .map_err(|failure| failure.under_index(index))
                    })
                    .collect::<Result<_, _>>()?,
            }),
            Json::Object(object) => {
                let mut entries = object.into_iter();
                match (entries.next(), entries.next()) {
                    (Some((key, content)), None) if key.starts_with('$') => {
                        self.decode_wrapper(&key, content)?
                    }
                    (first, second) => ValueType::MapValue(MapValue {
                        fields: self
                            .decode_entries(first.into_iter().chain(second).chain(entries))?,
                    }),
                }
            }
        };
        Ok(Value {
            value_type: Some(value_type),
        })
    }

    fn decode_number(number: &Number) -> Result<ValueType, JsonConversionError> {
        if let Some(integer) = number.as_i64() {
            Ok(ValueType::IntegerValue(integer))
        } else if number.is_u64() {
            Err(JsonConversionError::new(
                JSON_NUMBER_OUT_OF_RANGE,
                format!("{number} does not fit in an int64"),
            ))
        } else {
            number.as_f64().map(ValueType::DoubleValue).ok_or_else(|| {
                JsonConversionError::new(
                    JSON_NUMBER_OUT_OF_RANGE,
                    format!("{number} is not a number a double holds"),
                )
            })
        }
    }

    fn decode_entries(
        &self,
        entries: impl IntoIterator<Item = (String, Json)>,
    ) -> Result<HashMap<String, Value>, JsonConversionError> {
        entries
            .into_iter()
            .map(|(key, content)| match self.decode(content) {
                Ok(value) => Ok((key, value)),
                Err(failure) => Err(failure.under(&key)),
            })
            .collect()
    }

    fn decode_wrapper(&self, key: &str, content: Json) -> Result<ValueType, JsonConversionError> {
        let (expected, value_type) = match key {
            "$int" => (
                "a string holding an int64",
                content
                    .as_str()
                    .and_then(|text| text.parse().ok())
                    .map(ValueType::IntegerValue),
            ),
            "$double" => (
                "\"NaN\", \"Infinity\" or \"-Infinity\"",
                content
                    .as_str()
                    .and_then(Self::non_finite)
                    .map(ValueType::DoubleValue),
            ),
            "$timestamp" => (
                "an RFC 3339 string",
                content
                    .as_str()
                    .and_then(|text| text.parse().ok())
                    .map(ValueType::TimestampValue),
            ),
            "$bytes" => (
                "a base64 string",
                content
                    .as_str()
                    .and_then(|text| STANDARD.decode(text).ok())
                    .map(ValueType::BytesValue),
            ),
            "$ref" => (
                "a string",
                content
                    .as_str()
                    .map(|text| ValueType::ReferenceValue(self.resolve_name(text))),
            ),
            "$geo" => (
                "an array of two numbers",
                match content.as_array().map(Vec::as_slice) {
                    Some([latitude, longitude]) => Self::coordinate(latitude)
                        .zip(Self::coordinate(longitude))
                        .map(|(latitude, longitude)| {
                            ValueType::GeoPointValue(LatLng {
                                latitude,
                                longitude,
                            })
                        }),
                    _ => None,
                },
            ),
            "$vector" => (
                "an array of numbers",
                content
                    .as_array()
                    .and_then(|components| components.iter().map(Json::as_f64).collect())
                    .and_then(Self::vector),
            ),
            "$map" => {
                return match content {
                    Json::Object(object) => Ok(ValueType::MapValue(MapValue {
                        fields: self.decode_entries(object)?,
                    })),
                    other => Err(JsonConversionError::invalid_wrapper(
                        key,
                        "an object",
                        &other,
                    )),
                };
            }
            _ => {
                return Err(JsonConversionError::new(
                    JSON_UNKNOWN_WRAPPER,
                    format!("`{key}` is not one of the wrappers"),
                ))
            }
        };
        value_type.ok_or_else(|| JsonConversionError::invalid_wrapper(key, expected, &content))
    }

    fn non_finite(text: &str) -> Option<f64> {
        match text {
            "NaN" => Some(f64::NAN),
            "Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            _ => None,
        }
    }

    fn coordinate(json: &Json) -> Option<f64> {
        match json {
            Json::Number(number) => number.as_f64(),
            Json::Object(object) if object.len() == 1 => object
                .get("$double")
                .and_then(Json::as_str)
                .and_then(Self::non_finite),
            _ => None,
        }
    }

    fn vector(components: Vec<f64>) -> Option<ValueType> {
        let value: FirestoreValue = FirestoreVector::new(components).into();
        value.value.value_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pipeline::fixtures::json_fixture;
    use gcloud_sdk::google::firestore::v1::value::ValueType;

    const DOCUMENTS_PATH: &str = "projects/project/databases/database/documents";

    fn options(encoding: FirestoreJsonEncoding, metadata: bool) -> FirestoreJsonOptions {
        FirestoreJsonOptions::new()
            .with_encoding(encoding)
            .with_metadata(metadata)
    }

    fn lines(edition: &str, encoding: &str) -> Vec<Json> {
        let path = format!(
            "{}/tests/fixtures/json/{edition}.expected-{encoding}.jsonl",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn tagged_encoding_round_trips_every_value_type() {
        for edition in ["standard", "enterprise"] {
            let fixture = json_fixture(edition);
            let documents_path = format!("{}/documents", fixture.database);
            let expected_lines = lines(edition, "tagged");
            assert_eq!(fixture.documents.len(), expected_lines.len());
            for (document, expected) in fixture.documents.iter().zip(expected_lines) {
                let tagged = FirestoreDb::serialize_doc_to_json(
                    &documents_path,
                    document,
                    &options(FirestoreJsonEncoding::Tagged, true),
                )
                .unwrap();
                assert_eq!(tagged, expected, "{}", document.name);

                let text = serde_json::to_string(&tagged).unwrap();
                let read_back = serde_json::from_str::<Json>(&text).unwrap();
                let imported =
                    FirestoreDb::deserialize_doc_from_json(&documents_path, read_back).unwrap();
                assert_eq!(imported.name, document.name);

                let without_metadata = FirestoreJsonOptions::new();
                let exported_text = serde_json::to_string(
                    &FirestoreDb::serialize_doc_to_json(
                        &documents_path,
                        document,
                        &without_metadata,
                    )
                    .unwrap(),
                )
                .unwrap();
                let re_exported_text = serde_json::to_string(
                    &FirestoreDb::serialize_doc_to_json(
                        &documents_path,
                        &imported,
                        &without_metadata,
                    )
                    .unwrap(),
                )
                .unwrap();
                assert_eq!(re_exported_text, exported_text, "{}", document.name);
            }
        }
    }

    #[test]
    fn double_survives_a_text_round_trip() {
        const EMBEDDING_COMPONENT: f64 = 0.9996868371963501;
        let component = Value {
            value_type: Some(ValueType::DoubleValue(EMBEDDING_COMPONENT)),
        };
        let text = serde_json::to_string(
            &FirestoreDb::serialize_value_to_json(
                DOCUMENTS_PATH,
                &component,
                FirestoreJsonEncoding::Tagged,
            )
            .unwrap(),
        )
        .unwrap();
        let read_back = serde_json::from_str::<Json>(&text).unwrap();
        let imported = FirestoreDb::deserialize_value_from_json(DOCUMENTS_PATH, read_back).unwrap();
        assert_eq!(imported.value.value_type, component.value_type);
    }

    #[test]
    fn plain_encoding_writes_every_value_type() {
        for edition in ["standard", "enterprise"] {
            let fixture = json_fixture(edition);
            let documents_path = format!("{}/documents", fixture.database);
            let expected_lines = lines(edition, "plain");
            assert_eq!(fixture.documents.len(), expected_lines.len());
            for (document, expected) in fixture.documents.iter().zip(expected_lines) {
                let plain = FirestoreDb::serialize_doc_to_json(
                    &documents_path,
                    document,
                    &options(FirestoreJsonEncoding::Plain, true),
                )
                .unwrap();
                assert_eq!(plain, expected, "{}", document.name);
            }
        }
    }

    #[test]
    fn plain_timestamp_reads_back_as_a_string() {
        let timestamp = Value {
            value_type: Some(ValueType::TimestampValue(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            })),
        };
        let plain = FirestoreDb::serialize_value_to_json(
            DOCUMENTS_PATH,
            &timestamp,
            FirestoreJsonEncoding::Plain,
        )
        .unwrap();
        let read_back = FirestoreDb::deserialize_value_from_json(DOCUMENTS_PATH, plain).unwrap();
        assert_eq!(
            read_back.value.value_type,
            Some(ValueType::StringValue("2023-11-14T22:13:20Z".to_string()))
        );
    }

    fn read_tagged(text: &str) -> FirestoreResult<FirestoreValue> {
        FirestoreDb::deserialize_value_from_json(
            DOCUMENTS_PATH,
            serde_json::from_str::<Json>(text).unwrap(),
        )
    }

    #[test]
    fn unknown_wrapper_is_refused() {
        let error = read_tagged(r#"{"$set": 1}"#).unwrap_err();
        assert!(
            matches!(&error, FirestoreError::DeserializeError(error) if error.public.code == JSON_UNKNOWN_WRAPPER),
            "{error:?}"
        );
    }

    #[test]
    fn malformed_wrapper_is_refused() {
        let error = read_tagged(r#"{"$timestamp": 17}"#).unwrap_err();
        assert!(
            matches!(&error, FirestoreError::DeserializeError(error) if error.public.code == JSON_INVALID_WRAPPER),
            "{error:?}"
        );
    }

    #[test]
    fn integer_beyond_int64_is_refused() {
        let error = read_tagged("9223372036854775808").unwrap_err();
        assert!(
            matches!(&error, FirestoreError::DeserializeError(error) if error.public.code == JSON_NUMBER_OUT_OF_RANGE),
            "{error:?}"
        );
    }

    #[test]
    fn document_without_a_name_is_refused() {
        let error = FirestoreDb::deserialize_doc_from_json(
            DOCUMENTS_PATH,
            serde_json::from_str(r#"{"title": "Dune"}"#).unwrap(),
        )
        .unwrap_err();
        assert!(
            matches!(&error, FirestoreError::DeserializeError(error) if error.public.code == JSON_INVALID_DOCUMENT),
            "{error:?}"
        );
    }

    #[test]
    fn expression_value_has_no_tagged_form() {
        let expression = Value {
            value_type: Some(ValueType::FieldReferenceValue("title".to_string())),
        };
        let error = FirestoreDb::serialize_value_to_json(
            DOCUMENTS_PATH,
            &expression,
            FirestoreJsonEncoding::Tagged,
        )
        .unwrap_err();
        assert!(
            matches!(&error, FirestoreError::SerializeError(error) if error.public.code == JSON_EXPRESSION_VALUE),
            "{error:?}"
        );
    }

    #[test]
    fn name_in_another_database_stays_full() {
        let own = format!("{DOCUMENTS_PATH}/books/dune");
        let other = "projects/project/databases/other/documents/books/dune";
        assert_eq!(
            FirestoreDb::relative_document_name(DOCUMENTS_PATH, &own),
            "books/dune"
        );
        assert_eq!(
            FirestoreDb::relative_document_name(DOCUMENTS_PATH, other),
            other
        );
    }
}
