//! The captured pipeline fixtures in `tests/fixtures/pipelines`, decoded strictly from proto3
//! JSON into the gcloud-sdk types: a key the decoder does not know is an error, so no field of a
//! fixture is silently dropped.

use base64::Engine;
use gcloud_sdk::google::firestore::v1 as wire;
use gcloud_sdk::google::r#type::LatLng;
use gcloud_sdk::prost_types::{Any, Timestamp};
use serde_json::{Map, Value as Json};
use std::collections::HashMap;

type Decoded<T> = Result<T, String>;

/// The fixture file `name`, or its case `case` when the file holds several.
pub(crate) fn fixture_case(name: &str, case: Option<&str>) -> Json {
    let path = format!(
        "{}/tests/fixtures/pipelines/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let file: Json = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    match case {
        None => file,
        Some(case) => file["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|candidate| candidate["name"] == case)
            .unwrap_or_else(|| panic!("{path} has no case {case}"))
            .clone(),
    }
}

/// A captured database and its documents, from `tests/fixtures/json/{edition}.json`.
#[cfg(feature = "json")]
pub(crate) struct JsonFixture {
    /// The database name, as in `projects/{project}/databases/{database}`.
    pub(crate) database: String,
    pub(crate) documents: Vec<wire::Document>,
}

/// The documents captured on `edition`, `standard` or `enterprise`.
#[cfg(feature = "json")]
pub(crate) fn json_fixture(edition: &str) -> JsonFixture {
    let path = format!(
        "{}/tests/fixtures/json/{edition}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let file: Json = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    JsonFixture {
        database: string(&file["database"], "database").unwrap(),
        documents: file["documents"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(index, document)| decode_document(document, &format!("documents[{index}]")))
            .collect::<Decoded<_>>()
            .unwrap(),
    }
}

/// The request a fixture case sent.
pub(crate) fn request(case: &Json) -> wire::ExecutePipelineRequest {
    decode_request(&case["request"], "request").unwrap()
}

/// The pipeline of the request a fixture case sent.
pub(crate) fn pipeline(case: &Json) -> wire::Pipeline {
    match request(case).pipeline_type {
        Some(wire::execute_pipeline_request::PipelineType::StructuredPipeline(structured)) => {
            structured.pipeline.unwrap()
        }
        None => panic!("the request has no pipeline"),
    }
}

/// The responses a fixture case received.
pub(crate) fn responses(case: &Json) -> Vec<wire::ExecutePipelineResponse> {
    case["responses"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(index, response)| decode_response(response, &format!("responses[{index}]")))
        .collect::<Decoded<_>>()
        .unwrap()
}

fn object<'a>(json: &'a Json, context: &str) -> Decoded<&'a Map<String, Json>> {
    json.as_object()
        .ok_or_else(|| format!("{context}: expected an object, got {json}"))
}

fn reject_unknown_keys(object: &Map<String, Json>, allowed: &[&str], context: &str) -> Decoded<()> {
    match object.keys().find(|key| !allowed.contains(&key.as_str())) {
        Some(key) => Err(format!("{context}: unknown key `{key}`")),
        None => Ok(()),
    }
}

fn string(json: &Json, context: &str) -> Decoded<String> {
    json.as_str()
        .map(str::to_string)
        .ok_or_else(|| format!("{context}: expected a string, got {json}"))
}

fn bytes(json: &Json, context: &str) -> Decoded<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(string(json, context)?)
        .map_err(|err| format!("{context}: bad base64: {err}"))
}

fn timestamp(json: &Json, context: &str) -> Decoded<Timestamp> {
    string(json, context)?
        .parse()
        .map_err(|err| format!("{context}: bad timestamp: {err}"))
}

fn integer(json: &Json, context: &str) -> Decoded<i64> {
    match json {
        Json::String(text) => text
            .parse()
            .map_err(|err| format!("{context}: bad int64 {text}: {err}")),
        Json::Number(number) => number
            .as_i64()
            .ok_or_else(|| format!("{context}: bad int64 {number}")),
        _ => Err(format!("{context}: expected an int64, got {json}")),
    }
}

fn double(json: &Json, context: &str) -> Decoded<f64> {
    match json {
        Json::Number(number) => number
            .as_f64()
            .ok_or_else(|| format!("{context}: bad double {number}")),
        Json::String(text) if text == "NaN" => Ok(f64::NAN),
        Json::String(text) if text == "Infinity" => Ok(f64::INFINITY),
        Json::String(text) if text == "-Infinity" => Ok(f64::NEG_INFINITY),
        _ => Err(format!("{context}: expected a double, got {json}")),
    }
}

fn value_map(json: Option<&Json>, context: &str) -> Decoded<HashMap<String, wire::Value>> {
    let Some(json) = json else {
        return Ok(HashMap::new());
    };
    object(json, context)?
        .iter()
        .map(|(key, value)| {
            Ok((
                key.clone(),
                decode_value(value, &format!("{context}.{key}"))?,
            ))
        })
        .collect()
}

fn value_list(json: Option<&Json>, context: &str) -> Decoded<Vec<wire::Value>> {
    let Some(json) = json else {
        return Ok(Vec::new());
    };
    json.as_array()
        .ok_or_else(|| format!("{context}: expected an array"))?
        .iter()
        .enumerate()
        .map(|(index, value)| decode_value(value, &format!("{context}[{index}]")))
        .collect()
}

fn decode_value(json: &Json, context: &str) -> Decoded<wire::Value> {
    use wire::value::ValueType;
    let fields = object(json, context)?;
    let (kind, value) = match fields.iter().collect::<Vec<_>>().as_slice() {
        [(kind, value)] => (kind.as_str(), *value),
        _ => {
            return Err(format!(
                "{context}: a Value has exactly one key, got {fields:?}"
            ))
        }
    };
    let context = format!("{context}.{kind}");
    let value_type = match kind {
        "nullValue" => ValueType::NullValue(0),
        "booleanValue" => ValueType::BooleanValue(
            value
                .as_bool()
                .ok_or_else(|| format!("{context}: expected a bool"))?,
        ),
        "integerValue" => ValueType::IntegerValue(integer(value, &context)?),
        "doubleValue" => ValueType::DoubleValue(double(value, &context)?),
        "timestampValue" => ValueType::TimestampValue(timestamp(value, &context)?),
        "stringValue" => ValueType::StringValue(string(value, &context)?),
        "bytesValue" => ValueType::BytesValue(bytes(value, &context)?),
        "referenceValue" => ValueType::ReferenceValue(string(value, &context)?),
        "fieldReferenceValue" => ValueType::FieldReferenceValue(string(value, &context)?),
        "variableReferenceValue" => ValueType::VariableReferenceValue(string(value, &context)?),
        "geoPointValue" => {
            let point = object(value, &context)?;
            reject_unknown_keys(point, &["latitude", "longitude"], &context)?;
            let coordinate = |name: &str| -> Decoded<f64> {
                point
                    .get(name)
                    .map(|coordinate| double(coordinate, &context))
                    .transpose()
                    .map(Option::unwrap_or_default)
            };
            ValueType::GeoPointValue(LatLng {
                latitude: coordinate("latitude")?,
                longitude: coordinate("longitude")?,
            })
        }
        "arrayValue" => {
            let array = object(value, &context)?;
            reject_unknown_keys(array, &["values"], &context)?;
            ValueType::ArrayValue(wire::ArrayValue {
                values: value_list(array.get("values"), &context)?,
            })
        }
        "mapValue" => {
            let map = object(value, &context)?;
            reject_unknown_keys(map, &["fields"], &context)?;
            ValueType::MapValue(wire::MapValue {
                fields: value_map(map.get("fields"), &context)?,
            })
        }
        "functionValue" => ValueType::FunctionValue(decode_function(value, &context)?),
        "pipelineValue" => ValueType::PipelineValue(decode_pipeline(value, &context)?),
        other => return Err(format!("{context}: unknown Value kind `{other}`")),
    };
    Ok(wire::Value {
        value_type: Some(value_type),
    })
}

fn decode_function(json: &Json, context: &str) -> Decoded<wire::Function> {
    let function = object(json, context)?;
    reject_unknown_keys(function, &["name", "args", "options"], context)?;
    Ok(wire::Function {
        name: string(
            function
                .get("name")
                .ok_or_else(|| format!("{context}: no name"))?,
            context,
        )?,
        args: value_list(function.get("args"), &format!("{context}.args"))?,
        options: value_map(function.get("options"), &format!("{context}.options"))?,
    })
}

fn decode_pipeline(json: &Json, context: &str) -> Decoded<wire::Pipeline> {
    let pipeline = object(json, context)?;
    reject_unknown_keys(pipeline, &["stages"], context)?;
    let stages = pipeline
        .get("stages")
        .and_then(Json::as_array)
        .ok_or_else(|| format!("{context}: no stages"))?
        .iter()
        .enumerate()
        .map(|(index, stage)| {
            let context = format!("{context}.stages[{index}]");
            let fields = object(stage, &context)?;
            reject_unknown_keys(fields, &["name", "args", "options"], &context)?;
            Ok(wire::pipeline::Stage {
                name: string(
                    fields
                        .get("name")
                        .ok_or_else(|| format!("{context}: no name"))?,
                    &context,
                )?,
                args: value_list(fields.get("args"), &format!("{context}.args"))?,
                options: value_map(fields.get("options"), &format!("{context}.options"))?,
            })
        })
        .collect::<Decoded<_>>()?;
    Ok(wire::Pipeline { stages })
}

fn decode_transaction_options(json: &Json, context: &str) -> Decoded<wire::TransactionOptions> {
    use wire::transaction_options::{read_only, Mode, ReadOnly, ReadWrite};
    let options = object(json, context)?;
    reject_unknown_keys(options, &["readOnly", "readWrite"], context)?;
    let mode = if let Some(read_only) = options.get("readOnly") {
        let fields = object(read_only, context)?;
        reject_unknown_keys(fields, &["readTime"], context)?;
        Mode::ReadOnly(ReadOnly {
            consistency_selector: fields
                .get("readTime")
                .map(|read_time| timestamp(read_time, context))
                .transpose()?
                .map(read_only::ConsistencySelector::ReadTime),
        })
    } else if let Some(read_write) = options.get("readWrite") {
        let fields = object(read_write, context)?;
        reject_unknown_keys(fields, &["retryTransaction"], context)?;
        Mode::ReadWrite(ReadWrite {
            retry_transaction: fields
                .get("retryTransaction")
                .map(|transaction| bytes(transaction, context))
                .transpose()?
                .unwrap_or_default(),
            ..ReadWrite::default()
        })
    } else {
        return Err(format!("{context}: no mode"));
    };
    Ok(wire::TransactionOptions { mode: Some(mode) })
}

fn decode_request(json: &Json, context: &str) -> Decoded<wire::ExecutePipelineRequest> {
    use wire::execute_pipeline_request::{ConsistencySelector, PipelineType};
    let request = object(json, context)?;
    reject_unknown_keys(
        request,
        &[
            "database",
            "structuredPipeline",
            "transaction",
            "newTransaction",
            "readTime",
            "autoCommitTransaction",
        ],
        context,
    )?;
    let structured = object(
        request
            .get("structuredPipeline")
            .ok_or_else(|| format!("{context}: no structuredPipeline"))?,
        context,
    )?;
    reject_unknown_keys(structured, &["pipeline", "options"], context)?;
    let consistency_selector = if let Some(transaction) = request.get("transaction") {
        Some(ConsistencySelector::Transaction(bytes(
            transaction,
            context,
        )?))
    } else if let Some(options) = request.get("newTransaction") {
        Some(ConsistencySelector::NewTransaction(
            decode_transaction_options(options, context)?,
        ))
    } else if let Some(read_time) = request.get("readTime") {
        Some(ConsistencySelector::ReadTime(timestamp(
            read_time, context,
        )?))
    } else {
        None
    };
    Ok(wire::ExecutePipelineRequest {
        database: request
            .get("database")
            .map(|database| string(database, context))
            .transpose()?
            .unwrap_or_default(),
        auto_commit_transaction: request
            .get("autoCommitTransaction")
            .and_then(Json::as_bool)
            .unwrap_or(false),
        request_options: None,
        pipeline_type: Some(PipelineType::StructuredPipeline(wire::StructuredPipeline {
            pipeline: Some(decode_pipeline(
                structured
                    .get("pipeline")
                    .ok_or_else(|| format!("{context}: no pipeline"))?,
                &format!("{context}.pipeline"),
            )?),
            options: value_map(structured.get("options"), &format!("{context}.options"))?,
        })),
        consistency_selector,
    })
}

fn decode_document(json: &Json, context: &str) -> Decoded<wire::Document> {
    let document = object(json, context)?;
    reject_unknown_keys(
        document,
        &["name", "fields", "createTime", "updateTime"],
        context,
    )?;
    Ok(wire::Document {
        name: document
            .get("name")
            .map(|name| string(name, context))
            .transpose()?
            .unwrap_or_default(),
        fields: value_map(document.get("fields"), &format!("{context}.fields"))?,
        create_time: document
            .get("createTime")
            .map(|time| timestamp(time, context))
            .transpose()?,
        update_time: document
            .get("updateTime")
            .map(|time| timestamp(time, context))
            .transpose()?,
    })
}

fn decode_any(json: &Json, context: &str) -> Decoded<Any> {
    let any = object(json, context)?;
    reject_unknown_keys(any, &["@type", "value"], context)?;
    let type_url = string(
        any.get("@type")
            .ok_or_else(|| format!("{context}: no @type"))?,
        context,
    )?;
    let text = string(
        any.get("value")
            .ok_or_else(|| format!("{context}: no value"))?,
        context,
    )?;
    let decoded = Any::from_msg(&text).map_err(|err| format!("{context}: {err}"))?;
    if decoded.type_url != type_url {
        return Err(format!("{context}: unsupported Any type {type_url}"));
    }
    Ok(decoded)
}

fn decode_response(json: &Json, context: &str) -> Decoded<wire::ExecutePipelineResponse> {
    let response = object(json, context)?;
    reject_unknown_keys(
        response,
        &["transaction", "results", "executionTime", "explainStats"],
        context,
    )?;
    Ok(wire::ExecutePipelineResponse {
        transaction: response
            .get("transaction")
            .map(|transaction| bytes(transaction, context))
            .transpose()?
            .unwrap_or_default(),
        results: response
            .get("results")
            .and_then(Json::as_array)
            .map(|documents| {
                documents
                    .iter()
                    .enumerate()
                    .map(|(index, document)| {
                        decode_document(document, &format!("{context}.results[{index}]"))
                    })
                    .collect::<Decoded<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default(),
        execution_time: response
            .get("executionTime")
            .map(|time| timestamp(time, context))
            .transpose()?,
        explain_stats: response
            .get("explainStats")
            .map(|stats| {
                let fields = object(stats, context)?;
                reject_unknown_keys(fields, &["data"], context)?;
                Ok::<_, String>(wire::ExplainStats {
                    data: fields
                        .get("data")
                        .map(|data| decode_any(data, context))
                        .transpose()?,
                })
            })
            .transpose()?,
    })
}
