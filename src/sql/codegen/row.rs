use super::FirestoreSqlIdentifiers;
use crate::firestore_serde::VectorComponents;
use crate::sql::expr::SQL_DOCUMENT_NAME_FIELD;
use crate::{
    FirestoreDocument, FirestoreFieldsStage, FirestorePipeline, FirestorePipelineStage,
    FirestoreResult,
};
use gcloud_sdk::google::firestore::v1::value::ValueType;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};

// The Rust type a Firestore value is read into, by a field of the row struct or as an
// argument of the snippet's function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum FirestoreSqlRustType {
    Integer,
    Double,
    String,
    Boolean,
    Timestamp,
    Vector,
    Reference,
    GeoPoint,
    Bytes,
    Json,
}

impl FirestoreSqlRustType {
    // `None` for null, which is a value of every type.
    pub(super) fn of(value: &ValueType) -> Option<Self> {
        Some(match value {
            ValueType::NullValue(_) => return None,
            ValueType::IntegerValue(_) => Self::Integer,
            ValueType::DoubleValue(_) => Self::Double,
            ValueType::StringValue(_) => Self::String,
            ValueType::BooleanValue(_) => Self::Boolean,
            ValueType::TimestampValue(_) => Self::Timestamp,
            ValueType::MapValue(map) if VectorComponents::of(map).is_some() => Self::Vector,
            ValueType::ReferenceValue(_) => Self::Reference,
            ValueType::GeoPointValue(_) => Self::GeoPoint,
            ValueType::BytesValue(_) => Self::Bytes,
            ValueType::MapValue(_)
            | ValueType::ArrayValue(_)
            | ValueType::FieldReferenceValue(_)
            | ValueType::VariableReferenceValue(_)
            | ValueType::FunctionValue(_)
            | ValueType::PipelineValue(_) => Self::Json,
        })
    }

    fn field_type(self) -> &'static str {
        match self {
            Self::Integer => "i64",
            Self::Double => "f64",
            Self::String => "String",
            Self::Boolean => "bool",
            Self::Timestamp => "FirestoreTimestamp",
            Self::Vector => "FirestoreVector",
            Self::Reference => "FirestoreReference",
            Self::GeoPoint => "FirestoreLatLng",
            Self::Bytes => "Vec<u8>",
            Self::Json => "serde_json::Value",
        }
    }

    // A borrowed type for every value that is not `Copy`, so that more than one closure can
    // use the argument.
    pub(super) fn argument_type(self) -> &'static str {
        match self {
            Self::String => "&str",
            Self::Vector => "&FirestoreVector",
            Self::Reference => "&FirestoreReference",
            Self::GeoPoint => "&FirestoreLatLng",
            Self::Bytes => "&serde_bytes::Bytes",
            Self::Json => "&serde_json::Value",
            other => other.field_type(),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Integer => "integer",
            Self::Double => "double",
            Self::String => "string",
            Self::Boolean => "boolean",
            Self::Timestamp => "timestamp",
            Self::Vector => "vector",
            Self::Reference => "reference",
            Self::GeoPoint => "geo point",
            Self::Bytes => "bytes",
            Self::Json => "JSON",
        }
    }
}

// The struct the snippet reads its rows into.
pub(super) struct FirestoreSqlRow {
    name: String,
    // Whether the rows carry a document name, which the field `id` reads from `_firestore_id`.
    named: bool,
    fields: Vec<FirestoreSqlRowField>,
}

struct FirestoreSqlRowField {
    column: String,
    identifier: String,
    rust_type: FirestoreSqlRustType,
    // Whether a sampled row lacks the column or holds null in it.
    optional: bool,
    doc: Option<String>,
}

impl FirestoreSqlRow {
    // The row struct `name` of `pipeline`, typed from the `sample` rows. `None` when nothing
    // names the columns, with no sample and no select list: the rows are then documents.
    pub(super) fn new(
        name: &str,
        pipeline: &FirestorePipeline,
        sample: &[FirestoreDocument],
    ) -> FirestoreResult<Option<Self>> {
        let Some(mut columns) = Self::columns(pipeline, sample)? else {
            return Ok(None);
        };
        let named_column = columns
            .iter()
            .position(|column| column == SQL_DOCUMENT_NAME_FIELD)
            .map(|position| columns.remove(position));
        let named = match sample {
            [] => named_column.is_some(),
            rows => rows.iter().any(|row| !row.name.is_empty()),
        };
        let mut identifiers = match named {
            true => FirestoreSqlIdentifiers::reserving(&["id"]),
            false => FirestoreSqlIdentifiers::default(),
        };
        let fields = columns
            .into_iter()
            .map(|column| FirestoreSqlRowField::new(column, &mut identifiers, sample))
            .collect();
        Ok(Some(Self {
            name: name.to_string(),
            named,
            fields,
        }))
    }

    // The columns in order: the names of the select list when the pipeline ends with one, and
    // otherwise the sampled rows' field names alphabetically, since a document's fields have no
    // order.
    fn columns(
        pipeline: &FirestorePipeline,
        sample: &[FirestoreDocument],
    ) -> FirestoreResult<Option<Vec<String>>> {
        let projection = pipeline.stages.iter().rev().find(|stage| {
            !matches!(
                stage,
                FirestorePipelineStage::Sort(_)
                    | FirestorePipelineStage::Limit(_)
                    | FirestorePipelineStage::Offset(_)
            )
        });
        Ok(match projection {
            Some(FirestorePipelineStage::Fields(FirestoreFieldsStage { fields })) => Some(
                fields
                    .iter()
                    .map(|selection| selection.name("select").map(str::to_string))
                    .collect::<FirestoreResult<_>>()?,
            ),
            _ if sample.is_empty() => None,
            _ => Some(
                sample
                    .iter()
                    .flat_map(|row| row.fields.keys().cloned())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            ),
        })
    }
}

impl FirestoreSqlRowField {
    // The field of `column`, typed across the sampled rows, with a doc comment that says why
    // when the type is less than the values suggest.
    fn new(
        column: String,
        identifiers: &mut FirestoreSqlIdentifiers,
        sample: &[FirestoreDocument],
    ) -> Self {
        let identifier = identifiers.name(&column);
        if sample.is_empty() {
            return Self {
                column,
                identifier,
                rust_type: FirestoreSqlRustType::Json,
                optional: true,
                doc: None,
            };
        }
        let mut types = BTreeSet::new();
        let mut optional = false;
        for row in sample {
            let observed = row
                .fields
                .get(&column)
                .and_then(|value| value.value_type.as_ref())
                .and_then(FirestoreSqlRustType::of);
            match observed {
                Some(observed) => {
                    types.insert(observed);
                }
                None => optional = true,
            }
        }
        let numbers = types.len() == 2
            && types.contains(&FirestoreSqlRustType::Integer)
            && types.contains(&FirestoreSqlRustType::Double);
        let types: Vec<FirestoreSqlRustType> = types.into_iter().collect();
        let (rust_type, doc) = match types[..] {
            [] => (
                FirestoreSqlRustType::Json,
                Some("Absent or null in every sampled row.".to_string()),
            ),
            [only] => (only, None),
            _ if numbers => (FirestoreSqlRustType::Double, None),
            [ref head @ .., last] => {
                let head: Vec<&str> = head.iter().map(|observed| observed.name()).collect();
                let doc = format!(
                    "Observed as {} and {} in the sample.",
                    head.join(", "),
                    last.name()
                );
                (FirestoreSqlRustType::Json, Some(doc))
            }
        };
        Self {
            column,
            identifier,
            rust_type,
            optional,
            doc,
        }
    }
}

impl Display for FirestoreSqlRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "#[derive(Debug, Clone, Deserialize)]")?;
        writeln!(f, "struct {} {{", self.name)?;
        // A rename, not an alias, so that `id` does not also read a column named `id`.
        if self.named {
            writeln!(
                f,
                "#[serde(rename = \"_firestore_id\")] id: Option<String>,"
            )?;
        }
        for field in &self.fields {
            if let Some(doc) = &field.doc {
                writeln!(f, "/// {doc}")?;
            }
            if field.identifier.trim_start_matches("r#") != field.column {
                writeln!(f, "#[serde(rename = {:?})]", field.column)?;
            }
            if field.rust_type == FirestoreSqlRustType::Bytes {
                // `Vec<u8>` is a sequence to serde, so `serde_bytes` reads it as a byte string. A
                // field with `with` is no longer `None` when missing, so `default` restores that.
                let default = if field.optional { "default, " } else { "" };
                writeln!(f, "#[serde({default}with = \"serde_bytes\")]")?;
            }
            let rust_type = field.rust_type.field_type();
            match field.optional {
                true => writeln!(f, "{}: Option<{rust_type}>,", field.identifier)?,
                false => writeln!(f, "{}: {rust_type},", field.identifier)?,
            }
        }
        write!(f, "}}")
    }
}
