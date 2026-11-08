//! Support for storing a `Vec<f64>` as a native Firestore vector value, for nearest-neighbour
//! vector search.
//!
//! [`FirestoreVector`] needs no attribute - its derived `Serialize` and manual `Deserialize`
//! already route through [`serialize_vector_for_firestore`].

use crate::errors::FirestoreError;
use crate::firestore_serde::serializer::FirestoreValueSerializer;
use crate::{FirestoreResult, FirestoreValue};
use gcloud_sdk::google::firestore::v1::value::ValueType;
use gcloud_sdk::google::firestore::v1::{MapValue, Value};
use serde::de::{MapAccess, Visitor};
use serde::{Deserializer, Serialize};

pub(crate) const FIRESTORE_VECTOR_TYPE_TAG_TYPE: &str = "FirestoreVector";

#[derive(Serialize, Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct FirestoreVector(pub Vec<f64>);

impl FirestoreVector {
    /// Wraps `vec` as a Firestore vector value.
    pub fn new(vec: Vec<f64>) -> Self {
        FirestoreVector(vec)
    }

    /// Returns the wrapped components, consuming the vector.
    pub fn into_vec(self) -> Vec<f64> {
        self.0
    }

    /// Returns a reference to the wrapped components.
    pub fn as_vec(&self) -> &Vec<f64> {
        &self.0
    }

    /// Reads a vector from a document field, which Firestore stores as a map of exactly two
    /// fields: `__type__` set to `__vector__`, and `value`, an array of doubles.
    ///
    /// Fails with a [`DeserializeError`](FirestoreError::DeserializeError) of code
    /// [`NOT_A_VECTOR`](crate::errors::codes::NOT_A_VECTOR) for any other value. Only the form is
    /// checked, not Firestore's limits on the components, such as that a vector is not empty.
    pub fn from_value(value: &Value) -> FirestoreResult<Self> {
        match &value.value_type {
            Some(ValueType::MapValue(map)) => Self::from_map_value(map),
            _ => Err(FirestoreError::not_a_vector()),
        }
    }

    pub(crate) fn from_map_value(map: &MapValue) -> FirestoreResult<Self> {
        match (map.fields.get("__type__"), map.fields.get("value")) {
            (
                Some(Value {
                    value_type: Some(ValueType::StringValue(marker)),
                }),
                Some(Value {
                    value_type: Some(ValueType::ArrayValue(components)),
                }),
            ) if marker == "__vector__" && map.fields.len() == 2 => components
                .values
                .iter()
                .map(|component| match component.value_type {
                    Some(ValueType::DoubleValue(double)) => Ok(double),
                    _ => Err(FirestoreError::not_a_vector()),
                })
                .collect::<FirestoreResult<_>>()
                .map(Self),
            _ => Err(FirestoreError::not_a_vector()),
        }
    }
}

impl From<FirestoreVector> for Vec<f64> {
    fn from(val: FirestoreVector) -> Self {
        val.into_vec()
    }
}

impl<I> From<I> for FirestoreVector
where
    I: IntoIterator<Item = f64>,
{
    fn from(vec: I) -> Self {
        FirestoreVector(vec.into_iter().collect())
    }
}

/// Serializes `value` as a Firestore vector value, the shape Firestore's nearest-neighbour
/// queries expect.
///
/// Called internally once [`FirestoreVector`]'s derived `Serialize` has tagged the value; not
/// meant to be called directly, since its `FirestoreValueSerializer` parameter cannot be
/// constructed outside this crate.
///
/// Returns an error only if `value`'s own `Serialize` impl fails.
pub fn serialize_vector_for_firestore<T: ?Sized + Serialize>(
    firestore_value_serializer: FirestoreValueSerializer,
    value: &T,
) -> Result<FirestoreValue, FirestoreError> {
    let value_with_array = value.serialize(firestore_value_serializer)?;

    Ok(FirestoreValue::from(
        gcloud_sdk::google::firestore::v1::Value {
            value_type: Some(gcloud_sdk::google::firestore::v1::value::ValueType::MapValue(
                gcloud_sdk::google::firestore::v1::MapValue {
                    fields: vec![
                        (
                            "__type__".to_string(),
                            gcloud_sdk::google::firestore::v1::Value {
                                value_type: Some(gcloud_sdk::google::firestore::v1::value::ValueType::StringValue(
                                    "__vector__".to_string()
                                )),
                            }
                        ),
                        (
                            "value".to_string(),
                            value_with_array.value
                        )].into_iter().collect()
                }
            ))
        }),
    )
}

struct FirestoreVectorVisitor;

impl<'de> Visitor<'de> for FirestoreVectorVisitor {
    type Value = FirestoreVector;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a FirestoreVector")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut vec = Vec::new();

        while let Some(value) = seq.next_element()? {
            vec.push(value);
        }

        Ok(FirestoreVector(vec))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(field) = map.next_key::<String>()? {
            match field.as_str() {
                "__type__" => {
                    let value = map.next_value::<String>()?;
                    if value != "__vector__" {
                        return Err(serde::de::Error::custom(
                            "Expected __vector__  for FirestoreVector",
                        ));
                    }
                }
                "value" => {
                    let value = map.next_value::<Vec<f64>>()?;
                    return Ok(FirestoreVector(value));
                }
                _ => {
                    return Err(serde::de::Error::custom(
                        "Unknown field for FirestoreVector",
                    ));
                }
            }
        }
        Err(serde::de::Error::custom(
            "Unknown structure for FirestoreVector",
        ))
    }
}

impl<'de> serde::Deserialize<'de> for FirestoreVector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(FirestoreVectorVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pipeline::fixtures::{fixture_case, responses};
    use crate::errors::codes::NOT_A_VECTOR;

    /// A field of the book `Dune` as an Enterprise edition pipeline returned it.
    fn dune_field(name: &str) -> Value {
        responses(&fixture_case("stage-sample-documents", None))
            .into_iter()
            .flat_map(|response| response.results)
            .find(|document| document.name.ends_with("/pipeline-books/book-10"))
            .and_then(|mut document| document.fields.remove(name))
            .unwrap()
    }

    fn is_not_a_vector(result: FirestoreResult<FirestoreVector>) -> bool {
        matches!(
            result,
            Err(FirestoreError::DeserializeError(error)) if error.public.code == NOT_A_VECTOR
        )
    }

    #[test]
    fn stored_vector_reads_as_its_components() {
        assert_eq!(
            FirestoreVector::from_value(&dune_field("embedding")).unwrap(),
            FirestoreVector::new(vec![0.95, 0.15, 0.35])
        );
    }

    #[test]
    fn marker_without_components_is_not_a_vector() {
        let marker = Value {
            value_type: Some(ValueType::StringValue("__vector__".to_string())),
        };
        let marker_only = Value {
            value_type: Some(ValueType::MapValue(MapValue {
                fields: [("__type__".to_string(), marker)].into(),
            })),
        };
        assert!(is_not_a_vector(FirestoreVector::from_value(&marker_only)));
    }

    #[test]
    fn plain_map_is_not_a_vector() {
        assert!(is_not_a_vector(FirestoreVector::from_value(&dune_field(
            "awards"
        ))));
    }
}
