use crate::errors::FirestoreError;
use crate::firestore_serde::serializer::FirestoreValueSerializer;
use crate::FirestoreValue;
use serde::de::{MapAccess, Visitor};
use serde::{Deserializer, Serialize};

pub(crate) const FIRESTORE_VECTOR_TYPE_TAG_TYPE: &str = "FirestoreVector";

#[derive(Serialize, Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct FirestoreVector(pub Vec<f64>);

impl FirestoreVector {
    pub fn new(vec: Vec<f64>) -> Self {
        FirestoreVector(vec)
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
