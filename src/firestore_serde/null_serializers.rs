pub(crate) const FIRESTORE_NULL_TYPE_TAG_TYPE: &str = "FirestoreNull";

pub mod serialize_as_null {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S, T>(date: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        serializer
            .serialize_newtype_struct(crate::firestore_serde::FIRESTORE_NULL_TYPE_TAG_TYPE, &date)
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: for<'tde> Deserialize<'tde>,
    {
        Option::<T>::deserialize(deserializer)
    }
}
