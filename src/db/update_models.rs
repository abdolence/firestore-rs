use chrono::prelude::*;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreUpdatePrecondition {
    Exists(bool),
    UpdateTime(DateTime<Utc>),
}
