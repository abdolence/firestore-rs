use rsb_derive::Builder;
use serde::*;
use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;

#[derive(Debug)]
pub enum FirestoreError {
    SystemError(FirestoreSystemError),
    DatabaseError(FirestoreDatabaseError),
    DataConflictError(FirestoreDataConflictError),
    DataNotFoundError(FirestoreDataNotFoundError),
    InvalidParametersError(FirestoreInvalidParametersError),
    InvalidJsonError(FirestoreInvalidJsonError),
    NetworkError(FirestoreNetworkError),
}

impl Display for FirestoreError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            FirestoreError::SystemError(ref err) => err.fmt(f),
            FirestoreError::DatabaseError(ref err) => err.fmt(f),
            FirestoreError::DataConflictError(ref err) => err.fmt(f),
            FirestoreError::DataNotFoundError(ref err) => err.fmt(f),
            FirestoreError::InvalidParametersError(ref err) => err.fmt(f),
            FirestoreError::InvalidJsonError(ref err) => err.fmt(f),
            FirestoreError::NetworkError(ref err) => err.fmt(f),
        }
    }
}

impl Error for FirestoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            FirestoreError::SystemError(ref err) => Some(err),
            FirestoreError::DatabaseError(ref err) => Some(err),
            FirestoreError::DataConflictError(ref err) => Some(err),
            FirestoreError::DataNotFoundError(ref err) => Some(err),
            FirestoreError::InvalidParametersError(ref err) => Some(err),
            FirestoreError::InvalidJsonError(ref err) => Some(err),
            FirestoreError::NetworkError(ref err) => Some(err),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Builder, Serialize, Deserialize)]
pub struct FirestoreErrorPublicGenericDetails {
    pub code: String,
}

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreSystemError {
    pub public: FirestoreErrorPublicGenericDetails,
    pub message: String,
}

impl Display for FirestoreSystemError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Firestore system/internal error: {}", self.message)
    }
}

impl std::error::Error for FirestoreSystemError {}

#[derive(Debug, Clone, Builder)]
pub struct FirestoreDatabaseError {
    pub public: FirestoreErrorPublicGenericDetails,
    pub details: String,
    pub retry_possible: bool,
}

impl Display for FirestoreDatabaseError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Database general error occurred: {}", self.details)
    }
}

impl std::error::Error for FirestoreDatabaseError {}

#[derive(Debug, Clone, Builder)]
pub struct FirestoreDataConflictError {
    pub public: FirestoreErrorPublicGenericDetails,
    pub details: String,
}

impl Display for FirestoreDataConflictError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Database conflict error occurred: {}", self.details)
    }
}

impl std::error::Error for FirestoreDataConflictError {}

#[derive(Debug, Clone, Builder)]
pub struct FirestoreDataNotFoundError {
    pub public: FirestoreErrorPublicGenericDetails,
    pub data_detail_message: String,
}

impl Display for FirestoreDataNotFoundError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Data not found error occurred: {:?}", self.public)
    }
}

impl std::error::Error for FirestoreDataNotFoundError {}

#[derive(Debug, Eq, PartialEq, Clone, Builder, Serialize, Deserialize)]
pub struct FirestoreInvalidParametersPublicDetails {
    pub field: String,
    pub error: String,
}

#[derive(Debug, Clone, Builder)]
pub struct FirestoreInvalidParametersError {
    pub public: FirestoreInvalidParametersPublicDetails,
}

impl Display for FirestoreInvalidParametersError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Data not found error occurred: {:?}", self.public)
    }
}

impl std::error::Error for FirestoreInvalidParametersError {}

#[derive(Debug, Eq, PartialEq, Clone, Builder, Serialize, Deserialize)]
pub struct FirestoreInvalidJsonErrorPublicDetails {
    pub code: String,
}

#[derive(Debug, Builder)]
pub struct FirestoreInvalidJsonError {
    pub public: FirestoreInvalidJsonErrorPublicDetails,
    pub details: serde_json::Error,
}

impl Display for FirestoreInvalidJsonError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Invalid JSON: {:?}", self.public)
    }
}

impl std::error::Error for FirestoreInvalidJsonError {}

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreNetworkError {
    pub public: FirestoreErrorPublicGenericDetails,
    pub message: String,
}

impl Display for FirestoreNetworkError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Network error: {}", self.message)
    }
}

impl std::error::Error for FirestoreNetworkError {}

impl From<gcloud_sdk::error::Error> for FirestoreError {
    fn from(e: gcloud_sdk::error::Error) -> Self {
        FirestoreError::SystemError(FirestoreSystemError::new(
            FirestoreErrorPublicGenericDetails::new(format!("{:?}", e.kind())),
            format!("GCloud system error: {}", e),
        ))
    }
}

impl From<serde_json::Error> for FirestoreError {
    fn from(e: serde_json::Error) -> Self {
        FirestoreError::InvalidJsonError(FirestoreInvalidJsonError::new(
            FirestoreInvalidJsonErrorPublicDetails::new(format!(
                "Firestore json parse error: {:?}",
                e.classify()
            )),
            e,
        ))
    }
}

impl From<tonic::Status> for FirestoreError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::AlreadyExists => {
                FirestoreError::DataConflictError(FirestoreDataConflictError::new(
                    FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                    format!("{}", status),
                ))
            }
            tonic::Code::NotFound => {
                FirestoreError::DataNotFoundError(FirestoreDataNotFoundError::new(
                    FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                    format!("{}", status),
                ))
            }
            tonic::Code::Aborted
            | tonic::Code::Cancelled
            | tonic::Code::Unavailable
            | tonic::Code::ResourceExhausted => {
                FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                    FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                    format!("{}", status),
                    true,
                ))
            }
            tonic::Code::Unknown => check_hyper_errors(status),
            _ => FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                format!("{}", status),
                false,
            )),
        }
    }
}

fn check_hyper_errors(status: tonic::Status) -> FirestoreError {
    match status.source() {
        Some(hyper_error) => match hyper_error.downcast_ref::<hyper::Error>() {
            Some(err) if err.is_closed() => {
                FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                    FirestoreErrorPublicGenericDetails::new("CONNECTION_CLOSED".into()),
                    format!("Hyper error: {}", err),
                    true,
                ))
            }
            Some(err) if err.is_timeout() => {
                FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                    FirestoreErrorPublicGenericDetails::new("CONNECTION_TIMEOUT".into()),
                    format!("Hyper error: {}", err),
                    true,
                ))
            }
            Some(err) => FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                format!("Hyper error: {}", err),
                false,
            )),
            _ => FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                format!("{}", status),
                false,
            )),
        },
        _ => FirestoreError::DatabaseError(FirestoreDatabaseError::new(
            FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
            format!("{}", status),
            false,
        )),
    }
}

impl serde::ser::Error for FirestoreError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        let msg_str = msg.to_string();
        FirestoreError::SystemError(FirestoreSystemError::new(
            FirestoreErrorPublicGenericDetails::new(msg_str.clone()),
            msg_str,
        ))
    }
}
