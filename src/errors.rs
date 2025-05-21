use crate::{FirestoreTransaction, FirestoreTransactionId};
use gcloud_sdk::google::firestore::v1::WriteRequest;
use rsb_derive::Builder;
use serde::*;
use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;

/// The main error type for all Firestore operations.
///
/// This enum consolidates various specific error types that can occur
/// during interactions with Google Firestore.
#[derive(Debug)]
pub enum FirestoreError {
    /// An error originating from the underlying system or a dependency, not directly
    /// from a Firestore API interaction. This could include issues with the gRPC client,
    /// I/O errors, etc.
    SystemError(FirestoreSystemError),
    /// A general error reported by the Firestore database.
    /// This often wraps errors returned by the Firestore gRPC API.
    DatabaseError(FirestoreDatabaseError),
    /// An error indicating a data conflict, such as trying to create a document
    /// that already exists, or an optimistic locking failure.
    DataConflictError(FirestoreDataConflictError),
    /// An error indicating that the requested data (e.g., a document or collection)
    /// was not found.
    DataNotFoundError(FirestoreDataNotFoundError),
    /// An error due to invalid parameters provided by the client for an operation.
    InvalidParametersError(FirestoreInvalidParametersError),
    /// An error that occurred during the serialization of data to be sent to Firestore.
    SerializeError(FirestoreSerializationError),
    /// An error that occurred during the deserialization of data received from Firestore.
    DeserializeError(FirestoreSerializationError),
    /// An error related to network connectivity or communication with the Firestore service.
    NetworkError(FirestoreNetworkError),
    /// An error that occurred specifically within the context of a Firestore transaction.
    ErrorInTransaction(FirestoreErrorInTransaction),
    /// An error related to the caching layer, if enabled and used.
    CacheError(FirestoreCacheError),
}

impl Display for FirestoreError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            FirestoreError::SystemError(ref err) => err.fmt(f),
            FirestoreError::DatabaseError(ref err) => err.fmt(f),
            FirestoreError::DataConflictError(ref err) => err.fmt(f),
            FirestoreError::DataNotFoundError(ref err) => err.fmt(f),
            FirestoreError::InvalidParametersError(ref err) => err.fmt(f),
            FirestoreError::SerializeError(ref err) => err.fmt(f),
            FirestoreError::DeserializeError(ref err) => err.fmt(f),
            FirestoreError::NetworkError(ref err) => err.fmt(f),
            FirestoreError::ErrorInTransaction(ref err) => err.fmt(f),
            FirestoreError::CacheError(ref err) => err.fmt(f),
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
            FirestoreError::SerializeError(ref err) => Some(err),
            FirestoreError::DeserializeError(ref err) => Some(err),
            FirestoreError::NetworkError(ref err) => Some(err),
            FirestoreError::ErrorInTransaction(ref err) => Some(err),
            FirestoreError::CacheError(ref err) => Some(err),
        }
    }
}

/// Generic public details for Firestore errors.
///
/// This struct is often embedded in more specific error types to provide
/// a common way to access a general error code or identifier.
#[derive(Debug, Eq, PartialEq, Clone, Builder, Serialize, Deserialize)]
pub struct FirestoreErrorPublicGenericDetails {
    /// A string code representing the error, often derived from gRPC status codes
    /// or other specific error identifiers.
    pub code: String,
}

impl Display for FirestoreErrorPublicGenericDetails {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Error code: {}", self.code)
    }
}

/// Represents a system-level or internal error.
///
/// These errors are typically not directly from the Firestore API but from underlying
/// components like the gRPC client, I/O operations, or other system interactions.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreSystemError {
    /// Generic public details about the error.
    pub public: FirestoreErrorPublicGenericDetails,
    /// A descriptive message detailing the system error.
    pub message: String,
}

impl Display for FirestoreSystemError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Firestore system/internal error: {}. {}",
            self.public, self.message
        )
    }
}

impl std::error::Error for FirestoreSystemError {}

/// Represents a general error reported by the Firestore database.
///
/// This often wraps errors returned by the Firestore gRPC API.
#[derive(Debug, Clone, Builder)]
pub struct FirestoreDatabaseError {
    /// Generic public details about the error.
    pub public: FirestoreErrorPublicGenericDetails,
    /// Specific details about the database error.
    pub details: String,
    /// Indicates whether retrying the operation might succeed.
    pub retry_possible: bool,
}

impl Display for FirestoreDatabaseError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Database general error occurred: {}. {}. Retry possibility: {}",
            self.public, self.details, self.retry_possible
        )
    }
}

impl std::error::Error for FirestoreDatabaseError {}

/// Represents an error due to a data conflict.
///
/// This can occur, for example, if trying to create a document that already exists
/// or if an optimistic locking condition (e.g., based on `update_time`) is not met.
#[derive(Debug, Clone, Builder)]
pub struct FirestoreDataConflictError {
    /// Generic public details about the error.
    pub public: FirestoreErrorPublicGenericDetails,
    /// Specific details about the data conflict.
    pub details: String,
}

impl Display for FirestoreDataConflictError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Database conflict error occurred: {}. {}",
            self.public, self.details
        )
    }
}

impl std::error::Error for FirestoreDataConflictError {}

/// Represents an error indicating that requested data was not found.
///
/// This is typically returned when trying to access a document or resource
/// that does not exist in Firestore.
#[derive(Debug, Clone, Builder)]
pub struct FirestoreDataNotFoundError {
    /// Generic public details about the error.
    pub public: FirestoreErrorPublicGenericDetails,
    /// A message providing more details about what data was not found.
    pub data_detail_message: String,
}

impl Display for FirestoreDataNotFoundError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Data not found error occurred: {}. {}",
            self.public, self.data_detail_message
        )
    }
}

impl std::error::Error for FirestoreDataNotFoundError {}

/// Public details for an invalid parameters error.
///
/// Provides information about which parameter was invalid and why.
#[derive(Debug, Eq, PartialEq, Clone, Builder, Serialize, Deserialize)]
pub struct FirestoreInvalidParametersPublicDetails {
    /// The name of the field or parameter that was invalid.
    pub field: String,
    /// A description of why the parameter is considered invalid.
    pub error: String,
}

impl Display for FirestoreInvalidParametersPublicDetails {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Invalid parameters error: {}. {}",
            self.field, self.error
        )
    }
}

/// Represents an error due to invalid parameters provided for an operation.
///
/// This error occurs when the client sends a request with parameters that
/// do not meet the Firestore API's requirements (e.g., invalid document ID format).
#[derive(Debug, Clone, Builder)]
pub struct FirestoreInvalidParametersError {
    /// Detailed information about the invalid parameter.
    pub public: FirestoreInvalidParametersPublicDetails,
}

impl Display for FirestoreInvalidParametersError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Data not found error occurred: {}", self.public)
    }
}

impl std::error::Error for FirestoreInvalidParametersError {}

/// Public details for an error related to invalid JSON.
///
/// Note: This error type appears to be defined but might not be actively used
/// throughout the crate in favor of `FirestoreSerializationError` for broader
/// serialization issues.
#[derive(Debug, Eq, PartialEq, Clone, Builder, Serialize, Deserialize)]
pub struct FirestoreInvalidJsonErrorPublicDetails {
    /// A code identifying the nature of the JSON error.
    pub code: String,
}

/// Represents an error related to network connectivity or communication.
///
/// This can include issues like timeouts, connection refused, or other problems
/// encountered while trying to communicate with the Firestore service.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreNetworkError {
    /// Generic public details about the error.
    pub public: FirestoreErrorPublicGenericDetails,
    /// A descriptive message detailing the network error.
    pub message: String,
}

impl Display for FirestoreNetworkError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Network error: {}. {}", self.public, self.message)
    }
}

impl std::error::Error for FirestoreNetworkError {}

impl From<gcloud_sdk::error::Error> for FirestoreError {
    fn from(e: gcloud_sdk::error::Error) -> Self {
        FirestoreError::SystemError(FirestoreSystemError::new(
            FirestoreErrorPublicGenericDetails::new(format!("{:?}", e.kind())),
            format!("GCloud system error: {e}"),
        ))
    }
}

impl From<gcloud_sdk::tonic::Status> for FirestoreError {
    fn from(status: gcloud_sdk::tonic::Status) -> Self {
        match status.code() {
            gcloud_sdk::tonic::Code::AlreadyExists => {
                FirestoreError::DataConflictError(FirestoreDataConflictError::new(
                    FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                    format!("{status}"),
                ))
            }
            gcloud_sdk::tonic::Code::NotFound => {
                FirestoreError::DataNotFoundError(FirestoreDataNotFoundError::new(
                    FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                    format!("{status}"),
                ))
            }
            gcloud_sdk::tonic::Code::Aborted
            | gcloud_sdk::tonic::Code::Cancelled
            | gcloud_sdk::tonic::Code::Unavailable
            | gcloud_sdk::tonic::Code::ResourceExhausted => {
                FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                    FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                    format!("{status}"),
                    true,
                ))
            }
            gcloud_sdk::tonic::Code::Unknown => check_hyper_errors(status),
            _ => FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                format!("{status}"),
                false,
            )),
        }
    }
}

fn check_hyper_errors(status: gcloud_sdk::tonic::Status) -> FirestoreError {
    match status.source() {
        Some(hyper_error) => match hyper_error.downcast_ref::<hyper::Error>() {
            Some(err) if err.is_closed() => {
                FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                    FirestoreErrorPublicGenericDetails::new("CONNECTION_CLOSED".into()),
                    format!("Hyper error: {err}"),
                    true,
                ))
            }
            Some(err) if err.is_timeout() => {
                FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                    FirestoreErrorPublicGenericDetails::new("CONNECTION_TIMEOUT".into()),
                    format!("Hyper error: {err}"),
                    true,
                ))
            }
            Some(err) => FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                format!("Hyper error: {err}"),
                false,
            )),
            _ if status.code() == gcloud_sdk::tonic::Code::Unknown
                && status.message().contains("transport error") =>
            {
                FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                    FirestoreErrorPublicGenericDetails::new("CONNECTION_ERROR".into()),
                    format!("{status}"),
                    true,
                ))
            }
            _ => FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                format!("{status}"),
                false,
            )),
        },
        _ => FirestoreError::DatabaseError(FirestoreDatabaseError::new(
            FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
            format!("{status} without root cause"),
            false,
        )),
    }
}

impl serde::ser::Error for FirestoreError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        FirestoreError::SerializeError(FirestoreSerializationError::from_message(msg.to_string()))
    }
}

impl serde::de::Error for FirestoreError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        FirestoreError::DeserializeError(FirestoreSerializationError::from_message(msg.to_string()))
    }
}

/// Represents an error that occurred during data serialization or deserialization.
///
/// This is used when converting Rust types to Firestore's format or vice-versa,
/// and an issue arises (e.g., unsupported types, malformed data).
#[derive(Debug, Builder)]
pub struct FirestoreSerializationError {
    /// Generic public details about the error.
    pub public: FirestoreErrorPublicGenericDetails,
    /// A descriptive message detailing the serialization/deserialization error.
    pub message: String,
    /// The path of the document being processed when the error occurred, if applicable.
    pub document_path: Option<String>,
}

impl FirestoreSerializationError {
    /// Creates a `FirestoreSerializationError` from a message string.
    pub fn from_message<S: AsRef<str>>(message: S) -> FirestoreSerializationError {
        let message_str = message.as_ref().to_string();
        FirestoreSerializationError::new(
            FirestoreErrorPublicGenericDetails::new("SerializationError".to_string()),
            message_str,
        )
    }
}

impl Display for FirestoreSerializationError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Invalid serialization: {}. {}. Document path: {}",
            self.public,
            self.message,
            self.document_path.as_deref().unwrap_or("-")
        )
    }
}

impl std::error::Error for FirestoreSerializationError {}

/// Represents an error related to the caching layer.
///
/// This error is used if the `caching` feature is enabled and an issue
/// occurs with cache operations (e.g., backend storage error, cache inconsistency).
#[derive(Debug, Builder)]
pub struct FirestoreCacheError {
    /// Generic public details about the error.
    pub public: FirestoreErrorPublicGenericDetails,
    /// A descriptive message detailing the cache error.
    pub message: String,
}

impl Display for FirestoreCacheError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Cache error: {}. {}", self.public, self.message)
    }
}

impl std::error::Error for FirestoreCacheError {}

impl From<chrono::ParseError> for FirestoreError {
    fn from(parse_err: chrono::ParseError) -> Self {
        FirestoreError::DeserializeError(FirestoreSerializationError::from_message(format!(
            "Parse error: {parse_err}"
        )))
    }
}

impl From<chrono::OutOfRangeError> for FirestoreError {
    fn from(out_of_range: chrono::OutOfRangeError) -> Self {
        FirestoreError::InvalidParametersError(FirestoreInvalidParametersError::new(
            FirestoreInvalidParametersPublicDetails::new(
                format!("Out of range: {out_of_range}"),
                "duration".to_string(),
            ),
        ))
    }
}

impl From<tokio::sync::mpsc::error::SendError<gcloud_sdk::google::firestore::v1::WriteRequest>>
    for FirestoreError
{
    fn from(send_error: tokio::sync::mpsc::error::SendError<WriteRequest>) -> Self {
        FirestoreError::NetworkError(FirestoreNetworkError::new(
            FirestoreErrorPublicGenericDetails::new("SEND_STREAM_ERROR".into()),
            format!("Send stream error: {send_error}"),
        ))
    }
}

/// Represents an error that occurred within the scope of a Firestore transaction.
///
/// This struct captures errors that happen during the execution of user-provided
/// code within a transaction block, or errors from Firestore related to the transaction itself.
#[derive(Debug, Builder)]
pub struct FirestoreErrorInTransaction {
    /// The ID of the transaction in which the error occurred.
    pub transaction_id: FirestoreTransactionId,
    /// The underlying error that caused the transaction to fail.
    pub source: Box<dyn std::error::Error + Send + Sync>,
}

impl FirestoreErrorInTransaction {
    /// Wraps an error as a permanent `BackoffError` within a transaction context.
    ///
    /// Permanent errors are those that are unlikely to be resolved by retrying
    /// the transaction (e.g., data validation errors in user code).
    pub fn permanent<E: std::error::Error + Send + Sync + 'static>(
        transaction: &FirestoreTransaction,
        source: E,
    ) -> BackoffError<FirestoreError> {
        BackoffError::permanent(FirestoreError::ErrorInTransaction(
            FirestoreErrorInTransaction {
                transaction_id: transaction.transaction_id.clone(),
                source: Box::new(source),
            },
        ))
    }

    /// Wraps an error as a transient `BackoffError` within a transaction context.
    ///
    /// Transient errors are those that might be resolved by retrying the transaction
    /// (e.g., temporary network issues, concurrent modification conflicts).
    pub fn transient<E: std::error::Error + Send + Sync + 'static>(
        transaction: &FirestoreTransaction,
        source: E,
    ) -> BackoffError<FirestoreError> {
        BackoffError::transient(FirestoreError::ErrorInTransaction(
            FirestoreErrorInTransaction {
                transaction_id: transaction.transaction_id.clone(),
                source: Box::new(source),
            },
        ))
    }

    /// Wraps an error as a `BackoffError` that should be retried after a specific duration.
    pub fn retry_after<E: std::error::Error + Send + Sync + 'static>(
        transaction: &FirestoreTransaction,
        source: E,
        retry_after: chrono::Duration,
    ) -> BackoffError<FirestoreError> {
        BackoffError::retry_after(
            FirestoreError::ErrorInTransaction(FirestoreErrorInTransaction {
                transaction_id: transaction.transaction_id.clone(),
                source: Box::new(source),
            }),
            std::time::Duration::from_millis(retry_after.num_milliseconds() as u64),
        )
    }
}

impl Display for FirestoreErrorInTransaction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Error occurred inside run transaction scope {}: {}",
            hex::encode(&self.transaction_id),
            self.source
        )
    }
}

impl std::error::Error for FirestoreErrorInTransaction {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// A type alias for `backoff::Error<E>`, commonly used for operations
/// that support retry mechanisms with backoff strategies.
pub type BackoffError<E> = backoff::Error<E>;

pub(crate) fn firestore_err_to_backoff(err: FirestoreError) -> BackoffError<FirestoreError> {
    match err {
        FirestoreError::DatabaseError(ref db_err) if db_err.retry_possible => {
            backoff::Error::transient(err)
        }
        other => backoff::Error::permanent(other),
    }
}

pub(crate) type AnyBoxedErrResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

impl From<std::io::Error> for FirestoreError {
    fn from(io_error: std::io::Error) -> Self {
        FirestoreError::SystemError(FirestoreSystemError::new(
            FirestoreErrorPublicGenericDetails::new(format!("{:?}", io_error.kind())),
            format!("I/O error: {io_error}"),
        ))
    }
}

#[cfg(feature = "caching-persistent")]
impl From<gcloud_sdk::prost::EncodeError> for FirestoreError {
    fn from(err: gcloud_sdk::prost::EncodeError) -> Self {
        FirestoreError::SerializeError(FirestoreSerializationError::new(
            FirestoreErrorPublicGenericDetails::new("PrototBufEncodeError".into()),
            format!("Protobuf serialization error: {err}"),
        ))
    }
}

#[cfg(feature = "caching-persistent")]
impl From<gcloud_sdk::prost::DecodeError> for FirestoreError {
    fn from(err: gcloud_sdk::prost::DecodeError) -> Self {
        FirestoreError::SerializeError(FirestoreSerializationError::new(
            FirestoreErrorPublicGenericDetails::new("PrototBufDecodeError".into()),
            format!("Protobuf deserialization error: {err}"),
        ))
    }
}
