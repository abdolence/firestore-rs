//! The crate's error type and the pieces it is built from.
//!
//! Reach for [`FirestoreError`] to match on what went wrong - every fallible call in this crate
//! returns it. [`crate::errors::BackoffError`] is the separate type a
//! [`FirestoreDb::run_transaction`](crate::FirestoreDb::run_transaction) closure returns, to say
//! whether a failure should be retried.

use crate::{FirestoreTransaction, FirestoreTransactionId};
use gcloud_sdk::google::firestore::v1::WriteRequest;
use rsb_derive::Builder;
use serde::*;
use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;
use std::time::Duration;

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
    /// that already exists, a write that would duplicate an entry of a unique index,
    /// or an optimistic locking failure.
    DataConflictError(FirestoreDataConflictError),
    /// An error indicating that the requested data (e.g., a document or collection)
    /// was not found.
    DataNotFoundError(FirestoreDataNotFoundError),
    /// An error due to invalid parameters provided by the client for an operation.
    InvalidParametersError(FirestoreInvalidParametersError),
    /// An error that occurred during the serialisation of data to be sent to Firestore, or
    /// while converting it to JSON.
    SerializeError(FirestoreSerializationError),
    /// An error that occurred during the deserialisation of data received from Firestore, or
    /// while converting it from JSON.
    DeserializeError(FirestoreSerializationError),
    /// An error related to network connectivity or communication with the Firestore service.
    NetworkError(FirestoreNetworkError),
    /// An error that occurred specifically within the context of a Firestore transaction.
    ErrorInTransaction(FirestoreErrorInTransaction),
    /// An error related to the caching layer, if enabled and used.
    CacheError(FirestoreCacheError),
}

impl FirestoreError {
    /// Builds an [`InvalidParametersError`](FirestoreError::InvalidParametersError) naming the
    /// offending field and why it was rejected.
    pub(crate) fn invalid_parameters(field: impl Into<String>, error: impl Into<String>) -> Self {
        FirestoreError::InvalidParametersError(FirestoreInvalidParametersError::new(
            FirestoreInvalidParametersPublicDetails::new(field.into(), error.into()),
        ))
    }

    /// Builds an [`InvalidParametersError`](FirestoreError::InvalidParametersError) for SQL text,
    /// naming the refused `construct` and where it starts.
    #[cfg(feature = "sql")]
    pub(crate) fn invalid_sql(
        construct: impl Into<String>,
        error: impl Into<String>,
        location: Option<FirestoreSqlLocation>,
    ) -> Self {
        Self::invalid_parameters(construct, error).located(location)
    }

    /// Builds an [`InvalidParametersError`](FirestoreError::InvalidParametersError) for a SQL
    /// `construct` that has no pipeline form.
    #[cfg(feature = "sql")]
    pub(crate) fn unsupported_sql(construct: &str, location: Option<FirestoreSqlLocation>) -> Self {
        FirestoreError::invalid_sql(
            construct,
            format!("{construct} has no pipeline form in SQL"),
            location,
        )
    }

    /// Builds an [`InvalidParametersError`](FirestoreError::InvalidParametersError) for a SQL
    /// `parameter` used where a field name belongs.
    #[cfg(feature = "sql")]
    pub(crate) fn sql_parameter_names_field(
        parameter: &str,
        location: Option<FirestoreSqlLocation>,
    ) -> Self {
        FirestoreError::invalid_sql(
            parameter,
            "a parameter is a value and cannot name a field",
            location,
        )
    }

    /// Builds an [`InvalidParametersError`](FirestoreError::InvalidParametersError) for SQL text
    /// that is no statement the crate runs, at the word where it stops being one.
    #[cfg(feature = "sql")]
    pub(crate) fn sql_not_a_statement(location: Option<FirestoreSqlLocation>) -> Self {
        FirestoreError::invalid_sql(
            "statement",
            "a statement is a SELECT, INSERT, UPDATE, DELETE, EXPLAIN, SHOW, \
             CREATE [UNIQUE | SEARCH | VECTOR] INDEX, DROP INDEX or ALTER COLLECTION",
            location,
        )
    }

    /// Builds an [`InvalidParametersError`](FirestoreError::InvalidParametersError) for a SQL
    /// index statement in a build without the `admin` feature.
    #[cfg(all(feature = "sql", not(feature = "admin")))]
    pub(crate) fn sql_needs_admin(statement: &str, location: Option<FirestoreSqlLocation>) -> Self {
        FirestoreError::invalid_sql(
            statement,
            format!("{statement} needs the admin feature"),
            location,
        )
    }

    /// Builds an [`InvalidParametersError`](FirestoreError::InvalidParametersError) for a SQL
    /// statement sent to a terminal other than `run()`, the one that runs it.
    #[cfg(feature = "sql")]
    pub(crate) fn sql_runs_with_run() -> Self {
        FirestoreError::invalid_parameters(
            "sql",
            "SHOW and the index statements run with run(), which returns their outcome",
        )
    }

    /// Builds an [`InvalidParametersError`](FirestoreError::InvalidParametersError) for a SQL
    /// construct that codegen does not show, such as `construct`.
    #[cfg(feature = "codegen")]
    pub(crate) fn sql_codegen_refused(construct: &str) -> Self {
        FirestoreError::invalid_parameters("codegen", format!("codegen does not show {construct}"))
    }

    /// Points an [`InvalidParametersError`](FirestoreError::InvalidParametersError) from a check
    /// the crate shares, such as an ID's, at where the value starts in SQL text.
    #[cfg(feature = "sql")]
    pub(crate) fn located(self, location: Option<FirestoreSqlLocation>) -> Self {
        match self {
            FirestoreError::InvalidParametersError(mut error) => {
                error.public.location = location;
                FirestoreError::InvalidParametersError(error)
            }
            other => other,
        }
    }

    /// The error for SQL text that sqlparser's tokenizer rejects.
    #[cfg(feature = "sql")]
    pub(crate) fn from_sql_tokenizer(error: sqlparser::tokenizer::TokenizerError) -> Self {
        FirestoreError::invalid_sql(
            "sql",
            error.message,
            FirestoreSqlLocation::from_sqlparser(error.location),
        )
    }

    /// The error for SQL text that sqlparser's parser rejects.
    #[cfg(feature = "sql")]
    pub(crate) fn from_sql_parser(error: sqlparser::parser::ParserError) -> Self {
        use sqlparser::parser::ParserError;
        match error {
            ParserError::RecursionLimitExceeded => FirestoreError::invalid_sql(
                "nesting",
                "the statement nests deeper than the parser allows",
                None,
            ),
            ParserError::TokenizerError(message) | ParserError::ParserError(message) => {
                // sqlparser gives the position only as the end of its message.
                let located = message
                    .rsplit_once(" at Line: ")
                    .and_then(|(text, position)| {
                        let (line, column) = position.split_once(", Column: ")?;
                        let location =
                            FirestoreSqlLocation::new(line.parse().ok()?, column.parse().ok()?);
                        Some((text.to_string(), location))
                    });
                match located {
                    Some((text, location)) => {
                        FirestoreError::invalid_sql("sql", text, Some(location))
                    }
                    None => FirestoreError::invalid_sql("sql", message, None),
                }
            }
        }
    }

    /// Builds a [`SystemError`](FirestoreError::SystemError) with the generic `SystemError` code.
    pub(crate) fn system_error(message: impl Into<String>) -> Self {
        FirestoreError::SystemError(FirestoreSystemError::new(
            FirestoreErrorPublicGenericDetails::new("SystemError".into()),
            message.into(),
        ))
    }

    /// Builds a [`SystemError`](FirestoreError::SystemError) with `code`, for a failure the crate
    /// itself found rather than one Firestore reported.
    #[cfg(feature = "admin")]
    pub(crate) fn system_error_with_code(code: &str, message: impl Into<String>) -> Self {
        FirestoreError::SystemError(FirestoreSystemError::new(
            FirestoreErrorPublicGenericDetails::new(code.to_string()),
            message.into(),
        ))
    }

    /// Builds a [`DataNotFoundError`](FirestoreError::DataNotFoundError) with `code`, for data
    /// the crate itself found missing rather than Firestore's `NOT_FOUND`.
    #[cfg(feature = "admin")]
    pub(crate) fn data_not_found(code: &str, message: impl Into<String>) -> Self {
        FirestoreError::DataNotFoundError(FirestoreDataNotFoundError::new(
            FirestoreErrorPublicGenericDetails::new(code.to_string()),
            message.into(),
        ))
    }

    /// Builds a [`DataConflictError`](FirestoreError::DataConflictError) with `code`, for a
    /// conflict the crate itself found rather than Firestore's `ALREADY_EXISTS`.
    #[cfg(feature = "admin")]
    pub(crate) fn data_conflict(code: &str, message: impl Into<String>) -> Self {
        FirestoreError::DataConflictError(FirestoreDataConflictError::new(
            FirestoreErrorPublicGenericDetails::new(code.to_string()),
            message.into(),
        ))
    }

    pub(crate) fn retry_possible(&self) -> bool {
        matches!(self, FirestoreError::DatabaseError(error) if error.retry_possible)
    }

    /// Transient when the request may succeed if sent again, permanent otherwise.
    pub(crate) fn into_backoff_error(self) -> BackoffError<FirestoreError> {
        if self.retry_possible() {
            BackoffError::transient(self)
        } else {
            BackoffError::permanent(self)
        }
    }

    /// Clears `retry_possible`, for the failure of a write that may have left part of its
    /// documents written, which nothing may send again.
    pub(crate) fn without_retry(self) -> Self {
        match self {
            FirestoreError::DatabaseError(error) => {
                FirestoreError::DatabaseError(FirestoreDatabaseError {
                    retry_possible: false,
                    ..error
                })
            }
            other => other,
        }
    }

    /// Converts the failed `status` of a write that commits, like the conversion from
    /// `tonic::Status` except that only `ABORTED` is `retry_possible`: it means nothing was
    /// applied, while any other code can leave the outcome unknown, so the write must not be
    /// sent again.
    pub(crate) fn from_commit_status(status: gcloud_sdk::tonic::Status) -> Self {
        let aborted = status.code() == gcloud_sdk::tonic::Code::Aborted;
        let error = FirestoreError::from(status);
        if aborted {
            error
        } else {
            error.without_retry()
        }
    }

    /// Converts a read's failed `status` like the conversion from `tonic::Status`, except that any
    /// `DEADLINE_EXCEEDED`, the server's own included, is marked `retry_possible`: a read, unlike
    /// a write, can safely be sent again.
    pub(crate) fn from_read_status(status: gcloud_sdk::tonic::Status) -> Self {
        match status.code() {
            gcloud_sdk::tonic::Code::DeadlineExceeded => FirestoreError::retryable_read(status),
            _ => FirestoreError::from(status),
        }
    }

    /// Builds a [`DatabaseError`](FirestoreError::DatabaseError) for a read's failed `status`,
    /// marked `retry_possible` whatever its code. The conversion from `tonic::Status` also covers
    /// writes, so it leaves some codes a read may safely retry unretried; callers choose which
    /// codes qualify by matching on `status.code()` first.
    pub(crate) fn retryable_read(status: gcloud_sdk::tonic::Status) -> Self {
        FirestoreError::DatabaseError(FirestoreDatabaseError::new(
            FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
            status.to_string(),
            true,
        ))
    }
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
/// This can occur, for example, if trying to create a document that already exists,
/// if a write would give two documents the same entry in a unique index, or if an
/// optimistic locking condition (e.g., based on `update_time`) is not met.
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
    /// The name of the field or parameter that was invalid. For SQL text, the construct that was
    /// refused, such as `TABLESAMPLE` or `@genre`, or `sql` for text that does not parse.
    pub field: String,
    /// A description of why the parameter is considered invalid.
    pub error: String,
    /// Where the refused construct starts in SQL text, when the error is about SQL text and the
    /// position is known.
    pub location: Option<FirestoreSqlLocation>,
}

impl Display for FirestoreInvalidParametersPublicDetails {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Invalid parameters error: {}. {}",
            self.field, self.error
        )?;
        if let Some(location) = &self.location {
            write!(f, " (at {location})")?;
        }
        Ok(())
    }
}

/// A position in SQL text: lines and columns count from 1, and columns count characters.
#[derive(Debug, Eq, PartialEq, Clone, Copy, Builder, Serialize, Deserialize)]
pub struct FirestoreSqlLocation {
    pub line: u64,
    pub column: u64,
}

impl Display for FirestoreSqlLocation {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "line {}, column {}", self.line, self.column)
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
        write!(f, "{}", self.public)
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

#[cfg(feature = "sql")]
impl FirestoreSqlLocation {
    /// `None` for the empty location sqlparser gives what it did not read from the text.
    pub(crate) fn from_sqlparser(location: sqlparser::tokenizer::Location) -> Option<Self> {
        (location.line > 0).then_some(Self::new(location.line, location.column))
    }
}

/// Converts a failed long-running admin operation's status. Its `code` is a `google.rpc.Code`
/// value with the same numbering as [`gcloud_sdk::tonic::Code`], but the operation carries it as
/// a bare `i32` rather than a `tonic::Status`. Rebuilding a `tonic::Status` from it and reusing
/// `From<tonic::Status>` below keeps the code-name reporting and the retryable classification
/// (`Unavailable`, `Aborted`, ...) in one place instead of a second copy that numbered the code
/// and never marked anything retryable.
#[cfg(feature = "admin")]
impl From<gcloud_sdk::google::rpc::Status> for FirestoreError {
    fn from(status: gcloud_sdk::google::rpc::Status) -> Self {
        gcloud_sdk::tonic::Status::new(gcloud_sdk::tonic::Code::from(status.code), status.message)
            .into()
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
            gcloud_sdk::tonic::Code::Unknown => check_hyper_errors(status),
            code => {
                let retry_possible = match code {
                    gcloud_sdk::tonic::Code::Aborted
                    | gcloud_sdk::tonic::Code::Cancelled
                    | gcloud_sdk::tonic::Code::Unavailable
                    | gcloud_sdk::tonic::Code::ResourceExhausted => true,
                    // A request timeout that ran out leaves the outcome as unknown as a dropped
                    // connection does, so it is retried like one. The server's own deadline is
                    // retried by reads alone, through `from_read_status`.
                    gcloud_sdk::tonic::Code::DeadlineExceeded => status
                        .source()
                        .is_some_and(|source| source.is::<gcloud_sdk::RequestTimeoutElapsed>()),
                    _ => false,
                };
                FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                    FirestoreErrorPublicGenericDetails::new(format!("{code:?}")),
                    format!("{status}"),
                    retry_possible,
                ))
            }
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
    /// Builds the error of a JSON conversion, with `code`.
    #[cfg(feature = "json")]
    pub(crate) fn json(
        code: &str,
        message: impl Into<String>,
        document_path: Option<&str>,
    ) -> Self {
        FirestoreSerializationError::new(
            FirestoreErrorPublicGenericDetails::new(code.to_string()),
            message.into(),
        )
        .opt_document_path(document_path.map(str::to_string))
    }

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

impl From<jiff::Error> for FirestoreError {
    fn from(err: jiff::Error) -> Self {
        FirestoreError::DeserializeError(FirestoreSerializationError::from_message(format!(
            "Date/time error: {err}"
        )))
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
                transaction_id: transaction.transaction_id().clone(),
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
                transaction_id: transaction.transaction_id().clone(),
                source: Box::new(source),
            },
        ))
    }

    /// Wraps an error as a transient `BackoffError` within a transaction context, retried after
    /// `retry_after` instead of the backoff delay. The retry still counts against
    /// `max_retries`, and it does not run if `retry_after` would end past
    /// `retry_max_elapsed_time`.
    pub fn retry_after<E: std::error::Error + Send + Sync + 'static>(
        transaction: &FirestoreTransaction,
        source: E,
        retry_after: Duration,
    ) -> BackoffError<FirestoreError> {
        BackoffError::retry_after(
            FirestoreError::ErrorInTransaction(FirestoreErrorInTransaction {
                transaction_id: transaction.transaction_id().clone(),
                source: Box::new(source),
            }),
            retry_after,
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

/// The error a [`FirestoreDb::run_transaction`](crate::FirestoreDb::run_transaction) closure
/// returns, to say whether the transaction should be retried.
///
/// `?` inside the closure converts any error into [`BackoffError::Transient`] through the blanket
/// `From`, so the ordinary error paths retry. Return [`BackoffError::Permanent`] for an error that
/// must not be retried.
#[derive(Debug, PartialEq)]
pub enum BackoffError<E> {
    /// An error that is not retried: the transaction is rolled back and the error returned.
    Permanent(E),
    /// An error that retries the transaction, after `retry_after` when it is set and after the
    /// next backoff delay otherwise.
    //
    // A struct variant, not a named struct, so that closures written against the `backoff` crate's
    // `Error::Transient` compile unchanged.
    Transient {
        err: E,
        retry_after: Option<Duration>,
    },
}

impl<E> BackoffError<E> {
    /// An error that is not retried.
    pub fn permanent(err: E) -> Self {
        Self::Permanent(err)
    }

    /// An error that is retried after the next backoff delay.
    pub fn transient(err: E) -> Self {
        Self::Transient {
            err,
            retry_after: None,
        }
    }

    /// An error that is retried after `retry_after` instead of the backoff delay. The retry still
    /// counts against `max_retries` and `retry_max_elapsed_time`.
    pub fn retry_after(err: E, retry_after: Duration) -> Self {
        Self::Transient {
            err,
            retry_after: Some(retry_after),
        }
    }
}

impl<E> From<E> for BackoffError<E> {
    fn from(err: E) -> Self {
        Self::transient(err)
    }
}

impl<E: std::fmt::Display> std::fmt::Display for BackoffError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Permanent(err) | Self::Transient { err, .. } => err.fmt(formatter),
        }
    }
}

impl<E: std::error::Error> std::error::Error for BackoffError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Permanent(err) | Self::Transient { err, .. } => err.source(),
        }
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

#[cfg(any(feature = "caching-persistent", feature = "admin"))]
impl From<gcloud_sdk::prost::DecodeError> for FirestoreError {
    fn from(err: gcloud_sdk::prost::DecodeError) -> Self {
        FirestoreError::SerializeError(FirestoreSerializationError::new(
            FirestoreErrorPublicGenericDetails::new("PrototBufDecodeError".into()),
            format!("Protobuf deserialization error: {err}"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn a_client_timeout_is_retryable_and_a_server_deadline_is_not() {
        let client_timeout = gcloud_sdk::tonic::Status::from(
            gcloud_sdk::RequestTimeoutElapsed::Idle(Duration::from_secs(5)),
        );
        let server_deadline = gcloud_sdk::tonic::Status::deadline_exceeded("Deadline Exceeded");
        for (status, retry_possible) in [(client_timeout, true), (server_deadline, false)] {
            match FirestoreError::from(status) {
                FirestoreError::DatabaseError(err) => {
                    assert_eq!(err.public.code, "DeadlineExceeded");
                    assert_eq!(err.retry_possible, retry_possible, "{err:?}");
                }
                other => panic!("expected DatabaseError, got {other:?}"),
            }
        }
    }

    #[cfg(feature = "admin")]
    #[test]
    fn rpc_status_maps_like_the_equivalent_tonic_status() {
        let rpc_status = gcloud_sdk::google::rpc::Status {
            code: gcloud_sdk::tonic::Code::FailedPrecondition as i32,
            message: "precondition failed".to_string(),
            details: vec![],
        };
        let from_rpc = FirestoreError::from(rpc_status);
        let from_tonic = FirestoreError::from(gcloud_sdk::tonic::Status::new(
            gcloud_sdk::tonic::Code::FailedPrecondition,
            "precondition failed",
        ));
        match (from_rpc, from_tonic) {
            (
                FirestoreError::DatabaseError(from_rpc),
                FirestoreError::DatabaseError(from_tonic),
            ) => {
                assert_eq!(from_rpc.public.code, from_tonic.public.code);
                assert_eq!(from_rpc.public.code, "FailedPrecondition");
                assert_eq!(from_rpc.retry_possible, from_tonic.retry_possible);
                assert!(!from_rpc.retry_possible);
            }
            other => panic!("expected DatabaseError for both conversions, got {other:?}"),
        }
    }

    #[cfg(feature = "admin")]
    #[test]
    fn rpc_status_marks_unavailable_as_retryable() {
        let rpc_status = gcloud_sdk::google::rpc::Status {
            code: gcloud_sdk::tonic::Code::Unavailable as i32,
            message: "backend unavailable".to_string(),
            details: vec![],
        };
        match FirestoreError::from(rpc_status) {
            FirestoreError::DatabaseError(err) => {
                assert_eq!(err.public.code, "Unavailable");
                assert!(err.retry_possible);
            }
            other => panic!("expected DatabaseError, got {other:?}"),
        }
    }
}
