use rsb_derive::Builder;
use rvstruct::ValueStruct;

/// A single request tag attached to a Firestore request.
///
/// Request tags are arbitrary strings that Firestore associates with a request and
/// reports back in its monitoring and billing breakdowns. This makes them useful to
/// attribute reads and writes to a particular feature, tenant or background job.
#[derive(Debug, Clone, Eq, PartialEq, Hash, ValueStruct)]
pub struct FirestoreRequestTag(String);

/// Options attached to an individual Firestore server request.
///
/// Request options can be specified:
/// - session wide, for every request issued by a [`FirestoreDb`](crate::FirestoreDb)
///   instance, using
///   [`FirestoreDb::clone_with_request_tags()`](crate::FirestoreDb::clone_with_request_tags)
///   or
///   [`FirestoreDb::clone_with_request_options()`](crate::FirestoreDb::clone_with_request_options);
/// - per operation, on the params structures such as
///   [`FirestoreQueryParams`](crate::FirestoreQueryParams), or through the
///   `request_tags()` / `request_options()` methods of the fluent API.
///
/// A per operation value replaces the session wide default rather than merging
/// with it.
///
/// # Examples
///
/// ```rust
/// use firestore::*;
///
/// let options = FirestoreRequestOptions::from_tags(["checkout", "tenant-42"]);
///
/// assert_eq!(
///     options.request_tags,
///     vec![
///         FirestoreRequestTag::from("checkout"),
///         FirestoreRequestTag::from("tenant-42"),
///     ]
/// );
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Builder)]
pub struct FirestoreRequestOptions {
    /// The request tags for the request.
    ///
    /// Defaults to an empty list, which is equivalent to sending no tags at all.
    #[default = "Vec::new()"]
    pub request_tags: Vec<FirestoreRequestTag>,
}

impl FirestoreRequestOptions {
    /// Creates request options from an iterator of tags.
    ///
    /// # Arguments
    /// * `tags`: An iterator of anything convertible into a [`FirestoreRequestTag`],
    ///   such as `&str` or `String`.
    #[inline]
    pub fn from_tags<I>(tags: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<FirestoreRequestTag>,
    {
        Self {
            request_tags: tags.into_iter().map(Into::into).collect(),
        }
    }

    /// Resolves the effective request options for a single operation.
    ///
    /// A per operation override takes precedence over the session wide default.
    /// Returns `None` when the resolved options carry no tags, so that no request
    /// options are attached to the request at all.
    #[inline]
    pub(crate) fn resolve(
        operation_options: Option<&FirestoreRequestOptions>,
        session_options: Option<&FirestoreRequestOptions>,
    ) -> Option<gcloud_sdk::google::firestore::v1::RequestOptions> {
        operation_options
            .or(session_options)
            .filter(|options| !options.request_tags.is_empty())
            .map(|options| options.into())
    }
}

impl From<&FirestoreRequestOptions> for gcloud_sdk::google::firestore::v1::RequestOptions {
    fn from(options: &FirestoreRequestOptions) -> Self {
        gcloud_sdk::google::firestore::v1::RequestOptions {
            request_tags: options
                .request_tags
                .iter()
                .map(|tag| tag.value().clone())
                .collect(),
        }
    }
}

impl From<FirestoreRequestOptions> for gcloud_sdk::google::firestore::v1::RequestOptions {
    fn from(options: FirestoreRequestOptions) -> Self {
        gcloud_sdk::google::firestore::v1::RequestOptions {
            request_tags: options
                .request_tags
                .into_iter()
                .map(|tag| tag.into_value())
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_tag_from_str() {
        let tag: FirestoreRequestTag = "checkout".into();
        assert_eq!(tag.value(), "checkout");
    }

    #[test]
    fn test_request_options_from_tags() {
        let options = FirestoreRequestOptions::from_tags(["tag-1", "tag-2"]);

        assert_eq!(
            options.request_tags,
            vec![
                FirestoreRequestTag::from("tag-1"),
                FirestoreRequestTag::from("tag-2"),
            ]
        );
    }

    #[test]
    fn test_request_options_new_is_empty() {
        assert!(FirestoreRequestOptions::new().request_tags.is_empty());
    }

    #[test]
    fn test_request_options_conversion() {
        let options = FirestoreRequestOptions::from_tags(["a", "b"]);

        let proto: gcloud_sdk::google::firestore::v1::RequestOptions = (&options).into();
        assert_eq!(proto.request_tags, vec!["a".to_string(), "b".to_string()]);

        let owned_proto: gcloud_sdk::google::firestore::v1::RequestOptions = options.into();
        assert_eq!(
            owned_proto.request_tags,
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn test_resolve_prefers_operation_options() {
        let operation_options = FirestoreRequestOptions::from_tags(["operation"]);
        let session_options = FirestoreRequestOptions::from_tags(["session"]);

        assert_eq!(
            FirestoreRequestOptions::resolve(Some(&operation_options), Some(&session_options))
                .map(|options| options.request_tags),
            Some(vec!["operation".to_string()])
        );

        assert_eq!(
            FirestoreRequestOptions::resolve(None, Some(&session_options))
                .map(|options| options.request_tags),
            Some(vec!["session".to_string()])
        );

        assert_eq!(
            FirestoreRequestOptions::resolve(Some(&operation_options), None)
                .map(|options| options.request_tags),
            Some(vec!["operation".to_string()])
        );

        assert_eq!(FirestoreRequestOptions::resolve(None, None), None);
    }

    #[test]
    fn test_resolve_skips_empty_tags() {
        let empty_options = FirestoreRequestOptions::new();
        let session_options = FirestoreRequestOptions::from_tags(["session"]);

        // Nothing configured at all: no request options on the wire.
        assert_eq!(FirestoreRequestOptions::resolve(None, None), None);

        // Explicitly empty options are still an override, they clear the session
        // wide tags instead of falling back to them, and nothing is sent.
        assert_eq!(
            FirestoreRequestOptions::resolve(Some(&empty_options), Some(&session_options)),
            None
        );

        assert_eq!(
            FirestoreRequestOptions::resolve(None, Some(&empty_options)),
            None
        );
    }
}
