/// Builds the Firestore field path for one field of `Struct` as a `String`.
///
/// Use it wherever a fluent call takes a field path, such as a query filter or an `.order()`
/// clause. Reach into a nested struct with `.`: `path!(Parent::child.field)`. The path is built
/// from the token stream at compile time, so a renamed field shows up as a compile error at the
/// call site instead of a query that silently matches nothing.
///
/// ```rust
/// use firestore::path;
///
/// struct MyTestStructure {
///     some_id: String,
///     some_num: u64,
/// }
///
/// assert_eq!(path!(MyTestStructure::some_id), "some_id");
/// ```
///
/// Options follow after a `;`: `delim` sets the separator between path segments, and
/// `case="camel"` or `case="pascal"` converts each segment. The `struct_path` crate docs list
/// the rest.
///
/// ```rust
/// use firestore::path;
///
/// struct Child {
///     some_value: u64,
/// }
///
/// struct Parent {
///     some_child: Child,
/// }
///
/// assert_eq!(
///     path!(Parent::some_child.some_value; delim="/", case="camel"),
///     "someChild/someValue"
/// );
/// ```
#[macro_export]
macro_rules! path {
    ($($x:tt)*) => {{
        $crate::struct_path::path!($($x)*).to_string()
    }};
}

/// Builds the Firestore field paths for several fields of `Struct` as a `Vec<String>`.
///
/// Use it to select a subset of a document's fields, for example
/// `.fields(paths!(Struct::{a, b, c}))`. See [`path!`] for a single field.
///
/// ```rust
/// use firestore::paths;
///
/// struct MyTestStructure {
///     some_id: String,
///     some_num: u64,
/// }
///
/// assert_eq!(
///     paths!(MyTestStructure::{some_id, some_num}),
///     vec!["some_id".to_string(), "some_num".to_string()]
/// );
/// ```
///
/// `Struct::*` returns every field declared with plain `pub`, without listing them by hand;
/// it needs `Struct` to carry `#[derive(firestore::struct_path::StructPath)]`.
/// `Struct::*; visibility="all"` returns every declared field instead. It only works inside the
/// crate that defines `Struct`, because the derive caps the consts for non-`pub` fields at
/// `pub(crate)`; from another crate the call fails with E0624.
///
/// ```rust
/// use firestore::paths;
///
/// #[derive(firestore::struct_path::StructPath)]
/// struct MyTestStructure {
///     pub some_id: String,
///     some_internal: u64,
/// }
///
/// assert_eq!(paths!(MyTestStructure::*), vec!["some_id".to_string()]);
/// assert_eq!(
///     paths!(MyTestStructure::*; visibility="all"),
///     vec!["some_id".to_string(), "some_internal".to_string()]
/// );
/// ```
///
/// `Parent::child.(Child::*)` prefixes every field of `Child` with the `child` path, and
/// `Parent::child~(Child::*)` does the same for an `Option<Child>` field.
///
/// ```rust
/// use firestore::paths;
///
/// #[derive(firestore::struct_path::StructPath)]
/// struct Child {
///     pub child_id: String,
/// }
///
/// struct Parent {
///     child: Child,
///     opt_child: Option<Child>,
/// }
///
/// assert_eq!(paths!(Parent::child.(Child::*)), vec!["child.child_id".to_string()]);
/// assert_eq!(paths!(Parent::opt_child~(Child::*)), vec!["opt_child.child_id".to_string()]);
/// ```
#[macro_export]
macro_rules! paths {
    ($($x:tt)*) => {{
        $crate::struct_path::paths!($($x)*).iter().map(|s| s.to_string()).collect::<Vec<String>>()
    }};
}

/// Builds the Firestore field path for one field of `Struct`, converting it to camelCase.
///
/// Use this instead of [`path!`] when the struct carries `#[serde(rename_all = "camelCase")]`,
/// so the path matches the field name Firestore actually stores.
///
/// ```rust
/// use firestore::path_camel_case;
///
/// struct MyTestStructure {
///     one_more_string: String,
/// }
///
/// assert_eq!(path_camel_case!(MyTestStructure::one_more_string), "oneMoreString");
/// ```
///
/// Options can follow after a `;`, same as [`path!`]. This macro appends `case="camel"` after
/// them, so a `case` passed by the caller is overridden and the path is always camelCase.
#[macro_export]
macro_rules! path_camel_case {
    ($($x:tt)*) => {{
        $crate::struct_path::path!($($x)*; case="camel").to_string()
    }};
}

/// Builds the Firestore field paths for several fields of `Struct`, converting each to
/// camelCase.
///
/// The camelCase counterpart of [`paths!`]; use it the same way with a
/// `#[serde(rename_all = "camelCase")]` struct.
///
/// ```rust
/// use firestore::paths_camel_case;
///
/// struct MyTestStructure {
///     some_id: String,
///     one_more_string: String,
/// }
///
/// assert_eq!(
///     paths_camel_case!(MyTestStructure::{some_id, one_more_string}),
///     vec!["someId".to_string(), "oneMoreString".to_string()]
/// );
/// ```
///
/// `Struct::*` works the same way as in [`paths!`], with the same `visibility="all"` option.
/// This macro appends `case="camel"` after the caller's options. A field list ignores a
/// `case` passed by the caller and stays camelCase. `Struct::*` accepts `case="camel"` from
/// the caller, and any other `case` is a compile error.
///
/// ```rust
/// use firestore::paths_camel_case;
///
/// #[derive(firestore::struct_path::StructPath)]
/// struct MyTestStructure {
///     pub some_id: String,
///     some_internal: u64,
/// }
///
/// assert_eq!(paths_camel_case!(MyTestStructure::*), vec!["someId".to_string()]);
/// assert_eq!(
///     paths_camel_case!(MyTestStructure::*; visibility="all"),
///     vec!["someId".to_string(), "someInternal".to_string()]
/// );
/// ```
///
/// ```compile_fail
/// use firestore::paths_camel_case;
///
/// #[derive(firestore::struct_path::StructPath)]
/// struct MyTestStructure {
///     pub some_id: String,
/// }
///
/// let _ = paths_camel_case!(MyTestStructure::*; case="pascal");
/// ```
#[macro_export]
macro_rules! paths_camel_case {
    ($($x:tt)*) => {{
        $crate::struct_path::paths!($($x)*; case="camel").into_iter().map(|s| s.to_string()).collect::<Vec<String>>()
    }};
}
