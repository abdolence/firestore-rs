#[test]
fn test_ambiguous_path_macro() {
    struct MyTestStructure {
        some_id: String,
        some_num: u64,
    }
    assert_eq!(firestore::path!(MyTestStructure::some_id), "some_id");
    assert_eq!(
        firestore::paths!(MyTestStructure::{some_id, some_num}),
        vec!["some_id".to_string(), "some_num".to_string()]
    );
    assert_eq!(
        firestore::path_camel_case!(MyTestStructure::some_id),
        "someId"
    );
    assert_eq!(
        firestore::paths_camel_case!(MyTestStructure::{some_id, some_num}),
        vec!["someId".to_string(), "someNum".to_string()]
    );
}

mod struct_path {
    #[macro_export]
    macro_rules! path {
        () => {
            unreachable!()
        };
    }

    #[macro_export]
    macro_rules! paths {
        ($($x:tt)*) => {{
            unreachable!()
        }};
    }

    #[macro_export]
    macro_rules! path_camel_case {
        ($($x:tt)*) => {{
            unreachable!()
        }};
    }

    #[macro_export]
    macro_rules! paths_camel_case {
        ($($x:tt)*) => {{
            unreachable!()
        }};
    }
}
