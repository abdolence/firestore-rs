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

#[test]
fn test_paths_star_pub_only() {
    #[derive(firestore::struct_path::StructPath)]
    #[allow(dead_code)]
    struct MyTestStructure {
        pub some_id: String,
        some_internal: u64,
    }
    assert_eq!(
        firestore::paths!(MyTestStructure::*),
        vec!["some_id".to_string()]
    );
}

#[test]
fn test_paths_star_visibility_all() {
    #[derive(firestore::struct_path::StructPath)]
    #[allow(dead_code)]
    struct MyTestStructure {
        pub some_id: String,
        some_internal: u64,
    }
    assert_eq!(
        firestore::paths!(MyTestStructure::*; visibility = "all"),
        vec!["some_id".to_string(), "some_internal".to_string()]
    );
}

#[test]
fn test_paths_camel_case_star() {
    #[derive(firestore::struct_path::StructPath)]
    #[allow(dead_code)]
    struct MyTestStructure {
        pub one_more_string: String,
        some_internal: u64,
    }
    assert_eq!(
        firestore::paths_camel_case!(MyTestStructure::*),
        vec!["oneMoreString".to_string()]
    );
}

#[test]
fn test_paths_camel_case_star_with_options() {
    #[derive(firestore::struct_path::StructPath)]
    #[allow(dead_code)]
    struct MyTestStructure {
        pub one_more_string: String,
        some_internal: u64,
    }
    assert_eq!(
        firestore::paths_camel_case!(MyTestStructure::*; visibility = "all"),
        vec!["oneMoreString".to_string(), "someInternal".to_string()]
    );
}

#[allow(dead_code)]
#[derive(firestore::struct_path::StructPath)]
struct NestedChild {
    pub child_id: String,
    pub child_value: u64,
}

#[allow(dead_code)]
struct NestedParent {
    some_id: String,
    child: NestedChild,
    opt_child: Option<NestedChild>,
}

#[test]
fn test_paths_nested_star() {
    assert_eq!(
        firestore::paths!(NestedParent::child.(NestedChild::*)),
        vec![
            "child.child_id".to_string(),
            "child.child_value".to_string()
        ]
    );
    assert_eq!(
        firestore::paths!(NestedParent::opt_child~(NestedChild::*)),
        vec![
            "opt_child.child_id".to_string(),
            "opt_child.child_value".to_string()
        ]
    );
}

#[test]
fn test_paths_camel_case_nested_star() {
    assert_eq!(
        firestore::paths_camel_case!(NestedParent::child.(NestedChild::*)),
        vec!["child.childId".to_string(), "child.childValue".to_string()]
    );
    assert_eq!(
        firestore::paths_camel_case!(NestedParent::opt_child~(NestedChild::*)),
        vec![
            "optChild.childId".to_string(),
            "optChild.childValue".to_string()
        ]
    );
}

#[allow(dead_code)]
#[derive(firestore::struct_path::StructPath)]
struct NestedPrivateChild {
    pub child_id: String,
    child_secret: u64,
}

#[allow(dead_code)]
struct NestedPrivateParent {
    child: NestedPrivateChild,
}

#[test]
fn test_paths_nested_star_with_private_field() {
    assert_eq!(
        firestore::paths!(NestedPrivateParent::child.(NestedPrivateChild::*)),
        vec!["child.child_id".to_string()]
    );
    assert_eq!(
        firestore::paths!(NestedPrivateParent::child.(NestedPrivateChild::*); visibility = "all"),
        vec![
            "child.child_id".to_string(),
            "child.child_secret".to_string()
        ]
    );
    assert_eq!(
        firestore::paths_camel_case!(NestedPrivateParent::child.(NestedPrivateChild::*)),
        vec!["child.childId".to_string()]
    );
    assert_eq!(
        firestore::paths_camel_case!(
            NestedPrivateParent::child.(NestedPrivateChild::*); visibility = "all"
        ),
        vec!["child.childId".to_string(), "child.childSecret".to_string()]
    );
}

#[test]
fn test_camel_case_star_options_trailing_comma() {
    #[derive(firestore::struct_path::StructPath)]
    #[allow(dead_code)]
    struct MyTestStructure {
        pub one_more_string: String,
        some_internal: u64,
    }
    assert_eq!(
        firestore::paths_camel_case!(MyTestStructure::*; visibility = "all",),
        vec!["oneMoreString".to_string(), "someInternal".to_string()]
    );
    assert_eq!(
        firestore::paths_camel_case!(NestedParent::child.(NestedChild::*); delim = "/",),
        vec!["child/childId".to_string(), "child/childValue".to_string()]
    );
}

#[test]
fn test_camel_case_star_accepts_caller_camel_case() {
    #[derive(firestore::struct_path::StructPath)]
    #[allow(dead_code)]
    struct MyTestStructure {
        pub one_more_string: String,
    }
    assert_eq!(
        firestore::paths_camel_case!(MyTestStructure::*; case = "camel"),
        vec!["oneMoreString".to_string()]
    );
}

#[test]
fn test_path_camel_case_with_options() {
    assert_eq!(
        firestore::path_camel_case!(NestedParent::child.child_value; delim = "/"),
        "child/childValue"
    );
}

#[test]
fn test_paths_camel_case_fields_with_options() {
    assert_eq!(
        firestore::paths_camel_case!(NestedParent::{some_id, child.child_value}; delim = "/"),
        vec!["someId".to_string(), "child/childValue".to_string()]
    );
}

#[test]
fn test_camel_case_overrides_caller_case_in_field_paths() {
    assert_eq!(
        firestore::path_camel_case!(NestedParent::child.child_value; case = "pascal"),
        "child.childValue"
    );
    assert_eq!(
        firestore::paths_camel_case!(NestedParent::{some_id, child.child_value}; case = "pascal"),
        vec!["someId".to_string(), "child.childValue".to_string()]
    );
}

#[test]
fn test_camel_case_empty_options() {
    assert_eq!(
        firestore::path_camel_case!(NestedParent::child.child_value;),
        "child.childValue"
    );
    assert_eq!(
        firestore::paths_camel_case!(NestedParent::{some_id, child.child_value};),
        vec!["someId".to_string(), "child.childValue".to_string()]
    );
}

#[test]
fn test_camel_case_options_trailing_comma() {
    assert_eq!(
        firestore::path_camel_case!(NestedParent::child.child_value; delim = "/",),
        "child/childValue"
    );
    assert_eq!(
        firestore::paths_camel_case!(NestedParent::{some_id, child.child_value}; delim = "/",),
        vec!["someId".to_string(), "child/childValue".to_string()]
    );
}

#[test]
fn test_multi_struct_paths() {
    assert_eq!(
        firestore::path!(NestedParent::child, NestedChild::child_value),
        "child.child_value"
    );
    assert_eq!(
        firestore::path_camel_case!(NestedParent::child, NestedChild::child_value; delim = "/"),
        "child/childValue"
    );
    assert_eq!(
        firestore::paths_camel_case!(NestedParent::{some_id, child}, NestedChild::child_id),
        vec![
            "someId".to_string(),
            "child".to_string(),
            "childId".to_string()
        ]
    );
}

#[allow(dead_code)]
struct LongPathChild {
    field_aa: u64,
    field_ab: u64,
    field_ac: u64,
    field_ad: u64,
    field_ae: u64,
    field_ba: u64,
    field_bb: u64,
    field_bc: u64,
    field_bd: u64,
    field_be: u64,
    field_ca: u64,
    field_cb: u64,
    field_cc: u64,
    field_cd: u64,
    field_ce: u64,
    field_da: u64,
    field_db: u64,
    field_dc: u64,
    field_dd: u64,
    field_de: u64,
    field_ea: u64,
    field_eb: u64,
    field_ec: u64,
    field_ed: u64,
    field_ee: u64,
    field_fa: u64,
    field_fb: u64,
    field_fc: u64,
    field_fd: u64,
    field_fe: u64,
    field_ga: u64,
    field_gb: u64,
    field_gc: u64,
    field_gd: u64,
    field_ge: u64,
    field_ha: u64,
    field_hb: u64,
    field_hc: u64,
    field_hd: u64,
    field_he: u64,
}

#[allow(dead_code)]
struct LongPathParent {
    child: LongPathChild,
}

#[test]
fn test_paths_camel_case_many_dotted_paths_with_options() {
    let expected: Vec<String> = vec![
        "child/fieldAa",
        "child/fieldAb",
        "child/fieldAc",
        "child/fieldAd",
        "child/fieldAe",
        "child/fieldBa",
        "child/fieldBb",
        "child/fieldBc",
        "child/fieldBd",
        "child/fieldBe",
        "child/fieldCa",
        "child/fieldCb",
        "child/fieldCc",
        "child/fieldCd",
        "child/fieldCe",
        "child/fieldDa",
        "child/fieldDb",
        "child/fieldDc",
        "child/fieldDd",
        "child/fieldDe",
        "child/fieldEa",
        "child/fieldEb",
        "child/fieldEc",
        "child/fieldEd",
        "child/fieldEe",
        "child/fieldFa",
        "child/fieldFb",
        "child/fieldFc",
        "child/fieldFd",
        "child/fieldFe",
        "child/fieldGa",
        "child/fieldGb",
        "child/fieldGc",
        "child/fieldGd",
        "child/fieldGe",
        "child/fieldHa",
        "child/fieldHb",
        "child/fieldHc",
        "child/fieldHd",
        "child/fieldHe",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    assert_eq!(
        firestore::paths_camel_case!(
            LongPathParent::child.field_aa,
            LongPathParent::child.field_ab,
            LongPathParent::child.field_ac,
            LongPathParent::child.field_ad,
            LongPathParent::child.field_ae,
            LongPathParent::child.field_ba,
            LongPathParent::child.field_bb,
            LongPathParent::child.field_bc,
            LongPathParent::child.field_bd,
            LongPathParent::child.field_be,
            LongPathParent::child.field_ca,
            LongPathParent::child.field_cb,
            LongPathParent::child.field_cc,
            LongPathParent::child.field_cd,
            LongPathParent::child.field_ce,
            LongPathParent::child.field_da,
            LongPathParent::child.field_db,
            LongPathParent::child.field_dc,
            LongPathParent::child.field_dd,
            LongPathParent::child.field_de,
            LongPathParent::child.field_ea,
            LongPathParent::child.field_eb,
            LongPathParent::child.field_ec,
            LongPathParent::child.field_ed,
            LongPathParent::child.field_ee,
            LongPathParent::child.field_fa,
            LongPathParent::child.field_fb,
            LongPathParent::child.field_fc,
            LongPathParent::child.field_fd,
            LongPathParent::child.field_fe,
            LongPathParent::child.field_ga,
            LongPathParent::child.field_gb,
            LongPathParent::child.field_gc,
            LongPathParent::child.field_gd,
            LongPathParent::child.field_ge,
            LongPathParent::child.field_ha,
            LongPathParent::child.field_hb,
            LongPathParent::child.field_hc,
            LongPathParent::child.field_hd,
            LongPathParent::child.field_he;
            delim = "/"
        ),
        expected
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
