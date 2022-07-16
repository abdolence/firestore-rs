#[macro_export]
macro_rules! path {
    ($($t:ident :: $f:ident),+) => {{
        let mut vec_path= vec![];

        $(
            #[allow(dead_code, unused_variables)]
            {
                fn _test_struct_field(test_struct: &$t) {
                    let _t = &test_struct.$f;
                }
                vec_path.push(
                    stringify!($f)
                );
            }
        )*

        vec_path.join(".")
    }};
}

#[macro_export]
macro_rules! paths {
    ($t:ident::{$($fs:ident),+}) => {{
        vec![
            $(
                path!($t::$fs)
            ),*
        ]
    }};
}

#[macro_export]
macro_rules! path_camel_case {
    ($($t:ident :: $f:ident),+) => {{
        use convert_case::{Case, Casing};
        let mut vec_path= vec![];

        $(
            #[allow(dead_code, unused_variables)]
            {
                fn _test_struct_field(test_struct: &$t) {
                    let _t = &test_struct.$f;
                }
                vec_path.push(
                    stringify!($f).to_case(Case::Camel)
                );
            }
        )*

        vec_path.join(".")
    }};
}

#[macro_export]
macro_rules! paths_camel_case {
    ($t:ident::{$($fs:ident),+}) => {{
        vec![
            $(
                path_camel_case!($t::$fs)
            ),*
        ]
    }};
}
