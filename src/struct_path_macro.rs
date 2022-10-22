#[macro_export]
macro_rules! path {
    ($($x:tt)*) => {{
        struct_path::path!($($x)*).to_string()
    }};
}

#[macro_export]
macro_rules! paths {
    ($($x:tt)*) => {{
        struct_path::path!($($x)*).into_iter().map(|s| s.to_string()).collect::<Vec<String>>()
    }};
}

#[macro_export]
macro_rules! path_camel_case {
    ($($x:tt)*) => {{
        struct_path::path!($($x)*;case="camel").to_string()
    }};
}

#[macro_export]
macro_rules! paths_camel_case {
    ($($x:tt)*) => {{
        struct_path::path!($($x)*;case="camel").into_iter().map(|s| s.to_string()).collect::<Vec<String>>()
    }}
}
