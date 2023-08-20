use std::path::PathBuf;

use rsb_derive::Builder;
use rvstruct::ValueStruct;

#[derive(Clone, Debug, Eq, PartialEq, Hash, ValueStruct)]
pub struct FirestoreCacheName(String);

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreCacheOptions {
    pub name: FirestoreCacheName,
    pub cache_dir: PathBuf,
}
