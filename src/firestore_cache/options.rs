use std::ops::RangeInclusive;
use std::path::PathBuf;

use crate::FirestoreListenerTarget;
use rsb_derive::Builder;
use rvstruct::ValueStruct;

#[derive(Clone, Debug, Eq, PartialEq, Hash, ValueStruct)]
pub struct FirestoreCacheName(String);

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreCacheOptions {
    pub name: FirestoreCacheName,
    pub cache_dir: PathBuf,

    #[default = "FIRESTORE_CACHE_DEFAULT_ALLOCATED_TARGET_ID_RANGE.clone()"]
    pub allocated_target_id_range: RangeInclusive<FirestoreListenerTarget>,
}

static FIRESTORE_CACHE_DEFAULT_ALLOCATED_TARGET_ID_RANGE: RangeInclusive<FirestoreListenerTarget> =
    FirestoreListenerTarget::new(1000)..=FirestoreListenerTarget::new(2000);
