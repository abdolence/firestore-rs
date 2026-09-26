use rand::RngExt;
use std::time::Duration;

/// How long to wait before retry number `retries + 1`: a random delay of up to `2^retries`
/// seconds.
pub(crate) fn retry_delay(retries: usize) -> Duration {
    Duration::from_millis(rand::rng().random_range(0..2u64.pow(retries as u32) * 1000 + 1))
}
