//! Listening to document and query changes over Firestore's `Listen` stream.
//!
//! [`FirestoreListener`] is the handle a caller holds. It registers targets, and starts and stops
//! a loop that keeps one listen stream open for all of them.

mod listener_target;
pub use listener_target::*;

mod listener;
pub use listener::*;

mod registration;

mod event_loop;

mod stream_targets;

mod listen_support;
