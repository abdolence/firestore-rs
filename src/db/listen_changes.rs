//! Listening to document and query changes over Firestore's `Listen` stream.
//!
//! [`FirestoreListener`](crate::FirestoreListener) is the handle a caller holds. It registers
//! targets, and starts and stops a loop that keeps one listen stream open for all of them:
//!
//! - `listener_target`: what a caller listens on, and the resume state a target continues from;
//! - `listener`: the handle, and the control messages it sends to the running loop;
//! - `registration`: the targets registered on a listener, shared by the handle and its loop;
//! - `event_loop`: the loop that keeps the stream open and reacts to what arrives on it;
//! - `stream_targets`: where each target stands on the open stream;
//! - `listen_support`: how a [`FirestoreDb`](crate::FirestoreDb) opens the stream and builds the
//!   requests sent on it.

mod listener_target;
pub use listener_target::*;

mod listener;
pub use listener::*;

mod registration;

mod event_loop;

mod stream_targets;

mod listen_support;

#[cfg(test)]
mod lifecycle_tests;
