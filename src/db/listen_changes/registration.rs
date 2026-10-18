use crate::errors::*;
use crate::{FirestoreListenerTarget, FirestoreListenerTargetParams, FirestoreResult};
use rvstruct::ValueStruct;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Tells one registration of a target ID apart from an earlier or later one under the same ID, so
/// that what a stream reports for one is never applied to the other.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) struct FirestoreListenerRegistration(u64);

/// A target registered on a listener.
#[derive(Debug, Clone)]
pub(super) struct FirestoreListenerRegisteredTarget {
    pub(super) params: FirestoreListenerTargetParams,
    pub(super) registration: FirestoreListenerRegistration,
}

/// The targets a listener listens on, by ID.
pub(super) type FirestoreListenerRegisteredTargets =
    HashMap<FirestoreListenerTarget, FirestoreListenerRegisteredTarget>;

/// The set of targets a listener listens on, shared between the handle and its running loop.
///
/// A `std::sync::RwLock` rather than tokio's: it keeps
/// [`FirestoreListener::add_target`](crate::FirestoreListener::add_target) synchronous, and the
/// compiler stops a guard being held across an await. Every use site clones out what it needs and
/// drops the guard before awaiting. Across an await a target can be removed and registered again
/// under the same ID, so a change for a registration read before the await goes through the
/// `_if_current` methods.
#[derive(Clone, Default)]
pub(super) struct FirestoreListenerTargetsState {
    targets: Arc<std::sync::RwLock<FirestoreListenerRegisteredTargets>>,
    /// Shared by every clone, so that a registration value is never handed out twice.
    next_registration: Arc<AtomicU64>,
}

impl FirestoreListenerTargetsState {
    pub(super) fn read(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, FirestoreListenerRegisteredTargets> {
        self.targets.read().expect("listener targets lock poisoned")
    }

    pub(super) fn write(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, FirestoreListenerRegisteredTargets> {
        self.targets
            .write()
            .expect("listener targets lock poisoned")
    }

    pub(super) fn register(
        &self,
        target_params: FirestoreListenerTargetParams,
    ) -> FirestoreResult<()> {
        let mut targets = self.write();
        if targets.contains_key(&target_params.target) {
            return Err(FirestoreError::invalid_parameters(
                "target",
                format!(
                    "Listener target {} is already registered on this listener",
                    target_params.target.value()
                ),
            ));
        }
        let registration =
            FirestoreListenerRegistration(self.next_registration.fetch_add(1, Ordering::Relaxed));
        targets.insert(
            target_params.target.clone(),
            FirestoreListenerRegisteredTarget {
                params: target_params,
                registration,
            },
        );
        Ok(())
    }

    pub(super) fn registration_of(
        &self,
        target: &FirestoreListenerTarget,
    ) -> Option<FirestoreListenerRegistration> {
        self.read()
            .get(target)
            .map(|registered| registered.registration)
    }

    /// Applies `update` to the target's parameters if it is still registered under
    /// `registration`, and returns what it returned.
    pub(super) fn update_if_current<R>(
        &self,
        target: &FirestoreListenerTarget,
        registration: FirestoreListenerRegistration,
        update: impl FnOnce(&mut FirestoreListenerTargetParams) -> R,
    ) -> Option<R> {
        self.write()
            .get_mut(target)
            .filter(|registered| registered.registration == registration)
            .map(|registered| update(&mut registered.params))
    }

    /// Unregisters the target if it is still registered under `registration`, and returns whether
    /// it did.
    pub(super) fn remove_if_current(
        &self,
        target: &FirestoreListenerTarget,
        registration: FirestoreListenerRegistration,
    ) -> bool {
        let mut targets = self.write();
        let current = targets
            .get(target)
            .is_some_and(|registered| registered.registration == registration);
        if current {
            targets.remove(target);
        }
        current
    }
}
