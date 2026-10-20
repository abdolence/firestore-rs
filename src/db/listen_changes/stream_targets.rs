use super::registration::{
    FirestoreListenerRegisteredTargets, FirestoreListenerRegistration,
    FirestoreListenerTargetsState,
};
use crate::db::support::FirestoreListenSupport;
use crate::{
    FirestoreListenerTarget, FirestoreListenerTargetParams, FirestoreRequestOptions,
    FirestoreResult,
};
use gcloud_sdk::google::firestore::v1::*;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Instant;
use tracing::*;

/// Where a target stands on one open listen stream, going by the requests sent on it and the
/// target changes Firestore answered them with.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum FirestoreListenerStreamTargetState {
    /// `AddTarget` was sent, and Firestore has not acknowledged it yet.
    Adding,
    /// Firestore acknowledged the `AddTarget`, so a change for every target includes this one.
    Listening,
    /// Being taken off the stream, and Firestore has not answered yet. The ID is not added again
    /// before it does, so that changes still in flight for the removed target are never taken for
    /// a new one. With `add_back_at`, the target comes back from scratch at that time.
    ///
    /// `requested` is whether a `RemoveTarget` was sent. It is not for a `once` target that is
    /// already current: Firestore ends that one by itself, and a `RemoveTarget` crossing the end
    /// would fail the whole stream.
    Removing {
        add_back_at: Option<Instant>,
        requested: bool,
    },
    /// Off the stream, and added back from scratch at that time.
    Waiting(Instant),
}

/// One registration of a target on an open listen stream.
#[derive(Debug, Clone)]
pub(super) struct FirestoreListenerStreamTarget {
    registration: FirestoreListenerRegistration,
    state: FirestoreListenerStreamTargetState,
    /// The `AddTarget` asked Firestore to remove the target once it is current.
    once: bool,
    /// Firestore has reported the target current on this stream.
    current: bool,
    /// The `AddTarget` carried a resume state, which Firestore can reject as stale.
    resumed: bool,
    /// What the target was added with, for the `RemoveTarget` that takes it off again.
    request_options: Option<FirestoreRequestOptions>,
}

impl FirestoreListenerStreamTarget {
    /// Whether Firestore removing this target with `cause` ends it for good, rather than calling
    /// for it to be added back after the retry delay. A removal with no cause ends a `once` target.
    fn ended_by(&self, cause: Option<&gcloud_sdk::google::rpc::Status>) -> bool {
        use gcloud_sdk::tonic::Code;
        let Some(cause) = cause else {
            return self.once;
        };
        match Code::from(cause.code) {
            // Denied by security rules or IAM, a missing database or parent, or a query Firestore
            // does not support: adding the target back asks for the same thing again.
            Code::PermissionDenied | Code::NotFound | Code::Unimplemented => true,
            // An invalid query, or one that needs an index that does not exist, but a stale resume
            // token or read time is rejected the same way, and the add-back drops it. Only a
            // target added from scratch shows that the query itself is at fault.
            Code::InvalidArgument | Code::FailedPrecondition => !self.resumed,
            _ => false,
        }
    }
}

/// What Firestore removing targets leaves the listener to do.
#[derive(Debug, Default)]
pub(super) struct FirestoreListenerRemoval {
    /// Registrations to add back from scratch after the retry delay.
    pub(super) retried: Vec<(FirestoreListenerTarget, FirestoreListenerRegistration)>,
    /// Registrations that have ended for good.
    pub(super) ended: Vec<(FirestoreListenerTarget, FirestoreListenerRegistration)>,
}

/// The targets on one open listen stream, and the request side that changes them.
pub(super) struct FirestoreListenerStreamTargets {
    requests: UnboundedSender<ListenRequest>,
    targets: HashMap<FirestoreListenerTarget, FirestoreListenerStreamTarget>,
    /// A `RemoveTarget` was in flight for a target Firestore removed first, with a cause or by
    /// ending a `once` target it had reported current. Firestore answers a `RemoveTarget` for a
    /// target it no longer has by failing the stream with `NOT_FOUND` instead of acknowledging it,
    /// so that failure is expected and the stream is reopened at once.
    crossed_removal: bool,
}

impl FirestoreListenerStreamTargets {
    pub(super) fn new(requests: UnboundedSender<ListenRequest>) -> Self {
        Self {
            requests,
            targets: HashMap::new(),
            crossed_removal: false,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.targets.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    pub(super) fn crossed_removal(&self) -> bool {
        self.crossed_removal
    }

    /// Whether a change that names `target` is about this registration of it.
    fn carries(
        &self,
        target: &FirestoreListenerTarget,
        registration: FirestoreListenerRegistration,
    ) -> bool {
        self.targets.get(target).is_some_and(|on_stream| {
            on_stream.registration == registration
                && matches!(
                    on_stream.state,
                    FirestoreListenerStreamTargetState::Adding
                        | FirestoreListenerStreamTargetState::Listening
                )
        })
    }

    /// Whether a change for every target on the stream includes this registration of `target`.
    fn includes(
        &self,
        target: &FirestoreListenerTarget,
        registration: FirestoreListenerRegistration,
    ) -> bool {
        self.targets.get(target).is_some_and(|on_stream| {
            on_stream.registration == registration
                && on_stream.state == FirestoreListenerStreamTargetState::Listening
        })
    }

    /// Resolves the targets a target change applies to, each with the registration it applies to.
    ///
    /// Firestore uses an empty set of target IDs to mean every target on the stream, which is how
    /// global resume tokens and stream-wide resets arrive. Such a change includes a target only
    /// once Firestore has acknowledged adding it. A change that names a target applies to it only
    /// while the stream carries the target's current registration, so a change still in flight
    /// for a removed target is not taken for a new one under the same ID. Target IDs this listener
    /// does not know about are ignored; an ID that is not a valid target at all is a protocol
    /// error and returns `Err`.
    pub(super) fn affected_targets(
        &self,
        targets_state: &FirestoreListenerTargetsState,
        target_ids: &[i32],
    ) -> Result<Vec<(FirestoreListenerTarget, FirestoreListenerRegistration)>, ()> {
        let targets = targets_state.read();

        if target_ids.is_empty() {
            return Ok(targets
                .iter()
                .filter(|(target, registered)| self.includes(target, registered.registration))
                .map(|(target, registered)| (target.clone(), registered.registration))
                .collect());
        }

        let mut affected = Vec::with_capacity(target_ids.len());
        for target_id_num in target_ids {
            match FirestoreListenerTarget::try_from(*target_id_num) {
                Ok(target_id) => {
                    if let Some(registered) = targets.get(&target_id) {
                        if self.carries(&target_id, registered.registration) {
                            affected.push((target_id, registered.registration));
                        }
                    }
                }
                Err(err) => {
                    error!(%err, target_id_num, "Listener system error - unexpected target ID.");
                    return Err(());
                }
            }
        }
        Ok(affected)
    }

    /// Whether nothing is registered and the stream has nothing left to remove, so that it can
    /// close. Removals are waited for, so that their acknowledgements still reach the callback.
    pub(super) fn is_idle(&self, targets_state: &FirestoreListenerTargetsState) -> bool {
        targets_state.read().is_empty() && !self.awaits_firestore()
    }

    /// Whether Firestore owes the stream an answer: some target on it is not waiting to be added
    /// back.
    pub(super) fn awaits_firestore(&self) -> bool {
        self.targets.values().any(|on_stream| {
            !matches!(
                on_stream.state,
                FirestoreListenerStreamTargetState::Waiting(_)
            )
        })
    }

    /// When the next waiting target is added back.
    pub(super) fn next_add_back(&self) -> Option<Instant> {
        self.targets
            .values()
            .filter_map(|on_stream| match on_stream.state {
                FirestoreListenerStreamTargetState::Waiting(at) => Some(at),
                _ => None,
            })
            .min()
    }

    /// The registered targets to add now: those the stream lacks and those whose wait is over.
    /// A waiting target that has been removed or registered again since is dropped, so that a new
    /// registration does not wait out the old one's delay.
    pub(super) fn take_due(
        &mut self,
        registered: &FirestoreListenerRegisteredTargets,
        now: Instant,
    ) -> Vec<FirestoreListenerTarget> {
        self.targets.retain(|target, on_stream| {
            !matches!(
                on_stream.state,
                FirestoreListenerStreamTargetState::Waiting(_)
            ) || registered
                .get(target)
                .is_some_and(|current| current.registration == on_stream.registration)
        });
        registered
            .keys()
            .filter(
                |target| match self.targets.get(target).map(|on_stream| on_stream.state) {
                    None => true,
                    Some(FirestoreListenerStreamTargetState::Waiting(at)) => at <= now,
                    Some(_) => false,
                },
            )
            .cloned()
            .collect()
    }

    pub(super) fn add<D: FirestoreListenSupport>(
        &mut self,
        db: &D,
        target_params: FirestoreListenerTargetParams,
        registration: FirestoreListenerRegistration,
    ) -> FirestoreResult<()> {
        let target = target_params.target.clone();
        let on_stream = FirestoreListenerStreamTarget {
            registration,
            state: FirestoreListenerStreamTargetState::Adding,
            once: target_params.add_target_once.unwrap_or(false),
            current: false,
            resumed: target_params.resume_type.is_some(),
            request_options: target_params.request_options.clone(),
        };
        // A send fails only once the stream is going away, and the reconnect adds every target.
        self.requests
            .send(db.add_target_request(target_params)?)
            .ok();
        self.targets.insert(target, on_stream);
        Ok(())
    }

    /// Takes this registration of `target` off the stream, to come back from scratch at
    /// `add_back_at` if that is set. Does nothing unless the stream carries it.
    pub(super) fn remove<D: FirestoreListenSupport>(
        &mut self,
        db: &D,
        target: &FirestoreListenerTarget,
        registration: FirestoreListenerRegistration,
        add_back_at: Option<Instant>,
    ) -> FirestoreResult<()> {
        if !self.carries(target, registration) {
            return Ok(());
        }
        if let Some(on_stream) = self.targets.get_mut(target) {
            let requested = !(on_stream.once && on_stream.current);
            if requested {
                self.requests
                    .send(db.remove_target_request(target, on_stream.request_options.as_ref())?)
                    .ok();
            }
            on_stream.state = FirestoreListenerStreamTargetState::Removing {
                add_back_at,
                requested,
            };
        }
        Ok(())
    }

    /// Records that Firestore acknowledged adding the targets.
    pub(super) fn added(&mut self, target_ids: &[i32]) {
        for target in target_ids
            .iter()
            .filter_map(|id| FirestoreListenerTarget::try_from(*id).ok())
        {
            if let Some(on_stream) = self.targets.get_mut(&target) {
                if on_stream.state == FirestoreListenerStreamTargetState::Adding {
                    on_stream.state = FirestoreListenerStreamTargetState::Listening;
                }
            }
        }
    }

    /// The targets a change for `target_ids` is about. No IDs means every target Firestore still
    /// has on the stream: those it acknowledged adding, and those a `RemoveTarget` is on its way
    /// for, which Firestore has not read yet.
    fn reported(&self, target_ids: &[i32]) -> Vec<FirestoreListenerTarget> {
        if target_ids.is_empty() {
            self.targets
                .iter()
                .filter(|(_, on_stream)| {
                    matches!(
                        on_stream.state,
                        FirestoreListenerStreamTargetState::Listening
                            | FirestoreListenerStreamTargetState::Removing {
                                requested: true,
                                ..
                            }
                    )
                })
                .map(|(target, _)| target.clone())
                .collect()
        } else {
            target_ids
                .iter()
                .filter_map(|id| FirestoreListenerTarget::try_from(*id).ok())
                .collect()
        }
    }

    /// Records that Firestore reported the targets current, those being removed included: a
    /// `once` target's `CURRENT` can arrive after our `RemoveTarget` has left.
    pub(super) fn current(&mut self, target_ids: &[i32]) {
        for target in self.reported(target_ids) {
            if let Some(on_stream) = self.targets.get_mut(&target) {
                if !matches!(
                    on_stream.state,
                    FirestoreListenerStreamTargetState::Waiting(_)
                ) {
                    on_stream.current = true;
                }
            }
        }
    }

    /// Records that Firestore removed the targets.
    ///
    /// A removal with no `cause` acknowledges a `RemoveTarget`, or ends a `once` target; one with
    /// a `cause` is Firestore removing the target on its own. Either completes a target being
    /// removed. The targets Firestore removed on its own are added back at `add_back_at`, unless
    /// the cause shows that they have ended for good.
    pub(super) fn removed(
        &mut self,
        target_ids: &[i32],
        cause: Option<&gcloud_sdk::google::rpc::Status>,
        add_back_at: Instant,
    ) -> FirestoreListenerRemoval {
        let mut removal = FirestoreListenerRemoval::default();
        for target in self.reported(target_ids) {
            let Some(on_stream) = self.targets.get(&target) else {
                continue;
            };
            let next_state = match on_stream.state {
                FirestoreListenerStreamTargetState::Removing {
                    add_back_at,
                    requested,
                } => {
                    // Firestore removing the target on its own, or ending a `once` one it reported
                    // current, crosses our `RemoveTarget`. A `once` target removed before it was
                    // current is an acknowledgement. One removed after may still be, in which case
                    // the stream simply does not fail.
                    if requested && (cause.is_some() || (on_stream.once && on_stream.current)) {
                        self.crossed_removal = true;
                    }
                    add_back_at.map(FirestoreListenerStreamTargetState::Waiting)
                }
                FirestoreListenerStreamTargetState::Adding
                | FirestoreListenerStreamTargetState::Listening => {
                    if on_stream.ended_by(cause) {
                        removal.ended.push((target.clone(), on_stream.registration));
                        None
                    } else {
                        removal
                            .retried
                            .push((target.clone(), on_stream.registration));
                        Some(FirestoreListenerStreamTargetState::Waiting(add_back_at))
                    }
                }
                FirestoreListenerStreamTargetState::Waiting(_) => continue,
            };
            match next_state {
                Some(state) => {
                    if let Some(on_stream) = self.targets.get_mut(&target) {
                        on_stream.state = state;
                    }
                }
                None => {
                    self.targets.remove(&target);
                }
            }
        }
        removal
    }
}
