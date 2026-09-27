//! Coordinates index syncs of one collection group across callers, such as the replicas of one
//! deployment, through one document per group: `{coordination_collection}/{collection_group}`.
//!
//! The document holds the highest generation any caller claimed the group with, and at most one
//! lease. Every read and write goes through the crate's own data API, in transactions, so two
//! callers deciding at once see each other's claim: Firestore aborts one of them and
//! [`FirestoreDb::run_transaction`] runs it again against the new state.
//!
//! A lease is judged expired from Firestore's clock alone: `lease_renewed_at`, set by the server
//! to the time of the claim or renewal that wrote it, plus the holder's `lease_ttl_ms`, against
//! the read time of the claiming transaction's query. The holder itself cannot read Firestore's
//! clock between renewals, so it stops trusting its lease nine tenths of `ttl` after it sent the
//! last write that confirmed it, which is always before Firestore's own expiry.
//!
//! No TTL policy is set on the collection: Firestore deletes expired documents only within about
//! a day, so it could decide nothing about a lease, and deleting the document would also forget
//! the generation. A release clears the lease fields instead, so the document is reused.

use crate::errors::{
    FirestoreDataConflictError, FirestoreError, FirestoreErrorPublicGenericDetails,
};
use crate::timestamp_utils::from_timestamp;
use crate::{
    FirestoreCollectionId, FirestoreDb, FirestoreDocumentId, FirestoreIndexGeneration,
    FirestoreIndexLeaseHeld, FirestoreIndexLeaseOnHeld, FirestoreIndexLeaseOptions,
    FirestoreIndexLeaseOwner, FirestoreIndexSuperseded, FirestoreIndexSyncOptions,
    FirestoreIndexSyncSkipReason, FirestoreInstant, FirestoreReference, FirestoreResult,
    FirestoreTimestamp, FirestoreTransformServerValue, FirestoreWritePrecondition,
};
use backoff::Error as BackoffError;
use futures::{FutureExt, StreamExt};
use gcloud_sdk::google::firestore::v1::Document;
use rand::RngExt;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};
use tracing::*;

const GENERATION_FIELD: &str = "generation";
const LEASE_RENEWED_AT_FIELD: &str = "lease_renewed_at";
/// The lease fields a claim or renewal writes itself; `lease_renewed_at` is set by the server.
const LEASE_WRITTEN_FIELDS: [&str; 3] = ["lease_owner", "lease_token", "lease_ttl_ms"];
const LEASE_ALL_FIELDS: [&str; 4] = [
    "lease_owner",
    "lease_token",
    "lease_ttl_ms",
    LEASE_RENEWED_AT_FIELD,
];

/// The coordination document as stored, before its lease fields are checked to come together.
#[derive(Debug, Deserialize)]
struct StoredCoordination {
    generation: Option<i64>,
    lease_owner: Option<String>,
    lease_token: Option<String>,
    lease_ttl_ms: Option<i64>,
    lease_renewed_at: Option<FirestoreTimestamp>,
}

/// A lease as stored, all of its fields present.
#[derive(Debug, Clone, PartialEq)]
struct StoredLease {
    owner: FirestoreIndexLeaseOwner,
    token: String,
    ttl_ms: i64,
    renewed_at: FirestoreInstant,
}

impl StoredLease {
    /// When the lease stops holding unless renewed, in Firestore's clock.
    fn expires_at(&self) -> FirestoreResult<FirestoreInstant> {
        self.renewed_at
            .checked_add(jiff::SignedDuration::from_millis(self.ttl_ms))
            .map_err(|err| {
                FirestoreError::invalid_parameters(
                    "lease_ttl_ms",
                    format!(
                        "lease renewed at {} with ttl {} ms has no representable expiry: {err}",
                        self.renewed_at, self.ttl_ms
                    ),
                )
            })
    }
}

/// The coordination document's content.
#[derive(Debug, Clone, PartialEq)]
struct CoordinationRecord {
    generation: Option<FirestoreIndexGeneration>,
    lease: Option<StoredLease>,
}

impl TryFrom<StoredCoordination> for CoordinationRecord {
    type Error = FirestoreError;

    fn try_from(stored: StoredCoordination) -> Result<Self, Self::Error> {
        let generation = stored
            .generation
            .map(FirestoreIndexGeneration::try_from)
            .transpose()?;
        let lease = match (
            stored.lease_owner,
            stored.lease_token,
            stored.lease_ttl_ms,
            stored.lease_renewed_at,
        ) {
            (None, None, None, None) => None,
            (Some(owner), Some(token), Some(ttl_ms), Some(renewed_at)) => {
                if ttl_ms < 0 {
                    return Err(FirestoreError::invalid_parameters(
                        "lease_ttl_ms",
                        format!("{ttl_ms} is negative"),
                    ));
                }
                Some(StoredLease {
                    owner: FirestoreIndexLeaseOwner::new(owner)?,
                    token,
                    ttl_ms,
                    renewed_at: renewed_at.0,
                })
            }
            (owner, token, ttl_ms, renewed_at) => {
                return Err(FirestoreError::invalid_parameters(
                    "lease",
                    format!(
                        "the lease fields must be all present or all absent; present: \
                         lease_owner {}, lease_token {}, lease_ttl_ms {}, lease_renewed_at {}",
                        owner.is_some(),
                        token.is_some(),
                        ttl_ms.is_some(),
                        renewed_at.is_some()
                    ),
                ))
            }
        };
        Ok(Self { generation, lease })
    }
}

/// The lease fields a claim or a renewal writes.
#[derive(Serialize, Deserialize)]
struct LeaseFields {
    lease_owner: String,
    lease_token: String,
    lease_ttl_ms: i64,
}

/// A write whose field mask removes every field it names, or names none and only transforms.
#[derive(Serialize, Deserialize)]
struct NoFields {}

/// Where a group's coordination document lives.
#[derive(Debug, Clone)]
struct CoordinationTarget {
    collection: FirestoreCollectionId,
    document_id: FirestoreDocumentId,
}

impl CoordinationTarget {
    fn new(
        options: &FirestoreIndexSyncOptions,
        collection_group: &FirestoreCollectionId,
    ) -> FirestoreResult<Self> {
        Ok(Self {
            collection: options.coordination_collection.clone(),
            document_id: FirestoreDocumentId::new(collection_group.as_str())?,
        })
    }

    fn document_path(&self, db: &FirestoreDb) -> String {
        format!(
            "{}/{}/{}",
            db.get_documents_path(),
            self.collection.as_str(),
            self.document_id.as_str()
        )
    }
}

/// The coordination document as one query read it: its content and last write time when it
/// exists, and the server time the query ran at.
#[derive(Debug, Clone, PartialEq)]
struct CoordinationSnapshot {
    stored: Option<StoredDocument>,
    read_time: Option<FirestoreInstant>,
}

#[derive(Debug, Clone, PartialEq)]
struct StoredDocument {
    record: CoordinationRecord,
    update_time: FirestoreInstant,
}

/// This caller's side of a lease: who it is, the token only this claim carries, and its `ttl`.
#[derive(Debug, Clone)]
struct LeaseClaim {
    owner: FirestoreIndexLeaseOwner,
    token: String,
    ttl: Duration,
    ttl_ms: i64,
}

impl LeaseClaim {
    fn new(options: &FirestoreIndexLeaseOptions) -> FirestoreResult<Self> {
        let ttl_ms = i64::try_from(options.ttl.as_millis()).map_err(|_| {
            FirestoreError::invalid_parameters(
                "lease_ttl",
                format!(
                    "{:?} has more milliseconds than Firestore stores",
                    options.ttl
                ),
            )
        })?;
        Ok(Self {
            owner: options.owner.clone(),
            token: format!("{:016x}", rand::rng().random::<u64>()),
            ttl: options.ttl,
            ttl_ms,
        })
    }

    fn fields(&self) -> LeaseFields {
        LeaseFields {
            lease_owner: self.owner.as_str().to_string(),
            lease_token: self.token.clone(),
            lease_ttl_ms: self.ttl_ms,
        }
    }

    /// How long after sending a write that confirms the lease this caller keeps trusting it.
    /// Firestore's expiry counts from when it applied that write, which is later still; the
    /// tenth held back covers an admin write already in flight when the trust runs out.
    fn trusted_for(&self) -> Duration {
        self.ttl - self.ttl / 10
    }

    fn is_ours(&self, lease: &StoredLease) -> bool {
        lease.token == self.token
    }
}

/// Whether a claim lets the sync run.
#[derive(Debug, Clone, PartialEq)]
enum ClaimVerdict {
    Proceed,
    Skip(FirestoreIndexSyncSkipReason),
}

/// What a claiming transaction decided from one snapshot.
#[derive(Debug, Clone, PartialEq)]
struct ClaimDecision {
    /// This caller's generation, when it is higher than the stored one or none is stored; it is
    /// recorded even when the lease is held, so older callers waiting for the same lease are
    /// turned away as soon as they next try.
    record_generation: Option<FirestoreIndexGeneration>,
    verdict: ClaimVerdict,
}

impl CoordinationSnapshot {
    fn record(&self) -> Option<&CoordinationRecord> {
        self.stored.as_ref().map(|stored| &stored.record)
    }

    fn precondition(&self) -> FirestoreWritePrecondition {
        match &self.stored {
            Some(stored) => FirestoreWritePrecondition::UpdateTime(stored.update_time),
            None => FirestoreWritePrecondition::Exists(false),
        }
    }

    /// `ours` against the stored generation: `Some` when a higher one is stored.
    fn superseded(&self, ours: FirestoreIndexGeneration) -> Option<FirestoreIndexSuperseded> {
        self.record()
            .and_then(|record| record.generation)
            .filter(|stored| *stored > ours)
            .map(|stored| FirestoreIndexSuperseded { stored, ours })
    }

    /// The stored lease when it belongs to another claim and has not expired at this snapshot's
    /// read time.
    ///
    /// # Errors
    /// Fails when there is such a lease and Firestore reported no read time to judge it by; a
    /// lease is never taken over on the client's own clock.
    fn held_by_another(
        &self,
        claim: &LeaseClaim,
    ) -> FirestoreResult<Option<FirestoreIndexLeaseHeld>> {
        let Some(lease) = self.record().and_then(|record| record.lease.as_ref()) else {
            return Ok(None);
        };
        if claim.is_ours(lease) {
            return Ok(None);
        }
        let expires_at = lease.expires_at()?;
        let read_time = self.read_time.ok_or_else(|| {
            FirestoreError::invalid_parameters(
                "read_time",
                format!(
                    "Firestore reported no read time for the coordination query, so the lease \
                     held by {} cannot be judged expired",
                    lease.owner
                ),
            )
        })?;
        Ok((read_time < expires_at).then(|| FirestoreIndexLeaseHeld {
            owner: lease.owner.clone(),
            expires_at,
        }))
    }

    fn decide_claim(
        &self,
        generation: Option<FirestoreIndexGeneration>,
        claim: Option<&LeaseClaim>,
    ) -> FirestoreResult<ClaimDecision> {
        if let Some(superseded) = generation.and_then(|ours| self.superseded(ours)) {
            return Ok(ClaimDecision {
                record_generation: None,
                verdict: ClaimVerdict::Skip(FirestoreIndexSyncSkipReason::Superseded(superseded)),
            });
        }
        let stored_generation = self.record().and_then(|record| record.generation);
        let record_generation =
            generation.filter(|ours| stored_generation.is_none_or(|stored| *ours > stored));
        let held = claim
            .map(|claim| self.held_by_another(claim))
            .transpose()?
            .flatten();
        Ok(ClaimDecision {
            record_generation,
            verdict: match held {
                Some(held) => ClaimVerdict::Skip(FirestoreIndexSyncSkipReason::LeaseHeld(held)),
                None => ClaimVerdict::Proceed,
            },
        })
    }
}

/// The outcome of trying to renew or release a lease.
#[derive(Debug, Clone, PartialEq)]
enum LeaseCheck {
    /// The lease was still this claim's, and the write went through.
    Ours,
    /// The lease is no longer this claim's, and nothing was written.
    Lost(LeaseLoss),
}

/// How a lease stopped being this claim's.
#[derive(Debug, Clone, PartialEq)]
enum LeaseLoss {
    /// Another claim holds it now.
    HeldBy(FirestoreIndexLeaseOwner),
    /// No claim holds it: its fields were cleared by someone else.
    Cleared,
}

impl std::fmt::Display for LeaseLoss {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseLoss::HeldBy(owner) => write!(f, "it is now held by {owner}"),
            LeaseLoss::Cleared => write!(f, "someone else cleared it"),
        }
    }
}

impl LeaseCheck {
    fn of(snapshot: &CoordinationSnapshot, claim: &LeaseClaim) -> Self {
        match snapshot.record().and_then(|record| record.lease.as_ref()) {
            Some(lease) if claim.is_ours(lease) => LeaseCheck::Ours,
            Some(lease) => LeaseCheck::Lost(LeaseLoss::HeldBy(lease.owner.clone())),
            None => LeaseCheck::Lost(LeaseLoss::Cleared),
        }
    }
}

/// What a claim decided about the sync.
pub(super) enum ClaimedCoordination {
    /// The sync runs, holding the lease when one was asked for.
    Proceed(Option<HeldLease>),
    /// The sync changes nothing.
    Skip(FirestoreIndexSyncSkipReason),
}

/// A lease this caller holds while its sync runs.
pub(super) struct HeldLease {
    target: CoordinationTarget,
    collection_group: FirestoreCollectionId,
    claim: LeaseClaim,
    state: Mutex<LeaseState>,
}

struct LeaseState {
    trusted_until: Instant,
    lost: Option<LeaseLoss>,
}

impl HeldLease {
    fn state(&self) -> std::sync::MutexGuard<'_, LeaseState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Fails when the lease was taken over, or was not confirmed recently enough to still be
    /// trusted; the sync calls this before every admin write.
    pub(super) fn ensure_held(&self) -> FirestoreResult<()> {
        let state = self.state();
        let problem = match &state.lost {
            Some(loss) => Some(loss.to_string()),
            None if Instant::now() >= state.trusted_until => Some(format!(
                "no renewal was confirmed within {:?}",
                self.claim.trusted_for()
            )),
            None => None,
        };
        match problem {
            None => Ok(()),
            Some(problem) => {
                error!(
                    collection_group = self.collection_group.as_str(),
                    owner = self.claim.owner.as_str(),
                    "Lost the index lease, so no further admin write is sent: {problem}.",
                );
                Err(FirestoreError::DataConflictError(
                    FirestoreDataConflictError::new(
                        FirestoreErrorPublicGenericDetails::new("IndexLeaseLost".to_string()),
                        format!(
                            "the index lease on collection group {} for {} is lost: {problem}",
                            self.collection_group.as_str(),
                            self.claim.owner
                        ),
                    ),
                ))
            }
        }
    }

    /// Renews the lease every third of its `ttl` until the lease is lost, then stays pending so
    /// the sync, which it runs beside, decides when both end. Never completes: dropping it is
    /// how it stops, so it outlives neither the sync nor a cancelled sync future.
    pub(super) async fn keep_renewed(&self, db: &FirestoreDb) -> Infallible {
        loop {
            tokio::time::sleep(self.claim.ttl / 3).await;
            let sent = Instant::now();
            match db.renew_lease_once(&self.target, &self.claim).await {
                Ok(LeaseCheck::Ours) => {
                    self.state().trusted_until = sent + self.claim.trusted_for();
                    info!(
                        collection_group = self.collection_group.as_str(),
                        owner = self.claim.owner.as_str(),
                        "Renewed the index lease.",
                    );
                }
                Ok(LeaseCheck::Lost(loss)) => {
                    warn!(
                        collection_group = self.collection_group.as_str(),
                        owner = self.claim.owner.as_str(),
                        "The index lease is no longer ours, {loss}; the sync stops before its next admin write.",
                    );
                    self.state().lost = Some(loss);
                    return std::future::pending().await;
                }
                Err(err) => {
                    warn!(
                        %err,
                        collection_group = self.collection_group.as_str(),
                        owner = self.claim.owner.as_str(),
                        "Failed to renew the index lease; trying again in a third of its ttl.",
                    );
                }
            }
        }
    }
}

impl FirestoreDb {
    /// Reads the coordination document with a query, the read that also reports Firestore's
    /// time. Inside a transaction, `self` is the transaction-bound client.
    async fn read_coordination(
        &self,
        target: &CoordinationTarget,
    ) -> FirestoreResult<CoordinationSnapshot> {
        let path = target.document_path(self);
        let mut responses = self
            .fluent()
            .select()
            .from(target.collection.as_str())
            .filter(|q| q.field("__name__").eq(FirestoreReference(path.clone())))
            .stream_query_with_metadata()
            .await?;
        let mut document: Option<Document> = None;
        let mut read_time = None;
        while let Some(response) = responses.next().await {
            let response = response?;
            read_time = response.metadata.read_time.or(read_time);
            if response.document.is_some() {
                document = response.document;
            }
        }
        let stored = document
            .map(|document| {
                let record = FirestoreDb::deserialize_doc_to::<StoredCoordination>(&document)
                    .and_then(CoordinationRecord::try_from)
                    .inspect_err(|err| {
                        error!(%err, document = document.name.as_str(), "The index coordination document is unreadable.");
                    })?;
                let update_time = document
                    .update_time
                    .ok_or_else(|| {
                        FirestoreError::invalid_parameters(
                            "update_time",
                            format!("Firestore returned {} without its update time", document.name),
                        )
                    })
                    .and_then(from_timestamp)?;
                Ok::<_, FirestoreError>(StoredDocument {
                    record,
                    update_time,
                })
            })
            .transpose()?;
        Ok(CoordinationSnapshot { stored, read_time })
    }

    /// One claiming transaction: reads the document, decides, and writes what the decision
    /// records.
    async fn claim_once(
        &self,
        target: &CoordinationTarget,
        generation: Option<FirestoreIndexGeneration>,
        claim: Option<&LeaseClaim>,
    ) -> FirestoreResult<ClaimVerdict> {
        self.run_transaction(|db, transaction| {
            let target = target.clone();
            let claim = claim.cloned();
            async move {
                let snapshot = db.read_coordination(&target).await?;
                let decision = snapshot
                    .decide_claim(generation, claim.as_ref())
                    .map_err(BackoffError::permanent)?;
                let take_lease = claim
                    .as_ref()
                    .filter(|_| decision.verdict == ClaimVerdict::Proceed);
                if decision.record_generation.is_none() && take_lease.is_none() {
                    return Ok(decision.verdict);
                }
                let mask: &[&str] = if take_lease.is_some() {
                    &LEASE_WRITTEN_FIELDS
                } else {
                    &[]
                };
                let recorded = decision.record_generation;
                let renewed = take_lease.is_some();
                let update = db
                    .fluent()
                    .update()
                    .fields(mask)
                    .in_col(target.collection.as_str())
                    .precondition(snapshot.precondition())
                    .document_id(target.document_id.as_str());
                let transforms =
                    |t: crate::document_transform_builder::FirestoreTransformBuilder| {
                        t.fields([
                            recorded.and_then(|generation| {
                                t.field(GENERATION_FIELD).maximum(i64::from(generation))
                            }),
                            renewed
                                .then(|| {
                                    t.field(LEASE_RENEWED_AT_FIELD)
                                        .server_value(FirestoreTransformServerValue::RequestTime)
                                })
                                .flatten(),
                        ])
                    };
                match take_lease {
                    Some(claim) => update
                        .object(&claim.fields())
                        .transforms(transforms)
                        .add_to_transaction(transaction)?,
                    None => update
                        .object(&NoFields {})
                        .transforms(transforms)
                        .add_to_transaction(transaction)?,
                };
                Ok(decision.verdict)
            }
            .boxed()
        })
        .await
    }

    /// One renewing transaction: rewrites the lease, with a fresh server time, when it is still
    /// this claim's.
    async fn renew_lease_once(
        &self,
        target: &CoordinationTarget,
        claim: &LeaseClaim,
    ) -> FirestoreResult<LeaseCheck> {
        self.run_transaction(|db, transaction| {
            let target = target.clone();
            let claim = claim.clone();
            async move {
                let snapshot = db.read_coordination(&target).await?;
                let check = LeaseCheck::of(&snapshot, &claim);
                if check == LeaseCheck::Ours {
                    db.fluent()
                        .update()
                        .fields(LEASE_WRITTEN_FIELDS)
                        .in_col(target.collection.as_str())
                        .precondition(snapshot.precondition())
                        .document_id(target.document_id.as_str())
                        .object(&claim.fields())
                        .transforms(|t| {
                            t.fields([t
                                .field(LEASE_RENEWED_AT_FIELD)
                                .server_value(FirestoreTransformServerValue::RequestTime)])
                        })
                        .add_to_transaction(transaction)?;
                }
                Ok(check)
            }
            .boxed()
        })
        .await
    }

    /// One releasing transaction: clears the lease fields when the lease is still this claim's,
    /// and leaves another caller's lease alone.
    async fn release_lease_once(
        &self,
        target: &CoordinationTarget,
        claim: &LeaseClaim,
    ) -> FirestoreResult<LeaseCheck> {
        self.run_transaction(|db, transaction| {
            let target = target.clone();
            let claim = claim.clone();
            async move {
                let snapshot = db.read_coordination(&target).await?;
                let check = LeaseCheck::of(&snapshot, &claim);
                if check == LeaseCheck::Ours {
                    db.fluent()
                        .update()
                        .fields(LEASE_ALL_FIELDS)
                        .in_col(target.collection.as_str())
                        .precondition(snapshot.precondition())
                        .document_id(target.document_id.as_str())
                        .object(&NoFields {})
                        .add_to_transaction(transaction)?;
                }
                Ok(check)
            }
            .boxed()
        })
        .await
    }

    /// Claims the group for a sync, as `options.generation` and `options.lease` ask: records the
    /// generation, takes the lease, and waits for a held lease when told to. Sends nothing when
    /// neither is set.
    pub(super) async fn claim_index_coordination(
        &self,
        collection_group: &FirestoreCollectionId,
        options: &FirestoreIndexSyncOptions,
    ) -> FirestoreResult<ClaimedCoordination> {
        if options.generation.is_none() && options.lease.is_none() {
            return Ok(ClaimedCoordination::Proceed(None));
        }
        let span = span!(
            Level::INFO,
            "Firestore Index Coordination",
            "/firestore/collection_group" = collection_group.as_str(),
            "/firestore/coordination_collection" = options.coordination_collection.as_str(),
            "/firestore/generation" = options.generation.map(FirestoreIndexGeneration::value),
            "/firestore/lease" = options.lease.is_some(),
            "/firestore/response_time" = field::Empty,
        );
        let began = Instant::now();
        let claimed = self
            .claim_waiting_while_held(collection_group, options)
            .instrument(span.clone())
            .await;
        span.record("/firestore/response_time", began.elapsed().as_millis());
        claimed
    }

    async fn claim_waiting_while_held(
        &self,
        collection_group: &FirestoreCollectionId,
        options: &FirestoreIndexSyncOptions,
    ) -> FirestoreResult<ClaimedCoordination> {
        let target = CoordinationTarget::new(options, collection_group)?;
        let claim = options.lease.as_ref().map(LeaseClaim::new).transpose()?;
        let first_attempt = Instant::now();
        let wait = options
            .lease
            .as_ref()
            .and_then(|lease| match &lease.on_held {
                FirestoreIndexLeaseOnHeld::Skip => None,
                FirestoreIndexLeaseOnHeld::Wait(wait) => Some(wait),
            });
        loop {
            let sent = Instant::now();
            let reason = match self
                .claim_once(&target, options.generation, claim.as_ref())
                .await?
            {
                ClaimVerdict::Proceed => {
                    info!(
                        collection_group = collection_group.as_str(),
                        generation = options.generation.map(FirestoreIndexGeneration::value),
                        owner = claim.as_ref().map(|claim| claim.owner.as_str()),
                        "Claimed the collection group for index sync.",
                    );
                    return Ok(ClaimedCoordination::Proceed(claim.map(|claim| HeldLease {
                        target,
                        collection_group: collection_group.clone(),
                        state: Mutex::new(LeaseState {
                            trusted_until: sent + claim.trusted_for(),
                            lost: None,
                        }),
                        claim,
                    })));
                }
                ClaimVerdict::Skip(reason) => reason,
            };
            let wait = match (&reason, wait) {
                (FirestoreIndexSyncSkipReason::LeaseHeld(_), Some(wait)) => wait,
                _ => {
                    info!(
                        collection_group = collection_group.as_str(),
                        "Skipping index sync: {reason}.",
                    );
                    return Ok(ClaimedCoordination::Skip(reason));
                }
            };
            let waited = first_attempt.elapsed();
            if waited >= wait.timeout {
                warn!(
                    collection_group = collection_group.as_str(),
                    waited_ms = waited.as_millis(),
                    "Gave up waiting for the index lease; skipping index sync: {reason}.",
                );
                return Ok(ClaimedCoordination::Skip(reason));
            }
            info!(
                collection_group = collection_group.as_str(),
                "Waiting to claim the collection group for index sync: {reason}.",
            );
            tokio::time::sleep(wait.poll_interval.min(wait.timeout - waited)).await;
        }
    }

    /// Releases `lease` after its sync. A failure is logged, never returned: the sync's own
    /// outcome stands, and an unreleased lease expires on its own.
    pub(super) async fn release_index_lease(&self, lease: &HeldLease) {
        match self.release_lease_once(&lease.target, &lease.claim).await {
            Ok(LeaseCheck::Ours) => info!(
                collection_group = lease.collection_group.as_str(),
                owner = lease.claim.owner.as_str(),
                "Released the index lease.",
            ),
            Ok(LeaseCheck::Lost(loss)) => warn!(
                collection_group = lease.collection_group.as_str(),
                owner = lease.claim.owner.as_str(),
                "The index lease was no longer ours at release, {loss}; left it as it is.",
            ),
            Err(err) => error!(
                %err,
                collection_group = lease.collection_group.as_str(),
                owner = lease.claim.owner.as_str(),
                "Failed to release the index lease; it expires on its own.",
            ),
        }
    }

    /// The generation check `.plan()` makes: reads the coordination document, outside any
    /// transaction, and writes nothing.
    pub(super) async fn read_superseded(
        &self,
        collection_group: &FirestoreCollectionId,
        options: &FirestoreIndexSyncOptions,
    ) -> FirestoreResult<Option<FirestoreIndexSuperseded>> {
        let Some(ours) = options.generation else {
            return Ok(None);
        };
        let target = CoordinationTarget::new(options, collection_group)?;
        let snapshot = self.read_coordination(&target).await?;
        Ok(snapshot.superseded(ours))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::admin::indexes::tests::{
        declared_index, group, listed_declared_index, operation_name, CREATE_INDEX, DELETE_INDEX,
        GET_OPERATION, GROUP_PATH, LIST_FIELDS, LIST_INDEXES, MODULE_TEST_LOCK,
    };
    use crate::db::fake_firestore::{
        begin_response, failed_operation_response, list_fields_response, list_indexes_response,
        pending_operation_response, FakeFirestore, FakeResponse,
    };
    use crate::db::FirestoreDbInner;
    use crate::timestamp_utils::to_timestamp;
    use crate::{
        FirestoreIndexLeaseWait, FirestoreIndexParams, FirestoreIndexSupport,
        FirestoreOperationWaitOptions,
    };
    use gcloud_sdk::google::firestore::admin::v1::index::State as ProtoState;
    use gcloud_sdk::google::firestore::v1::document_transform::field_transform::TransformType;
    use gcloud_sdk::google::firestore::v1::precondition::ConditionType;
    use gcloud_sdk::google::firestore::v1::value::ValueType;
    use gcloud_sdk::google::firestore::v1::{
        write, CommitRequest, CommitResponse, RunQueryResponse, Value, WriteResult,
    };
    use gcloud_sdk::prost::Message as _;
    use gcloud_sdk::tonic::Code;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU8;
    use std::sync::Arc;

    const BEGIN: &str = "/google.firestore.v1.Firestore/BeginTransaction";
    const RUN_QUERY: &str = "/google.firestore.v1.Firestore/RunQuery";
    const COMMIT: &str = "/google.firestore.v1.Firestore/Commit";
    const ROLLBACK: &str = "/google.firestore.v1.Firestore/Rollback";

    /// The fake server's clock starts decades before the test's own, so a check that used the
    /// client clock would call every lease here long expired.
    fn server_epoch() -> FirestoreInstant {
        "2001-01-01T00:00:00Z".parse().unwrap()
    }

    fn at(offset: Duration) -> FirestoreInstant {
        server_epoch() + jiff::SignedDuration::try_from(offset).unwrap()
    }

    fn integer(value: i64) -> Value {
        Value {
            value_type: Some(ValueType::IntegerValue(value)),
        }
    }

    fn string(value: &str) -> Value {
        Value {
            value_type: Some(ValueType::StringValue(value.to_string())),
        }
    }

    fn timestamp(value: FirestoreInstant) -> Value {
        Value {
            value_type: Some(ValueType::TimestampValue(to_timestamp(value))),
        }
    }

    fn lease_fields(
        owner: &str,
        token: &str,
        ttl: Duration,
        renewed_at: FirestoreInstant,
    ) -> Vec<(&'static str, Value)> {
        vec![
            ("lease_owner", string(owner)),
            ("lease_token", string(token)),
            ("lease_ttl_ms", integer(ttl.as_millis() as i64)),
            (LEASE_RENEWED_AT_FIELD, timestamp(renewed_at)),
        ]
    }

    struct StoreState {
        document: Option<Document>,
        now: FirestoreInstant,
        /// The server time of every commit that wrote `lease_renewed_at`: claims and renewals.
        lease_writes: Vec<FirestoreInstant>,
    }

    /// One coordination document behind the data RPCs a claim, renewal and release send:
    /// transactional queries answered with the document and the server's read time, and commits
    /// applied with their masks, preconditions and transforms at the server's time. Every query
    /// and commit moves the server clock on by a millisecond.
    #[derive(Clone)]
    struct FakeCoordinationStore {
        state: Arc<Mutex<StoreState>>,
        next_transaction: Arc<AtomicU8>,
    }

    impl FakeCoordinationStore {
        fn new(fields: Vec<(&str, Value)>) -> Self {
            let document = (!fields.is_empty()).then(|| Document {
                name: format!(
                    "projects/fake-firestore/databases/(default)/documents/{}/users",
                    crate::DEFAULT_INDEX_COORDINATION_COLLECTION
                ),
                fields: fields
                    .into_iter()
                    .map(|(name, value)| (name.to_string(), value))
                    .collect(),
                create_time: Some(to_timestamp(server_epoch())),
                update_time: Some(to_timestamp(server_epoch())),
            });
            Self {
                state: Arc::new(Mutex::new(StoreState {
                    document,
                    now: server_epoch(),
                    lease_writes: Vec::new(),
                })),
                next_transaction: Arc::new(AtomicU8::new(0)),
            }
        }

        fn state(&self) -> std::sync::MutexGuard<'_, StoreState> {
            self.state.lock().unwrap()
        }

        fn set_now(&self, now: FirestoreInstant) {
            self.state().now = now;
        }

        fn fields(&self) -> HashMap<String, Value> {
            self.state()
                .document
                .as_ref()
                .map(|document| document.fields.clone())
                .unwrap_or_default()
        }

        fn lease_owner(&self) -> Option<String> {
            match self.fields().get("lease_owner")?.value_type.as_ref()? {
                ValueType::StringValue(owner) => Some(owner.clone()),
                other => panic!("lease_owner is not a string: {other:?}"),
            }
        }

        fn generation(&self) -> Option<i64> {
            match self.fields().get(GENERATION_FIELD)?.value_type.as_ref()? {
                ValueType::IntegerValue(generation) => Some(*generation),
                other => panic!("generation is not an integer: {other:?}"),
            }
        }

        fn lease_writes(&self) -> Vec<FirestoreInstant> {
            self.state().lease_writes.clone()
        }

        /// Replaces the lease with another caller's, as a caller that took it over would.
        fn replace_lease(&self, owner: &str, token: &str) {
            let mut state = self.state();
            let now = state.now;
            let document = state.document.as_mut().expect("a lease to replace");
            for (name, value) in lease_fields(owner, token, Duration::from_secs(3600), now) {
                document.fields.insert(name.to_string(), value);
            }
            document.update_time = Some(to_timestamp(now));
        }

        fn clear_lease(&self) {
            let mut state = self.state();
            let now = state.now;
            let document = state.document.as_mut().expect("a lease to clear");
            for name in LEASE_ALL_FIELDS {
                document.fields.remove(name);
            }
            document.update_time = Some(to_timestamp(now));
        }

        fn tick(state: &mut StoreState) -> FirestoreInstant {
            state.now += jiff::SignedDuration::from_millis(1);
            state.now
        }

        /// Answers a data RPC, or `None` for any other RPC.
        fn answer(&self, method: &str, bytes: &[u8]) -> Option<(String, FakeResponse)> {
            match method {
                BEGIN => Some(begin_response(&self.next_transaction)),
                ROLLBACK => Some(("Rollback".to_string(), FakeResponse::empty())),
                RUN_QUERY => {
                    let mut state = self.state();
                    let read_time = Self::tick(&mut state);
                    let response = RunQueryResponse {
                        document: state.document.clone(),
                        read_time: Some(to_timestamp(read_time)),
                        ..Default::default()
                    };
                    Some((
                        "RunQuery".to_string(),
                        FakeResponse::Message(response.encode_to_vec()),
                    ))
                }
                COMMIT => {
                    let request = CommitRequest::decode(bytes).unwrap();
                    let mut state = self.state();
                    let commit_time = Self::tick(&mut state);
                    let writes = request.writes.len();
                    for write in request.writes {
                        if let Err(code) = Self::apply(&mut state, write, commit_time) {
                            return Some((
                                format!("Commit refused: {code:?}"),
                                FakeResponse::Status(code),
                            ));
                        }
                    }
                    let response = CommitResponse {
                        write_results: (0..writes)
                            .map(|_| WriteResult {
                                update_time: Some(to_timestamp(commit_time)),
                                transform_results: vec![],
                            })
                            .collect(),
                        commit_time: Some(to_timestamp(commit_time)),
                    };
                    Some((
                        format!("Commit({writes})"),
                        FakeResponse::Message(response.encode_to_vec()),
                    ))
                }
                _ => None,
            }
        }

        fn apply(
            state: &mut StoreState,
            write: gcloud_sdk::google::firestore::v1::Write,
            commit_time: FirestoreInstant,
        ) -> Result<(), Code> {
            match write.current_document.and_then(|p| p.condition_type) {
                Some(ConditionType::Exists(exists)) if state.document.is_some() != exists => {
                    return Err(Code::FailedPrecondition)
                }
                Some(ConditionType::UpdateTime(update_time))
                    if state.document.as_ref().and_then(|d| d.update_time) != Some(update_time) =>
                {
                    return Err(Code::FailedPrecondition)
                }
                _ => {}
            }
            let Some(write::Operation::Update(update)) = write.operation else {
                panic!("the coordination document is only ever updated");
            };
            let mut fields = state
                .document
                .as_ref()
                .map(|document| document.fields.clone())
                .unwrap_or_default();
            match write.update_mask {
                Some(mask) => {
                    for path in mask.field_paths {
                        match update.fields.get(&path) {
                            Some(value) => fields.insert(path, value.clone()),
                            None => fields.remove(&path),
                        };
                    }
                }
                None => fields = update.fields.clone(),
            }
            for transform in write.update_transforms {
                match transform.transform_type {
                    Some(TransformType::SetToServerValue(_)) => {
                        assert_eq!(transform.field_path, LEASE_RENEWED_AT_FIELD);
                        state.lease_writes.push(commit_time);
                        fields.insert(transform.field_path, timestamp(commit_time));
                    }
                    Some(TransformType::Maximum(Value {
                        value_type: Some(ValueType::IntegerValue(candidate)),
                    })) => {
                        let current = match fields.get(&transform.field_path) {
                            Some(Value {
                                value_type: Some(ValueType::IntegerValue(current)),
                            }) => *current,
                            _ => i64::MIN,
                        };
                        fields.insert(transform.field_path, integer(current.max(candidate)));
                    }
                    other => panic!("unexpected transform {other:?}"),
                }
            }
            let create_time = state
                .document
                .as_ref()
                .and_then(|document| document.create_time)
                .unwrap_or_else(|| to_timestamp(commit_time));
            state.document = Some(Document {
                name: update.name,
                fields,
                create_time: Some(create_time),
                update_time: Some(to_timestamp(commit_time)),
            });
            Ok(())
        }
    }

    async fn start_with<F>(store: &FakeCoordinationStore, admin: F) -> FakeFirestore
    where
        F: Fn(&str, &[u8]) -> (String, FakeResponse) + Send + Sync + 'static,
    {
        let store = store.clone();
        FakeFirestore::start(move |method, bytes| {
            store
                .answer(method, bytes)
                .unwrap_or_else(|| admin(method, bytes))
        })
        .await
    }

    /// An empty group: listing answers nothing, so a sync that runs sends no admin write.
    fn empty_group(method: &str, _: &[u8]) -> (String, FakeResponse) {
        match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            other => panic!("unexpected admin RPC: {other}"),
        }
    }

    /// A group with one declared index to create, whose build stays pending for the first
    /// `pending_polls` polls and then finishes with `finished`.
    fn slow_create(
        pending_polls: usize,
        finished: FakeResponse,
    ) -> impl Fn(&str, &[u8]) -> (String, FakeResponse) + Send + Sync + 'static {
        let polls = std::sync::atomic::AtomicUsize::new(0);
        let finished = Mutex::new(Some(finished));
        move |method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            CREATE_INDEX => (
                "CreateIndex".to_string(),
                pending_operation_response(&operation_name("op1")),
            ),
            GET_OPERATION => {
                let poll = polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if poll < pending_polls {
                    (
                        "GetOperation".to_string(),
                        pending_operation_response(&operation_name("op1")),
                    )
                } else {
                    (
                        "GetOperation".to_string(),
                        finished
                            .lock()
                            .unwrap()
                            .take()
                            .expect("the operation finishes once"),
                    )
                }
            }
            other => panic!("unexpected admin RPC: {other}"),
        }
    }

    fn generation(value: u64) -> FirestoreIndexGeneration {
        FirestoreIndexGeneration::try_from(value).unwrap()
    }

    fn owner(name: &str) -> FirestoreIndexLeaseOwner {
        FirestoreIndexLeaseOwner::new(name).unwrap()
    }

    fn lease(ttl: Duration) -> FirestoreIndexLeaseOptions {
        FirestoreIndexLeaseOptions::new()
            .with_ttl(ttl)
            .with_owner(owner("replica-a"))
    }

    fn polling_wait() -> FirestoreOperationWaitOptions {
        FirestoreOperationWaitOptions::new(Duration::from_secs(10))
            .with_poll_interval(Duration::from_millis(50))
    }

    fn is_admin(call: &str) -> bool {
        !(call.starts_with("Begin")
            || call.starts_with("RunQuery")
            || call.starts_with("Commit")
            || call.starts_with("Rollback"))
    }

    fn position(calls: &[String], call: &str) -> usize {
        calls
            .iter()
            .position(|c| c == call)
            .unwrap_or_else(|| panic!("{call} not in {calls:?}"))
    }

    // Decisions on one snapshot, without a server.

    fn snapshot(
        stored_generation: Option<u64>,
        lease: Option<StoredLease>,
        read_time: Option<FirestoreInstant>,
    ) -> CoordinationSnapshot {
        CoordinationSnapshot {
            stored: Some(StoredDocument {
                record: CoordinationRecord {
                    generation: stored_generation.map(generation),
                    lease,
                },
                update_time: server_epoch(),
            }),
            read_time,
        }
    }

    fn claim_by(name: &str, token: &str) -> LeaseClaim {
        LeaseClaim {
            owner: owner(name),
            token: token.to_string(),
            ttl: Duration::from_secs(60),
            ttl_ms: 60_000,
        }
    }

    fn stored_lease(token: &str, renewed_at: FirestoreInstant) -> StoredLease {
        StoredLease {
            owner: owner("replica-b"),
            token: token.to_string(),
            ttl_ms: 60_000,
            renewed_at,
        }
    }

    #[test]
    fn a_lease_holds_until_the_server_time_of_its_renewal_plus_its_ttl() {
        let claim = claim_by("replica-a", "a");
        let expiry = at(Duration::from_secs(60));
        let just_before = expiry - jiff::SignedDuration::from_micros(1);

        let held = snapshot(
            None,
            Some(stored_lease("b", server_epoch())),
            Some(just_before),
        )
        .decide_claim(None, Some(&claim))
        .unwrap();
        assert_eq!(
            held.verdict,
            ClaimVerdict::Skip(FirestoreIndexSyncSkipReason::LeaseHeld(
                FirestoreIndexLeaseHeld {
                    owner: owner("replica-b"),
                    expires_at: expiry,
                }
            ))
        );

        let expired = snapshot(None, Some(stored_lease("b", server_epoch())), Some(expiry))
            .decide_claim(None, Some(&claim))
            .unwrap();
        assert_eq!(expired.verdict, ClaimVerdict::Proceed);
    }

    #[test]
    fn a_lease_is_judged_by_the_server_read_time_never_the_client_clock() {
        // Renewed in 2001 with a one-minute ttl, read one second later by the server: held,
        // although by the client's clock it expired decades ago.
        let decision = snapshot(
            None,
            Some(stored_lease("b", server_epoch())),
            Some(at(Duration::from_secs(1))),
        )
        .decide_claim(None, Some(&claim_by("replica-a", "a")))
        .unwrap();
        assert!(matches!(
            decision.verdict,
            ClaimVerdict::Skip(FirestoreIndexSyncSkipReason::LeaseHeld(_))
        ));
    }

    #[test]
    fn a_foreign_lease_without_a_server_read_time_fails_rather_than_being_taken() {
        let err = snapshot(None, Some(stored_lease("b", server_epoch())), None)
            .decide_claim(None, Some(&claim_by("replica-a", "a")))
            .unwrap_err();
        assert!(err.to_string().contains("no read time"), "{err}");
    }

    #[test]
    fn its_own_lease_never_holds_a_claim_back() {
        let decision = snapshot(
            None,
            Some(stored_lease("a", server_epoch())),
            Some(server_epoch()),
        )
        .decide_claim(None, Some(&claim_by("replica-a", "a")))
        .unwrap();
        assert_eq!(decision.verdict, ClaimVerdict::Proceed);
    }

    #[test]
    fn a_generation_is_recorded_only_when_it_is_the_highest_and_skipped_when_lower() {
        let read = Some(server_epoch());
        let decide = |stored: Option<u64>, ours: u64| {
            snapshot(stored, None, read)
                .decide_claim(Some(generation(ours)), None)
                .unwrap()
        };
        assert_eq!(
            decide(Some(7), 5),
            ClaimDecision {
                record_generation: None,
                verdict: ClaimVerdict::Skip(FirestoreIndexSyncSkipReason::Superseded(
                    FirestoreIndexSuperseded {
                        stored: generation(7),
                        ours: generation(5),
                    }
                )),
            }
        );
        assert_eq!(
            decide(Some(7), 7),
            ClaimDecision {
                record_generation: None,
                verdict: ClaimVerdict::Proceed,
            }
        );
        assert_eq!(
            decide(Some(5), 7),
            ClaimDecision {
                record_generation: Some(generation(7)),
                verdict: ClaimVerdict::Proceed,
            }
        );
        assert_eq!(
            decide(None, 0),
            ClaimDecision {
                record_generation: Some(generation(0)),
                verdict: ClaimVerdict::Proceed,
            }
        );
    }

    #[test]
    fn a_newer_generation_is_recorded_even_while_another_caller_holds_the_lease() {
        let decision = snapshot(
            Some(5),
            Some(stored_lease("b", server_epoch())),
            Some(server_epoch()),
        )
        .decide_claim(Some(generation(7)), Some(&claim_by("replica-a", "a")))
        .unwrap();
        assert_eq!(decision.record_generation, Some(generation(7)));
        assert!(matches!(
            decision.verdict,
            ClaimVerdict::Skip(FirestoreIndexSyncSkipReason::LeaseHeld(_))
        ));
    }

    #[test]
    fn a_release_or_renewal_finds_the_lease_ours_only_by_its_claim_token() {
        let ours = claim_by("replica-a", "a");
        let check = |lease: Option<StoredLease>| {
            LeaseCheck::of(&snapshot(None, lease, Some(server_epoch())), &ours)
        };
        assert_eq!(
            check(Some(stored_lease("a", server_epoch()))),
            LeaseCheck::Ours
        );
        let same_owner_other_claim = StoredLease {
            owner: owner("replica-a"),
            ..stored_lease("other", server_epoch())
        };
        assert_eq!(
            check(Some(same_owner_other_claim)),
            LeaseCheck::Lost(LeaseLoss::HeldBy(owner("replica-a")))
        );
        assert_eq!(check(None), LeaseCheck::Lost(LeaseLoss::Cleared));
    }

    #[test]
    fn partial_lease_fields_are_unreadable_rather_than_absent() {
        let err = CoordinationRecord::try_from(StoredCoordination {
            generation: Some(3),
            lease_owner: Some("replica-b".to_string()),
            lease_token: None,
            lease_ttl_ms: Some(1000),
            lease_renewed_at: None,
        })
        .unwrap_err();
        assert!(
            err.to_string().contains("all present or all absent"),
            "{err}"
        );
    }

    // Whole syncs against the fake server.

    #[tokio::test]
    async fn an_older_generation_is_skipped_with_no_admin_rpc() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let store = FakeCoordinationStore::new(vec![(GENERATION_FIELD, integer(7))]);
        let fake = start_with(&store, empty_group).await;

        let report = fake
            .db
            .sync_indexes(
                FirestoreIndexParams::new(group()),
                FirestoreIndexSyncOptions::new().with_generation(generation(5)),
            )
            .await
            .unwrap();

        assert_eq!(
            report.skipped,
            Some(FirestoreIndexSyncSkipReason::Superseded(
                FirestoreIndexSuperseded {
                    stored: generation(7),
                    ours: generation(5),
                }
            ))
        );
        assert!(report.timings.coordination.is_some());
        assert_eq!(report.timings.list, None);
        let calls = fake.calls();
        assert!(!calls.iter().any(|call| is_admin(call)), "{calls:?}");
        assert!(calls.contains(&"Commit(0)".to_string()), "{calls:?}");
        assert_eq!(store.generation(), Some(7));
    }

    #[tokio::test]
    async fn a_newer_generation_is_recorded_before_any_admin_rpc() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let store = FakeCoordinationStore::new(vec![(GENERATION_FIELD, integer(5))]);
        let fake = start_with(&store, empty_group).await;
        let options = FirestoreIndexSyncOptions::new().with_generation(generation(7));

        let report = fake
            .db
            .sync_indexes(FirestoreIndexParams::new(group()), options.clone())
            .await
            .unwrap();

        assert_eq!(report.skipped, None);
        assert_eq!(store.generation(), Some(7));
        let calls = fake.calls();
        assert!(
            position(&calls, "Commit(1)") < position(&calls, "ListIndexes"),
            "{calls:?}"
        );

        // The same generation again: nothing left to record.
        let before = fake.calls().len();
        fake.db
            .sync_indexes(FirestoreIndexParams::new(group()), options)
            .await
            .unwrap();
        let again = fake.calls()[before..].to_vec();
        assert!(again.contains(&"Commit(0)".to_string()), "{again:?}");
        assert!(!again.contains(&"Commit(1)".to_string()), "{again:?}");
    }

    #[tokio::test]
    async fn a_generation_creates_the_coordination_document_when_there_is_none() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let store = FakeCoordinationStore::new(vec![]);
        let fake = start_with(&store, empty_group).await;

        fake.db
            .sync_indexes(
                FirestoreIndexParams::new(group()),
                FirestoreIndexSyncOptions::new().with_generation(generation(3)),
            )
            .await
            .unwrap();

        assert_eq!(store.generation(), Some(3));
    }

    #[tokio::test]
    async fn a_plan_with_an_older_generation_reports_superseded_and_writes_nothing() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let store = FakeCoordinationStore::new(vec![(GENERATION_FIELD, integer(7))]);
        let fake = start_with(&store, empty_group).await;

        let plan = fake
            .db
            .plan_indexes(
                FirestoreIndexParams::new(group()),
                FirestoreIndexSyncOptions::new().with_generation(generation(5)),
            )
            .await
            .unwrap();
        assert!(matches!(
            plan.skipped,
            Some(FirestoreIndexSyncSkipReason::Superseded(_))
        ));
        assert_eq!(fake.calls(), vec!["RunQuery".to_string()]);

        let plan = fake
            .db
            .plan_indexes(
                FirestoreIndexParams::new(group()),
                FirestoreIndexSyncOptions::new().with_generation(generation(9)),
            )
            .await
            .unwrap();
        assert_eq!(plan.skipped, None);
        assert_eq!(store.generation(), Some(7), "a plan records nothing");
        assert!(!fake.calls().iter().any(|call| call.starts_with("Commit")));
    }

    #[tokio::test]
    async fn a_held_lease_makes_the_sync_skip_naming_its_owner_and_expiry() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let store = FakeCoordinationStore::new(lease_fields(
            "replica-b",
            "b",
            Duration::from_secs(60),
            server_epoch(),
        ));
        store.set_now(at(Duration::from_secs(10)));
        let fake = start_with(&store, empty_group).await;

        let report = fake
            .db
            .sync_indexes(
                FirestoreIndexParams::new(group()),
                FirestoreIndexSyncOptions::new().with_lease(lease(Duration::from_secs(60))),
            )
            .await
            .unwrap();

        assert_eq!(
            report.skipped,
            Some(FirestoreIndexSyncSkipReason::LeaseHeld(
                FirestoreIndexLeaseHeld {
                    owner: owner("replica-b"),
                    expires_at: at(Duration::from_secs(60)),
                }
            ))
        );
        assert!(!fake.calls().iter().any(|call| is_admin(call)));
        assert_eq!(store.lease_owner(), Some("replica-b".to_string()));
    }

    #[tokio::test]
    async fn an_expired_lease_is_taken_over_and_released_after_the_sync() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let mut fields = lease_fields("replica-b", "b", Duration::from_secs(60), server_epoch());
        fields.push((GENERATION_FIELD, integer(4)));
        let store = FakeCoordinationStore::new(fields);
        store.set_now(at(Duration::from_secs(61)));
        let fake = start_with(&store, empty_group).await;

        let report = fake
            .db
            .sync_indexes(
                FirestoreIndexParams::new(group()),
                FirestoreIndexSyncOptions::new().with_lease(lease(Duration::from_secs(60))),
            )
            .await
            .unwrap();

        assert_eq!(report.skipped, None);
        assert!(fake.calls().contains(&"ListIndexes".to_string()));
        assert_eq!(store.lease_writes().len(), 1, "claimed once");
        assert_eq!(store.lease_owner(), None, "released after the sync");
        let fields = store.fields();
        assert!(LEASE_ALL_FIELDS
            .iter()
            .all(|name| !fields.contains_key(*name)));
        assert_eq!(
            store.generation(),
            Some(4),
            "a release keeps the generation"
        );
    }

    #[tokio::test]
    async fn a_waiting_sync_proceeds_once_the_holder_releases() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let store = FakeCoordinationStore::new(lease_fields(
            "replica-b",
            "b",
            Duration::from_secs(60),
            server_epoch(),
        ));
        let fake = start_with(&store, empty_group).await;
        let options = FirestoreIndexSyncOptions::new().with_lease(
            lease(Duration::from_secs(60)).with_on_held(FirestoreIndexLeaseOnHeld::Wait(
                FirestoreIndexLeaseWait::new(Duration::from_secs(10))
                    .with_poll_interval(Duration::from_millis(20)),
            )),
        );

        let (report, ()) = tokio::join!(
            fake.db
                .sync_indexes(FirestoreIndexParams::new(group()), options),
            async {
                // Two claim attempts (Begin, RunQuery, Commit each) find the lease held.
                tokio::time::timeout(Duration::from_secs(5), fake.wait_for_calls(6))
                    .await
                    .expect("two claim attempts within five seconds");
                store.clear_lease();
            }
        );

        let report = report.unwrap();
        assert_eq!(report.skipped, None);
        let calls = fake.calls();
        let claimed = position(&calls, "Commit(1)");
        let attempts = calls[..claimed]
            .iter()
            .filter(|call| *call == "RunQuery")
            .count();
        assert!(attempts >= 3, "{calls:?}");
        assert!(claimed < position(&calls, "ListIndexes"));
    }

    #[tokio::test]
    async fn a_lease_wait_that_times_out_skips() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let store = FakeCoordinationStore::new(lease_fields(
            "replica-b",
            "b",
            Duration::from_secs(60),
            server_epoch(),
        ));
        let fake = start_with(&store, empty_group).await;
        let options = FirestoreIndexSyncOptions::new().with_lease(
            lease(Duration::from_secs(60)).with_on_held(FirestoreIndexLeaseOnHeld::Wait(
                FirestoreIndexLeaseWait::new(Duration::from_millis(100))
                    .with_poll_interval(Duration::from_millis(20)),
            )),
        );

        let report = fake
            .db
            .sync_indexes(FirestoreIndexParams::new(group()), options)
            .await
            .unwrap();

        assert!(matches!(
            report.skipped,
            Some(FirestoreIndexSyncSkipReason::LeaseHeld(_))
        ));
        assert!(
            fake.calls()
                .iter()
                .filter(|call| *call == "RunQuery")
                .count()
                >= 2
        );
        assert!(!fake.calls().iter().any(|call| is_admin(call)));
    }

    #[tokio::test]
    async fn the_lease_is_renewed_while_a_long_sync_runs() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let store = FakeCoordinationStore::new(vec![]);
        let fake = start_with(
            &store,
            slow_create(
                15,
                crate::db::fake_firestore::done_operation_response(&operation_name("op1")),
            ),
        )
        .await;

        let report = fake
            .db
            .sync_indexes(
                FirestoreIndexParams::new(group()).with_composite_indexes(vec![declared_index()]),
                FirestoreIndexSyncOptions::new()
                    .with_wait(polling_wait())
                    .with_lease(lease(Duration::from_millis(600))),
            )
            .await
            .unwrap();

        assert_eq!(report.created_indexes, vec![declared_index()]);
        let writes = store.lease_writes();
        assert!(
            writes.len() >= 3,
            "a claim and at least two renewals: {writes:?}"
        );
        assert!(
            writes.windows(2).all(|pair| pair[0] < pair[1]),
            "{writes:?}"
        );
        assert_eq!(store.lease_owner(), None, "released after the sync");
    }

    #[tokio::test]
    async fn a_lost_lease_stops_the_sync_before_its_next_admin_write() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let store = FakeCoordinationStore::new(vec![]);
        let create = slow_create(
            12,
            crate::db::fake_firestore::done_operation_response(&operation_name("op1")),
        );
        let fake = start_with(&store, move |method, bytes| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(vec![listed_declared_index(
                    &format!("{GROUP_PATH}/indexes/legacy"),
                    ProtoState::Ready,
                )]),
            ),
            DELETE_INDEX => ("DeleteIndex".to_string(), FakeResponse::empty()),
            other => create(other, bytes),
        })
        .await;

        let params = FirestoreIndexParams::new(group()).with_composite_indexes(vec![
            crate::FirestoreCompositeIndex::new(vec![
                crate::FirestoreIndexField::new(
                    "b".to_string(),
                    crate::FirestoreIndexFieldMode::Order(
                        crate::FirestoreQueryDirection::Ascending,
                    ),
                ),
                crate::FirestoreIndexField::new(
                    "c".to_string(),
                    crate::FirestoreIndexFieldMode::Order(
                        crate::FirestoreQueryDirection::Ascending,
                    ),
                ),
            ]),
        ]);
        let options = FirestoreIndexSyncOptions::new()
            .with_prune(true)
            .with_wait(polling_wait())
            .with_lease(lease(Duration::from_millis(600)));
        let (result, ()) = tokio::join!(fake.db.sync_indexes(params, options), async {
            tokio::time::timeout(Duration::from_secs(5), async {
                while !fake.calls().iter().any(|call| call == "GetOperation") {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("the create is polled within five seconds");
            store.replace_lease("thief", "t");
        });

        let err = result.unwrap_err();
        assert!(matches!(err, FirestoreError::DataConflictError(_)), "{err}");
        assert!(err.to_string().contains("held by thief"), "{err}");
        let calls = fake.calls();
        assert!(calls.contains(&"CreateIndex".to_string()), "{calls:?}");
        assert!(!calls.contains(&"DeleteIndex".to_string()), "{calls:?}");
        assert_eq!(
            store.lease_owner(),
            Some("thief".to_string()),
            "a release leaves another caller's lease alone"
        );
    }

    #[tokio::test]
    async fn renewal_stops_when_the_sync_fails() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let store = FakeCoordinationStore::new(vec![]);
        let fake = start_with(
            &store,
            slow_create(
                6,
                failed_operation_response(&operation_name("op1"), 9, "build failed"),
            ),
        )
        .await;

        let result = fake
            .db
            .sync_indexes(
                FirestoreIndexParams::new(group()).with_composite_indexes(vec![declared_index()]),
                FirestoreIndexSyncOptions::new()
                    .with_wait(polling_wait())
                    .with_lease(lease(Duration::from_millis(300))),
            )
            .await;

        assert!(result.is_err());
        let writes = store.lease_writes();
        assert!(writes.len() >= 2, "renewed while the sync ran: {writes:?}");
        assert_eq!(store.lease_owner(), None, "released after the failure");
        let calls = fake.calls().len();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(store.lease_writes(), writes, "no renewal after the sync");
        assert_eq!(fake.calls().len(), calls, "no RPC after the sync");
    }

    #[tokio::test]
    async fn renewal_stops_when_the_sync_is_cancelled_and_the_lease_is_left_to_expire() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let store = FakeCoordinationStore::new(vec![]);
        let fake = start_with(&store, |method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            CREATE_INDEX => ("CreateIndex".to_string(), FakeResponse::Hang),
            other => panic!("unexpected admin RPC: {other}"),
        })
        .await;

        let cancelled = tokio::time::timeout(
            Duration::from_millis(350),
            fake.db.sync_indexes(
                FirestoreIndexParams::new(group()).with_composite_indexes(vec![declared_index()]),
                FirestoreIndexSyncOptions::new().with_lease(lease(Duration::from_millis(300))),
            ),
        )
        .await;

        assert!(cancelled.is_err(), "the sync was still hanging");
        let writes = store.lease_writes();
        assert!(writes.len() >= 2, "renewed while the sync ran: {writes:?}");
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(store.lease_writes(), writes, "no renewal after the cancel");
        assert_eq!(store.lease_owner(), Some("replica-a".to_string()));
    }

    #[tokio::test]
    async fn without_generation_or_lease_no_coordination_rpc_is_sent() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, bytes| match method {
            BEGIN | RUN_QUERY | COMMIT | ROLLBACK => panic!("coordination RPC {method}"),
            other => empty_group(other, bytes),
        })
        .await;

        fake.db
            .plan_indexes(
                FirestoreIndexParams::new(group()),
                FirestoreIndexSyncOptions::new(),
            )
            .await
            .unwrap();
        let report = fake
            .db
            .sync_indexes(
                FirestoreIndexParams::new(group()),
                FirestoreIndexSyncOptions::new(),
            )
            .await
            .unwrap();

        assert_eq!(report.timings.coordination, None);
        assert!(fake.calls().iter().all(|call| is_admin(call)));
    }

    #[tokio::test]
    async fn the_emulator_skips_coordination_along_with_the_admin_api() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| panic!("unexpected RPC: {method}")).await;
        let emulator_db = FirestoreDb {
            inner: Arc::new(FirestoreDbInner {
                database_path: fake.db.get_database_path().clone(),
                doc_path: fake.db.get_documents_path().clone(),
                options: fake.db.get_options().clone(),
                client: fake.db.client().clone(),
                is_emulator: true,
            }),
            session_params: fake.db.get_session_params().clone().into(),
        };
        let options = FirestoreIndexSyncOptions::new()
            .with_generation(generation(1))
            .with_lease(lease(Duration::from_secs(60)));

        let plan = emulator_db
            .plan_indexes(FirestoreIndexParams::new(group()), options.clone())
            .await
            .unwrap();
        let report = emulator_db
            .sync_indexes(FirestoreIndexParams::new(group()), options)
            .await
            .unwrap();

        assert_eq!(plan.skipped, Some(FirestoreIndexSyncSkipReason::Emulator));
        assert_eq!(report.skipped, Some(FirestoreIndexSyncSkipReason::Emulator));
        assert!(fake.calls().is_empty());
    }
}
