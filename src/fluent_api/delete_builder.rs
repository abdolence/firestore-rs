use crate::{
    FirestoreBatch, FirestoreDeleteSupport, FirestoreResult, FirestoreTransaction,
    FirestoreWritePrecondition,
};

#[derive(Clone, Debug)]
pub struct FirestoreDeleteInitialBuilder<'a, D>
where
    D: FirestoreDeleteSupport,
{
    db: &'a D,
}

impl<'a, D> FirestoreDeleteInitialBuilder<'a, D>
where
    D: FirestoreDeleteSupport,
{
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }

    #[inline]
    pub fn from(self, collection_id: &str) -> FirestoreDeleteDocIdBuilder<'a, D> {
        FirestoreDeleteDocIdBuilder::new(self.db, collection_id.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreDeleteDocIdBuilder<'a, D>
where
    D: FirestoreDeleteSupport,
{
    db: &'a D,
    collection_id: String,
    parent: Option<String>,
    precondition: Option<FirestoreWritePrecondition>,
}

impl<'a, D> FirestoreDeleteDocIdBuilder<'a, D>
where
    D: FirestoreDeleteSupport,
{
    #[inline]
    pub(crate) fn new(db: &'a D, collection_id: String) -> Self {
        Self {
            db,
            collection_id,
            parent: None,
            precondition: None,
        }
    }

    #[inline]
    pub fn parent<S>(self, parent: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            parent: Some(parent.as_ref().to_string()),
            ..self
        }
    }

    #[inline]
    pub fn precondition(self, precondition: FirestoreWritePrecondition) -> Self {
        Self {
            precondition: Some(precondition),
            ..self
        }
    }

    #[inline]
    pub fn document_id<S>(self, document_id: S) -> FirestoreDeleteExecuteBuilder<'a, D>
    where
        S: AsRef<str> + Send,
    {
        FirestoreDeleteExecuteBuilder::new(
            self.db,
            self.collection_id.to_string(),
            document_id.as_ref().to_string(),
            self.parent,
            self.precondition,
        )
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreDeleteExecuteBuilder<'a, D>
where
    D: FirestoreDeleteSupport,
{
    db: &'a D,
    collection_id: String,
    document_id: String,
    parent: Option<String>,
    precondition: Option<FirestoreWritePrecondition>,
}

impl<'a, D> FirestoreDeleteExecuteBuilder<'a, D>
where
    D: FirestoreDeleteSupport,
{
    #[inline]
    pub(crate) fn new(
        db: &'a D,
        collection_id: String,
        document_id: String,
        parent: Option<String>,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> Self {
        Self {
            db,
            collection_id,
            document_id,
            parent,
            precondition,
        }
    }

    #[inline]
    pub fn parent<S>(self, parent: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            parent: Some(parent.as_ref().to_string()),
            ..self
        }
    }

    #[inline]
    pub fn precondition(self, precondition: FirestoreWritePrecondition) -> Self {
        Self {
            precondition: Some(precondition),
            ..self
        }
    }

    pub async fn execute(self) -> FirestoreResult<()> {
        if let Some(parent) = self.parent {
            self.db
                .delete_by_id_at(
                    parent.as_str(),
                    self.collection_id.as_str(),
                    self.document_id,
                    self.precondition,
                )
                .await
        } else {
            self.db
                .delete_by_id(
                    self.collection_id.as_str(),
                    self.document_id,
                    self.precondition,
                )
                .await
        }
    }

    #[inline]
    pub fn add_to_transaction<'t>(
        self,
        transaction: &'a mut FirestoreTransaction<'t>,
    ) -> FirestoreResult<&'a mut FirestoreTransaction<'t>> {
        if let Some(parent) = self.parent {
            transaction.delete_by_id_at(
                parent.as_str(),
                self.collection_id.as_str(),
                self.document_id,
                self.precondition,
            )
        } else {
            transaction.delete_by_id(
                self.collection_id.as_str(),
                self.document_id,
                self.precondition,
            )
        }
    }

    #[inline]
    pub fn add_to_batch<'t>(
        self,
        batch: &'a mut FirestoreBatch<'t>,
    ) -> FirestoreResult<&'a mut FirestoreBatch<'t>> {
        if let Some(parent) = self.parent {
            batch.delete_by_id_at(
                parent.as_str(),
                self.collection_id.as_str(),
                self.document_id,
                self.precondition,
            )
        } else {
            batch.delete_by_id(
                self.collection_id.as_str(),
                self.document_id,
                self.precondition,
            )
        }
    }
}
