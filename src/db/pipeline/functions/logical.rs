use crate::{FirestoreBoolExpr, FirestoreExpr};

impl FirestoreBoolExpr {
    /// This condition and `other`, or this condition unchanged when `other` is `None`.
    ///
    /// Called on an `and`, it adds `other` to that `and`, so a chain of calls builds one `and`
    /// of every condition present.
    pub fn and_maybe(self, other: Option<FirestoreBoolExpr>) -> Self {
        match other {
            Some(other) => self.connect("and", other),
            None => self,
        }
    }

    /// This condition or `other`, or this condition unchanged when `other` is `None`.
    ///
    /// Called on an `or`, it adds `other` to that `or`, so a chain of calls builds one `or` of
    /// every condition present.
    pub fn or_maybe(self, other: Option<FirestoreBoolExpr>) -> Self {
        match other {
            Some(other) => self.connect("or", other),
            None => self,
        }
    }

    fn connect(self, function: &str, other: Self) -> Self {
        match self.0 {
            FirestoreExpr::Function(mut connective) if connective.name == function => {
                connective.args.push(other.0);
                Self(FirestoreExpr::Function(connective))
            }
            condition => Self(FirestoreExpr::raw(function, [condition, other.0])),
        }
    }
}
