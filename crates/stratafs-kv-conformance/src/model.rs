use std::collections::BTreeMap;
use std::ops::Bound;

use bytes::Bytes;
use stateright::semantics::SequentialSpec;

use crate::activity::{SimulatedOperation, SimulatedRead, SimulatedTransaction};

#[derive(Clone, Debug)]
pub enum HistoryOperation {
    Transaction(SimulatedTransaction),
    ObservationFired { key: Bytes, seen: Option<Bytes> },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    Completed(Vec<Seen>),
    Panicked,
    Changed(bool),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Seen {
    Get(Option<Bytes>),
    GetRange(Vec<(Bytes, Bytes)>),
    Observe(Option<Bytes>),
}

#[derive(Clone, Debug, Default)]
pub struct BTreeModel(pub(crate) BTreeMap<Bytes, Bytes>);

impl SequentialSpec for BTreeModel {
    type Op = HistoryOperation;
    type Ret = Outcome;

    fn invoke(&mut self, operation: &HistoryOperation) -> Outcome {
        match operation {
            HistoryOperation::Transaction(transaction) => self.transact(transaction),
            HistoryOperation::ObservationFired { key, seen } => {
                Outcome::Changed(self.0.get(key) != seen.as_ref())
            }
        }
    }
}

impl BTreeModel {
    fn transact(&mut self, transaction: &SimulatedTransaction) -> Outcome {
        match transaction {
            SimulatedTransaction::Read(reads) => {
                Outcome::Completed(reads.iter().map(|read| self.read(read)).collect())
            }
            SimulatedTransaction::Write(operations) => {
                let mut after = self.clone();
                let mut seen = Vec::new();
                for operation in operations {
                    match operation {
                        SimulatedOperation::Read(read) => seen.push(after.read(read)),
                        SimulatedOperation::Set(key, value) => {
                            after.0.insert(key.clone(), value.clone());
                        }
                        SimulatedOperation::Clear(key) => {
                            after.0.remove(key);
                        }
                        SimulatedOperation::Panic => return Outcome::Panicked,
                    }
                }
                *self = after;
                Outcome::Completed(seen)
            }
        }
    }

    fn read(&self, read: &SimulatedRead) -> Seen {
        match read {
            SimulatedRead::Get(key) => {
                let key = Bound::Included(key.clone());
                let found = self.range(&key, &key, 1);
                Seen::Get(found.into_iter().next().map(|(_, value)| value))
            }
            SimulatedRead::GetRange(start, end, limit) => {
                Seen::GetRange(self.range(start, end, *limit))
            }
            SimulatedRead::Observe(key) => Seen::Observe(self.0.get(key).cloned()),
        }
    }

    fn range(&self, start: &Bound<Bytes>, end: &Bound<Bytes>, limit: usize) -> Vec<(Bytes, Bytes)> {
        self.0
            .range::<Bytes, _>((start.as_ref(), end.as_ref()))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}
