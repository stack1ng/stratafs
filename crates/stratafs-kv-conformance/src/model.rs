use std::collections::BTreeMap;
use std::ops::Bound;

use bytes::Bytes;
use stateright::semantics::SequentialSpec;

use crate::activity::{SimulatedOperation, SimulatedRead, SimulatedTransaction};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionId {
    pub client: usize,
    pub transaction: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObservationId {
    pub transaction: TransactionId,
    pub observation: usize,
}

#[derive(Clone, Debug)]
pub enum HistoryOperation {
    Transaction(TransactionId, SimulatedTransaction),
    ObservationFired(ObservationId),
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
pub struct BTreeModel {
    values: BTreeMap<Bytes, Bytes>,
    observations: BTreeMap<ObservationId, ObservationMarker>,
}

#[derive(Clone, Debug)]
struct ObservationMarker {
    key: Bytes,
    seen: Option<Bytes>,
    differed: bool,
}

impl SequentialSpec for BTreeModel {
    type Op = HistoryOperation;
    type Ret = Outcome;

    fn invoke(&mut self, operation: &HistoryOperation) -> Outcome {
        match operation {
            HistoryOperation::Transaction(id, transaction) => self.transact(*id, transaction),
            HistoryOperation::ObservationFired(observation) => Outcome::Changed(
                self.observations
                    .get(observation)
                    .is_some_and(|marker| marker.differed),
            ),
        }
    }
}

impl FromIterator<(Bytes, Bytes)> for BTreeModel {
    fn from_iter<I: IntoIterator<Item = (Bytes, Bytes)>>(entries: I) -> Self {
        BTreeModel {
            values: entries.into_iter().collect(),
            observations: BTreeMap::new(),
        }
    }
}

impl BTreeModel {
    fn transact(&mut self, id: TransactionId, transaction: &SimulatedTransaction) -> Outcome {
        let mut observations = 0;
        match transaction {
            SimulatedTransaction::Read(groups) => Outcome::Completed(
                groups
                    .iter()
                    .flatten()
                    .map(|read| self.read(read, id, &mut observations))
                    .collect(),
            ),
            SimulatedTransaction::Write(operations) => {
                let mut after = self.clone();
                let mut seen = Vec::new();
                for operation in operations {
                    match operation {
                        SimulatedOperation::Read(reads) => {
                            for read in reads {
                                seen.push(after.read(read, id, &mut observations));
                            }
                        }
                        SimulatedOperation::Set(key, value) => {
                            after.values.insert(key.clone(), value.clone());
                        }
                        SimulatedOperation::Clear(key) => {
                            after.values.remove(key);
                        }
                        SimulatedOperation::Panic => return Outcome::Panicked,
                    }
                }
                for marker in after.observations.values_mut() {
                    if after.values.get(&marker.key) != marker.seen.as_ref() {
                        marker.differed = true;
                    }
                }
                *self = after;
                Outcome::Completed(seen)
            }
        }
    }

    fn read(
        &mut self,
        read: &SimulatedRead,
        transaction: TransactionId,
        observations: &mut usize,
    ) -> Seen {
        match read {
            SimulatedRead::Get(key) => {
                let key = Bound::Included(key.clone());
                let found = self.range(&key, &key, 1);
                Seen::Get(found.into_iter().next().map(|(_, value)| value))
            }
            SimulatedRead::GetRange(start, end, limit) => {
                Seen::GetRange(self.range(start, end, *limit))
            }
            SimulatedRead::Observe(key) => {
                let value = self.values.get(key).cloned();
                self.observations.insert(
                    ObservationId {
                        transaction,
                        observation: *observations,
                    },
                    ObservationMarker {
                        key: key.clone(),
                        seen: value.clone(),
                        differed: false,
                    },
                );
                *observations += 1;
                Seen::Observe(value)
            }
        }
    }

    fn range(&self, start: &Bound<Bytes>, end: &Bound<Bytes>, limit: usize) -> Vec<(Bytes, Bytes)> {
        self.values
            .range::<Bytes, _>((start.as_ref(), end.as_ref()))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}
