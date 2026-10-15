use std::collections::BTreeMap;
use std::ops::Bound;

use bytes::Bytes;
use stateright::semantics::SequentialSpec;

use crate::history::{SimulatedOperation, SimulatedRead, SimulatedTransaction};

#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    Completed(Vec<Seen>),
    Panicked,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Seen {
    Get(Option<Bytes>),
    GetRange(Vec<(Bytes, Bytes)>),
}

#[derive(Clone, Debug, Default)]
pub struct BTreeModel(BTreeMap<Bytes, Bytes>);

impl SequentialSpec for BTreeModel {
    type Op = SimulatedTransaction;
    type Ret = Outcome;

    fn invoke(&mut self, transaction: &SimulatedTransaction) -> Outcome {
        match transaction {
            SimulatedTransaction::Read(reads) => reads
                .iter()
                .map(|read| self.read(read))
                .collect::<Option<_>>()
                .map_or(Outcome::Panicked, Outcome::Completed),
            SimulatedTransaction::Write(operations) => {
                let mut after = self.clone();
                let mut seen = Vec::new();
                for operation in operations {
                    match operation {
                        SimulatedOperation::Read(read) => {
                            let Some(result) = after.read(read) else {
                                return Outcome::Panicked;
                            };
                            seen.push(result);
                        }
                        SimulatedOperation::Set(key, value) => {
                            after.0.insert(key.clone(), value.clone());
                        }
                        SimulatedOperation::Clear(key) => {
                            after.0.remove(key);
                        }
                    }
                }
                *self = after;
                Outcome::Completed(seen)
            }
        }
    }
}

impl BTreeModel {
    fn read(&self, read: &SimulatedRead) -> Option<Seen> {
        match read {
            SimulatedRead::Get(key) => {
                let key = Bound::Included(key.clone());
                let found = self.range(&key, &key, 1);
                Some(Seen::Get(found.into_iter().next().map(|(_, value)| value)))
            }
            SimulatedRead::GetRange(start, end, limit) => {
                if invalid_range(start, end) {
                    return None;
                }
                Some(Seen::GetRange(self.range(start, end, *limit)))
            }
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

fn invalid_range(start: &Bound<Bytes>, end: &Bound<Bytes>) -> bool {
    match (start, end) {
        (Bound::Excluded(start), Bound::Excluded(end)) => start >= end,
        (
            Bound::Included(start) | Bound::Excluded(start),
            Bound::Included(end) | Bound::Excluded(end),
        ) => start > end,
        _ => false,
    }
}
