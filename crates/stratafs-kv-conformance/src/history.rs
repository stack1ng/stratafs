use std::ops::Bound;

use bytes::Bytes;
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::select;

static KEYS: [&[u8]; 7] = [b"", b"\0", b"a", b"a\0", b"aa", b"b", b"\xff"];
static VALUES: [&[u8]; 3] = [b"", b"1", b"2"];
static LIMITS: [usize; 4] = [0, 1, 2, usize::MAX];

#[derive(Clone, Debug)]
pub enum SimulatedTransaction {
    Read(Vec<SimulatedRead>),
    Write(Vec<SimulatedOperation>),
}

#[derive(Clone, Debug)]
pub enum SimulatedOperation {
    Read(SimulatedRead),
    Set(Bytes, Bytes),
    Clear(Bytes),
}

#[derive(Clone, Debug)]
pub enum SimulatedRead {
    Get(Bytes),
    GetRange(Bound<Bytes>, Bound<Bytes>, usize),
}

pub fn simulated_client_activity() -> impl Strategy<Value = Vec<Vec<SimulatedTransaction>>> {
    vec(vec(transaction(), 1..=6), 1..=4)
}

fn transaction() -> impl Strategy<Value = SimulatedTransaction> {
    prop_oneof![
        vec(read(), 1..=4).prop_map(SimulatedTransaction::Read),
        vec(operation(), 1..=4).prop_map(SimulatedTransaction::Write),
    ]
}

fn operation() -> impl Strategy<Value = SimulatedOperation> {
    prop_oneof![
        read().prop_map(SimulatedOperation::Read),
        (key(), value()).prop_map(|(key, value)| SimulatedOperation::Set(key, value)),
        key().prop_map(SimulatedOperation::Clear),
    ]
}

fn read() -> impl Strategy<Value = SimulatedRead> {
    prop_oneof![
        key().prop_map(SimulatedRead::Get),
        (bound(), bound(), limit())
            .prop_map(|(start, end, limit)| SimulatedRead::GetRange(start, end, limit)),
    ]
}

fn bound() -> impl Strategy<Value = Bound<Bytes>> {
    prop_oneof![
        key().prop_map(Bound::Included),
        key().prop_map(Bound::Excluded),
        Just(Bound::Unbounded),
    ]
}

fn key() -> impl Strategy<Value = Bytes> {
    select(KEYS.as_slice()).prop_map(Bytes::from_static)
}

fn value() -> impl Strategy<Value = Bytes> {
    select(VALUES.as_slice()).prop_map(Bytes::from_static)
}

fn limit() -> impl Strategy<Value = usize> {
    select(LIMITS.as_slice())
}
