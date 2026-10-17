use std::ops::Bound;

use bytes::Bytes;
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::select;
use stratafs_store::kv::is_valid_range;

static KEYS: [&[u8]; 7] = [b"", b"\0", b"a", b"a\0", b"aa", b"b", b"\xff"];
static VALUES: [&[u8]; 3] = [b"", b"1", b"2"];
static LIMITS: [usize; 4] = [0, 1, 2, usize::MAX];

#[derive(Clone, Debug)]
pub enum SimulatedTransaction {
    // all read transaction operations do not require exclusive access to the transaction,
    // so they can happen concurrently. We make a vec of them to simulate this.
    // The groups run one after another, so other commits can land between them.
    Read(Vec<Vec<SimulatedRead>>),
    Write(Vec<SimulatedOperation>),
}

#[derive(Clone, Debug)]
pub enum SimulatedOperation {
    // read operations in a write tx ALSO do not require exclusive access to the transaction,
    // so we make a vec of concurrent read operations to simulate this.
    Read(Vec<SimulatedRead>),
    Set(Bytes, Bytes),
    Clear(Bytes),
    Panic,
}

#[derive(Clone, Debug)]
pub enum SimulatedRead {
    Get(Bytes),
    GetRange(Bound<Bytes>, Bound<Bytes>, usize),
    Observe(Bytes),
}

#[derive(Clone, Debug)]
pub struct SimulatedCall {
    pub transaction: SimulatedTransaction,
    pub cancelled_at_fraction: Option<f64>,
}

#[derive(Clone, Copy, Debug)]
pub enum SimulatedStop {
    Clean,
    CrashAtFraction(f64),
}

pub fn simulated_stop() -> impl Strategy<Value = SimulatedStop> {
    prop_oneof![
        1 => Just(SimulatedStop::Clean),
        3 => (0.0..1.0).prop_map(SimulatedStop::CrashAtFraction),
    ]
}

pub fn simulated_client_activity() -> impl Strategy<Value = Vec<Vec<SimulatedCall>>> {
    concurrent_clients(call())
}

pub fn simulated_client_transactions() -> impl Strategy<Value = Vec<Vec<SimulatedTransaction>>> {
    concurrent_clients(transaction())
}

fn concurrent_clients<C: Strategy>(call: C) -> impl Strategy<Value = Vec<Vec<C::Value>>> {
    vec(vec(call, 1..=6), 1..=4)
}

fn call() -> impl Strategy<Value = SimulatedCall> {
    (transaction(), cancel_at()).prop_map(|(transaction, cancelled_at_fraction)| SimulatedCall {
        transaction,
        cancelled_at_fraction,
    })
}

// does_cancel either doesnt cancel, or it says what
// % of the way through the transaction to cancel at
fn cancel_at() -> impl Strategy<Value = Option<f64>> {
    prop_oneof![
        7 => Just(None),
        1 => (0.0..1.0).prop_map(Some),
    ]
}

fn transaction() -> impl Strategy<Value = SimulatedTransaction> {
    prop_oneof![
        vec(vec(read(), 1..=3), 1..=3).prop_map(SimulatedTransaction::Read),
        vec(operation(), 1..=4).prop_map(SimulatedTransaction::Write),
    ]
}

fn operation() -> impl Strategy<Value = SimulatedOperation> {
    prop_oneof![
        6 => vec(read(), 1..=3).prop_map(SimulatedOperation::Read),
        6 => (key(), value()).prop_map(|(key, value)| SimulatedOperation::Set(key, value)),
        6 => key().prop_map(SimulatedOperation::Clear),
        1 => Just(SimulatedOperation::Panic),
    ]
}

fn read() -> impl Strategy<Value = SimulatedRead> {
    prop_oneof![
        4 => key().prop_map(SimulatedRead::Get),
        4 => (bound(), bound(), limit())
            .prop_filter("valid range", |(start, end, _)| {
                is_valid_range::<Bytes>(&(start.as_ref(), end.as_ref()))
            })
            .prop_map(|(start, end, limit)| SimulatedRead::GetRange(start, end, limit)),
        1 => key().prop_map(SimulatedRead::Observe),
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
