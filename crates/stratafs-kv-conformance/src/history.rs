use std::ops::Bound;

use bytes::Bytes;
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::select;

static KEYS: [&[u8]; 7] = [b"", b"\0", b"a", b"a\0", b"aa", b"b", b"\xff"];
static VALUES: [&[u8]; 3] = [b"", b"1", b"2"];
static LIMITS: [usize; 4] = [0, 1, 2, usize::MAX];

#[derive(Clone, Debug)]
pub enum Transaction {
    Read(Vec<Read>),
    Write(Vec<Operation>),
}

#[derive(Clone, Debug)]
pub enum Operation {
    Read(Read),
    Set(Bytes, Bytes),
    Clear(Bytes),
}

#[derive(Clone, Debug)]
pub enum Read {
    Get(Bytes),
    GetRange(Bound<Bytes>, Bound<Bytes>, usize),
}

pub fn simulated_client_activity() -> impl Strategy<Value = Vec<Vec<Transaction>>> {
    vec(vec(transaction(), 1..=6), 1..=4)
}

fn transaction() -> impl Strategy<Value = Transaction> {
    prop_oneof![
        vec(read(), 1..=4).prop_map(Transaction::Read),
        vec(operation(), 1..=4).prop_map(Transaction::Write),
    ]
}

fn operation() -> impl Strategy<Value = Operation> {
    prop_oneof![
        read().prop_map(Operation::Read),
        (key(), value()).prop_map(|(key, value)| Operation::Set(key, value)),
        key().prop_map(Operation::Clear),
    ]
}

fn read() -> impl Strategy<Value = Read> {
    prop_oneof![
        key().prop_map(Read::Get),
        (bound(), bound(), limit())
            .prop_map(|(start, end, limit)| Read::GetRange(start, end, limit)),
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
