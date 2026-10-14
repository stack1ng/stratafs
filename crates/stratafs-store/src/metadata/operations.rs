use bytes::Bytes;
use derive_more::{From, TryInto};
use impl_trait_for_tuples::impl_for_tuples;
use vec1::Vec1;

use crate::content::ContentHash;
use crate::metadata::{FileContent, FileId, FileInfo, Key, KeyValue, Seq, Stored, Written};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetKey {
    pub key: Key,
    pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteKey {
    pub key: Key,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetKey {
    pub key: Key,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Condition {
    Equals {
        key: Key,
        // a None check means that the key does not exist or was deleted
        value: Option<Bytes>,
    },
    Empty {
        prefix: Key,
    },
    FileVersion {
        file: FileId,
        seq: Option<Seq>,
    },
    // the head of the volume must be equal to the given sequence
    Head(Seq),
    Not(Box<Condition>),
    All(Vec1<Condition>),
    Any(Vec1<Condition>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assert(pub Condition);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteAt {
    // pin the write to a specific location in the file
    Offset(u64),
    // pin the write to the end of the file, wherever that is at commit time
    End,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    pub file: FileId,
    pub at: WriteAt,
    pub size: u64,
    pub content: ContentHash,
    pub content_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncate {
    pub file: FileId,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleBlobs {
    pub file: FileId,
    pub offset: u64,
    pub size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetHead;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetFileInfo {
    pub file: FileId,
}

#[derive(Debug, Clone, PartialEq, Eq, From)]
pub enum Op {
    // set the value for a metadata key, this creates the key if it does not exist
    SetKey(SetKey),
    // delete a metadata key, this does nothing if the key does not exist or was already deleted
    DeleteKey(DeleteKey),
    // get the value for a metadata key, this returns None if the key does not exist or was deleted
    GetKey(GetKey),
    // assert a condition on the state of the store, this will fail the transaction if the condition is not met
    Assert(Assert),
    // write data to a file
    Write(Write),
    // truncate a file to a new length
    Truncate(Truncate),
    // get all applicable blobs for a range within a file
    VisibleBlobs(VisibleBlobs),
    // retrieve file specific state
    GetFileInfo(GetFileInfo),
    GetHead(GetHead),
}

#[derive(Debug, Clone, PartialEq, Eq, TryInto)]
pub enum Outcome {
    Done,
    Written(Written),
    Value(Option<Stored<KeyValue>>),
    FileContent(FileContent),
    FileInfo(FileInfo),
    Head(Seq),
}

pub trait Operation: Into<Op> {
    type Output: TryFrom<Outcome>;
}

impl Operation for SetKey {
    type Output = ();
}

impl Operation for DeleteKey {
    type Output = ();
}

impl Operation for GetKey {
    type Output = Option<Stored<KeyValue>>;
}

impl Operation for Assert {
    type Output = ();
}

impl Operation for Write {
    type Output = Written;
}

impl Operation for Truncate {
    type Output = ();
}

impl Operation for VisibleBlobs {
    type Output = FileContent;
}

impl Operation for GetFileInfo {
    type Output = FileInfo;
}

impl Operation for GetHead {
    type Output = Seq;
}

pub trait Operations {
    type Output;
    type Layout;

    fn into_ops(self, ops: &mut Vec<Op>) -> Self::Layout;

    fn from_outcomes<I: Iterator<Item = Outcome>>(
        layout: Self::Layout,
        outcomes: &mut I,
    ) -> Option<Self::Output>;
}

impl<O: Operation> Operations for O {
    type Output = O::Output;
    type Layout = ();

    fn into_ops(self, ops: &mut Vec<Op>) -> Self::Layout {
        ops.push(self.into());
    }

    fn from_outcomes<I: Iterator<Item = Outcome>>(
        (): Self::Layout,
        outcomes: &mut I,
    ) -> Option<Self::Output> {
        O::Output::try_from(outcomes.next()?).ok()
    }
}

#[impl_for_tuples(1, 12)]
impl Operations for Tuple {
    for_tuples!( type Output = ( #( Tuple::Output ),* ); );
    for_tuples!( type Layout = ( #( Tuple::Layout ),* ); );

    fn into_ops(self, ops: &mut Vec<Op>) -> Self::Layout {
        for_tuples!( ( #( self.Tuple.into_ops(ops) ),* ) )
    }

    fn from_outcomes<I: Iterator<Item = Outcome>>(
        layout: Self::Layout,
        outcomes: &mut I,
    ) -> Option<Self::Output> {
        Some(for_tuples!( ( #( Tuple::from_outcomes(layout.Tuple, outcomes)? ),* ) ))
    }
}

impl<T: Operations> Operations for Vec1<T> {
    type Output = Vec1<T::Output>;
    type Layout = Vec1<T::Layout>;

    fn into_ops(self, ops: &mut Vec<Op>) -> Self::Layout {
        self.mapped(|part| part.into_ops(ops))
    }

    fn from_outcomes<I: Iterator<Item = Outcome>>(
        layout: Self::Layout,
        outcomes: &mut I,
    ) -> Option<Self::Output> {
        layout
            .try_mapped(|part| T::from_outcomes(part, outcomes).ok_or(()))
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use vec1::vec1;

    use super::*;

    fn key(name: &str) -> Key {
        Key(Bytes::copy_from_slice(name.as_bytes()))
    }

    #[test]
    fn typed_results_come_back_in_operation_order() {
        let operations = (
            Assert(Condition::Equals {
                key: key("d/1/a"),
                value: None,
            }),
            SetKey {
                key: key("d/1/a"),
                value: Bytes::from_static(b"2"),
            },
            vec1![
                GetKey {
                    key: key("a/2/mode"),
                },
                GetKey {
                    key: key("a/3/mode"),
                },
            ],
        );

        let mut ops = Vec::new();
        let layout = operations.into_ops(&mut ops);
        assert_eq!(ops.len(), 4);
        assert!(matches!(ops[0], Op::Assert(_)));
        assert!(matches!(ops[3], Op::GetKey(_)));

        let mode = Stored {
            seq: Seq(7),
            entry: KeyValue {
                key: key("a/2/mode"),
                value: Bytes::from_static(b"0644"),
            },
        };
        let mut outcomes = vec![
            Outcome::Done,
            Outcome::Done,
            Outcome::Value(Some(mode.clone())),
            Outcome::Value(None),
        ]
        .into_iter();

        let ((), (), modes): ((), (), Vec1<Option<Stored<KeyValue>>>) =
            <(Assert, SetKey, Vec1<GetKey>)>::from_outcomes(layout, &mut outcomes).unwrap();
        assert_eq!(modes, vec1![Some(mode), None]);
        assert!(outcomes.next().is_none());
    }

    #[test]
    fn mismatched_outcome_is_rejected() {
        GetKey { key: key("x") }.into_ops(&mut Vec::new());
        let mut outcomes = vec![Outcome::Done].into_iter();
        assert!(GetKey::from_outcomes((), &mut outcomes).is_none());
    }
}
