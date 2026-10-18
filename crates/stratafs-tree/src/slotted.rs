use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::Range;

use bytes::BytesMut;
use zerocopy::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::serialized::CorruptValue;

pub trait AddressableRange: Send + Sync + 'static {
    type Stored: FromBytes + IntoBytes + KnownLayout + Immutable + Unaligned;
    const MAX: usize;
    const SMALLEST_OPTIMAL_CAPACITY: usize;

    fn decode(stored: &Self::Stored) -> usize;
    fn encode(address: usize) -> Option<Self::Stored>;
}

impl AddressableRange for u8 {
    type Stored = u8;
    const MAX: usize = u8::MAX as usize;
    const SMALLEST_OPTIMAL_CAPACITY: usize = 0;

    fn decode(stored: &u8) -> usize {
        usize::from(*stored)
    }

    fn encode(address: usize) -> Option<u8> {
        u8::try_from(address).ok()
    }
}

impl AddressableRange for u16 {
    type Stored = U16;
    const MAX: usize = u16::MAX as usize;
    const SMALLEST_OPTIMAL_CAPACITY: usize = u8::MAX as usize + 1;

    fn decode(stored: &U16) -> usize {
        usize::from(stored.get())
    }

    fn encode(address: usize) -> Option<U16> {
        u16::try_from(address).ok().map(U16::new)
    }
}

impl AddressableRange for u32 {
    type Stored = U32;
    const MAX: usize = u32::MAX as usize;
    const SMALLEST_OPTIMAL_CAPACITY: usize = u16::MAX as usize + 1;

    fn decode(stored: &U32) -> usize {
        usize::try_from(stored.get()).expect("usize holds a u32 on every supported target")
    }

    fn encode(address: usize) -> Option<U32> {
        u32::try_from(address).ok().map(U32::new)
    }
}

// SlottedList is a size optimized zero copy list.
pub(crate) struct SlottedList<B, T: ?Sized, R> {
    bytes: B,
    entry: PhantomData<T>,
    address: PhantomData<R>,
}

impl<B, T, R> SlottedList<B, T, R>
where
    B: AsRef<[u8]>,
    T: ?Sized + FromBytes + KnownLayout + Immutable + Unaligned,
    R: AddressableRange,
{
    const ADDRESS_LENGTH: usize = size_of::<R::Stored>();

    pub(crate) fn new(bytes: B) -> Result<Self, CorruptValue> {
        let (data, starts) = split::<R>(bytes.as_ref()).ok_or(CorruptValue::WrongLength)?;
        if data.len() > R::MAX {
            return Err(CorruptValue::WrongLength);
        }
        let mut boundaries = starts.iter().map(R::decode).chain([data.len()]);
        if boundaries.next() != Some(0) || !boundaries.is_sorted() {
            return Err(CorruptValue::InvalidEntryPositions);
        }
        Ok(Self {
            bytes,
            entry: PhantomData,
            address: PhantomData,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.parts().1.len()
    }

    pub(crate) fn stored_length(&self) -> usize {
        self.bytes.as_ref().len()
    }

    pub(crate) fn get(&self, index: usize) -> Option<Result<&T, CorruptValue>> {
        let entry = self.entry_range(index)?;
        Some(T::ref_from_bytes(&self.bytes.as_ref()[entry]).map_err(|_| CorruptValue::WrongLength))
    }

    pub(crate) fn binary_search_by(
        &self,
        mut compare: impl FnMut(&T) -> Result<Ordering, CorruptValue>,
    ) -> Result<Result<usize, usize>, CorruptValue> {
        let (mut low, mut high) = (0, self.len());
        while low < high {
            let middle = low + (high - low) / 2;
            let entry = self.get(middle).expect("middle is below the length")?;
            match compare(entry)? {
                Ordering::Less => low = middle + 1,
                Ordering::Greater => high = middle,
                Ordering::Equal => return Ok(Ok(middle)),
            }
        }
        Ok(Err(low))
    }

    fn parts(&self) -> (&[u8], &[R::Stored]) {
        split::<R>(self.bytes.as_ref())
            .expect("new checked the list's lengths, and edits keep them consistent")
    }

    fn entry_range(&self, index: usize) -> Option<Range<usize>> {
        let (data, starts) = self.parts();
        let start = R::decode(starts.get(index)?);
        let end = starts.get(index + 1).map_or(data.len(), R::decode);
        Some(start..end)
    }
}

impl<B, T: ?Sized, R> SlottedList<B, T, R> {
    pub(crate) fn into_inner(self) -> B {
        self.bytes
    }
}

impl<T: ?Sized, R: AddressableRange> SlottedList<BytesMut, T, R> {
    pub(crate) fn empty() -> Self {
        Self {
            bytes: BytesMut::from(address::<R>(0).as_bytes()),
            entry: PhantomData,
            address: PhantomData,
        }
    }
}

impl<T, R> SlottedList<BytesMut, T, R>
where
    T: ?Sized + FromBytes + IntoBytes + KnownLayout + Immutable + Unaligned,
    R: AddressableRange,
{
    pub(crate) fn insert(&mut self, index: usize, entry: &T) {
        let entry = entry.as_bytes();
        let (data_length, count) = self.lengths();
        assert!(
            index <= count,
            "insert index {index} is past the end of a list of {count}"
        );
        assert!(
            data_length + entry.len() <= R::MAX && count < R::MAX,
            "a slotted list holds at most {max} entries and {max} bytes of entry data",
            max = R::MAX
        );
        let at = self
            .entry_range(index)
            .map_or(data_length, |next| next.start);
        let slot = data_length + index * Self::ADDRESS_LENGTH;
        let old_length = self.bytes.len();
        self.bytes
            .resize(old_length + entry.len() + Self::ADDRESS_LENGTH, 0);
        self.bytes
            .copy_within(slot..old_length, slot + entry.len() + Self::ADDRESS_LENGTH);
        self.bytes.copy_within(at..slot, at + entry.len());
        self.bytes[at..at + entry.len()].copy_from_slice(entry);
        self.set_count(count + 1);
        let starts = self.starts_mut();
        starts[index] = address::<R>(at);
        for later in &mut starts[index + 1..] {
            *later = address::<R>(R::decode(later) + entry.len());
        }
    }

    pub(crate) fn remove(&mut self, index: usize) {
        let (data_length, count) = self.lengths();
        let Some(removed) = self.entry_range(index) else {
            panic!("remove index {index} is past the end of a list of {count}");
        };
        for later in &mut self.starts_mut()[index + 1..] {
            *later = address::<R>(R::decode(later) - removed.len());
        }
        let slot = data_length + index * Self::ADDRESS_LENGTH;
        let old_length = self.bytes.len();
        self.bytes.copy_within(removed.end..slot, removed.start);
        self.bytes.copy_within(
            slot + Self::ADDRESS_LENGTH..old_length,
            slot - removed.len(),
        );
        self.bytes
            .truncate(old_length - removed.len() - Self::ADDRESS_LENGTH);
        self.set_count(count - 1);
    }

    pub(crate) fn replace(&mut self, index: usize, entry: &T) {
        let entry = entry.as_bytes();
        let (data_length, count) = self.lengths();
        let Some(replaced) = self.entry_range(index) else {
            panic!("replace index {index} is past the end of a list of {count}");
        };
        assert!(
            data_length - replaced.len() + entry.len() <= R::MAX,
            "a slotted list holds at most {max} bytes of entry data",
            max = R::MAX
        );
        if entry.len() != replaced.len() {
            let old_length = self.bytes.len();
            let new_length = old_length - replaced.len() + entry.len();
            if new_length > old_length {
                self.bytes.resize(new_length, 0);
            }
            self.bytes
                .copy_within(replaced.end..old_length, replaced.start + entry.len());
            self.bytes.truncate(new_length);
            for later in &mut self.starts_mut()[index + 1..] {
                *later = address::<R>(R::decode(later) - replaced.len() + entry.len());
            }
        }
        self.bytes[replaced.start..replaced.start + entry.len()].copy_from_slice(entry);
    }

    fn lengths(&self) -> (usize, usize) {
        let (data, starts) = self.parts();
        (data.len(), starts.len())
    }

    fn starts_mut(&mut self) -> &mut [R::Stored] {
        let (rest, count) = R::Stored::mut_from_suffix(&mut self.bytes[..])
            .expect("the list always ends with its count");
        let count = R::decode(count);
        <[R::Stored]>::mut_from_suffix_with_elems(rest, count)
            .expect("the list always has room for its starts")
            .1
    }

    fn set_count(&mut self, count: usize) {
        *R::Stored::mut_from_suffix(&mut self.bytes[..])
            .expect("the list always ends with its count")
            .1 = address::<R>(count);
    }
}

fn split<R: AddressableRange>(list: &[u8]) -> Option<(&[u8], &[R::Stored])> {
    let (rest, count) = R::Stored::ref_from_suffix(list).ok()?;
    <[R::Stored]>::ref_from_suffix_with_elems(rest, R::decode(count)).ok()
}

fn address<R: AddressableRange>(value: usize) -> R::Stored {
    R::encode(value).expect("edits keep addresses and the count within the addressable range")
}

#[cfg(test)]
mod tests;
