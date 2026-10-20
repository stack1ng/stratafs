use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::Range;

use bytes::{Bytes, BytesMut};
use zerocopy::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::serialized::CorruptValue;

pub trait AddressableRange: Send + Sync + 'static {
    type Stored: FromBytes + IntoBytes + KnownLayout + Immutable + Unaligned + Send + Sync;
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

    // find the index where the entries before it (each one's slot + data) add up closest to half
    // of all entries' size (stored_length() without the trailing count).
    // The starts already store those running totals, so it can use binary search on them
    // to find the first index that reaches half, then compare it with the index before it.
    pub(crate) fn midpoint_by_size(&self) -> usize {
        let (data, starts) = self.parts();
        let total = data.len() + starts.len() * Self::ADDRESS_LENGTH;
        let front_size = |index: usize| {
            starts.get(index).map_or(data.len(), R::decode) + index * Self::ADDRESS_LENGTH
        };
        let (mut low, mut high) = (0, starts.len());
        while low < high {
            let middle = low + (high - low) / 2;
            if 2 * front_size(middle) < total {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let below = low.saturating_sub(1);
        let overshoot = 2 * front_size(low) - total;
        let undershoot = total - 2 * front_size(below);
        if undershoot < overshoot { below } else { low }
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

impl<T: ?Sized, R> From<SlottedList<Bytes, T, R>> for SlottedList<BytesMut, T, R> {
    fn from(list: SlottedList<Bytes, T, R>) -> Self {
        Self {
            bytes: BytesMut::from(list.bytes),
            entry: PhantomData,
            address: PhantomData,
        }
    }
}

impl<B, T: ?Sized, R> SlottedList<B, T, R> {
    pub(crate) fn get_ref(&self) -> &B {
        &self.bytes
    }

    pub(crate) fn into_inner(self) -> B {
        self.bytes
    }

    pub(crate) fn into_untyped(self) -> SlottedList<B, [u8], R> {
        SlottedList {
            bytes: self.bytes,
            entry: PhantomData,
            address: PhantomData,
        }
    }

    pub(crate) fn into_typed<U: ?Sized>(self) -> SlottedList<B, U, R> {
        SlottedList {
            bytes: self.bytes,
            entry: PhantomData,
            address: PhantomData,
        }
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
        let space = self.insert_space(index, entry.len());
        self.bytes[space].copy_from_slice(entry);
    }

    pub(crate) fn insert_zeroed(&mut self, index: usize, elems: usize) -> &mut T
    where
        T: KnownLayout<PointerMetadata = usize>,
    {
        let length = T::size_for_metadata(elems).expect("an entry fits in memory");
        let space = self.insert_space(index, length);
        zeroed_entry(&mut self.bytes[space], elems)
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

    // split_off has the same semantics as Vec::split_off: this list keeps the entries [0, index),
    // and the returned list holds the entries [index, len). It panics if index is past the end.
    pub(crate) fn split_off(&mut self, index: usize) -> Self {
        let (data_length, count) = self.lengths();
        assert!(
            index <= count,
            "split index {index} is past the end of a list of {count}"
        );
        let at = self
            .entry_range(index)
            .map_or(data_length, |first| first.start);
        let moved_starts = &self.parts().1[index..];
        let mut back = BytesMut::with_capacity(
            data_length - at + (moved_starts.len() + 1) * Self::ADDRESS_LENGTH,
        );
        back.extend_from_slice(&self.bytes[at..data_length]);
        for start in moved_starts {
            back.extend_from_slice(address::<R>(R::decode(start) - at).as_bytes());
        }
        back.extend_from_slice(address::<R>(count - index).as_bytes());
        self.bytes
            .copy_within(data_length..data_length + index * Self::ADDRESS_LENGTH, at);
        self.bytes.truncate(at + (index + 1) * Self::ADDRESS_LENGTH);
        self.set_count(index);
        Self {
            bytes: back,
            entry: PhantomData,
            address: PhantomData,
        }
    }

    pub(crate) fn replace(&mut self, index: usize, entry: &T) {
        let entry = entry.as_bytes();
        let space = self.replace_space(index, entry.len());
        self.bytes[space].copy_from_slice(entry);
    }

    pub(crate) fn replace_zeroed(&mut self, index: usize, elems: usize) -> &mut T
    where
        T: KnownLayout<PointerMetadata = usize>,
    {
        let length = T::size_for_metadata(elems).expect("an entry fits in memory");
        let space = self.replace_space(index, length);
        zeroed_entry(&mut self.bytes[space], elems)
    }

    pub(crate) fn get_mut(&mut self, index: usize) -> Option<Result<&mut T, CorruptValue>> {
        let entry = self.entry_range(index)?;
        Some(T::mut_from_bytes(&mut self.bytes[entry]).map_err(|_| CorruptValue::WrongLength))
    }

    fn insert_space(&mut self, index: usize, length: usize) -> Range<usize> {
        let (data_length, count) = self.lengths();
        assert!(
            index <= count,
            "insert index {index} is past the end of a list of {count}"
        );
        assert!(
            data_length + length <= R::MAX && count < R::MAX,
            "a slotted list holds at most {max} entries and {max} bytes of entry data",
            max = R::MAX
        );
        let at = self
            .entry_range(index)
            .map_or(data_length, |next| next.start);
        let slot = data_length + index * Self::ADDRESS_LENGTH;
        let old_length = self.bytes.len();
        self.bytes
            .resize(old_length + length + Self::ADDRESS_LENGTH, 0);
        self.bytes
            .copy_within(slot..old_length, slot + length + Self::ADDRESS_LENGTH);
        self.bytes.copy_within(at..slot, at + length);
        self.set_count(count + 1);
        let starts = self.starts_mut();
        starts[index] = address::<R>(at);
        for later in &mut starts[index + 1..] {
            *later = address::<R>(R::decode(later) + length);
        }
        at..at + length
    }

    fn replace_space(&mut self, index: usize, length: usize) -> Range<usize> {
        let (data_length, count) = self.lengths();
        let Some(replaced) = self.entry_range(index) else {
            panic!("replace index {index} is past the end of a list of {count}");
        };
        assert!(
            data_length - replaced.len() + length <= R::MAX,
            "a slotted list holds at most {max} bytes of entry data",
            max = R::MAX
        );
        if length != replaced.len() {
            let old_length = self.bytes.len();
            let new_length = old_length - replaced.len() + length;
            if new_length > old_length {
                self.bytes.resize(new_length, 0);
            }
            self.bytes
                .copy_within(replaced.end..old_length, replaced.start + length);
            self.bytes.truncate(new_length);
            for later in &mut self.starts_mut()[index + 1..] {
                *later = address::<R>(R::decode(later) - replaced.len() + length);
            }
        }
        replaced.start..replaced.start + length
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

fn zeroed_entry<T>(space: &mut [u8], elems: usize) -> &mut T
where
    T: ?Sized + FromBytes + IntoBytes + KnownLayout<PointerMetadata = usize> + Immutable,
{
    space.fill(0);
    T::mut_from_bytes_with_elems(space, elems).expect("the space holds exactly one entry")
}

#[cfg(test)]
mod tests;
