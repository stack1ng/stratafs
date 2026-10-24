use std::cmp::Reverse;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

use bytes::Bytes;
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use zerocopy::little_endian::U128;

use super::*;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct ChildEntry {
    child: U128,
    key: [u8],
}

fn list_of<R: AddressableRange>(entries: &[&[u8]]) -> SlottedList<BytesMut, [u8], R> {
    let mut list = SlottedList::<BytesMut, [u8], R>::empty();
    for (index, entry) in entries.iter().copied().enumerate() {
        list.insert(index, entry);
    }
    list
}

fn entries<B: AsRef<[u8]>, R: AddressableRange>(list: &SlottedList<B, [u8], R>) -> Vec<Vec<u8>> {
    (0..list.len())
        .map(|index| list.get(index).unwrap().unwrap().to_vec())
        .collect()
}

fn stored<R: AddressableRange>(data: &[u8], starts: &[usize]) -> Vec<u8> {
    let mut bytes = data.to_vec();
    for start in starts {
        bytes.extend_from_slice(R::encode(*start).unwrap().as_bytes());
    }
    bytes.extend_from_slice(R::encode(starts.len()).unwrap().as_bytes());
    bytes
}

fn corruption<R: AddressableRange>(bytes: &[u8]) -> Option<CorruptValue> {
    SlottedList::<&[u8], [u8], R>::new(bytes).err()
}

fn editable<R: AddressableRange>(bytes: &[u8]) -> SlottedList<BytesMut, [u8], R> {
    SlottedList::new(BytesMut::from(bytes)).unwrap()
}

fn edit_panics_without_changing<R: AddressableRange>(
    bytes: &[u8],
    edit: impl FnOnce(&mut SlottedList<BytesMut, [u8], R>),
) {
    let mut list = editable::<R>(bytes);
    let panic = catch_unwind(AssertUnwindSafe(|| edit(&mut list))).unwrap_err();
    assert_eq!(&list.into_inner()[..], bytes);
    resume_unwind(panic);
}

#[test]
fn an_empty_list_is_only_its_count() {
    let list = SlottedList::<BytesMut, [u8], u16>::empty();
    assert_eq!(list.len(), 0);
    assert!(list.get(0).is_none());
    assert_eq!(&list.into_inner()[..], [0, 0]);
}

#[test]
fn entries_are_stored_before_their_starts_and_count() {
    let mut list = list_of::<u16>(&[b"ab", b"cde"]);
    list.insert(1, b"x");
    assert_eq!(entries(&list), [&b"ab"[..], b"x", b"cde"]);
    assert_eq!(&list.into_inner()[..], stored::<u16>(b"abxcde", &[0, 2, 3]));
}

#[test]
fn addresses_take_the_width_of_their_range() {
    assert_eq!(
        &list_of::<u8>(&[b"ab", b"c"]).into_inner()[..],
        b"abc\x00\x02\x02"
    );
    assert_eq!(
        &list_of::<u16>(&[b"ab", b"c"]).into_inner()[..],
        b"abc\x00\x00\x02\x00\x02\x00"
    );
    assert_eq!(
        &list_of::<u32>(&[b"ab", b"c"]).into_inner()[..],
        b"abc\x00\x00\x00\x00\x02\x00\x00\x00\x02\x00\x00\x00"
    );
}

#[test]
fn inserts_go_before_the_entry_at_their_index() {
    let mut list = SlottedList::<BytesMut, [u8], u16>::empty();
    list.insert(0, b"m");
    list.insert(0, b"a");
    list.insert(2, b"z");
    list.insert(2, b"q");
    assert_eq!(entries(&list), [&b"a"[..], b"m", b"q", b"z"]);
}

#[test]
fn removes_close_the_gap() {
    let mut list = list_of::<u16>(&[b"ab", b"cde", b"f", b"ghij"]);
    list.remove(1);
    assert_eq!(entries(&list), [&b"ab"[..], b"f", b"ghij"]);
    list.remove(2);
    assert_eq!(entries(&list), [&b"ab"[..], b"f"]);
    list.remove(0);
    assert_eq!(entries(&list), [&b"f"[..]]);
    list.remove(0);
    assert_eq!(&list.into_inner()[..], [0, 0]);
}

#[test]
fn replaces_resize_only_their_entry() {
    let mut list = list_of::<u16>(&[b"ab", b"cde", b"f"]);
    list.replace(1, b"xyz");
    assert_eq!(entries(&list), [&b"ab"[..], b"xyz", b"f"]);
    list.replace(1, b"longer entry");
    assert_eq!(entries(&list), [&b"ab"[..], b"longer entry", b"f"]);
    list.replace(1, b"");
    assert_eq!(entries(&list), [&b"ab"[..], b"", b"f"]);
    list.replace(2, b"end");
    assert_eq!(entries(&list), [&b"ab"[..], b"", b"end"]);
    assert_eq!(&list.into_inner()[..], stored::<u16>(b"abend", &[0, 2, 2]));
}

#[test]
fn split_off_keeps_the_front_and_returns_the_back() {
    let mut front = list_of::<u16>(&[b"ab", b"cde", b"f", b"ghij"]);
    let back = front.split_off(2);
    assert_eq!(&front.into_inner()[..], stored::<u16>(b"abcde", &[0, 2]));
    assert_eq!(&back.into_inner()[..], stored::<u16>(b"fghij", &[0, 1]));
}

#[test]
fn extend_from_list_copies_the_other_lists_entries_onto_the_end() {
    let mut front = list_of::<u16>(&[b"ab", b"cde"]);
    let back_bytes = list_of::<u16>(&[b"f", b"ghij"]).into_inner().freeze();
    let back = SlottedList::<Bytes, [u8], u16>::new(back_bytes.clone()).unwrap();
    front.extend_from_list(&back);
    assert_eq!(
        &front.into_inner()[..],
        stored::<u16>(b"abcdefghij", &[0, 2, 5, 6])
    );
    assert_eq!(back.into_inner(), back_bytes);
}

#[test]
fn extend_from_list_with_an_empty_side() {
    let mut list = list_of::<u16>(&[b"ab", b"cde"]);
    list.extend_from_list(&list_of::<u16>(&[]));
    assert_eq!(entries(&list), [&b"ab"[..], b"cde"]);
    let mut empty = list_of::<u16>(&[]);
    empty.extend_from_list(&list);
    assert_eq!(entries(&empty), [&b"ab"[..], b"cde"]);
}

#[test]
fn split_off_at_either_end_leaves_one_side_empty() {
    let mut front = list_of::<u16>(&[b"ab", b"cde"]);
    let back = front.split_off(2);
    assert_eq!(entries(&front), [&b"ab"[..], b"cde"]);
    assert_eq!(&back.into_inner()[..], stored::<u16>(b"", &[]));
    let back = front.split_off(0);
    assert_eq!(&front.into_inner()[..], stored::<u16>(b"", &[]));
    assert_eq!(entries(&back), [&b"ab"[..], b"cde"]);
}

#[test]
fn zeroed_edits_hand_back_their_entry_cleared() {
    let mut list = list_of::<u16>(&[b"ab", b"cde"]);
    let inserted = list.insert_zeroed(1, 3);
    assert_eq!(inserted, [0, 0, 0]);
    inserted.copy_from_slice(b"xyz");
    let replaced = list.replace_zeroed(0, 4);
    assert_eq!(replaced, [0, 0, 0, 0]);
    replaced.copy_from_slice(b"long");
    assert_eq!(list.replace_zeroed(2, 1), [0]);
    assert_eq!(entries(&list), [&b"long"[..], b"xyz", b"\0"]);
}

#[test]
fn stored_bytes_are_read_in_place() {
    let bytes = list_of::<u16>(&[b"ab", b"cde"]).into_inner().freeze();
    let list = SlottedList::<Bytes, [u8], u16>::new(bytes.clone()).unwrap();
    assert_eq!(entries(&list), [&b"ab"[..], b"cde"]);
    assert_eq!(list.get(0).unwrap().unwrap().as_ptr(), bytes.as_ptr());
    assert_eq!(list.get_ref().as_ptr(), bytes.as_ptr());
}

#[test]
fn a_stored_list_becomes_editable_in_place_when_nothing_else_holds_it() {
    let bytes = list_of::<u16>(&[b"ab", b"cde"]).into_inner().freeze();
    let address = bytes.as_ptr();
    let mut list = SlottedList::<BytesMut, [u8], u16>::from(
        SlottedList::<Bytes, [u8], u16>::new(bytes).unwrap(),
    );
    assert_eq!(list.get_ref().as_ptr(), address);
    list.replace(0, b"xy");
    assert_eq!(entries(&list), [&b"xy"[..], b"cde"]);
}

#[test]
fn a_shared_stored_list_is_copied_when_made_editable() {
    let bytes = list_of::<u16>(&[b"ab", b"cde"]).into_inner().freeze();
    let mut list = SlottedList::<BytesMut, [u8], u16>::from(
        SlottedList::<Bytes, [u8], u16>::new(bytes.clone()).unwrap(),
    );
    assert_ne!(list.get_ref().as_ptr(), bytes.as_ptr());
    list.insert(2, b"f");
    assert_eq!(entries(&list), [&b"ab"[..], b"cde", b"f"]);
    let unchanged = SlottedList::<Bytes, [u8], u16>::new(bytes).unwrap();
    assert_eq!(entries(&unchanged), [&b"ab"[..], b"cde"]);
}

fn child_entries(children: &[u128]) -> SlottedList<BytesMut, ChildEntry, u16> {
    let mut list = SlottedList::<BytesMut, ChildEntry, u16>::empty();
    for (index, child) in children.iter().enumerate() {
        let entry = [U128::new(*child).as_bytes(), b"k"].concat();
        list.insert(index, ChildEntry::ref_from_bytes(&entry).unwrap());
    }
    list
}

#[test]
fn entries_can_be_edited_in_place() {
    let mut list = child_entries(&[7, 8]);
    let stored_length = list.stored_length();
    list.get_mut(1).unwrap().unwrap().child = U128::new(9);
    let children: Vec<_> = (0..list.len())
        .map(|index| list.get(index).unwrap().unwrap().child.get())
        .collect();
    assert_eq!(children, [7, 9]);
    assert_eq!(list.stored_length(), stored_length);
    assert!(list.get_mut(2).is_none());
}

#[test]
fn an_untyped_list_reads_each_entry_as_its_bytes() {
    let list = child_entries(&[7, 8]);
    let typed: Vec<Vec<u8>> = (0..list.len())
        .map(|index| list.get(index).unwrap().unwrap().as_bytes().to_vec())
        .collect();
    let untyped = list.into_untyped();
    assert_eq!(entries(&untyped), typed);
}

#[test]
fn entries_can_be_dynamically_sized_structs() {
    let first = U128::new(7).as_bytes().to_vec();
    let mut second = U128::new(8).as_bytes().to_vec();
    second.extend_from_slice(b"m");
    let mut list = SlottedList::<BytesMut, ChildEntry, u16>::empty();
    list.insert(0, ChildEntry::ref_from_bytes(&first).unwrap());
    list.insert(1, ChildEntry::ref_from_bytes(&second).unwrap());
    let entry = list.get(1).unwrap().unwrap();
    assert_eq!((entry.child.get(), &entry.key), (8, &b"m"[..]));
}

#[test]
fn entries_of_the_wrong_length_for_their_type_are_reported() {
    let bytes = stored::<u16>(b"abc", &[0]);
    let too_short = SlottedList::<&[u8], ChildEntry, u16>::new(&bytes[..]).unwrap();
    assert!(matches!(
        too_short.get(0),
        Some(Err(CorruptValue::WrongLength))
    ));
    let too_long = SlottedList::<&[u8], U16, u16>::new(&bytes[..]).unwrap();
    assert!(matches!(
        too_long.get(0),
        Some(Err(CorruptValue::WrongLength))
    ));
}

#[test]
fn lists_too_short_for_their_count_are_rejected() {
    assert!(matches!(
        corruption::<u16>(&[]),
        Some(CorruptValue::WrongLength)
    ));
    assert!(matches!(
        corruption::<u16>(&[0]),
        Some(CorruptValue::WrongLength)
    ));
    assert!(matches!(
        corruption::<u16>(&[0, 0, 2, 0]),
        Some(CorruptValue::WrongLength)
    ));
}

#[test]
fn starts_that_do_not_describe_the_data_are_rejected() {
    for bytes in [
        stored::<u16>(b"ab", &[]),
        stored::<u16>(b"ab", &[1]),
        stored::<u16>(b"abc", &[0, 2, 1]),
        stored::<u16>(b"ab", &[0, 3]),
    ] {
        assert!(matches!(
            corruption::<u16>(&bytes),
            Some(CorruptValue::InvalidEntryPositions)
        ));
    }
}

#[test]
#[should_panic(expected = "insert index 2 is past the end of a list of 1")]
fn inserting_past_the_end_panics() {
    edit_panics_without_changing::<u16>(&stored::<u16>(b"a", &[0]), |list| list.insert(2, b"b"));
}

#[test]
#[should_panic(expected = "remove index 1 is past the end of a list of 1")]
fn removing_past_the_end_panics() {
    edit_panics_without_changing::<u16>(&stored::<u16>(b"a", &[0]), |list| list.remove(1));
}

#[test]
#[should_panic(expected = "replace index 1 is past the end of a list of 1")]
fn replacing_past_the_end_panics() {
    edit_panics_without_changing::<u16>(&stored::<u16>(b"a", &[0]), |list| list.replace(1, b"b"));
}

#[test]
#[should_panic(expected = "split index 2 is past the end of a list of 1")]
fn splitting_past_the_end_panics() {
    edit_panics_without_changing::<u16>(&stored::<u16>(b"a", &[0]), |list| {
        list.split_off(2);
    });
}

fn the_largest_stored_address_is_the_max<R: AddressableRange>() {
    let largest = R::Stored::read_from_bytes(&[u8::MAX; 4][..size_of::<R::Stored>()]).unwrap();
    assert_eq!(R::decode(&largest), R::MAX);
    assert_eq!(
        R::encode(R::MAX).map(|stored| R::decode(&stored)),
        Some(R::MAX)
    );
    assert!(R::MAX.checked_add(1).and_then(R::encode).is_none());
}

#[test]
fn addresses_span_exactly_their_stored_width() {
    the_largest_stored_address_is_the_max::<u8>();
    the_largest_stored_address_is_the_max::<u16>();
    the_largest_stored_address_is_the_max::<u32>();
}

#[test]
fn each_width_is_optimal_just_past_the_narrower_max() {
    assert_eq!(<u8 as AddressableRange>::SMALLEST_OPTIMAL_CAPACITY, 0);
    assert_eq!(
        <u16 as AddressableRange>::SMALLEST_OPTIMAL_CAPACITY,
        <u8 as AddressableRange>::MAX + 1
    );
    assert_eq!(
        <u32 as AddressableRange>::SMALLEST_OPTIMAL_CAPACITY,
        <u16 as AddressableRange>::MAX + 1
    );
}

fn search(list: &SlottedList<BytesMut, [u8], u16>, key: &[u8]) -> Result<usize, usize> {
    list.binary_search_by(|entry| Ok(entry.cmp(key))).unwrap()
}

#[test]
fn binary_search_finds_entries_and_their_insertion_points() {
    let list = list_of::<u16>(&[b"b", b"d", b"f"]);
    assert_eq!(search(&list, b"a"), Err(0));
    assert_eq!(search(&list, b"b"), Ok(0));
    assert_eq!(search(&list, b"c"), Err(1));
    assert_eq!(search(&list, b"d"), Ok(1));
    assert_eq!(search(&list, b"e"), Err(2));
    assert_eq!(search(&list, b"f"), Ok(2));
    assert_eq!(search(&list, b"g"), Err(3));
}

#[test]
fn binary_search_on_an_empty_list_inserts_at_the_start() {
    assert_eq!(search(&list_of::<u16>(&[]), b"a"), Err(0));
}

#[test]
fn the_midpoint_is_the_index_where_the_front_is_closest_to_half_the_size() {
    assert_eq!(
        list_of::<u16>(&[b"aaaa", b"b", b"c", b"dddd"]).n_midpoints_by_size(1),
        vec![2]
    );
    assert_eq!(
        list_of::<u16>(&[b"aaaaaaaa", b"b", b"c"]).n_midpoints_by_size(1),
        vec![1]
    );
    assert_eq!(
        list_of::<u16>(&[b"b", b"c", b"aaaaaaaa"]).n_midpoints_by_size(1),
        vec![2]
    );
    assert_eq!(list_of::<u16>(&[b"a"]).n_midpoints_by_size(1), vec![1]);
    assert_eq!(list_of::<u16>(&[]).n_midpoints_by_size(1), vec![0]);
}

#[test]
fn the_size_before_an_index_is_the_data_and_slots_of_the_entries_before_it() {
    let list = list_of::<u16>(&[b"aa", b"b"]);
    assert_eq!(list.size_before(0), 0);
    assert_eq!(list.size_before(1), 4);
    assert_eq!(list.size_before(2), 7);
}

#[test]
fn n_midpoints_are_the_indices_where_the_front_is_closest_to_each_share_of_the_size() {
    assert_eq!(
        list_of::<u16>(&[b"aa", b"bb", b"cc", b"dd", b"ee", b"ff"]).n_midpoints_by_size(2),
        vec![2, 4]
    );
    assert_eq!(
        list_of::<u16>(&[b"aaaaaaaaaa", b"b", b"c", b"d", b"e"]).n_midpoints_by_size(2),
        vec![1, 2]
    );
    assert_eq!(list_of::<u16>(&[b"a"]).n_midpoints_by_size(0), vec![]);
}

fn midpoint_matches_the_index_closest_to_half_the_size<R: AddressableRange>(
    entries: Vec<Vec<u8>>,
) -> Result<(), TestCaseError> {
    let list = list_of::<R>(&entries.iter().map(Vec::as_slice).collect::<Vec<_>>());
    let mut fronts = vec![0];
    for entry in &entries {
        fronts.push(fronts[fronts.len() - 1] + entry.len() + size_of::<R::Stored>());
    }
    let total = fronts[fronts.len() - 1];
    let expected = (0..fronts.len())
        .min_by_key(|&index| {
            (
                fronts[index].abs_diff(total - fronts[index]),
                Reverse(index),
            )
        })
        .unwrap();
    prop_assert_eq!(list.n_midpoints_by_size(1), vec![expected]);
    Ok(())
}

#[test]
fn binary_search_reports_corrupt_entries() {
    let bytes = stored::<u16>(b"abc", &[0]);
    let list = SlottedList::<&[u8], ChildEntry, u16>::new(&bytes[..]).unwrap();
    assert!(matches!(
        list.binary_search_by(|_| Ok(Ordering::Equal)),
        Err(CorruptValue::WrongLength)
    ));
}

#[test]
fn binary_search_reports_errors_from_its_comparison() {
    let list = list_of::<u16>(&[b"a"]);
    assert!(matches!(
        list.binary_search_by(|_| Err(CorruptValue::KeyLongerThanEntry)),
        Err(CorruptValue::KeyLongerThanEntry)
    ));
}

fn lists_at_the_limits_are_accepted<R: AddressableRange>() {
    for bytes in [
        stored::<R>(&vec![0; R::MAX], &[0, R::MAX]),
        stored::<R>(b"", &vec![0; R::MAX]),
    ] {
        assert!(corruption::<R>(&bytes).is_none());
    }
}

fn entry_data_beyond_the_range_is_rejected<R: AddressableRange>() {
    let bytes = stored::<R>(&vec![0; R::MAX + 1], &[0]);
    assert!(matches!(
        corruption::<R>(&bytes),
        Some(CorruptValue::WrongLength)
    ));
}

fn entry_data_can_fill_the_range<R: AddressableRange>() {
    let mut list = list_of::<R>(&[&vec![0; R::MAX][..]]);
    list.replace(0, &vec![1; R::MAX]);
    assert!(corruption::<R>(&list.into_inner()).is_none());
}

fn inserted_entry_data_cannot_outgrow_the_range<R: AddressableRange>() {
    edit_panics_without_changing::<R>(&stored::<R>(&vec![0; R::MAX], &[0]), |list| {
        list.insert(1, b"x")
    });
}

fn replaced_entry_data_cannot_outgrow_the_range<R: AddressableRange>() {
    edit_panics_without_changing::<R>(&stored::<R>(&vec![0; R::MAX], &[0]), |list| {
        list.replace(0, &vec![0; R::MAX + 1])
    });
}

fn extended_entry_data_cannot_outgrow_the_range<R: AddressableRange>() {
    edit_panics_without_changing::<R>(&stored::<R>(&vec![0; R::MAX], &[0]), |list| {
        list.extend_from_list(&list_of::<R>(&[b"x"]))
    });
}

fn entry_count_can_fill_the_range<R: AddressableRange>() {
    let mut list = editable::<R>(&stored::<R>(b"", &vec![0; R::MAX - 1]));
    list.insert(0, b"");
    assert_eq!(list.len(), R::MAX);
}

fn entry_count_cannot_outgrow_the_range<R: AddressableRange>() {
    edit_panics_without_changing::<R>(&stored::<R>(b"", &vec![0; R::MAX]), |list| {
        list.insert(0, b"")
    });
}

macro_rules! limit_tests {
    ($($module:ident: $range:ty),*) => {$(
        mod $module {
            #[test]
            fn lists_at_the_limits_are_accepted() {
                super::lists_at_the_limits_are_accepted::<$range>();
            }

            #[test]
            fn entry_data_beyond_the_range_is_rejected() {
                super::entry_data_beyond_the_range_is_rejected::<$range>();
            }

            #[test]
            fn entry_data_can_fill_the_range() {
                super::entry_data_can_fill_the_range::<$range>();
            }

            #[test]
            #[should_panic(expected = "bytes of entry data")]
            fn inserted_entry_data_cannot_outgrow_the_range() {
                super::inserted_entry_data_cannot_outgrow_the_range::<$range>();
            }

            #[test]
            #[should_panic(expected = "bytes of entry data")]
            fn replaced_entry_data_cannot_outgrow_the_range() {
                super::replaced_entry_data_cannot_outgrow_the_range::<$range>();
            }

            #[test]
            #[should_panic(expected = "bytes of entry data")]
            fn extended_entry_data_cannot_outgrow_the_range() {
                super::extended_entry_data_cannot_outgrow_the_range::<$range>();
            }

            #[test]
            fn entry_count_can_fill_the_range() {
                super::entry_count_can_fill_the_range::<$range>();
            }

            #[test]
            #[should_panic(expected = "entries and")]
            fn entry_count_cannot_outgrow_the_range() {
                super::entry_count_cannot_outgrow_the_range::<$range>();
            }
        }
    )*};
}

limit_tests!(u8_limits: u8, u16_limits: u16);

#[derive(Debug, Clone)]
enum Edit {
    Insert(usize, Vec<u8>),
    Remove(usize),
    Replace(usize, Vec<u8>),
    SplitOff(usize),
    ExtendFromList(Vec<Vec<u8>>),
}

fn edit() -> impl Strategy<Value = Edit> {
    prop_oneof![
        (any::<usize>(), vec(any::<u8>(), 0..24))
            .prop_map(|(index, entry)| Edit::Insert(index, entry)),
        any::<usize>().prop_map(Edit::Remove),
        (any::<usize>(), vec(any::<u8>(), 0..24))
            .prop_map(|(index, entry)| Edit::Replace(index, entry)),
        any::<usize>().prop_map(Edit::SplitOff),
        vec(vec(any::<u8>(), 0..24), 0..4).prop_map(Edit::ExtendFromList),
    ]
}

fn edits_match_a_vector_of_entries<R: AddressableRange>(
    edits: Vec<Edit>,
) -> Result<(), TestCaseError> {
    let mut list = SlottedList::<BytesMut, [u8], R>::empty();
    let mut model: Vec<Vec<u8>> = Vec::new();
    for edit in edits {
        let data_length = model.iter().map(Vec::len).sum::<usize>();
        match edit {
            Edit::Insert(index, entry)
                if data_length + entry.len() <= R::MAX && model.len() < R::MAX =>
            {
                let index = index % (model.len() + 1);
                list.insert(index, &entry);
                model.insert(index, entry);
            }
            Edit::Remove(index) if !model.is_empty() => {
                let index = index % model.len();
                list.remove(index);
                model.remove(index);
            }
            Edit::Replace(index, entry) if !model.is_empty() => {
                let index = index % model.len();
                if data_length - model[index].len() + entry.len() <= R::MAX {
                    list.replace(index, &entry);
                    model[index] = entry;
                }
            }
            Edit::SplitOff(index) => {
                let index = index % (model.len() + 1);
                let back =
                    SlottedList::<_, [u8], R>::new(list.split_off(index).into_inner()).unwrap();
                prop_assert_eq!(entries(&back), model.split_off(index));
            }
            Edit::ExtendFromList(others)
                if data_length + others.iter().map(Vec::len).sum::<usize>() <= R::MAX
                    && model.len() + others.len() <= R::MAX =>
            {
                list.extend_from_list(&list_of::<R>(
                    &others.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                ));
                model.extend(others);
            }
            Edit::Insert(..) | Edit::Remove(_) | Edit::Replace(..) | Edit::ExtendFromList(_) => {}
        }
        list = SlottedList::new(list.into_inner()).unwrap();
        prop_assert_eq!(&entries(&list), &model);
        let data_length = model.iter().map(Vec::len).sum::<usize>();
        let expected_length = data_length + (model.len() + 1) * size_of::<R::Stored>();
        prop_assert_eq!(list.stored_length(), expected_length);
    }
    Ok(())
}

proptest! {
    #[test]
    fn u8_edits_match_a_vector_of_entries(edits in vec(edit(), 0..64)) {
        edits_match_a_vector_of_entries::<u8>(edits)?;
    }

    #[test]
    fn u16_edits_match_a_vector_of_entries(edits in vec(edit(), 0..64)) {
        edits_match_a_vector_of_entries::<u16>(edits)?;
    }

    #[test]
    fn u32_edits_match_a_vector_of_entries(edits in vec(edit(), 0..64)) {
        edits_match_a_vector_of_entries::<u32>(edits)?;
    }

    #[test]
    fn u8_midpoint_matches_the_index_closest_to_half_the_size(entries in vec(vec(any::<u8>(), 0..8), 0..16)) {
        midpoint_matches_the_index_closest_to_half_the_size::<u8>(entries)?;
    }

    #[test]
    fn u16_midpoint_matches_the_index_closest_to_half_the_size(entries in vec(vec(any::<u8>(), 0..24), 0..32)) {
        midpoint_matches_the_index_closest_to_half_the_size::<u16>(entries)?;
    }
}

#[test]
fn truncate_keeps_the_front_entries() {
    let mut list = list_of::<u16>(&[b"aa", b"b", b"cccc"]);
    list.truncate(2);
    assert_eq!(list.len(), 2);
    assert_eq!(list.get(0).unwrap().unwrap(), b"aa");
    assert_eq!(list.get(1).unwrap().unwrap(), b"b");
    list.truncate(5);
    assert_eq!(list.len(), 2);
    list.truncate(0);
    assert_eq!(list.len(), 0);
}

#[test]
fn extending_from_an_index_appends_only_the_entries_from_there() {
    let mut list = list_of::<u16>(&[b"aa"]);
    list.extend_from_list_starting_at(&list_of::<u16>(&[b"b", b"cc", b"ddd"]), 1);
    assert_eq!(list.len(), 3);
    assert_eq!(list.get(0).unwrap().unwrap(), b"aa");
    assert_eq!(list.get(1).unwrap().unwrap(), b"cc");
    assert_eq!(list.get(2).unwrap().unwrap(), b"ddd");
    list.extend_from_list_starting_at(&list_of::<u16>(&[b"e"]), 1);
    assert_eq!(list.len(), 3);
}

#[test]
fn splice_replaces_a_range_of_entries() {
    let mut list = list_of::<u16>(&[b"a", b"bb", b"c", b"dd"]);
    list.splice(1..3, &list_of::<u16>(&[b"x", b"yyy", b"z"]));
    assert_eq!(
        entries(&list),
        [
            b"a".to_vec(),
            b"x".to_vec(),
            b"yyy".to_vec(),
            b"z".to_vec(),
            b"dd".to_vec()
        ]
    );
    list.splice(0..5, &list_of::<u16>(&[]));
    assert!(entries(&list).is_empty());
}
