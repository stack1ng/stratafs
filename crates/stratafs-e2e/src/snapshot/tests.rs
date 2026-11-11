use super::*;

fn file(content: &[u8], links: u64) -> Entry {
    Entry::File {
        content: content.to_vec(),
        links,
    }
}

fn entries<const N: usize>(entries: [(&str, Entry); N]) -> Snapshot {
    entries
        .into_iter()
        .map(|(path, entry)| (PathBuf::from(path), entry))
        .collect()
}

#[test]
fn equal_snapshots_have_no_difference() {
    let snapshot = entries([("a", Entry::Directory), ("a/b", file(b"content", 1))]);
    assert_eq!(difference(&snapshot, &snapshot.clone()), None);
}

#[test]
fn a_missing_entry_is_a_difference() {
    let expected = entries([("a", Entry::Directory)]);
    let difference = difference(&expected, &Snapshot::new()).unwrap();
    assert_eq!(difference, "a is missing; it is a directory");
}

#[test]
fn an_entry_that_should_not_exist_is_a_difference() {
    let found = entries([("a", Entry::Symlink(PathBuf::from("b")))]);
    let difference = difference(&Snapshot::new(), &found).unwrap();
    assert_eq!(difference, "a is a symlink to b, and should not exist");
}

#[test]
fn different_content_is_a_difference_at_its_first_byte() {
    let expected = entries([("a", file(b"same then new", 1))]);
    let found = entries([("a", file(b"same then old", 1))]);
    let difference = difference(&expected, &found).unwrap();
    assert_eq!(
        difference,
        "a is a file of 13 bytes with 1 links whose content departs at byte 10, not a file of 13 bytes with 1 links"
    );
}

#[test]
fn content_cut_short_is_a_difference_where_it_ends() {
    let expected = entries([("a", file(b"longer", 1))]);
    let found = entries([("a", file(b"long", 1))]);
    assert!(
        difference(&expected, &found)
            .unwrap()
            .starts_with("a is a file of 4 bytes with 1 links whose content departs at byte 4")
    );
}

#[test]
fn a_different_link_count_is_a_difference() {
    let expected = entries([("a", file(b"content", 2))]);
    let found = entries([("a", file(b"content", 1))]);
    assert!(difference(&expected, &found).is_some());
}

#[test]
fn a_different_kind_of_entry_is_a_difference() {
    let expected = entries([("a", Entry::Directory)]);
    let found = entries([("a", file(b"", 1))]);
    assert_eq!(
        difference(&expected, &found).unwrap(),
        "a is a file of 0 bytes with 1 links, not a directory"
    );
}
