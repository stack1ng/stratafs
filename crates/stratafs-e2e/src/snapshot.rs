use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

// Entry is what a path holds, as far as two file systems given the same operations must agree on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Directory,
    File { content: Vec<u8>, links: u64 },
    Symlink(PathBuf),
}

// Snapshot is every entry below a directory, by its path from there.
pub type Snapshot = BTreeMap<PathBuf, Entry>;

// snapshot reads every entry below root, following no symlink.
pub fn snapshot(root: &Path) -> io::Result<Snapshot> {
    let mut entries = Snapshot::new();
    let mut directories = vec![PathBuf::new()];
    while let Some(directory) = directories.pop() {
        for listed in fs::read_dir(root.join(&directory))? {
            let listed = listed?;
            let path = directory.join(listed.file_name());
            let metadata = fs::symlink_metadata(root.join(&path))?;
            let entry = if metadata.is_dir() {
                directories.push(path.clone());
                Entry::Directory
            } else if metadata.is_symlink() {
                Entry::Symlink(fs::read_link(root.join(&path))?)
            } else {
                Entry::File {
                    content: fs::read(root.join(&path))?,
                    links: metadata.nlink(),
                }
            };
            entries.insert(path, entry);
        }
    }
    Ok(entries)
}

// difference says how found first departs from expected, by path, or nothing when they are the same.
pub fn difference(expected: &Snapshot, found: &Snapshot) -> Option<String> {
    for (path, entry) in expected {
        match found.get(path) {
            Some(other) if other == entry => {}
            Some(other) => {
                return Some(format!(
                    "{} is {}, not {}",
                    path.display(),
                    described(other, Some(entry)),
                    described(entry, None)
                ));
            }
            None => {
                return Some(format!(
                    "{} is missing; it is {}",
                    path.display(),
                    described(entry, None)
                ));
            }
        }
    }
    let extra = found.iter().find(|(path, _)| !expected.contains_key(*path));
    extra.map(|(path, entry)| {
        format!(
            "{} is {}, and should not exist",
            path.display(),
            described(entry, None)
        )
    })
}

// described is an entry in words, with where a file's content first departs from another's when one is given.
fn described(entry: &Entry, other: Option<&Entry>) -> String {
    match entry {
        Entry::Directory => "a directory".to_owned(),
        Entry::Symlink(target) => format!("a symlink to {}", target.display()),
        Entry::File { content, links } => {
            let mut words = format!("a file of {} bytes with {links} links", content.len());
            if let Some(Entry::File { content: other, .. }) = other {
                let same = content
                    .iter()
                    .zip(other)
                    .take_while(|(a, b)| a == b)
                    .count();
                if same < content.len().max(other.len()) {
                    words.push_str(&format!(" whose content departs at byte {same}"));
                }
            }
            words
        }
    }
}

#[cfg(test)]
mod tests;
