use std::collections::BTreeMap;

use stratafs_fs_engine::attributes::Attributes;
use stratafs_fs_engine::error::PosixError;

use crate::activity::Path;

// HistoryOperation is one operation the kernel asked of a mount, or answered from its cache, on inodes by number.
#[derive(Clone, Debug)]
pub(crate) enum HistoryOperation {
    // A lookup takes effect as two steps, its own, finding what the path refers to, then a stat of that file.
    Lookup(Path),
    List(&'static str),
    Stat(u64),
    Read(u64),
    Write(u64, &'static [u8]),
    Touch(u64, i64),
    Chmod(u64, u16),
    // Create carries the inode the mount allocated for it.
    Create(Path, u64),
    Unlink(Path, u64),
    // Rename moves the file from one path to another, replacing the file the kernel found there.
    Rename(Path, Path, u64, Option<u64>),
    Link(u64, Path),
    // Reclaim deletes the file once no name refers to it, as the kernel letting go of it does.
    Reclaim(u64),
}

impl HistoryOperation {
    // reads says whether the operation changes nothing.
    pub(crate) fn reads(&self) -> bool {
        matches!(
            self,
            HistoryOperation::Lookup(_)
                | HistoryOperation::List(_)
                | HistoryOperation::Stat(..)
                | HistoryOperation::Read(_)
        )
    }

    // steps is the operation as each step it takes effect in, in order, with what it returned there. POSIX lets a
    // lookup find the path at one moment and read the attributes at a later one, so a rename meanwhile on the same
    // mount can show the old path with the new attributes.
    pub(crate) fn steps(
        self,
        returned: Option<Outcome>,
    ) -> Vec<(HistoryOperation, Option<Outcome>)> {
        match (self, returned) {
            (HistoryOperation::Lookup(path), Some(Outcome::Found(found))) => {
                let resolved = Outcome::Resolved(found.as_ref().map(|(inode, _)| *inode));
                let stat = found.map(|(inode, stat)| (HistoryOperation::Stat(inode), Some(*stat)));
                [(HistoryOperation::Lookup(path), Some(resolved))]
                    .into_iter()
                    .chain(stat)
                    .collect()
            }
            (operation, returned) => vec![(operation, returned)],
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Outcome {
    // Found is what a lookup returned: the file it found with what its stat returned, or None for a free name. Resolved
    // is what its first step finds the name refers to.
    Found(Option<(u64, Box<Outcome>)>),
    Resolved(Option<u64>),
    // Listed is each entry of a directory, by name.
    Listed(Vec<(&'static str, u64, Seen)>),
    Seen(Seen),
    Read(Vec<u8>, Seen),
    Done,
    Failed(PosixError),
}

// Seen is what an app sees of a file's attributes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct Seen {
    size: u64,
    accessed: i64,
    mode: u16,
    links: u32,
}

impl From<&Attributes> for Seen {
    fn from(attributes: &Attributes) -> Self {
        Seen {
            size: attributes.settable.size,
            accessed: attributes.settable.last_accessed_at.0,
            mode: attributes.settable.mode.bits(),
            links: attributes.hard_link_count,
        }
    }
}

// Namespace is every path of the volume's directories, and every file the activity made, by inode.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct Namespace {
    names: BTreeMap<Path, u64>,
    files: BTreeMap<u64, File>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct File {
    content: Vec<u8>,
    accessed: i64,
    mode: u16,
    links: u32,
}

impl Namespace {
    pub(crate) fn invoke(&mut self, operation: &HistoryOperation) -> Outcome {
        match *operation {
            HistoryOperation::Lookup(path) => Outcome::Resolved(self.names.get(&path).copied()),
            HistoryOperation::List(directory) => Outcome::Listed(
                self.names
                    .iter()
                    .filter(|((listed, _), _)| *listed == directory)
                    .filter_map(|((_, name), inode)| Some((*name, *inode, self.seen(*inode)?)))
                    .collect(),
            ),
            HistoryOperation::Stat(inode) => self.changed(inode, |_| {}),
            HistoryOperation::Read(inode) => match self.files.get(&inode) {
                Some(file) => Outcome::Read(file.content.clone(), file.seen()),
                None => Outcome::Failed(PosixError::ESTALE),
            },
            HistoryOperation::Write(inode, data) => self.changed(inode, |file| {
                let rest = file.content.get(data.len()..).unwrap_or_default();
                file.content = [data, rest].concat();
            }),
            HistoryOperation::Touch(inode, at) => self.changed(inode, |file| file.accessed = at),
            HistoryOperation::Chmod(inode, mode) => self.changed(inode, |file| file.mode = mode),
            HistoryOperation::Create(path, inode) => {
                if self.names.contains_key(&path) {
                    return Outcome::Failed(PosixError::EEXIST);
                }
                self.names.insert(path, inode);
                let file = File {
                    links: 1,
                    ..File::default()
                };
                let seen = file.seen();
                self.files.insert(inode, file);
                Outcome::Seen(seen)
            }
            HistoryOperation::Unlink(path, inode) => match self.names.get(&path) {
                None => Outcome::Failed(PosixError::ENOENT),
                Some(found) if *found != inode => Outcome::Failed(PosixError::ESTALE),
                Some(_) => {
                    self.names.remove(&path);
                    self.changed(inode, |file| file.links -= 1)
                }
            },
            HistoryOperation::Rename(from, to, inode, replaced) => {
                if self.names.get(&to).copied() != replaced {
                    return Outcome::Failed(PosixError::ESTALE);
                }
                match self.names.get(&from) {
                    None => Outcome::Failed(PosixError::ENOENT),
                    Some(found) if *found != inode => Outcome::Failed(PosixError::ESTALE),
                    Some(_) => {
                        self.names.remove(&from);
                        self.names.insert(to, inode);
                        if let Some(replaced) = replaced {
                            self.changed(replaced, |file| file.links -= 1);
                        }
                        self.changed(inode, |_| {})
                    }
                }
            }
            HistoryOperation::Reclaim(inode) => {
                if self.files.get(&inode).is_some_and(|file| file.links == 0) {
                    self.files.remove(&inode);
                }
                Outcome::Done
            }
            HistoryOperation::Link(inode, path) => match self.files.get(&inode) {
                None => Outcome::Failed(PosixError::ESTALE),
                Some(file) if file.links == 0 => Outcome::Failed(PosixError::ENOENT),
                Some(_) if self.names.contains_key(&path) => Outcome::Failed(PosixError::EEXIST),
                Some(_) => {
                    self.names.insert(path, inode);
                    self.changed(inode, |file| file.links += 1)
                }
            },
        }
    }

    // made gives path a new, empty file.
    pub(crate) fn made(&mut self, path: Path, inode: u64) {
        self.invoke(&HistoryOperation::Create(path, inode));
    }

    fn seen(&self, inode: u64) -> Option<Seen> {
        self.files.get(&inode).map(File::seen)
    }

    // changed changes inode's file with change, and is what the app sees of it after.
    fn changed(&mut self, inode: u64, change: impl FnOnce(&mut File)) -> Outcome {
        match self.files.get_mut(&inode) {
            Some(file) => {
                change(file);
                Outcome::Seen(file.seen())
            }
            None => Outcome::Failed(PosixError::ESTALE),
        }
    }
}

impl File {
    fn seen(&self) -> Seen {
        Seen {
            size: self.content.len() as u64,
            accessed: self.accessed,
            mode: self.mode,
            links: self.links,
        }
    }
}
