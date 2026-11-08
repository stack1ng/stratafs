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
    // Where the kernel stats the file before it copies the data, a read takes effect as two steps, a stat of the file,
    // then its own, copying the data.
    Read(u64),
    Write(u64, &'static [u8]),
    Touch(u64, i64),
    Chmod(u64, u16),
    // Create carries the inode the mount allocated for it.
    Create(Path, u64),
    Unlink(Path, u64),
    // Rename moves the file from one path to another, replacing the file the kernel found there.
    Rename(Path, Path, u64, Option<u64>),
    // On Linux a link takes effect as two steps, the kernel finding the new name free, then the volume's link.
    Link(u64, Path),
    // NameFree is the kernel finding a name free under its directory's lock, failing with EEXIST when it is taken.
    NameFree(Path),
    // Reclaim deletes the file once no name refers to it, as the kernel letting go of it does.
    Reclaim(u64),
}

impl HistoryOperation {
    // reads says whether the operation changes nothing.
    pub(crate) fn reads(&self) -> bool {
        match *self {
            HistoryOperation::Lookup(_)
            | HistoryOperation::List(_)
            | HistoryOperation::Stat(..)
            | HistoryOperation::Read(_)
            | HistoryOperation::NameFree(_) => true,
            HistoryOperation::Rename(.., inode, replaced) => replaced == Some(inode),
            _ => false,
        }
    }

    // steps is the operation as each step it takes effect in, in order, with what it returned there. POSIX lets a
    // lookup find the path at one moment and read the attributes at a later one, so a rename meanwhile on the same
    // mount can show the old path with the new attributes, as a read that stats first can show new data with old ones.
    pub(crate) fn steps(
        self,
        returned: Option<Outcome>,
        finds_names: bool,
        stats_before_copying: bool,
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
            (HistoryOperation::Read(inode), Some(Outcome::Read(content, seen)))
                if stats_before_copying =>
            {
                vec![
                    (HistoryOperation::Stat(inode), Some(Outcome::Seen(seen))),
                    (
                        HistoryOperation::Read(inode),
                        Some(Outcome::Copied(content)),
                    ),
                ]
            }
            // A link that failed with EEXIST found the name taken at one of its steps.
            (HistoryOperation::Link(_, path), Some(Outcome::Failed(PosixError::EEXIST)))
                if finds_names =>
            {
                let taken = Outcome::Failed(PosixError::EEXIST);
                vec![(HistoryOperation::NameFree(path), Some(taken))]
            }
            (HistoryOperation::Link(inode, path), returned) if finds_names => {
                let free = returned.as_ref().map(|_| Outcome::Done);
                vec![
                    (HistoryOperation::NameFree(path), free),
                    (HistoryOperation::Link(inode, path), returned),
                ]
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
    // Listed is each entry of a directory, by name, with its attributes when the listing shows them.
    Listed(Vec<(&'static str, u64, Option<Seen>)>),
    Seen(Seen),
    // Read is what a read returned: the content with the attributes handed over with it. Copied is what its own step
    // copies when the kernel stats the file first.
    Read(Vec<u8>, Seen),
    Copied(Vec<u8>),
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

// Namespace is every path of the volume's directories, and every file the activity made, by inode. finds_names says
// whether the kernel finds the names a change works on under their directories' locks before it asks the volume,
// lists_attributes whether a listing shows each entry's attributes, and stats_before_copying whether a read stats the
// file before it copies the data.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct Namespace {
    names: BTreeMap<Path, u64>,
    files: BTreeMap<u64, File>,
    finds_names: bool,
    lists_attributes: bool,
    stats_before_copying: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct File {
    content: Vec<u8>,
    accessed: i64,
    mode: u16,
    links: u32,
}

impl Namespace {
    pub(crate) fn new(
        finds_names: bool,
        lists_attributes: bool,
        stats_before_copying: bool,
    ) -> Self {
        Namespace {
            finds_names,
            lists_attributes,
            stats_before_copying,
            ..Namespace::default()
        }
    }

    pub(crate) fn invoke(&mut self, operation: &HistoryOperation) -> Outcome {
        match *operation {
            HistoryOperation::Lookup(path) => Outcome::Resolved(self.names.get(&path).copied()),
            HistoryOperation::List(directory) => Outcome::Listed(
                self.names
                    .iter()
                    .filter(|((listed, _), _)| *listed == directory)
                    .filter_map(|((_, name), inode)| {
                        let seen = self.seen(*inode)?;
                        Some((*name, *inode, self.lists_attributes.then_some(seen)))
                    })
                    .collect(),
            ),
            HistoryOperation::Stat(inode) => self.changed(inode, |_| {}),
            HistoryOperation::Read(inode) => match self.files.get(&inode) {
                Some(file) if self.stats_before_copying => Outcome::Copied(file.content.clone()),
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
            // A kernel that finds the names fails a rename whose source is gone with ENOENT, whatever the destination
            // holds. The FSKit volume checks the destination first.
            HistoryOperation::Rename(from, to, inode, replaced) => {
                let destination_changed = self.names.get(&to).copied() != replaced;
                if destination_changed && !self.finds_names {
                    return Outcome::Failed(PosixError::ESTALE);
                }
                match self.names.get(&from) {
                    None => Outcome::Failed(PosixError::ENOENT),
                    Some(found) if *found != inode => Outcome::Failed(PosixError::ESTALE),
                    _ if destination_changed => Outcome::Failed(PosixError::ESTALE),
                    // A rename between two names of one file does nothing.
                    Some(_) if replaced == Some(inode) => self.changed(inode, |_| {}),
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
            HistoryOperation::NameFree(path) => match self.names.contains_key(&path) {
                true => Outcome::Failed(PosixError::EEXIST),
                false => Outcome::Done,
            },
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
