use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{FileExt, symlink};
use std::path::{Path, PathBuf};

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use crate::snapshot::{Entry, Snapshot};

// NAMES are the names entries get, few enough that operations often meet an entry another one made.
const NAMES: [&str; 6] = ["a", "b", "c", "d", "e", "f"];

// DEEPEST is how many directories deep a new entry's parent may be.
const DEEPEST: usize = 4;

// Content is a file's bytes, kept as the seed and length they are made from.
#[derive(Debug, Clone, Copy)]
pub struct Content {
    seed: u64,
    length: usize,
}

impl Content {
    // generate picks a length that is mostly small, often past what a store keeps inline, and sometimes large.
    fn generate(rng: &mut StdRng) -> Content {
        let length = match rng.random_range(0..20) {
            0 => rng.random_range(100_000..600_000),
            1..8 => rng.random_range(4_000..70_000),
            _ => rng.random_range(0..4_000),
        };
        Content {
            seed: rng.random(),
            length,
        }
    }

    fn bytes(&self) -> Vec<u8> {
        let mut bytes = vec![0; self.length];
        StdRng::seed_from_u64(self.seed).fill(&mut bytes[..]);
        bytes
    }
}

// Operation is one change to the entries below a directory, with its paths from there.
#[derive(Debug, Clone)]
pub enum Operation {
    // WriteFile makes the file or replaces its content.
    WriteFile {
        path: PathBuf,
        content: Content,
    },
    WriteAt {
        path: PathBuf,
        offset: u64,
        content: Content,
    },
    Append {
        path: PathBuf,
        content: Content,
    },
    Truncate {
        path: PathBuf,
        size: u64,
    },
    MakeDirectory {
        path: PathBuf,
    },
    Rename {
        from: PathBuf,
        to: PathBuf,
    },
    RemoveFile {
        path: PathBuf,
    },
    RemoveDirectory {
        path: PathBuf,
    },
    Link {
        from: PathBuf,
        to: PathBuf,
    },
    Symlink {
        target: PathBuf,
        path: PathBuf,
    },
}

impl Operation {
    // generate picks an operation for a directory that holds entries.
    pub fn generate(rng: &mut StdRng, entries: &Snapshot) -> Operation {
        match rng.random_range(0..100) {
            0..22 => Operation::WriteFile {
                path: either(rng, entries, 50),
                content: Content::generate(rng),
            },
            22..32 => {
                let path = either(rng, entries, 90);
                Operation::WriteAt {
                    offset: rng.random_range(0..=size(entries, &path) + 10_000),
                    content: Content::generate(rng),
                    path,
                }
            }
            32..42 => Operation::Append {
                path: either(rng, entries, 90),
                content: Content::generate(rng),
            },
            42..48 => {
                let path = either(rng, entries, 90);
                Operation::Truncate {
                    size: rng.random_range(0..=size(entries, &path) * 2 + 5_000),
                    path,
                }
            }
            48..62 => Operation::MakeDirectory {
                path: new(rng, entries),
            },
            62..76 => Operation::Rename {
                from: either(rng, entries, 90),
                to: either(rng, entries, 30),
            },
            76..84 => Operation::RemoveFile {
                path: either(rng, entries, 90),
            },
            84..90 => Operation::RemoveDirectory {
                path: either(rng, entries, 90),
            },
            90..95 => Operation::Link {
                from: either(rng, entries, 90),
                to: new(rng, entries),
            },
            _ => Operation::Symlink {
                target: PathBuf::from(NAMES[rng.random_range(0..NAMES.len())]),
                path: new(rng, entries),
            },
        }
    }

    // generate_shared picks an operation for the directory every thread changes: few names and no directories below,
    // so threads often meet.
    pub fn generate_shared(rng: &mut StdRng) -> Operation {
        let mut name = || PathBuf::from(NAMES[rng.random_range(0..3)]);
        let (path, to) = (name(), name());
        match rng.random_range(0..100) {
            0..40 => Operation::WriteFile {
                path,
                content: Content {
                    seed: rng.random(),
                    length: rng.random_range(0..6_000),
                },
            },
            40..55 => Operation::RemoveFile { path },
            55..70 => Operation::MakeDirectory { path },
            70..80 => Operation::RemoveDirectory { path },
            _ => Operation::Rename { from: path, to },
        }
    }

    // apply runs the operation below root, and returns the error number it failed with if it failed.
    pub fn apply(&self, root: &Path) -> Result<(), i32> {
        let applied = match self {
            Operation::WriteFile { path, content } => {
                File::create(root.join(path)).and_then(|file| written(file, &content.bytes()))
            }
            Operation::WriteAt {
                path,
                offset,
                content,
            } => OpenOptions::new()
                .write(true)
                .open(root.join(path))
                .and_then(|file| {
                    file.write_all_at(&content.bytes(), *offset)?;
                    closed(file)
                }),
            Operation::Append { path, content } => OpenOptions::new()
                .append(true)
                .open(root.join(path))
                .and_then(|file| written(file, &content.bytes())),
            Operation::Truncate { path, size } => OpenOptions::new()
                .write(true)
                .open(root.join(path))
                .and_then(|file| {
                    file.set_len(*size)?;
                    closed(file)
                }),
            Operation::MakeDirectory { path } => fs::create_dir(root.join(path)),
            Operation::Rename { from, to } => fs::rename(root.join(from), root.join(to)),
            Operation::RemoveFile { path } => fs::remove_file(root.join(path)),
            Operation::RemoveDirectory { path } => fs::remove_dir(root.join(path)),
            Operation::Link { from, to } => fs::hard_link(root.join(from), root.join(to)),
            Operation::Symlink { target, path } => symlink(target, root.join(path)),
        };
        applied.map_err(|error| error.raw_os_error().unwrap_or(0))
    }
}

fn written(mut file: File, content: &[u8]) -> io::Result<()> {
    file.write_all(content)?;
    closed(file)
}

// closed closes file and returns close's own result, which is where a kernel that keeps writes back reports them.
fn closed(file: File) -> io::Result<()> {
    nix::unistd::close(file).map_err(io::Error::from)
}

// either picks the path of an entry there is, existing times in a hundred, and otherwise a path for a new one.
fn either(rng: &mut StdRng, entries: &Snapshot, existing: u32) -> PathBuf {
    if !entries.is_empty() && rng.random_range(0..100) < existing {
        let chosen = rng.random_range(0..entries.len());
        return entries
            .keys()
            .nth(chosen)
            .expect("chosen is below the length")
            .clone();
    }
    new(rng, entries)
}

// new picks a path for a new entry: a name in the directory itself or in a directory below it. An entry may have the
// name already.
fn new(rng: &mut StdRng, entries: &Snapshot) -> PathBuf {
    let directories: Vec<&PathBuf> = entries
        .iter()
        .filter(|(path, entry)| **entry == Entry::Directory && path.components().count() <= DEEPEST)
        .map(|(path, _)| path)
        .collect();
    let name = NAMES[rng.random_range(0..NAMES.len())];
    if directories.is_empty() || rng.random_range(0..100) < 40 {
        return PathBuf::from(name);
    }
    directories[rng.random_range(0..directories.len())].join(name)
}

// size is the length of the file at path, or zero when no file is there.
fn size(entries: &Snapshot, path: &Path) -> u64 {
    match entries.get(path) {
        Some(Entry::File { content, .. }) => content.len() as u64,
        _ => 0,
    }
}
