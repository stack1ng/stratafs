// The only test in its binary, so no other test opens descriptors while it counts them.

use std::fs;
use std::sync::mpsc;
use std::time::Duration;

use stratafs_lossy_log::{Read, Reader, Writer};

const S: usize = 4;

#[test]
fn a_reader_closes_each_segment_it_leaves() {
    let directory = tempfile::tempdir().unwrap();
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    let (sender, receiver) = mpsc::channel();
    let _reader = Reader::start::<8, S>(directory.path(), move |read| {
        let _ = sender.send(read);
    })
    .unwrap();
    // Each append waits for its read, so the reader follows the log through every segment.
    let follow = |numbers: std::ops::Range<u64>| {
        for number in numbers {
            writer.append([number.to_le_bytes()]).unwrap();
            let read = receiver.recv_timeout(Duration::from_secs(10)).unwrap();
            assert_eq!(read, Read::Entry(number.to_le_bytes()));
        }
    };
    follow(0..2 * S as u64);
    let open = descriptors();
    follow(2 * S as u64..200 * S as u64);
    // The reader may hold the segment after its own at one count and not at the other.
    assert!(descriptors() <= open + 1);
}

fn descriptors() -> usize {
    fs::read_dir("/dev/fd").unwrap().count()
}
