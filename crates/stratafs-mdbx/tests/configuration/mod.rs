use std::path::Path;

use mdbx_sys as ffi;
use stratafs_mdbx::{MdbxConfiguration, MdbxGeometry, MdbxMetadata, MdbxOpenError};

// CONFIGURATION opens a test store in 4 KiB pages, committing without syncs, with libmdbx making no durable point of
// its own, so only ensure_durable syncs.
pub const CONFIGURATION: MdbxConfiguration<'static> = MdbxConfiguration {
    // libmdbx sizes how many changed pages a write transaction keeps in memory from the machine's memory, which it
    // fails to read on macOS 27.
    options: &[(ffi::MDBX_opt_txn_dp_limit, (1 << 30) / 4096)],
    geometry: MdbxGeometry {
        size_lower: -1,
        size_now: -1,
        size_upper: 1 << 34,
        growth_step: 16 << 20,
        shrink_threshold: 0,
        page_size: 4096,
    },
    flags: ffi::MDBX_NOSUBDIR | ffi::MDBX_SAFE_NOSYNC,
    mode: 0o644,
    shared_options: &[
        (ffi::MDBX_opt_sync_bytes, 1 << 34),
        (ffi::MDBX_opt_sync_period, 0),
    ],
};

// open opens a test store at path.
pub fn open<S>(path: impl AsRef<Path>, spawner: S) -> Result<MdbxMetadata<S>, MdbxOpenError> {
    MdbxMetadata::open(path, &CONFIGURATION, spawner)
}
