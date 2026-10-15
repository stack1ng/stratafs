use redb::Database;
use redb::backends::InMemoryBackend;
use stratafs_redb::{Inline, RedbStore};

pub fn new_store() -> RedbStore<Inline> {
    RedbStore::new(
        Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap(),
        Inline,
    )
    .unwrap()
}
