use std::cell::RefCell;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::LazyLock;

use bytes::Bytes;
use opendal::services::Memory;
use opendal::{ErrorKind, Operator};
use stratafs_opendal::OpendalContentStore;
use stratafs_store::content::ContentHash;
#[cfg(not(target_arch = "wasm32"))]
use tempfile::TempDir;
#[cfg(not(target_arch = "wasm32"))]
use tokio::runtime::{Builder, Runtime};

#[cfg(not(target_arch = "wasm32"))]
static FS_RUNTIME: LazyLock<Runtime> =
    LazyLock::new(|| Builder::new_current_thread().build().unwrap());

#[cfg(not(target_arch = "wasm32"))]
thread_local! {
    static CASE_DIRECTORY: RefCell<Option<TempDir>> = const { RefCell::new(None) };
}

thread_local! {
    static CASE_OPERATOR: RefCell<Option<Operator>> = const { RefCell::new(None) };
}

fn memory_store() -> OpendalContentStore {
    let operator = Operator::new(Memory::default()).unwrap();
    CASE_OPERATOR.with(|slot| *slot.borrow_mut() = Some(operator.clone()));
    OpendalContentStore::new(operator)
}

#[cfg(not(target_arch = "wasm32"))]
fn fs_store() -> OpendalContentStore {
    std::mem::forget(FS_RUNTIME.enter());
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("root");
    let store = OpendalContentStore::filesystem(&root, directory.path().join("staging")).unwrap();
    let operator =
        Operator::new(opendal::services::Fs::default().root(root.to_str().unwrap())).unwrap();
    CASE_OPERATOR.with(|slot| *slot.borrow_mut() = Some(operator));
    CASE_DIRECTORY.with(|slot| *slot.borrow_mut() = Some(directory));
    store
}

fn case_operator() -> Operator {
    CASE_OPERATOR.with(|slot| slot.borrow().clone().unwrap())
}

fn object_path(hash: ContentHash) -> String {
    format!(
        "blobs/{:02x}/{}",
        hash.0[0],
        blake3::Hash::from_bytes(hash.0)
    )
}

fn replace_stored_object(
    _store: &OpendalContentStore,
    hash: ContentHash,
    bytes: Bytes,
) -> impl Future<Output = ()> + use<> {
    let operator = case_operator();
    async move {
        operator.write(&object_path(hash), bytes).await.unwrap();
    }
}

fn stored_object_len(
    _store: &OpendalContentStore,
    hash: ContentHash,
) -> impl Future<Output = Option<u64>> + use<> {
    let operator = case_operator();
    async move {
        match operator.stat(&object_path(hash)).await {
            Ok(metadata) => Some(metadata.content_length()),
            Err(error) => match error.kind() {
                ErrorKind::NotFound => None,
                _ => panic!("stat of a stored object failed: {error}"),
            },
        }
    }
}

stratafs_conformance::content_store_tests!(
    memory,
    memory_store,
    corrupt: replace_stored_object,
    inspect: stored_object_len
);
#[cfg(not(target_arch = "wasm32"))]
stratafs_conformance::content_store_tests!(
    fs,
    fs_store,
    corrupt: replace_stored_object,
    inspect: stored_object_len
);
