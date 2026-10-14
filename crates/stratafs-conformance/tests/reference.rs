use bytes::Bytes;
use stratafs_conformance::reference::{ReferenceContentStore, ReferenceMetadataStore};
use stratafs_store::content::ContentHash;

stratafs_conformance::metadata_store_tests!(reference_metadata_store, ReferenceMetadataStore::new);
stratafs_conformance::metadata_store_reopen_tests!(
    reference_metadata_store_reopen,
    ReferenceMetadataStore::new,
    |store: &ReferenceMetadataStore| store.clone()
);
stratafs_conformance::content_store_tests!(
    reference_content_store,
    ReferenceContentStore::new,
    corrupt: |store: &ReferenceContentStore, hash: ContentHash, bytes: Bytes| {
        store.replace_stored_object(hash, bytes);
        std::future::ready(())
    },
    inspect: |store: &ReferenceContentStore, hash: ContentHash| {
        std::future::ready(store.stored_object_len(&hash))
    }
);
