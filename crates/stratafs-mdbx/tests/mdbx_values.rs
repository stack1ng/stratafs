mod common;
mod configuration;

use std::convert::Infallible;

use bytes::{Buf, Bytes};
use futures_lite::future::block_on;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction};

#[test]
fn a_value_reads_back_whole_from_one_chunk_or_several() {
    let store = common::open_store();
    block_on(store.transact(|mut transaction| {
        Box::pin(async move {
            transaction
                .set(Bytes::from_static(b"one chunk"), &b"value"[..])
                .unwrap();
            transaction
                .set(
                    Bytes::from_static(b"several chunks"),
                    (&b"val"[..]).chain(&b"ue"[..]),
                )
                .unwrap();
            Ok::<_, Infallible>(((), transaction))
        })
    }))
    .unwrap();
    let stored = block_on(store.read_transact(|transaction| {
        Box::pin(async move {
            Ok::<_, Infallible>([
                transaction
                    .get(Bytes::from_static(b"one chunk"))
                    .await
                    .unwrap(),
                transaction
                    .get(Bytes::from_static(b"several chunks"))
                    .await
                    .unwrap(),
            ])
        })
    }))
    .unwrap();
    assert_eq!(
        stored,
        [
            Some(Bytes::from_static(b"value")),
            Some(Bytes::from_static(b"value"))
        ]
    );
}

#[test]
fn a_set_consumes_its_value_from_one_chunk_or_several() {
    let store = common::open_store();
    let remaining = block_on(store.transact(|mut transaction| {
        Box::pin(async move {
            let mut one_chunk = Bytes::from_static(b"value");
            transaction
                .set(Bytes::from_static(b"one chunk"), &mut one_chunk)
                .unwrap();
            let mut several_chunks = (&b"val"[..]).chain(&b"ue"[..]);
            transaction
                .set(Bytes::from_static(b"several chunks"), &mut several_chunks)
                .unwrap();
            Ok::<_, Infallible>((
                [one_chunk.remaining(), several_chunks.remaining()],
                transaction,
            ))
        })
    }))
    .unwrap();
    assert_eq!(remaining, [0, 0]);
}
