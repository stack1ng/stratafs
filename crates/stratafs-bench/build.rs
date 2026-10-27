// The metadata store the build uses, from STRATAFS_BENCH_STORE: redb when it is unset.
fn main() {
    println!("cargo::rerun-if-env-changed=STRATAFS_BENCH_STORE");
    println!("cargo::rustc-check-cfg=cfg(store, values(\"redb\", \"sqlite\"))");
    let store = std::env::var("STRATAFS_BENCH_STORE").unwrap_or_else(|_| "redb".to_owned());
    match store.as_str() {
        "redb" | "sqlite" => println!("cargo::rustc-cfg=store=\"{store}\""),
        other => panic!("STRATAFS_BENCH_STORE is {other}, which is not redb or sqlite"),
    }
}
