// The store the build uses, from STRATAFS_BENCH_STORE: redb when it is unset. The consistency, from
// STRATAFS_BENCH_CONSISTENCY: eventual when it is unset.
fn main() {
    println!("cargo::rerun-if-env-changed=STRATAFS_BENCH_STORE");
    println!("cargo::rustc-check-cfg=cfg(store, values(\"redb\", \"sqlite\", \"mdbx\"))");
    let store = std::env::var("STRATAFS_BENCH_STORE").unwrap_or_else(|_| "redb".to_owned());
    match store.as_str() {
        "redb" | "sqlite" | "mdbx" => println!("cargo::rustc-cfg=store=\"{store}\""),
        other => panic!("STRATAFS_BENCH_STORE is {other}, which is not redb, sqlite or mdbx"),
    }
    println!("cargo::rerun-if-env-changed=STRATAFS_BENCH_CONSISTENCY");
    println!("cargo::rustc-check-cfg=cfg(consistency, values(\"eventual\", \"strong\"))");
    let consistency =
        std::env::var("STRATAFS_BENCH_CONSISTENCY").unwrap_or_else(|_| "eventual".to_owned());
    match consistency.as_str() {
        "eventual" | "strong" => println!("cargo::rustc-cfg=consistency=\"{consistency}\""),
        other => panic!("STRATAFS_BENCH_CONSISTENCY is {other}, which is not eventual or strong"),
    }
}
