use cfg_aliases::cfg_aliases;

fn main() {
    cfg_aliases! {
        threads: { not(target_family = "wasm") },
    }
}
