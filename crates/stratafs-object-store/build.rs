use cfg_aliases::cfg_aliases;

fn main() {
    cfg_aliases! {
        disk: { not(target_arch = "wasm32") },
    }
}
