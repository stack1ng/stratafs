{
  description = "stratafs";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];

      perSystem =
        { system, pkgs, ... }:
        {
          _module.args.pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = [ inputs.rust-overlay.overlays.default ];
          };

          devShells.default = pkgs.mkShell {
            packages = [
              (pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
              pkgs.nixd
              pkgs.nixfmt
              pkgs.nodejs
              pkgs.taplo
              pkgs.wasm-bindgen-cli_0_2_129
              # the benchmark's load and the file systems it compares against
              pkgs.fio
              pkgs.go
              pkgs.hyperfine
              pkgs.ripgrep
              pkgs.vmtouch
            ];
          };

          formatter = pkgs.nixfmt;
        };
    };
}
