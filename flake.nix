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
        let
          # rust-analyzer is the release the editor extension is built from, so the two read the same settings.
          # A newer extension needs this version and its hashes moved with it.
          rust-analyzer =
            let
              version = "2026-11-09";
              release =
                {
                  aarch64-darwin = {
                    target = "aarch64-apple-darwin";
                    hash = "sha256-5DplAI+PQpEyDzPAVF3Vd612I6dAe1hKJxVYM9NyLfs=";
                  };
                  aarch64-linux = {
                    target = "aarch64-unknown-linux-gnu";
                    hash = "sha256-gTyjzseKxtq5nHvJ1R5jV49tQBuZTs8pzA3K2G+bLNQ=";
                  };
                  x86_64-linux = {
                    target = "x86_64-unknown-linux-musl";
                    hash = "sha256-G2qfs2P7qv13Havv/gu+I4BHR2z7dJ9M3qmntQE1CNk=";
                  };
                }
                .${system};
            in
            pkgs.runCommand "rust-analyzer-${version}"
              {
                src = pkgs.fetchurl {
                  url = "https://github.com/rust-lang/rust-analyzer/releases/download/${version}/rust-analyzer-${release.target}.gz";
                  inherit (release) hash;
                };
              }
              ''
                mkdir -p $out/bin
                gunzip -c $src > $out/bin/rust-analyzer
                chmod +x $out/bin/rust-analyzer
              '';
        in
        {
          _module.args.pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = [ inputs.rust-overlay.overlays.default ];
          };

          devShells.default = pkgs.mkShell {
            packages = [
              (pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
              rust-analyzer
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
