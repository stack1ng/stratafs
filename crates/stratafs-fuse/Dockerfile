# The Linux environment stratafs-fuse builds and runs its tests in: Debian with the repository's Rust toolchain, FUSE 3,
# the compilers libmdbx and its bindings need, and rsync to copy the sources in. cargo xtask fuse-test builds it from
# the workspace root, and Dockerfile.dockerignore keeps every file but rust-toolchain.toml out of the context.
FROM debian:trixie

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential ca-certificates clang fuse3 libclang-dev pkg-config rsync rustup \
    && rm -rf /var/lib/apt/lists/*

# rustup show installs the toolchain rust-toolchain.toml names, with its components and targets.
WORKDIR /toolchain
COPY rust-toolchain.toml .
RUN rustup show

WORKDIR /stratafs
