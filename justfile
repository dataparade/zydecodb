# ZydecoDB dev tasks. Install just: `snap install just` or `cargo install just`.

build:
    cargo build --workspace

build-server:
    cargo build --release -p zydecodb

test:
    cargo test --workspace

lint:
    cargo fmt --check
    cargo clippy --workspace --all-targets -- -D warnings

run:
    cargo run -p zydecodb -- serve --config config/zydecodb.example.toml

fuzz:
    cargo +nightly fuzz run wal_parser -- -max_total_time=60
    cargo +nightly fuzz run ipc_envelope -- -max_total_time=60
    cargo +nightly fuzz run sstable_reader -- -max_total_time=60
    cargo +nightly fuzz run manifest_parser -- -max_total_time=60

soak-6m:
    HOURS=0.1 OPS=3000 OUT_DIR=soak-runs/phase1-quick scripts/soak.sh

soak-90m:
    HOURS=1.5 OPS=3000 OUT_DIR=soak-runs/phase1-memo6-90m scripts/soak.sh --no-analyze

cov:
    cargo llvm-cov --workspace --summary-only

clean:
    cargo clean
