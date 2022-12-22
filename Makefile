# export RUSTFLAGS=-Dwarnings
export RUST_TEST_THREADS=1
export LOG_LEVEL=info
export RUST_BACKTRACE=1

LOG_LEVEL ?= raft=info,percolator=info

check:
	cargo fmt --all -- --check
	cargo clippy --all --tests -- -D clippy::all

test: test_others test_2 test_3

test_2: test_2a test_2b test_2c test_2d

test_2a: cargo_test_2a
test_2a1: cargo_test_initial_election_2a
test_2a2: cargo_test_reelection_2a
test_2a3: cargo_test_many_election_2a

test_2b: cargo_test_2b

test_2b1: cargo_test_basic_agree_2b
test_2b2: cargo_test_fail_agree_2b
test_2b3: cargo_test_fail_no_agree_2b
test_2b4: cargo_test_concurrent_starts_2b

test_2c: cargo_test_2c

test_2d: cargo_test_2d

test_3: test_3a test_3b

test_3a: cargo_test_3a
test_3a1: cargo_test_basic_3a
test_3a2: cargo_test_concurrent_3a
test_3a3: cargo_test_unreliable_3a

test_3b: cargo_test_3b

cargo_test_%:
	RUST_LOG=${LOG_LEVEL} cargo test -p raft -- --nocapture --test $*

test_others: check
	RUST_LOG=${LOG_LEVEL} cargo test -p labrpc -p labcodec -- --nocapture

test_percolator: check
	RUST_LOG=${LOG_LEVEL} cargo test -p percolator -- --nocapture
