# plaita-rs — build / test / differential gates (mirror of plaita-go's shape)

.PHONY: build test clippy fmt go-ref build-cli diffcheck node-parity ci

build:
	cargo build --workspace

test:
	cargo test --workspace

clippy:
	cargo clippy --all-targets -- -D warnings

fmt:
	cargo fmt --all

# Go reference runner for the differential harness (needs the sibling
# plaita-go checkout; see AGENTS.md for the GOPROXY note)
go-ref:
	cd tools/go-ref && GOPROXY=https://goproxy.cn,direct GOSUMDB=off go build -o ../go-ref-bin .

# The CLI under test — diffcheck executes target/debug/plaita-rs directly,
# so the candidate side must enter the dependency graph too (issue #11):
# without this, a stale binary silently "agrees" with the freshly built
# go-ref and the gate goes green without the tested code participating.
build-cli:
	cargo build -p plaita-cli

# Differential acceptance: both engines must agree on every case
diffcheck: build-cli go-ref
	python3 tools/diffcheck.py

# Python↔Rust node-type parity gate (#80 alignment): extract the authoritative
# list from the sibling plaita + plaita-nodes checkouts, then run the
# env-gated diff test. Entry points come from INSTALLED distributions — the
# plaita venv must have both pip-installed (editable is fine).
node-parity:
	../plaita/.venv/bin/python scripts/extract_python_node_types.py \
		--plaita ../plaita --plaita-nodes ../plaita-nodes --out /tmp/plaita-rs-node-types.json
	PLAITA_NODE_TYPES_JSON=/tmp/plaita-rs-node-types.json \
		cargo test -p plaita-engine --test node_type_parity -- --nocapture

ci: test clippy node-parity
