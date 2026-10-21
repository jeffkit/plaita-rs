# plaita-rs — build / test / differential gates (mirror of plaita-go's shape)

.PHONY: build test clippy fmt go-ref diffcheck ci

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

# Differential acceptance: both engines must agree on every case
diffcheck: go-ref
	python3 tools/diffcheck.py

ci: test clippy
