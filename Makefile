GO ?= go
BIN := bin/mqtt-example

.PHONY: build run test deps-proof clean

build:
	$(GO) build -o $(BIN) ./cmd/example

run:
	$(GO) run ./cmd/example

test:
	$(GO) test ./...

# Regenerates deps-proof.txt: shows the go.mod has no require block.
deps-proof:
	@echo "== go.mod (dependency manifest) ==" > deps-proof.txt
	@cat go.mod >> deps-proof.txt
	@echo "" >> deps-proof.txt
	@echo "== 'require' directives: none expected ==" >> deps-proof.txt
	@if grep -q '^require' go.mod; then \
		echo "FAIL: go.mod has a require block" >> deps-proof.txt; \
	else \
		echo "PASS: no require block (zero third-party runtime deps)" >> deps-proof.txt; \
	fi
	@cat deps-proof.txt

clean:
	rm -rf bin
