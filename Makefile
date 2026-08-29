GO ?= go
BIN := bin/mqttaton

.PHONY: build run test deps-proof clean

build:
	$(GO) build -o $(BIN) .

run:
	$(GO) run . -port 1883

test:
	$(GO) test -v -count=1 .

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
