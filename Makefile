BINDIR ?= bin
BIN_MAIN ?= $(BINDIR)/mysql-clone
BIN_CHECKSUM ?= $(BINDIR)/innodb-checksum

.PHONY: build test test-it test-repl test-repl-native clean

build:
	mkdir -p $(BINDIR)
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_MAIN) ./cmd/mysql-clone
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_CHECKSUM) ./cmd/innodb-checksum

test:
	go test ./clone ./cmd/...

test-it:
	go test -v -tags=integration ./integration

test-repl:
	go test -v -tags=integration ./integration -run TestCloneReplication

test-repl-native:
	go test -v -tags=integration ./integration -run TestNativeCloneReplication

clean:
	rm -rf $(BINDIR)
