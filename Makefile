# gsyncd - convenience targets. Go 1.23+ on PATH (see go.mod).

BIN     := gsyncd
VERSION := 0.5.0
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build test vet fmt e2e smoke dist clean

all: build

build:
	CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/gsyncd

test:
	go test ./... -count=1

vet:
	go vet ./...
	gofmt -l .

fmt:
	gofmt -w .

e2e:
	bash scripts/e2e.sh

smoke:
	bash scripts/smoke.sh ./$(BIN)

dist:
	bash scripts/dist.sh

clean:
	rm -rf $(BIN) $(BIN).exe dist .e2e .gsyncd-smoke
