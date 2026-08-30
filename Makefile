SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

.PHONY: test fmt fmt-check vet staticcheck gosec vuln secure check build

GOFILES = GOWORK=off go run ./internal/modfiles/cmd/modfiles -root .

test:
	GOWORK=off go test -race ./...

fmt:
	$(GOFILES) | xargs -0 gofmt -w

fmt-check:
	@output="$$(mktemp)"; trap 'rm -f "$$output"' EXIT; \
		if ! $(GOFILES) | xargs -0 gofmt -l >"$$output"; then exit 1; fi; \
		if [[ -s "$$output" ]]; then cat "$$output"; exit 1; fi

vet:
	GOWORK=off go vet ./...

staticcheck:
	GOWORK=off go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...

CHECK_GO_DIRS = $(shell GOWORK=off go list -f '{{.Dir}}' ./...)

gosec:
	GOWORK=off go run github.com/securego/gosec/v2/cmd/gosec@v2.28.0 -quiet $(CHECK_GO_DIRS)

vuln:
	GOWORK=off go mod verify
	GOWORK=off go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...

secure: fmt-check vet staticcheck gosec vuln

build:
	GOWORK=off go build ./...

check: fmt-check vet staticcheck gosec vuln test build
