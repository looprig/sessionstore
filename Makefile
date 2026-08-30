.PHONY: test fmt fmt-check vet staticcheck gosec vuln secure check build

test:
	GOWORK=off go test -race ./...

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

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
