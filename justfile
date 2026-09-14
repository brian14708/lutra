set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

generate:
    go tool buf generate

fmt:
    go tool golangci-lint fmt

lint: generate
    go tool buf lint
    go tool golangci-lint run ./...

test:
    go test ./...

check: fmt lint test
