.PHONY: build e2e-kind test verify

build:
	@mkdir -p _bin
	go build -o _bin/controller ./cmd/controller

test:
	go test ./...

e2e-kind:
	./hack/e2e-kind.sh

verify:
	@test -z "$$(gofmt -l .)" || { echo "Go files need formatting"; gofmt -l .; exit 1; }
	go mod tidy -diff
