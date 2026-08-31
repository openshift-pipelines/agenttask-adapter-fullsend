.PHONY: test verify

test:
	go test ./...

verify:
	@test -z "$$(gofmt -l .)" || { echo "Go files need formatting"; gofmt -l .; exit 1; }
	go mod tidy -diff
