.PHONY: build e2e-kind e2e-openshell-kind install-openshell install-openshell-kind install-openshell-openshift test uninstall-openshell verify

build:
	@mkdir -p _bin
	go build -o _bin/controller ./cmd/controller

test:
	go test ./...

e2e-kind:
	./hack/e2e-kind.sh

e2e-openshell-kind:
	./hack/e2e-openshell-kind.sh

install-openshell:
	./hack/install-openshell.sh

install-openshell-kind:
	OPENSHELL_AUTH_MODE=kind-mtls OPENSHELL_PLATFORM_VALUES_FILE=$(CURDIR)/config/openshell/values-kind.yaml ./hack/install-openshell.sh

install-openshell-openshift:
	OPENSHELL_PLATFORM_VALUES_FILE=$(CURDIR)/config/openshell/values-openshift.yaml ./hack/install-openshell.sh

uninstall-openshell:
	./hack/uninstall-openshell.sh

verify:
	@test -z "$$(gofmt -l .)" || { echo "Go files need formatting"; gofmt -l .; exit 1; }
	@bash -n hack/*.sh
	go mod tidy -diff
