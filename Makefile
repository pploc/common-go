.DEFAULT_GOAL := verify

.PHONY: fmt-check vet staticcheck test-race coverage tidy-check vulncheck verify

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l .; exit 1)

vet:
	go vet ./...

staticcheck:
	staticcheck ./...

test-race:
	go test -race ./...

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

tidy-check:
	go mod tidy
	git diff --exit-code -- go.mod go.sum

vulncheck:
	govulncheck ./...

verify: fmt-check vet staticcheck test-race coverage tidy-check vulncheck
