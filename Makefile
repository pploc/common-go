.DEFAULT_GOAL := verify

# Ratchet this baseline as the new Kafka transport gains integration coverage.
COVERAGE_MIN ?= 60

.PHONY: fmt-check vet staticcheck test-race coverage tidy-check vulncheck integration verify

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l .; exit 1)

vet:
	go vet ./...

staticcheck:
	staticcheck ./...

test-race:
	go test -race ./...

coverage:
	@coverage_file="$$(mktemp)"; \
	go test -coverprofile="$$coverage_file" ./...; \
	coverage="$$(go tool cover -func="$$coverage_file" | awk '/^total:/ { gsub("%", "", $$3); print $$3 }')"; \
	rm -f "$$coverage_file"; \
	test "$$(awk "BEGIN { print ($$coverage >= $(COVERAGE_MIN)) ? 1 : 0 }")" = 1 || \
		(echo "coverage $$coverage% is below $(COVERAGE_MIN)%" >&2; exit 1)

tidy-check:
	go mod download
	go mod verify
	@test -z "$$(gofmt -l .)"

vulncheck:
	govulncheck ./...

# Integration services are external by design. CI seeds a clean Confluent 7.7.1
# Registry through gym-proto's fixture generator before running this target.
integration:
	@test -n "$(KAFKA_BROKERS)" || (echo "KAFKA_BROKERS is required" >&2; exit 1)
	@test -n "$(SCHEMA_REGISTRY_URL)" || (echo "SCHEMA_REGISTRY_URL is required" >&2; exit 1)
	KAFKA_BROKERS="$(KAFKA_BROKERS)" SCHEMA_REGISTRY_URL="$(SCHEMA_REGISTRY_URL)" go test -tags=integration -race ./...

verify: fmt-check vet staticcheck test-race coverage tidy-check vulncheck
