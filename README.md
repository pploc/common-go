# common-go

Shared Go contracts and utilities for the Gym microservices.

## Status

Phase 1 provides trusted-claim extraction, client-safe domain errors, gRPC
interceptors, structured logging helpers, and opt-in OpenTelemetry propagation
and metrics. Kafka, health, pagination, and test utilities remain out of scope.

## Compatibility policy

- Module path: `github.com/pploc/common-go`.
- Minimum supported Go version: 1.26.
- Releases follow semantic versioning. A breaking public API change requires a
  new Go module major version (`/v2`, `/v3`, and so on).
- Released versions do not contain `replace` directives.
- Generated domain Protobuf code is owned and released by `gym-proto` through
  the tagged `github.com/pploc/proto-go` module. This module does not copy
  generated domain stubs.
- Canonical compatibility fixtures are versioned with `gym-proto` under
  `contracts/v1` and must pass in both common libraries before a contract
  change is released.

## Contracts

- [Module path and compatibility](docs/adr/0001-module-path.md)
- [Authentication, tracing, and gRPC errors](docs/adr/0002-auth-and-tracing-contract.md)
- [Kafka wire format, retries, and DLQ](docs/adr/0003-kafka-wire-contract.md)

Applications own configuration loading. The shared module does not introduce
Gin, Viper, Spring concepts, concrete Kafka client types in public APIs, JWT
signature validation, or default event-payload logging.

## Phase 1 packages

- `auth`: validates gateway-injected trusted headers and stores typed claims in contexts.
- `errors`: maps categorized domain failures to client-safe gRPC statuses and `x-error-code` trailers.
- `grpc/interceptor` and `grpc/middleware`: compose recovery, error conversion, logging, metrics, authentication, and explicit authorization policies.
- `logging`: derives safe `log/slog` loggers without payloads, tokens, user IDs, or error details.
- `observability`: installs `otelgrpc` server instrumentation and exposes W3C propagation plus bounded metrics.

Use `interceptor.ServerOptions` when constructing a gRPC server. It installs the
recommended order: recovery, error conversion, logging, metrics,
authentication, and authorization, plus the official `otelgrpc` stats handler.

Kong remains responsible for validating JWTs and stripping client-provided
trusted headers before injecting claims. This library implements fail-closed
extraction only; it does not establish that production trust boundary. Do not
tag a Phase 1 release until the gateway evidence, owner approvals, cross-language
fixture runs, and external tagged-module smoke test are recorded in the contract
manifest.

## Verification

Install the pinned analysis tools, then run:

```bash
go install honnef.co/go/tools/cmd/staticcheck@v0.7.0
go install golang.org/x/vuln/cmd/govulncheck@v1.6.0
make verify
```

`make verify` runs formatting, vet, static analysis, race tests, coverage,
module-tidiness, and vulnerability checks. CI runs the same target on pushes
and pull requests to `develop`.
