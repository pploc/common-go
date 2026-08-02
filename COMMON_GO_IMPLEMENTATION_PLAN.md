# Common Go Implementation Plan

## Status

- **Repository:** `github.com/pploc/common-go`
- **Default branch:** `develop`
- **Target Go version:** Go 1.26
- **Current implementation:** Only `README.md`; no Go module, implementation, tests, or CI
- **Primary consumers:** `ms-gym-identifier`, `ms-gym-workout`, `ms-gym-checkin`, and `ms-gym-notification`

This document is the implementation handoff for building `common-go`. Implement it incrementally. Do not implement later phases before their listed contract decisions and dependencies are resolved.

---

## 1. Goals

Build a small, stable shared Go module that provides:

1. Trusted authentication claim extraction from Kong-injected metadata.
2. Reusable gRPC interceptors for authentication, authorization, errors, logging, recovery, metrics, and tracing.
3. Canonical domain-error-to-gRPC mapping shared with `common-java`.
4. Kafka Protobuf producer support with schema verification and trace propagation.
5. Kafka manual-commit consumer support with at-least-once delivery, retry, and DLQ routing.
6. Small health, pagination, and testing helpers when real service consumers require them.
7. Go/Java wire-contract compatibility tests.

The library must use explicit constructors and typed configuration. It must not become an application framework.

---

## 2. Relevant Existing Documentation

Implementation must stay aligned with:

- `../docs/PLAN.md`
- `../docs/architecture/02-shared-libraries.md`
- `../docs/architecture/03-kafka-events.md`
- `../docs/architecture/04-project-structure.md`
- `../docs/services/01-ms-gym-identifier.md`
- `../docs/services/04-ms-gym-workout.md`
- `../docs/services/06-ms-gym-checkin.md`
- `../docs/services/07-ms-gym-notification.md`
- `../gym-proto/proto/common/v1/common.proto`
- `../gym-proto/proto/events/v1/*.proto`
- `../common-java/src/main/java/com/gym/common/`

`common-java` is a compatibility reference for shared headers, event metadata, error categories, retry behavior, and DLQ behavior. Do not copy Spring-specific architecture into Go.

---

## 3. Decisions Required Before Implementation

### 3.1 Canonical module path

Use the actual repository path unless the repository is moved before its first release:

```go
module github.com/pploc/common-go
```

Do not publish a release using the documented placeholder `github.com/gym-chain/common-go`.

### 3.2 Generated Protobuf ownership

`common-go` must not own copies of generated domain event stubs.

Required ownership:

| Concern | Owner |
|---|---|
| Raw `.proto` files | `gym-proto` |
| Generated Go and Java stubs | `gym-proto` release pipeline |
| Kafka transport, headers, retry, DLQ | `common-go` / `common-java` |
| Domain event construction and handling | Individual services |

Before Kafka implementation, make generated Go stubs importable from a permanent, tagged Go module. Resolve the mismatch between:

- actual repository: `github.com/pploc/gym-proto`
- current proto `go_package`: `github.com/gym-chain/proto-go/...`
- current generated output: `gym-proto/gen/go`

Do not depend on local `replace` directives in a released `common-go` version.

### 3.3 Kafka wire format

Resolve this before Phase 2:

- Documentation requires Protobuf events and Confluent Schema Registry in Protobuf mode.
- `common-java` currently uses a JSON event envelope containing an inline Protobuf-as-JSON payload.

Recommended target contract:

- Kafka key: domain ordering key as bytes/string.
- Kafka value: Schema Registry-framed concrete Protobuf event.
- Kafka headers: event metadata and trace correlation.

If the Java JSON envelope must remain, document JSON as the actual wire format and define exactly how Protobuf schema verification works. Go and Java must use the same contract.

### 3.4 Authentication trust boundary

Kong validates external JWT signatures. `common-go` should normally extract trusted claims; it should not revalidate JWT signatures.

Canonical metadata/header names:

| Claim | Header/metadata |
|---|---|
| User ID | `x-user-id` |
| Role | `x-user-role` |
| Gym ID | `x-gym-id` |
| Membership status | `x-membership-status` |
| Correlation ID | `x-trace-id` |

Kong must strip any client-supplied copies before injecting trusted values.

### 3.5 Membership propagation

Workout requires `membership_status == ACTIVE`, but the current Java auth library extracts only user ID, role, and gym ID.

Before enabling the Workout membership guard:

1. Add `x-membership-status` to the gateway/upstream propagation contract.
2. Verify valid access tokens produce the header.
3. Verify missing or malformed membership status fails closed.

### 3.6 Delivery semantics

The supported Kafka guarantee is:

```text
Idempotent producer configuration + manual consumer commit + at-least-once handler execution
```

Do not describe this as end-to-end exactly-once processing. Service handlers remain responsible for idempotency. Database-and-Kafka atomicity requires an outbox or another service-level pattern.

### 3.7 Default retry semantics

Use:

1. Initial handler attempt.
2. Retry after 2 seconds.
3. Retry after 4 seconds.
4. Retry after 8 seconds.
5. Publish to `{topic}.DLQ` after the third retry fails.

Commit the original record only after handler success or successful DLQ publication.

---

## 4. Design Principles

1. **Use Go standard-library facilities where practical.** Prefer `log/slog` over forcing Zap or Zerolog.
2. **Avoid global state.** Use explicit constructors and dependency injection.
3. **Keep exported APIs independent of infrastructure clients.** Do not expose concrete Kafka client types.
4. **Keep framework dependencies narrow.** Core packages must not require Gin.
5. **Applications own configuration loading.** Do not add Viper to the shared library.
6. **Do not log payloads by default.** They may contain credentials, payment data, or personal information.
7. **Use low-cardinality metric labels.** Never label metrics with user IDs, gym IDs, trace IDs, event IDs, Kafka keys, or exception messages.
8. **Use standard OpenTelemetry propagation.** Prefer W3C `traceparent` and `tracestate`; retain `x-trace-id` only for compatibility/correlation.
9. **Fail closed for authorization.** Missing claims never imply access.
10. **Keep service-specific business logic in services.** Shared code provides mechanisms, not domain policy catalogs.

---

## 5. Target Repository Structure

Create packages incrementally; do not create empty placeholder packages.

```text
common-go/
├── go.mod
├── go.sum
├── README.md
├── LICENSE
├── Makefile
├── auth/
│   ├── claims.go
│   ├── headers.go
│   ├── metadata.go
│   ├── http.go
│   ├── context.go
│   └── claims_test.go
├── errors/
│   ├── error.go
│   ├── category.go
│   ├── codes.go
│   ├── grpc.go
│   └── grpc_test.go
├── logging/
│   ├── context.go
│   ├── fields.go
│   └── context_test.go
├── grpc/
│   ├── interceptor/
│   │   ├── auth.go
│   │   ├── authorization.go
│   │   ├── error.go
│   │   ├── logging.go
│   │   ├── recovery.go
│   │   ├── chain.go
│   │   └── interceptor_test.go
│   └── middleware/
│       ├── membership.go
│       └── membership_test.go
├── observability/
│   ├── tracing.go
│   ├── propagation.go
│   ├── metrics.go
│   └── observability_test.go
├── kafka/
│   ├── config.go
│   ├── event.go
│   ├── headers.go
│   ├── serializer.go
│   ├── schema.go
│   ├── producer.go
│   ├── consumer.go
│   ├── retry.go
│   ├── dlq.go
│   ├── errors.go
│   └── *_test.go
├── health/
│   ├── checker.go
│   └── checker_test.go
├── pagination/
│   ├── cursor.go
│   └── cursor_test.go
├── testutil/
│   ├── clock.go
│   ├── producer.go
│   ├── consumer.go
│   └── grpc.go
├── internal/
│   └── kafka/
│       ├── client.go
│       └── registry.go
└── .github/
    └── workflows/
        ├── ci.yml
        ├── contract.yml
        └── release.yml
```

---

# Phase 0 — Repository and Contract Foundation

## Objective

Create the Go module, baseline automation, and approved cross-language contracts before exposing public APIs.

## Files

```text
go.mod
go.sum
README.md
LICENSE
Makefile
.github/workflows/ci.yml
docs/adr/0001-module-path.md
docs/adr/0002-auth-and-tracing-contract.md
docs/adr/0003-kafka-wire-contract.md
```

## Tasks

1. Initialize `go.mod` with Go 1.26.
2. Set the permanent module path.
3. Add baseline dependencies only:
   - `google.golang.org/grpc`
   - `google.golang.org/protobuf`
   - OpenTelemetry API/SDK integration required by the chosen design
4. Add build commands:
   - format check
   - `go vet`
   - `staticcheck`
   - `go test -race ./...`
   - coverage
   - `govulncheck`
5. Document public API and compatibility policy.
6. Approve the auth header contract.
7. Approve the error category and `x-error-code` contract.
8. Approve the Kafka wire format and schema subject naming.
9. Make generated Go Protobuf stubs importable from a tagged module.
10. Correct documentation that runs Buf breaking checks against `main`; this workspace uses `develop` as the default branch.

## Acceptance Criteria

- `go mod tidy` succeeds without local `replace` directives.
- `go test -race ./...` runs in CI.
- A clean external sample module can import `common-go`.
- A clean external sample module can import generated `gym-proto` Go stubs.
- Go and Java owners approve versioned auth, error, Kafka, and DLQ fixtures.
- No public API depends on Gin, Viper, Spring concepts, or a concrete Kafka client.

## Suggested Release

No public feature release. Complete this phase before `v0.1.0`.

---

# Phase 1 — Claims, Errors, gRPC, Logging, and Observability

## Objective

Publish the first useful version for Go gRPC services without requiring Kafka.

## 1. Auth Package

### Proposed API

```go
package auth

type Role string

const (
    RoleCustomer Role = "CUSTOMER"
    RoleTrainer  Role = "TRAINER"
    RoleAdmin    Role = "ADMIN"
)

type MembershipStatus string

const (
    MembershipActive  MembershipStatus = "ACTIVE"
    MembershipPaused  MembershipStatus = "PAUSED"
    MembershipExpired MembershipStatus = "EXPIRED"
)

type Claims struct {
    UserID           string
    Role             Role
    GymID            string
    MembershipStatus MembershipStatus
}

func FromIncomingContext(ctx context.Context) (Claims, error)
func FromHTTPHeader(header http.Header) (Claims, error)
func NewContext(ctx context.Context, claims Claims) context.Context
func FromContext(ctx context.Context) (Claims, bool)
func MustFromContext(ctx context.Context) Claims
```

### Requirements

- Centralize header constants.
- Treat user ID and role as required for authenticated methods.
- Allow gym ID to be optional only when explicitly supported.
- Reject unknown roles when validation is enabled.
- Reject conflicting duplicate values.
- Normalize role and membership values consistently.
- Keep context keys unexported.
- Do not parse or validate JWT signatures.
- Avoid forcing UUID parsing unless all services approve it as a permanent contract.

### Tests

- all valid claims
- missing user ID
- missing role
- optional/missing gym ID
- active, paused, and expired membership
- missing membership
- unknown membership
- conflicting metadata values
- empty metadata values
- HTTP header extraction
- gRPC metadata extraction
- context round trip
- concurrent access under the race detector

## 2. Error Package

### Proposed API

```go
package errors

type Category string
type Code string

const (
    CategoryValidation    Category = "VALIDATION"
    CategoryUnauthorized  Category = "UNAUTHORIZED"
    CategoryForbidden     Category = "FORBIDDEN"
    CategoryNotFound      Category = "NOT_FOUND"
    CategoryConflict      Category = "CONFLICT"
    CategoryUnsupported   Category = "UNSUPPORTED"
    CategoryUnprocessable Category = "UNPROCESSABLE"
    CategoryRateLimited   Category = "RATE_LIMITED"
    CategoryUnavailable   Category = "UNAVAILABLE"
    CategoryInternal      Category = "INTERNAL"
)

type Error struct {
    Code     Code
    Category Category
    Message  string
    Cause    error
}

func New(code Code, category Category, message string) *Error
func Wrap(err error, code Code, category Category, message string) *Error
func ToGRPC(err error) error
```

### gRPC Mapping

| Category | gRPC code |
|---|---|
| `VALIDATION` | `InvalidArgument` |
| `UNAUTHORIZED` | `Unauthenticated` |
| `FORBIDDEN` | `PermissionDenied` |
| `NOT_FOUND` | `NotFound` |
| `CONFLICT` | `AlreadyExists` |
| `UNSUPPORTED` | `Unimplemented` |
| `UNPROCESSABLE` | `FailedPrecondition` |
| `RATE_LIMITED` | `ResourceExhausted` |
| `UNAVAILABLE` | `Unavailable` |
| `INTERNAL` / unknown | `Internal` |

### Requirements

- Emit `x-error-code` as gRPC trailer metadata.
- Preserve causes for `errors.Is` and `errors.As`.
- Redact internal error details from clients.
- Allow services to define business-specific error codes.
- Do not put all domain errors in `common-go`.

### Tests

- every category mapping
- `x-error-code` trailer
- wrapped errors
- unknown errors
- internal message redaction
- nil handling
- existing gRPC status handling

## 3. gRPC Interceptors

### Proposed Constructors

```go
func AuthUnary(opts AuthOptions) grpc.UnaryServerInterceptor
func AuthStream(opts AuthOptions) grpc.StreamServerInterceptor

func ErrorMappingUnary() grpc.UnaryServerInterceptor
func ErrorMappingStream() grpc.StreamServerInterceptor

func LoggingUnary(logger *slog.Logger) grpc.UnaryServerInterceptor
func LoggingStream(logger *slog.Logger) grpc.StreamServerInterceptor

func RecoveryUnary(logger *slog.Logger) grpc.UnaryServerInterceptor
func RecoveryStream(logger *slog.Logger) grpc.StreamServerInterceptor

func AuthorizationUnary(policy Policy) grpc.UnaryServerInterceptor
func AuthorizationStream(policy Policy) grpc.StreamServerInterceptor
```

### Requirements

- Public methods must be configurable explicitly.
- Authentication extraction must populate `auth.Claims` in context.
- Recovery must turn panics into `codes.Internal` without leaking details.
- Logging must record method, status, duration, trace ID, and safe claim fields.
- Request and response bodies must not be logged by default.
- Provide documented recommended interceptor ordering.
- Support unary and streaming calls where relevant.

## 4. Authorization Policies

### Proposed API

```go
type Policy interface {
    Authorize(ctx context.Context, fullMethod string, claims auth.Claims) error
}

func RequireRoles(roles ...auth.Role) Policy
func RequireActiveMembership() Policy
func PublicMethods(methods ...string) Policy
func Combine(policies ...Policy) Policy
```

### Membership Rules

- Allow only `ACTIVE`.
- Missing membership status fails closed.
- Unknown membership status fails closed.
- `PAUSED` and `EXPIRED` return `PermissionDenied`.
- Use a stable application error code such as `MEMBERSHIP_REQUIRED`.

Do not enable the policy in Workout until `x-membership-status` propagation is verified.

## 5. Logging

Use `log/slog` and context helpers:

```go
func NewContext(ctx context.Context, logger *slog.Logger) context.Context
func FromContext(ctx context.Context) *slog.Logger
func WithClaims(logger *slog.Logger, claims auth.Claims) *slog.Logger
func WithTrace(logger *slog.Logger, traceID string) *slog.Logger
```

Safe default fields:

- service
- gRPC method
- trace ID
- status code
- duration
- role
- gym ID where approved

Never include passwords, tokens, authorization headers, payment payloads, or full request/response objects by default.

## 6. OpenTelemetry and Metrics

Prefer official OpenTelemetry gRPC instrumentation such as `otelgrpc` rather than reimplementing span lifecycle.

Requirements:

- W3C `traceparent` and `tracestate` propagation.
- `x-trace-id` compatibility for logs and Kafka headers.
- Server request count.
- Server duration histogram.
- Status and method dimensions only.
- Panic counter.

Never use high-cardinality labels such as user ID, gym ID, trace ID, event ID, Kafka key, or exception message.

## Phase 1 Acceptance Criteria

- A sample gRPC server can install all interceptors without Gin or Kafka dependencies.
- Authenticated handlers can retrieve typed claims from context.
- Public methods can bypass authentication only when explicitly listed.
- Missing membership denies access when the membership policy is enabled.
- Error mappings and trailers match `common-java` contract fixtures.
- Panics return `Internal` and are logged safely.
- Trace context propagates through gRPC using OpenTelemetry standards.
- All tests pass with `go test -race ./...`.

## Suggested Release

`v0.1.0`

---

# Phase 2 — Kafka Protobuf Producer and Schema Verification

## Objective

Support conformant event publishing for Identifier, Workout, and Check-in.

## Client Selection

Recommended: `franz-go`.

Keep all client-specific types under `internal/kafka`. Export only project-owned interfaces and configuration types.

## Proposed API

```go
package kafka

type Event struct {
    Topic     string
    Key       []byte
    EventType string
    Payload   proto.Message

    Timestamp time.Time
    TraceID   string
    Source    string
    EventID   string
    Headers   map[string]string
}

type Producer interface {
    Publish(ctx context.Context, event Event) error
    Close() error
}

type ProducerConfig struct {
    Brokers        []string
    ClientID       string
    Source         string
    PublishTimeout time.Duration
    SchemaRegistry SchemaRegistryConfig
}
```

## Serialization Interfaces

```go
type Serializer interface {
    Serialize(ctx context.Context, topic string, message proto.Message) ([]byte, SchemaMetadata, error)
}

type SchemaVerifier interface {
    Verify(ctx context.Context, topic string, message proto.Message) error
}
```

## Producer Requirements

1. Validate topic naming: `{domain}.{entity}.{action}`.
2. Require a non-nil Protobuf payload.
3. Require source and event type.
4. Generate an event ID when absent using an injectable ID generator.
5. Use an injectable clock for deterministic tests.
6. Derive the trace ID from the current OpenTelemetry span unless explicitly provided according to documented precedence.
7. Normalize timestamps to UTC.
8. Wait for broker acknowledgement before returning success.
9. Configure `acks=all` and idempotent production where supported.
10. Honor context cancellation and publish timeout.
11. Verify schema before publishing.
12. Avoid automatic schema registration in production by default.

## Canonical Kafka Headers

```text
event-type
source
timestamp
event-id
traceparent
tracestate (optional)
```

Use decimal Unix epoch milliseconds for `timestamp`. `x-trace-id` remains a
compatibility fallback. Consumers may read legacy `x-event-*` headers only in
an explicitly versioned migration; new producers must emit the canonical names.

## Schema Registry Requirements

- Confluent-compatible Protobuf support.
- Explicit subject naming strategy shared with Java.
- `BACKWARD` compatibility.
- Cache schema lookups.
- Avoid a network schema discovery request for every record.
- Clearly distinguish incompatible schema, unavailable registry, and serialization errors.

## Producer Tests

### Unit

- invalid and valid topics
- nil payload
- source and event type validation
- timestamp generation
- event ID generation
- header generation
- serializer failure
- schema incompatibility
- registry unavailability
- broker failure
- context cancellation
- publish timeout
- close behavior

### Integration

- real Kafka broker
- real Schema Registry
- concrete event from `gym-proto`
- key-based partition behavior
- Go record consumed by Java
- Java record consumed by Go
- trace/header round trip

## Phase 2 Acceptance Criteria

- Identifier can publish `identity.user.registered`.
- Workout can publish `workout.logged`.
- Check-in can publish `checkin.recorded`.
- Java consumers can decode Go records with no custom transformation.
- Incompatible event schemas fail before broker publication.
- Producer documentation explicitly states that idempotent production is not end-to-end exactly-once processing.

## Suggested Release

`v0.2.0`

---

# Phase 3 — Manual-Commit Consumer, Retry, and DLQ

## Objective

Support safe at-least-once consumption for Notification and future Go consumers.

## Proposed API

```go
package kafka

type Message struct {
    Topic     string
    Partition int32
    Offset    int64

    Key       []byte
    Value     []byte
    Headers   Headers

    EventType string
    EventID   string
    TraceID   string
    Source    string
    Timestamp time.Time
}

type Handler func(context.Context, Message) error

type Consumer interface {
    Run(ctx context.Context, handler Handler) error
    Close() error
}

func Decode(msg Message, target proto.Message) error
```

Keep the base consumer untyped because Notification subscribes to many event types.

## Consumer Configuration

```go
type ConsumerConfig struct {
    Brokers          []string
    ClientID         string
    GroupID          string
    Topics           []string
    AutoOffsetReset  OffsetReset
    Retry            RetryPolicy
    DLQ              DLQConfig
    SchemaRegistry   SchemaRegistryConfig
}
```

Defaults for Notification should support:

- group: `ms-gym-notification-group`
- auto-offset reset: earliest
- auto-commit: disabled
- manual commit after successful processing

## Consumer Requirements

1. Disable automatic commits.
2. Process each partition sequentially by default.
3. Reconstruct trace context from Kafka headers.
4. Invoke the handler with a derived context.
5. Commit only after successful handler completion.
6. Retry in place to preserve partition ordering.
7. Do not commit a later offset while an earlier record is still incomplete.
8. Handle rebalances without committing unfinished records.
9. Shut down without leaking goroutines.
10. Redeliver uncommitted messages after restart.

## Retry API

```go
type RetryPolicy struct {
    MaxRetries int
    Backoff    func(retry int) time.Duration
    Retryable  func(error) bool
}

func Permanent(err error) error
func IsPermanent(err error) bool
```

Default retry sequence:

```text
initial attempt
2-second backoff
4-second backoff
8-second backoff
DLQ
```

Permanent errors may skip transient retries and go directly to DLQ.

Examples of permanent errors:

- invalid schema
- corrupt payload
- missing mandatory IDs
- unsupported event type

Examples of retryable errors:

- database timeout
- dependency unavailable
- temporary network failure
- external provider timeout

## DLQ Requirements

On retry exhaustion or permanent failure:

1. Publish to `{original-topic}.DLQ`.
2. Preserve the original record key.
3. Preserve the original serialized value unchanged.
4. Preserve original headers.
5. Add or replace:
   - `x-original-topic`
   - `x-exception-message`
   - `x-failed-at`
   - `x-retry-count`
6. Wait for DLQ broker acknowledgement.
7. Commit the original record only after successful DLQ publication.
8. Do not commit if DLQ publication fails.
9. Sanitize and truncate exception messages.

Potential optional headers, only after Go/Java agreement:

- `x-original-partition`
- `x-original-offset`
- `x-error-category`

## Notification Integration Rule

Notification must not enqueue work into a volatile in-memory worker pool and immediately return success. That would commit Kafka before delivery work is durable.

The handler must either:

1. wait for bounded worker completion, or
2. persist a durable notification job before returning success.

## Consumer Tests

- successful handler commits once
- first retry success
- second retry success
- third retry success
- retry exhaustion
- permanent error path
- exact 2/4/8 schedule using fake sleeper/clock
- one DLQ publication
- original key/value/header preservation
- DLQ publish failure leaves original uncommitted
- cancellation during handler
- cancellation during backoff
- rebalance during processing
- restart and redelivery
- per-partition ordering
- concurrent partitions
- no goroutine leaks
- race detector

## Phase 3 Acceptance Criteria

- Notification consumes multiple Protobuf event types.
- Handler success commits the correct contiguous offset.
- Failures follow the exact default retry policy.
- DLQ publication preserves the original record.
- Failed DLQ publication never commits the source record.
- Consumer restart redelivers uncommitted messages.
- Delivery semantics are documented as at-least-once.

## Suggested Release

`v0.3.0`

---

# Phase 4 — Utilities and Operational Hardening

Implement these only when at least one real service is ready to adopt them.

## Health

```go
type Checker interface {
    Name() string
    Check(context.Context) error
}
```

Requirements:

- Aggregate named dependency checks.
- Distinguish liveness from readiness.
- Do not require Gin.
- Let services expose results using gRPC health, HTTP, or both.

## Pagination

Align with `gym-proto/proto/common/v1/common.proto`.

Before implementation, define:

- cursor version
- URL-safe encoding
- compound key ordering
- maximum page size
- malformed cursor behavior
- whether cursors require HMAC signing

Do not create a second incompatible page model.

## Test Utilities

Useful shared fakes:

- fake clock
- fake retry sleeper
- fake producer
- fake consumer record source
- record collector
- fake schema verifier
- in-process gRPC test helper

Do not make Testcontainers or Docker orchestration a transitive dependency of normal consumers. Keep real Kafka/Schema Registry setup in integration CI or a separate integration-test module.

## Crypto

Do not add generic crypto or direct JWT validation helpers without a concrete adopter, key-management design, and threat model.

## Suggested Release

`v0.4.x`

---

# Cross-Language Contract Tests

## Location

Prefer a language-neutral, versioned location owned with the API contracts, for example:

```text
gym-proto/contracts/
```

Do not let either `common-go` or `common-java` privately define the canonical fixtures.

## Auth Fixtures

- valid user, role, gym, and membership
- optional gym ID
- missing user ID
- missing role
- invalid role
- active, paused, and expired membership
- missing membership
- conflicting duplicate headers

## Trace Fixtures

- valid W3C trace context
- `x-trace-id` compatibility
- missing trace context
- gRPC-to-Kafka propagation
- Java-to-Go and Go-to-Java propagation

## Error Fixtures

Each fixture should define:

- category
- application error code
- expected gRPC code
- expected `x-error-code` trailer
- client-safe message policy

## Kafka Fixtures

Each fixture should define:

- topic
- key
- event type
- Protobuf message type
- payload
- timestamp
- source
- trace ID
- event ID
- expected headers
- expected decoded record

## DLQ Fixtures

Each fixture should define:

- original topic
- original key/value/headers
- failure category
- retry count
- expected DLQ topic
- expected diagnostic headers
- expected commit decision

## Compatibility Matrix

For each release candidate, record:

| Component | Version |
|---|---|
| `common-go` | candidate version |
| `common-java` | supported version |
| `gym-proto` | pinned version |
| contract fixtures | pinned version |
| Kafka | production baseline |
| Schema Registry | production baseline |

A release candidate must pass its local tests and the shared contract fixture suite.

---

# CI and Release Plan

## Pull Request Checks

Run:

```bash
gofmt -l .
go vet ./...
staticcheck ./...
go test -race ./...
go test -coverprofile=coverage.out ./...
go mod tidy
git diff --exit-code -- go.mod go.sum
govulncheck ./...
```

Also run when relevant:

- Go/Java contract fixture suite
- Kafka and Schema Registry integration suite
- public API compatibility check against the previous release
- dependency/license scan
- secret scan

## Coverage Expectations

- Overall: at least 85–90%.
- Auth, error mapping, serializer, retry, commit, and DLQ branches: near-complete branch coverage.
- Do not rely on coverage percentage instead of real broker integration tests.

## Release Policy

- Develop on `develop`.
- Use semantic versioning.
- Use Go module tags:
  - `v0.1.0`: core auth/gRPC/errors/observability
  - `v0.2.0`: Kafka producer/schema support
  - `v0.3.0`: consumer/retry/DLQ
  - `v0.4.x`: selected utilities
  - `v1.0.0`: stable API after production adoption
- A future v2 must use a `/v2` module path.
- Release notes must identify:
  - supported Go version
  - supported `gym-proto` version
  - compatible `common-java` version
  - Kafka/Schema Registry baseline
  - delivery guarantees
  - migrations and breaking changes

## Commit Messages

All commits must begin with one of:

```text
feat:
fix:
refactor:
chore:
test:
```

Do not append `Co-Authored-By` trailers.

---

# Service Rollout Plan

## 1. Identifier Pilot

Adopt:

- shared error mapping
- logging and tracing
- gRPC request context
- Kafka producer for `identity.user.registered`

This validates producer behavior with a smaller operational surface.

## 2. Check-in

Adopt:

- trusted claims
- error mapping
- tracing and metrics
- Kafka producer for `checkin.recorded`

Verify that Kong strips spoofed external claim headers.

## 3. Workout

Adopt:

- trusted claims
- active membership policy
- Kafka producer for `workout.logged`

Do not enable the membership policy until `x-membership-status` is reliably propagated.

## 4. Notification

Adopt:

- broad Kafka consumer
- manual commit
- retry and DLQ
- trace propagation
- durable or completion-aware bounded worker processing

This should be the final initial adopter because it has the most consumer and DLQ complexity.

## Rollout Observability

Create dashboards and alerts for:

- producer failures
- Schema Registry failures
- consumer lag
- handler failures
- retry count
- DLQ rate
- DLQ publication failure
- unknown event types
- rebalance frequency

Avoid high-cardinality metric dimensions.

---

# Explicit Non-Goals

Do not include these in the initial implementation:

- JWT signature/JWKS validation for normal Kong-routed traffic
- Gin application setup
- global Viper configuration
- database repositories or transaction abstractions
- generated domain Protobuf files inside `common-go`
- automatic DLQ replay
- generic outbox framework
- end-to-end exactly-once claims
- universal Testcontainers framework
- generic crypto helpers
- service-specific topic catalogs
- service-specific business error catalogs
- request/response payload logging by default

---

# Suggested Pull Request Sequence

| PR | Scope | Expected outcome |
|---|---|---|
| 1 | Module, Go 1.26, CI, Makefile, ADRs | Buildable repository |
| 2 | Auth headers, claims, context, tests | Trusted identity context |
| 3 | Error model and gRPC mapping | Shared error contract |
| 4 | Logging, recovery, OTel, metrics, policies | `v0.1.0` |
| 5 | Kafka wire-contract fixtures and proto module integration | Kafka contract locked |
| 6 | Serializer, schema verifier, producer | `v0.2.0` |
| 7 | Base consumer and manual offset management | Consumer candidate |
| 8 | Retry classification and DLQ | `v0.3.0` |
| 9 | Cross-language integration and operational documentation | Hardened messaging |
| 10 | Health/pagination/test helpers with real adopters | `v0.4.x` |
| 11 | API review, migration cleanup, two-service validation | `v1.0.0` candidate |

Each PR must be independently testable. Avoid one repository-wide implementation PR.

---

# Rough Effort

| Area | Estimate |
|---|---:|
| Module, ADRs, proto/contract decisions | 3–7 engineering days |
| Auth, errors, gRPC, logging, observability | 8–12 engineering days |
| Kafka producer and Schema Registry | 8–15 engineering days |
| Consumer, retry, offset safety, and DLQ | 12–18 engineering days |
| Cross-language fixtures | 6–10 engineering days, partly parallel |
| CI, release automation, and docs | 4–7 engineering days |
| Selected utility packages | 5–12 engineering days |

Expected elapsed time:

- Producer-ready pilot: approximately 4–7 weeks.
- Production-tested consumer and DLQ: approximately 7–11 weeks.

---

# Definition of Done

`common-go` is ready for `v1.0.0` when:

1. The permanent module and generated-proto import paths are stable.
2. Kafka Go/Java wire compatibility is proven in CI.
3. Auth, error, trace, Kafka, retry, and DLQ fixtures pass in both languages.
4. At least two Go services use the core packages in production-like environments.
5. At least one producer and one consumer use the Kafka packages successfully.
6. Manual commit and DLQ failure behavior is tested against a real Kafka broker.
7. Public APIs do not expose concrete Kafka client types.
8. The race detector, static analysis, vulnerability checks, and integration tests pass.
9. Delivery guarantees and security trust boundaries are documented accurately.
10. No unresolved high-severity operational or contract issues remain.
