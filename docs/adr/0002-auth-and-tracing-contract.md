# ADR 0002: Trusted claims, tracing, and gRPC errors

- **Status:** Accepted
- **Date:** 2026-08-02

## Context

Kong is the external authentication boundary. The Go and Java shared libraries
must consume the same injected claims, tracing metadata, and error behavior.

## Decision

### Trusted claims

Kong validates external JWT signatures. Before forwarding a request, it must
strip client-provided copies of the following headers and inject trusted values:

| Claim | Header |
| --- | --- |
| User ID | `x-user-id` |
| Role | `x-user-role` |
| Gym ID | `x-gym-id` |
| Membership status | `x-membership-status` |
| Correlation ID | `x-trace-id` |

`common-go` and `common-java` extract these claims; they do not normally
revalidate JWT signatures. A feature that requires membership must reject a
missing or malformed membership status. Missing claims never grant access.

### Tracing

`traceparent` is the canonical trace header and `tracestate` is optional. When
a valid W3C context is present, it is authoritative. `x-trace-id` is retained
only for correlation compatibility: it is used when no valid W3C context is
present and must not overwrite an extracted W3C trace ID.

### gRPC errors

Domain errors carry a service-defined `x-error-code` ASCII trailer. Error
causes remain wrapped for server-side `errors.Is`/`errors.As` or Java exception
inspection. Internal implementation details are never returned to clients.

| Category | gRPC status |
| --- | --- |
| `VALIDATION` | `INVALID_ARGUMENT` |
| `UNAUTHORIZED` | `UNAUTHENTICATED` |
| `FORBIDDEN` | `PERMISSION_DENIED` |
| `NOT_FOUND` | `NOT_FOUND` |
| `CONFLICT` | `ALREADY_EXISTS` |
| `UNSUPPORTED` | `UNIMPLEMENTED` |
| `UNPROCESSABLE` | `FAILED_PRECONDITION` |
| `RATE_LIMITED` | `RESOURCE_EXHAUSTED` |
| `UNAVAILABLE` | `UNAVAILABLE` |
| `INTERNAL` or unknown | `INTERNAL` |

Unknown failures return the client-safe description `Internal server error`.

### Compatibility fixtures

The approved auth, tracing, and error fixtures are owned by
`gym-proto/contracts/v1`. Its manifest pins contract, protobuf, shared-library,
Kafka/Schema Registry, and gateway evidence versions. Go, Java, protobuf,
Kafka/Schema Registry, and gateway owners approve each fixture release.

## Consequences

The gateway must supply configuration and tests proving trusted-header stripping
and injection before Phase 1. Shared libraries may not claim that protection
without that evidence.
