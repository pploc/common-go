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
| Compatibility correlation ID | `x-trace-id` |

Canonical user roles are `CUSTOMER`, `TRAINER`, `ADMIN`, and `SUPER_ADMIN`.
Public registration creates only `CUSTOMER`; elevated roles require protected
administration or controlled out-of-band provisioning. Workload identities are
never represented in `x-user-role`.

Canonical membership statuses are `NONE`, `ACTIVE`, `PAUSED`, and `EXPIRED`.
New customers and non-customer roles use `NONE`. Ordinary authenticated methods
accept any known status, while membership-gated methods require `ACTIVE` and
fail closed on a missing, blank, malformed, conflicting, or unknown status.

`common-go` and `common-java` trim and normalize these values and reject
conflicting duplicates; they do not normally revalidate JWT signatures. Missing
claims never grant access. Claims are trusted only after the request has passed
through the authenticated Kong boundary or a separately verified workload
channel.

### Workload identity

Identifier-to-Member calls use mTLS. The certificate identity/SAN identifies
`ms-gym-identifier`, Member authorizes that verified peer, and NetworkPolicy
permits only intended callers on native gRPC port `50051`. Metadata such as
`x-service-id`, if retained for observability, never establishes trust unless it
is bound to the verified peer. The internal workload identity is not a user role
and cannot be injected by a public client.

### Tracing

`traceparent` is the canonical trace header and `tracestate` is optional. When
a valid W3C context is present, it is authoritative. `x-trace-id` is retained
only for correlation compatibility: it is used when no valid W3C context is
present, must not overwrite an extracted W3C trace ID, and never fabricates an
OpenTelemetry parent.

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
