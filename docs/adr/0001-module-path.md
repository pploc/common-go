# ADR 0001: Module path and generated Protobuf ownership

- **Status:** Accepted
- **Date:** 2026-08-02

## Context

The repository is beginning as a shared Go module. Historical documentation
uses placeholder paths, while generated Protobuf Go code is published by a
separate repository workflow. Consumers require stable, externally resolvable
imports.

## Decision

- The permanent module path is `github.com/pploc/common-go`.
- The minimum supported Go version is Go 1.26.
- Releases use semantic versioning. Breaking exported API changes require the
  normal Go major-version suffix (`/v2`, `/v3`, and later).
- `develop` is the integration branch. CI and Buf breaking comparisons use it
  unless a release branch explicitly supersedes it.
- Released `common-go` versions must not contain local `replace` directives.
- `gym-proto` owns source `.proto` schemas and canonical language-neutral
  compatibility fixtures. It publishes generated Go stubs as the separately
  versioned, tagged module `github.com/pploc/proto-go`.
- `common-go` may depend on a tagged `proto-go` version when a later feature
  requires generated stubs, but it never vendors or publishes domain stubs.

## Amendment

On 2026-08-02, before Phase 2 began, the Go 1.22 baseline was superseded by Go
1.26. The upgrade establishes the supported security and tooling floor after
reachable standard-library vulnerability findings under Go 1.22. It does not
change the module path or generated Protobuf ownership decision.

## Consequences

A fresh module must be able to import a tagged `common-go` release and a tagged
`proto-go` release without workspace paths or local replacements. The
`gym-proto` release pipeline is responsible for keeping every `go_package`
option and generated import consistent with `github.com/pploc/proto-go`.
