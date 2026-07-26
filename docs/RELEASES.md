# Releases

How ZydecoDB versions and publishes the server binary and official drivers.

## Unified semver

One release commit carries one logical version `X.Y.Z` (optionally with a
pre-release suffix such as `-beta.7`). Artifacts at that commit:

| Artifact | Version identity |
|----------|------------------|
| Server binary | Root git tag `vX.Y.Z` (GitHub Release) |
| Python driver | `clients/python/pyproject.toml` `version` (must match the tag under PEP 440) |
| TypeScript driver | `clients/typescript/package.json` `version` (must match the tag) |
| Go driver | Nested git tag `clients/go/vX.Y.Z` at the **same commit** as `vX.Y.Z` |

Go modules in a subdirectory do not inherit root `v*` tags. Without
`clients/go/vX.Y.Z`, `go get` falls back to a pseudo-version
(`v0.0.0-<timestamp>-<hash>`).

## Compatibility

| Server | Drivers | Wire |
|--------|---------|------|
| `0.9.x` | Python / npm / Go `0.9.x*` | `proto_version = 1` |

- Drivers tagged `v0.9.x` target server `0.9.x` with the frozen 0.9 wire
  (see [`DOCUMENT_STORE.md`](DOCUMENT_STORE.md#wire-protocol)).
- Applications should pin an explicit driver version, not `@latest`, until the
  release train is routine.
- Append-only opcodes and status bytes may appear in later `0.9.x` minors;
  older drivers ignore unknown opcodes by never sending them. New conditional
  write opcodes fail closed (`ProtocolError`) on older servers rather than
  degrading to unconditional writes.

## Cutting a release

Bump versions in `Cargo.toml`, `clients/python/pyproject.toml`, and
`clients/typescript/package.json` on the release commit. Update
[`CHANGELOG.md`](../CHANGELOG.md). Then tag **both** the root and Go module
tags at that commit and push them together:

```bash
ver=0.9.0   # or 0.9.0-beta.8
git tag "v${ver}"
git tag "clients/go/v${ver}" "$(git rev-parse "v${ver}^{commit}")"
git push origin "v${ver}" "clients/go/v${ver}"
```

Pushing only the root tag will fail the release workflow’s Go tag gate.
Pushing only `clients/go/v*` does nothing (the workflow triggers on root `v*`).

The release workflow then:

1. Verifies `clients/go/v${ver}` exists at the same commit as `v${ver}`
2. Verifies `clients/go/go.mod` module path
3. Resolves `github.com/dataparade/zydecodb/clients/go@v${ver}` with
   `GOPROXY=direct`
4. Publishes the server binary, PyPI package, and npm package

## Pinning (applications)

```bash
# Go — pin the module version (not @latest)
go get github.com/dataparade/zydecodb/clients/go@v0.9.0

# Python
pip install zydecodb==0.9.0

# TypeScript
npm install zydecodb@0.9.0
```

Keep the server binary and drivers on the same `0.9.x` minor line.
