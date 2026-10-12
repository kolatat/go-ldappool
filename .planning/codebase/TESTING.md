---
last_mapped_commit: 23ccb27db8c4e78284ada38c52c7e913f5fc89b1
last_mapped_at: 2026-10-12
---
# Testing Patterns

**Analysis Date:** 2026-10-12

## Current State

No test files exist in the repository. A search for `*_test.go` across the module returned zero results, and no test fixtures, mocks, testdata directories, or CI workflow files are present. Everything below is the recommended approach derived from the code's structure, not a description of existing tests.

## Test Framework

**Runner:**
- Go standard `testing` package (no third-party runner detected).
- Module: `github.com/kolatat/go-ldappool`, `go 1.19` (`go.mod`).
- Config: none. No `go test` flags are set in the repo.

**Assertion Library:**
- None installed. Use plain `t.Errorf` / `t.Fatalf` from the standard library. If an assertion library is added later, it must be added to `go.mod` and recorded in `go.sum`.

**Run Commands:**

```bash
go test ./...                    # Run all tests (root package and adhelper/)
go test -race ./...              # Run with the race detector (recommended: heavy mutex and channel use in directory.go)
go test -cover ./...             # Coverage summary
go test -coverprofile=cover.out ./... && go tool cover -html=cover.out   # HTML coverage report
go vet ./...                     # Static checks before committing
```

## Test File Organization

**Location:**
- Co-located with source, as is idiomatic for Go: `conn_test.go` next to `conn.go`, `connector_test.go` next to `connector.go`, `adhelper/interface_test.go` next to `adhelper/interface.go`.
- Use the same package name for white-box tests of unexported internals (`package ldappool`), and `package ldappool_test` for black-box tests of the public API.

**Naming:**
- `<source>_test.go`. Test function names: `TestXxx` for exported behaviour, `TestXxx_case` or `TestIsBadConn` style for unexported helpers.

**Structure:**

```
go-ldappool/
├── conn.go
├── conn_test.go          # white-box: expired(), closeDBLocked(), validateConnection()
├── connector.go
├── connector_test.go     # white-box: updateLatency(), decayLatencies(), srvHeap ordering
├── directory.go
├── directory_test.go     # white-box: maxIdleConnsLocked(), shortestIdleTimeLocked(), putConnDirLocked()
├── errors_test.go        # IsBadConn classification, adErr Unwrap
├── server.go             # srvHeap is tested via connector_test.go
└── adhelper/
    └── interface_test.go # ResetPassword encoding
```

## Test Structure

**Suite Organization:**
Use table-driven tests with `t.Run` subtests. Example for the pure classification function `IsBadConn` (`errors.go`):

```go
package ldappool

import (
    "errors"
    "fmt"
    "testing"
)

func TestIsBadConn(t *testing.T) {
    tests := []struct {
        name string
        err  error
        want bool
    }{
        {"nil", nil, false},
        {"sentinel", ErrBadConn, true},
        {"wrapped sentinel", fmt.Errorf("wrap: %w", ErrBadConn), true},
        {"expired conn", ErrConnExpired, true},
        {"unrelated", errors.New("other"), false},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            if got := IsBadConn(tt.err); got != tt.want {
                t.Errorf("IsBadConn(%v) = %v, want %v", tt.err, got, tt.want)
            }
        })
    }
}
```

**Patterns:**
- Setup: construct `connector` or `Directory` values directly in the test (`&connector{}`, `&Directory{connRequests: make(map[uint64]chan connRequest)}`). Do not call `OpenDirectory` in unit tests because it performs a DNS SRV lookup (`net.LookupSRV`) in `newSRVConnector`.
- Teardown: use `t.Cleanup` for anything that starts goroutines (for example `Directory.stop` is a `context.CancelFunc` set by `OpenDirectory`; call it in cleanup).
- Assertion: compare directly with `if got != want`. Use `reflect.DeepEqual` only for slices and structs.

## Mocking

**Framework:** None. No mocking library is in `go.mod`.

**Current coupling blockers:**
- `connector.connect` (`connector.go`) calls `ldap.DialURL` directly. To test the pool without a server, either:
  1. Introduce a small interface around the dialer, for example `type dialer func(ctx context.Context, addr string, tc *tls.Config) (*ldap.Conn, error)`, stored on `connector`, and default it to `ldap.DialURL`; or
  2. Test only the pool logic (`freeConn`, `connRequests`, `numOpen`) by pre-populating the `Directory` struct and calling the `Locked` helpers directly.
- `newSRVConnector` calls `net.LookupSRV` directly. Tests should construct `connector` values by hand rather than via `OpenDirectory`.
- `conn.internal` is a concrete `*ldap.Conn`. For tests of `validateConnection` or `rebind`, use an LDAP test server (see below) rather than a mock.

**What to Mock:**
- The LDAP dial step, behind a function field or small interface, when testing pool behaviour.
- `Option.BindFunc` is already a function field (`connector.go`). Use it to inject bind success or failure in tests without a real server.

**What NOT to Mock:**
- `srvHeap` (`server.go`). It is a plain `container/heap` implementation and should be tested directly with real `*Server` values.
- `sync.Mutex` and channels in `Directory`. Exercise them with real goroutines under `-race`.

## Fixtures and Factories

**Test Data:**
- Construct `Server` values inline with `net.SRV` for SRV-driven tests:
  ```go
  s := &Server{
      SRV:     &net.SRV{Target: "dc1.example.com", Port: 389, Priority: 0, Weight: 100},
      Service: "ldap",
      latency: 5 * time.Millisecond,
  }
  ```
- Build `connector` with `servers` populated and `heap.Init((*srvHeap)(&c.servers))` before calling `updateLatency` or `connect`, mirroring `newSRVConnector`.

**Location:**
- No fixtures directory exists. If sample LDIF or AD response data becomes necessary, add `testdata/` at the package root (Go tooling ignores this directory for builds).

## Coverage

**Requirements:** None enforced. No coverage threshold, no CI gate, no coverage badge.

**View Coverage:**

```bash
go test -coverprofile=cover.out ./...
go tool cover -func=cover.out
```

**Priority areas for first tests (highest value, lowest setup cost):**
1. `IsBadConn` and `adErr.Unwrap` in `errors.go`: pure functions.
2. `srvHeap` ordering and `Push`/`Pop` index maintenance in `server.go`.
3. `connector.updateLatency` and `decayLatencies` in `connector.go`: latency math and heap fix-up.
4. `adhelper.ResetPassword` in `adhelper/interface.go`: check that the output for `password` is UTF-16LE-encoded. Note the function formats the password with `%q` before encoding, so the encoded value includes surrounding quote characters. A test should pin this behaviour and flag it if it is not intended for AD `unicodePwd`, which normally expects the password wrapped in double quotes and encoded as UTF-16LE.
5. `Directory.maxIdleConnsLocked`, `shortestIdleTimeLocked`, `putConnDirLocked` in `directory.go`: pool bookkeeping with no I/O.
6. `connectionCleanerRunLocked` in `directory_workers.go`: idle and lifetime eviction against controlled `returnedAt` and `createdAt` values.

## Test Types

**Unit Tests:**
- Scope: pure helpers and locked-state helpers listed above. No network.
- Run with `go test -race` because `Directory` and `connector` are shared across goroutines.

**Integration Tests:**
- Not present. For end-to-end behaviour (`Search`, `Modify`, `VerifyPassword`, `StickyConn`) run against a real or containerised directory server (OpenLDAP or Samba AD). Gate these behind a build tag or an environment variable so `go test ./...` stays hermetic. Example: `//go:build integration` at the top of `interface_integration_test.go`, run with `go test -tags integration ./...`.
- Never commit real directory credentials. Read connection settings from environment variables in the integration test and skip when they are unset (`t.Skip`).

**E2E Tests:**
- Not used.

## Common Patterns

**Async Testing:**
Use channels and `context.WithTimeout` instead of `time.Sleep`. The opener and cleaner are background goroutines (`connectionOpener`, `connectionCleaner` in `directory_workers.go`); trigger them through their channels and wait on observable state with a bounded poll:

```go
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
    t.Helper()
    deadline := time.Now().Add(timeout)
    for !cond() {
        if time.Now().After(deadline) {
            t.Fatal("condition not met before timeout")
        }
        time.Sleep(5 * time.Millisecond)
    }
}
```

Read shared state through `dir.Stats()` (`directory.go`), which takes `dir.mu` and returns a consistent snapshot.

**Error Testing:**
Assert on sentinel identity with `errors.Is`, not string matching, because the package returns wrapped sentinels (`adErr` in `errors.go`):

```go
if !errors.Is(err, ErrDirClosed) {
    t.Fatalf("got %v, want ErrDirClosed", err)
}
```

For classification, assert through `IsBadConn(err)` rather than checking `ErrBadConn` directly, since `ldap.ErrorNetwork` is also classified as bad.

**Panic Testing:**
Two panics are part of the contract (`conn.close` on duplicate close and `putConn` on a connection never checked out). Test them with `defer func() { if recover() == nil { t.Fatal("expected panic") } }()`, and assert the panic value equals `ErrDuplicateConnClose` where applicable.

## Gaps To Address

- No tests cover the pool checkout path in `Directory.conn` (`directory.go`), which is the most complex function and holds the most locking logic.
- No tests cover `StickyConn` rebind behaviour (`interface.go`, `conn.rebind` in `conn.go`).
- The `defer dc.releaseConn(err)` pattern in `interface.go` captures `err` at defer time. A regression test that asserts the pool marks a connection bad after a network error from `Modify` would surface this.

---

*Testing analysis: 2026-10-12*
