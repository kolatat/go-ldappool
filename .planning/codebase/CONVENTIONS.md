---
last_mapped_commit: 23ccb27db8c4e78284ada38c52c7e913f5fc89b1
last_mapped_at: 2026-10-12
---
# Coding Conventions

**Analysis Date:** 2026-10-12

## Naming Patterns

**Files:**
- Flat, single-word lowercase file names in the package root: `conn.go`, `connector.go`, `directory.go`, `directory_workers.go`, `errors.go`, `interface.go`, `server.go`.
- Files are grouped by concern, not by type. `directory_workers.go` holds the background goroutines for `Directory` (`connectionOpener`, `connectionCleaner`); `interface.go` holds the public query methods on `*Directory`.
- Sub-package `adhelper/` (`adhelper/interface.go`) contains Active Directory-specific helpers. Keep AD-specific logic there, not in the core package.
- No `_test.go` files exist yet (see `TESTING.md`).

**Functions:**
- Exported API uses PascalCase on `*Directory`: `OpenDirectory`, `Search`, `SearchWithPaging`, `Modify`, `ModifyWithResult`, `Del`, `VerifyPassword`, `StickyConn`, `Stats`, `PrintSRVs`, `IsBadConn`, `ResetPassword`.
- Unexported helpers use camelCase and often pair with an exported wrapper. The exported method takes the retry loop; the lowercase method does one attempt with a `connReuseStrategy`:
  - `Search` -> `search` in `interface.go`
  - `SearchWithPaging` -> `searchWithPaging` in `interface.go`
- Suffix `Locked` marks a function that requires the caller to hold a mutex: `putConnDirLocked`, `maxIdleConnsLocked`, `shortestIdleTimeLocked`, `startCleanerLocked`, `closeDBLocked`, `connectionCleanerRunLocked`. Preserve this suffix when adding helpers that assume a lock is held (`dir.mu` or `c.Lock()`).
- Suffix `WithResult` marks variants that return the raw `*ldap.ModifyResult`: `ModifyWithResult` in `interface.go`.

**Variables:**
- Receiver names are short and fixed per type: `dir` for `*Directory`, `c` for `*connector` and `*conn` (`directory.go`, `connector.go`, `conn.go`), `pq` for `srvHeap` (`server.go`), `err` for `adErr`.
- Local `dc` is used for a `*conn` checked out of the pool inside `interface.go`. `c` is used for the same type in `directory.go`. Both forms appear; follow the file you are editing.
- Lock-guard comments are placed on struct fields: `sync.Mutex // guards following` in `conn` (`conn.go`), `// guarded by dir.mu` above `inUse`/`returnedAt`.

**Types:**
- Exported structs: `Directory`, `Server`, `Option`. Unexported: `conn`, `connector`, `connRequest`, `srvHeap`, `connReuseStrategy`, `adErr`.
- Enum-like constants use `iota` with a typed alias: `type connReuseStrategy uint8` with `alwaysNewConn` and `cachedOrNewConn` in `directory.go`.
- Type alias to a stdlib type for re-export: `type DBStats = sql.DBStats` in `directory.go`.

**Constants and package vars:**
- Unexported `const` blocks for tunables: `minValidationDuration` (`conn.go`), `badConnPenalty`, `decayRate` (`connector.go`), `maxBadConnRetries` (`directory.go`).
- Sentinel errors are package-level `var` with `Err` prefix: `ErrNoServerDefined`, `ErrDirClosed`, `ErrDuplicateConnClose`, `ErrConnExpired`, `ErrBadConn` (`errors.go`).
- Error message strings are prefixed `ad: ` (for example `"ad: directory closed"`). Keep this prefix for any new sentinel error.

## Code Style

**Formatting:**
- Standard `gofmt` layout (tabs for indentation, grouped struct field alignment). No `.editorconfig`, `.prettierrc`, or other formatter config is present in the repo.
- Keep struct fields aligned as `gofmt` does; the existing structs in `directory.go` and `conn.go` rely on column alignment.
- Keep struct-field and inline comments on the same line when they describe a single field (for example `badConnPenalty = 2 * time.Second // penalty added to server latency when they go bad`).

**Linting:**
- No linter configuration detected (no `.golangci.yml`, no `staticcheck` or `revive` config, no CI workflow under `.github/`).
- Run `go vet ./...` and `gofmt -l .` before committing. These are the only enforceable checks given the current repo.

**Go version:**
- Declared in `go.mod` as `go 1.19`. Use only features available in 1.19. The codebase uses `atomic.Int64` (`directory.go`) and the `any` alias, both 1.19-compatible.

## Import Organization

**Order:**
1. Standard library imports, alphabetically sorted (for example `context`, `net/url`, `sync`, `time` in `conn.go`).
2. A blank line, then third-party imports (`github.com/go-ldap/ldap/v3`, `golang.org/x/text/encoding/unicode`).

**Path Aliases:**
- None. Imports are always fully qualified. The only aliasing pattern is the package-level import `github.com/go-ldap/ldap/v3` used unaliased as `ldap`.

**Internal import:**
- The root package is `ldappool` (`package ldappool`); the module path is `github.com/kolatat/go-ldappool` (`go.mod`). The sub-package `adhelper` is `package adhelper`.

## Error Handling

**Patterns:**
- Return errors up the stack; do not log and continue. Errors are returned as the second value or as the single return.
- Wrap sentinel errors with custom type `adErr` (`errors.go`) when a classification is needed. `adErr` implements `Unwrap()` so `errors.Is` works. Example: `ErrConnExpired = adErr{ErrBadConn, "ad: connection expired"}`.
- Classify retryable failures with `IsBadConn(err)` (`errors.go`). It returns true for `ldap.ErrorNetwork` and for anything wrapping `ErrBadConn`. Use it instead of comparing error values directly.
- Retry logic lives in `Directory.retry` (`directory.go`): up to `maxBadConnRetries` attempts with `cachedOrNewConn`, then a final attempt with `alwaysNewConn`. New operations that touch a connection should be wrapped in `dir.retry(func(strategy connReuseStrategy) error { ... })` as in `interface.go`.
- Connection-state failures are reported to the pool through `releaseConn(err)` / `putConn(c, err)`, which closes the connection when `IsBadConn(err)` is true.
- `panic` is reserved for invariant violations only: duplicate close (`conn.close` in `conn.go`) and returning a connection that was never checked out (`putConn` in `directory.go`). Do not add `panic` for ordinary runtime errors.
- Some errors are intentionally ignored: `c.internal.Close()` in `conn.close` and `c.dir.connector.updateLatency(...)` path in `validateConnection`.

**Error-value timing gotcha:**
- Pattern in `interface.go`: `defer dc.releaseConn(err)` captures the value of `err` at the time the `defer` statement runs, not when the function returns. When the deferred release needs the final error, assign to a named return or use a closure: `defer func() { dc.releaseConn(err) }()`. The current code in `interface.go` (for example `Modify`, `Del`) reads `err` at defer time.

**Return style:**
- Methods that need the result and error from a retry closure assign to a named return captured by the closure (see `Search` and `SearchWithPaging` in `interface.go`).

## Logging

**Framework:** `fmt` to stdout. No structured logger or `log` package is used.

**Patterns:**
- `fmt.Println(err)` on bad-connection events in `connector.go` (`checkBadConn` closure in `connect`).
- `fmt.Println` for diagnostics in `PrintSRVs` (`directory.go`).
- New code should prefer returning errors over printing. If diagnostics are needed, follow the existing `fmt.Println` precedent rather than introducing a logger dependency, unless the phase explicitly calls for one.

## Comments

**When to Comment:**
- Inline comments explain non-obvious intent or external references, for example the links to the Microsoft `ms-adts` specification and the gist in `adhelper/interface.go` above `ResetPassword`.
- Lock-guard annotations are required on fields protected by a mutex (`// guards following`, `// guarded by dir.mu`).
- Field-level comments on configuration knobs explain semantics (`LDAPsPort` in `connector.go` `Option`).

**Doc comments:**
- Exported identifiers do not carry Go doc comments (`// Name ...` form). No `// ` doc comments precede exported functions or types in the codebase. When adding exported API, write a `// FuncName ...` doc comment; this is a deviation from current code but is the Go convention and is needed for `pkg.go.dev`.

**TODO markers:**
- One TODO exists: `// TODO might not work, sql sets err=ErrBadConn` in `putConn` (`directory.go`). Treat it as an open question, not a convention.

## Function Design

**Size:**
- Most functions are short (under 40 lines). The largest is `conn` in `directory.go` (about 100 lines) which handles pool checkout, wait queue, and dialing. Prefer splitting new pool logic into helpers rather than extending it.

**Parameters:**
- `context.Context` is the first parameter of any function that may block or dial: `connect(ctx)`, `conn(ctx, strategy)`, `Search(ctx, ...)`.
- Functional options are not used. Configuration is a plain `Option` struct passed as a variadic `opt ...*Option` to `OpenDirectory` and `newSRVConnector` (`directory.go`, `connector.go`). Only the first element is read.

**Return Values:**
- Pairs of `(result, error)` in the Go idiom. Helpers that cannot fail return only the value (`expired`, `maxIdleConnsLocked`).
- A helper that returns both a computed duration and a slice uses a named-less tuple: `connectionCleanerRunLocked(d) (time.Duration, []*conn)` in `directory_workers.go`.

**Closure-based retry:**
- Pattern for every pool-backed operation (`interface.go`):
  ```go
  func (dir *Directory) Del(ctx context.Context, delRequest *ldap.DelRequest) error {
      return dir.retry(func(strategy connReuseStrategy) error {
          dc, err := dir.conn(ctx, strategy)
          if err != nil {
              return err
          }
          defer dc.releaseConn(err)
          dc.Lock()
          err = dc.internal.Del(delRequest)
          dc.Unlock()
          return err
      })
  }
  ```

## Module Design

**Exports:**
- The public surface is `*Directory` methods, `Option`, `Server`, `DBStats`, and `IsBadConn`. Everything else is unexported. Keep new internals unexported unless callers need them.
- `Server` embeds `*net.SRV` (`server.go`) and exposes `Service`. `latency` and `index` are unexported heap bookkeeping fields.

**Concurrency rules:**
- Lock ordering: `connector.mu` and `Directory.mu` are separate. `conn.Mutex` guards the `ldap.Conn`. `conn.close()` takes `c.Lock()` then `c.dir.mu.Lock()`. Preserve that order; do not acquire `dir.mu` then `c.Lock()`.
- The `connector` is shared across goroutines. Access `c.servers` only under `c.mu`.
- Use `sync/atomic` for counters read without the directory lock (`waitDuration atomic.Int64`).

**Barrel Files:**
- None. There is no re-export file. Callers import `github.com/kolatat/go-ldappool` directly.

**Package-level state:**
- `connectionRequestQueueSize` is a package `var` (`directory.go`) used as the buffer size for the opener channel. Treat it as tuning, not as a per-directory setting.

---

*Convention analysis: 2026-10-12*
