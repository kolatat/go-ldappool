---
last_mapped_commit: 7f4e3b30fdd4733449a679ffc3ab431c184a6a36
last_mapped_at: 2026-10-12
---
# Codebase Concerns

**Analysis Date:** 2026-10-12

## Tech Debt

**Connection return path ignores the operation error (`defer` argument evaluated early):**
- Issue: Every operation calls `defer dc.releaseConn(err)` before the operation assigns `err`. Go evaluates deferred call arguments at the `defer` statement, so `releaseConn` always receives `nil`. `putConn` therefore never sees `IsBadConn` errors and never discards dead sockets; it returns them to the free list.
- Files: `interface.go` (lines 19, 39, 59, 72, 86, 100, 114), `directory.go` (`putConn`, lines 406-440)
- Impact: A dead connection is re-used by the next caller or by the retry loop (`dir.retry`, which tries `cachedOrNewConn` up to twice before `alwaysNewConn`). The retry may pick the same dead connection again. Network failures surface as user errors instead of being healed.
- Fix approach: Capture the error explicitly, e.g. `defer func() { dc.releaseConn(err) }()` with `err` as a named return value, or call `dc.releaseConn(err)` directly after the operation.

**Expiry check on return can never fire:**
- Issue: `putConn` tests `if !IsBadConn(ErrBadConn) && c.expired(dir.maxLifetime)`. `IsBadConn(ErrBadConn)` is always `true`, so the condition is always false.
- Files: `directory.go` (line 420)
- Impact: `maxLifetime` is only enforced by the background cleaner and when a connection is taken from the free list (`dir.conn`). `maxLifetimeClosed` stats under-count, and connections in use past their lifetime are re-pooled.
- Fix approach: Replace the condition with `if dir.maxLifetime > 0 && c.expired(dir.maxLifetime)`.

**`SetMaxIdleConns(0)` writes the wrong field:**
- Issue: The `else` branch sets `dir.maxIdleClosed = -1` where the intent is `dir.maxIdleCount = -1` (the database/sql convention for "no idle connections").
- Files: `directory.go` (lines 97-103, `maxIdleConnsLocked` lines 230-240)
- Impact: Passing 0 leaves `maxIdleCount` at 0, so the default of 2 idle connections applies, and the caller cannot disable idle pooling. `MaxIdleClosed` in `Stats()` is corrupted (starts at -1).
- Fix approach: Set `dir.maxIdleCount = -1` in the `else` branch and leave `maxIdleClosed` alone.

**Errors from rebind are silently dropped on reuse:**
- Issue: In `dir.conn`, only `IsBadConn` errors from `c.rebind(ctx)` are handled. Other errors (for example, the service-account `BindFunc` failing with invalid credentials) are ignored and the connection is handed to the caller.
- Files: `directory.go` (lines 331-336 and 381-386), `conn.go` (`rebind`, lines 87-94)
- Impact: A caller can run operations on a connection that is bound as the wrong identity, or not bound at all. The failure is invisible.
- Fix approach: Return any non-nil rebind error (close the connection first when it is a bad conn).

**`openNewConnection` double-decrements and self-deadlocks:**
- Issue: When `putConnDirLocked` returns false, the code does `dir.numOpen--` followed by `c.close()`. `conn.close()` also does `c.dir.numOpen--`, and it locks `c.dir.mu`, which `openNewConnection` already holds (via `defer dir.mu.Unlock()`).
- Files: `directory.go` (lines 151-175), `conn.go` (`close`, lines 51-64)
- Impact: Deadlock of the opener goroutine and every caller waiting on `dir.mu`. This is reached when a freshly opened connection cannot be added because the idle pool filled between request queueing and connection completion. Also, `numOpen` is decremented twice.
- Fix approach: Close the connection without re-locking (for example, a `closeLocked` variant), and decrement `numOpen` only once.

**Closed-directory path closes a connection whose `dir` field is nil:**
- Issue: In `openNewConnection`, when `dir.closed` is true, `c.close()` is called. `connector.connect` never sets `c.dir`, so `close()` dereferences a nil pointer at `c.dir.mu.Lock()`.
- Files: `directory.go` (lines 155-158), `connector.go` (`connect`, lines 180-187)
- Impact: Panic. Reachable only if `closed` is ever set, which currently never happens (see the next item), so the risk is latent.
- Fix approach: Set `c.dir = dir` before the closed check, or close the raw `ldap.Conn` directly.

**No `Close` API; shutdown machinery is unfinished:**
- Issue: `Directory` stores `stop: cancel` (`directory.go` line 61), but `stop` is never called, `dir.closed` is never set to `true`, and no exported `Close` method exists. `ErrDirClosed` and the `closed` checks are unreachable.
- Files: `directory.go` (lines 42, 61, 155-306), `directory_workers.go` (line 43), `errors.go` (`ErrDirClosed`)
- Impact: The connection-opener goroutine and any cleaner goroutine leak for the process lifetime. Idle sockets are never closed on shutdown. Callers cannot release resources.
- Fix approach: Add `func (dir *Directory) Close() error` that sets `closed`, calls `stop()`, drains and closes `freeConn`, and closes `openerCh` or signals waiters.

**Unused code left over from `database/sql`:**
- Issue: `closeDBLocked` (`conn.go` lines 41-49) is never called. The README still contains `// TODO` placeholders for the Connecting and Searching examples. `connRequest` comments and `numClosed` comment are stale.
- Files: `conn.go` (line 41), `directory.go` (line 22), `README.md` (lines 17-25)
- Impact: Dead code and documentation gaps raise maintenance cost and mislead readers.
- Fix approach: Delete `closeDBLocked`, fill in the README examples, and remove stale comments.

**Incorrect `ModifyWithResult` return value evaluation:**
- Issue: `return res, dir.retry(func ...)` reads `res` before the closure that assigns it runs. Go leaves evaluation order between a variable read and a function call in one return statement unspecified.
- Files: `interface.go` (lines 80-92)
- Impact: The returned `*ldap.ModifyResult` may be nil even when the server returned a result. Behavior depends on compiler details.
- Fix approach: Assign `err := dir.retry(...)` first, then `return res, err`.

**Stale or misleading comments and constants:**
- `directory.go` line 46: `connectionRequestQueueSize = 10000 // // 1Mil` has a double `//` and a wrong size comment. It is a `var` where a `const` fits.
- `directory.go` line 22 `// numClosed` and `conn.go` line 29 comment are stale.
- `conn.go` `validateConnection` return is discarded for non-bad errors, with a `// TODO might not work` comment at `directory.go` line 410.

## Known Bugs

**Passwords containing `"` or `\` are encoded incorrectly:**
- Symptoms: After `ResetPassword`, the directory password does not match the one supplied, or Active Directory rejects it under complexity rules.
- Files: `adhelper/interface.go` (line 17)
- Trigger: `fmt.Sprintf("%q", password)` applies Go string escaping. `"` becomes `\"` and `\` becomes `\\`. The quotes it adds are also Go-escaped rather than being the literal AD framing quotes. Control characters and some Unicode are escaped as well.
- Workaround: None in the package. Callers must avoid `"` and `\` in passwords.
- Fix approach: Build the value as `"\"" + password + "\""` and then UTF-16LE-encode it. Add unit tests that compare the encoded bytes against known vectors.

**`IsBadConn` may not classify raw network errors:**
- Symptoms: Unreachable or reset servers are not penalized, and dead connections are not discarded.
- Files: `errors.go` (lines 30-41), `connector.go` (`connect` checkBadConn, lines 140-145), `directory.go` (`putConn`)
- Trigger: `IsBadConn` only matches `ldap.ErrorNetwork` codes and its own sentinel. Errors returned directly by `net.Dial` or the TLS layer (for example `*net.OpError`, `io.EOF`) may not be wrapped as `ErrorNetwork`. Combined with the nil-error release bug above, this can leave a failing server at the top of the latency heap.
- Workaround: None.
- Fix approach: Also treat `net.Error`, `io.EOF`, `io.ErrUnexpectedEOF`, and `syscall.ECONNRESET` as bad connections, and add tests with a fake server that closes sockets.

**`SetMaxOpenConns` passes the wrong argument to `SetMaxIdleConns`:**
- Files: `directory.go` (lines 85-96)
- Trigger: When idle exceeds the new open limit, the code calls `dir.SetMaxIdleConns(n)` with the requested value instead of `dir.maxOpen`. The two are equal only when `n >= 0` and is the same value, but the clamp is also applied permanently to `maxIdleCount` (see `SetMaxIdleConns` lines 104-106), so raising the open limit later does not restore the idle limit.

**`StartTLS` and bind failures leak sockets:**
- Symptoms: File descriptors accumulate when the server rejects STARTTLS or the bind step.
- Files: `connector.go` (`connect`, lines 165-178)
- Trigger: The `ldap.Conn` created by `DialURL` is not closed when `StartTLS` or `BindFunc` returns an error. The function returns `nil, err` with `internal` still open.
- Workaround: None.
- Fix approach: Call `internal.Close()` on each error path after dialing.

**`ModifyWithResult` may return nil results:** see Tech Debt above (evaluation order in `return res, ...`).

## Security Considerations

**Plaintext LDAP is the default transport:**
- Risk: Bind credentials (`VerifyPassword`, `BindFunc`) and `unicodePwd` writes can be sent unencrypted on port 389.
- Files: `connector.go` (`newSRVConnector` lines 84-99; `connect` lines 152-170), `interface.go` (`VerifyPassword`, lines 108-121), `adhelper/interface.go`
- Current mitigation: `Option.StartTLS` defaults to `false`. `LDAPsPort` defaults to `0`. LDAP SRV records are used for any target that has no LDAPS record, so plaintext servers are used automatically.
- Recommendations: Default to `StartTLS` or LDAPS. Refuse to send bind credentials or `unicodePwd` over a connection that is not TLS-protected, or at least document it prominently in the README. Document that `unicodePwd` requires an encrypted connection.

**Credentials can leak to later operations on pooled connections:**
- Risk: `VerifyPassword` binds the pooled connection as the end user and sets `needRebind`. If `Option.BindFunc` is nil, `rebind` is a no-op, so subsequent `Search` or `Modify` calls on the same connection run as that user.
- Files: `interface.go` (lines 108-121), `conn.go` (`rebind`, lines 87-94)
- Current mitigation: A service `BindFunc` restores the identity before reuse.
- Recommendations: Always bind to a known identity after `VerifyPassword`, or close the connection after use. Use a separate pool for user-credential checks.

**`needRebind` is never cleared:**
- Risk: Every reuse of a connection that once went through `StickyConn` or `VerifyPassword` triggers an additional bind round-trip, and the rebind error is dropped (see Tech Debt).
- Files: `conn.go` (line 25), `interface.go` (lines 22, 117)
- Recommendations: Reset `needRebind = false` after a successful rebind, and surface rebind errors.

**Sensitive data written to stdout by the library:**
- Risk: Errors are printed unconditionally, which can expose hostnames, error text, and application data in logs.
- Files: `connector.go` (line 142, `fmt.Println(err)`), `directory.go` (`PrintSRVs`, lines 448-458)
- Recommendations: Replace with a pluggable logger or remove the prints.

**Unbounded DNS-discovered targets trusted for TLS:**
- Risk: The TLS `ServerName` is taken from SRV targets, and no check restricts targets to the expected domain.
- Files: `connector.go` (lines 147, 72, 89)
- Recommendations: Validate that SRV targets are inside `domain`, or document that SRV trust is inherited from DNS.

## Performance Bottlenecks

**Global mutex on every operation:**
- Problem: `connector.mu` is taken by `getNextId`, `connect`, `updateLatency`, `decayLatencies`, and `PrintSRVs`. `decayLatencies` runs on every `putConn`, so every operation serializes on the connector lock and on `dir.mu`.
- Files: `connector.go` (lines 105-111, 113-188, 190-200, 202-226), `directory.go` (`putConn`, line 413)
- Cause: Latency bookkeeping is performed synchronously on the hot path.
- Improvement path: Use atomics for latency, decay on a timer, and separate `connector` locking from `dir.mu`.

**Synchronous WhoAmI validation on return:**
- Problem: Each connection returned after 14 seconds (`minValidationDuration`) makes a `WhoAmI` network round-trip before going back to the pool, in the caller's request path.
- Files: `conn.go` (`validateConnection`, lines 66-81), `directory.go` (`putConn`, lines 406-412)
- Cause: Health check runs inline and holds the connection lock.
- Improvement path: Validate in the background, or validate only when a connection is taken from the pool after an idle period.

**Latency-only server selection with no real load balancing:**
- Problem: `connect` always uses `c.servers[0]`, the lowest-latency server. Shuffling happens only when latency decays to exactly 0, and `rand.Shuffle` covers only the leading zero-latency prefix of the heap, which is not necessarily contiguous.
- Files: `connector.go` (lines 113-129)
- Cause: The heap design sends nearly all new connections to one domain controller until its latency rises.
- Improvement path: Use weighted random selection among healthy servers, and shuffle the full set of equal-latency servers.

**Small default idle pool:**
- Problem: `defaultMaxIdleConns = 2` causes frequent connection churn under concurrent load.
- Files: `directory.go` (lines 228-240)
- Improvement path: Document the default and tune it for typical concurrency, or derive it from `MaxOpenConns`.

**Blocking send under lock:**
- Problem: `maybeOpenNewConnections` sends to `openerCh` while holding `dir.mu`. If the buffered channel is full, the goroutine blocks while holding the lock that `openNewConnection` needs.
- Files: `directory.go` (lines 210-226), `directory_workers.go` (`connectionOpener`, lines 8-17)
- Cause: The channel capacity (`connectionRequestQueueSize`, 10000) is assumed to never fill.
- Improvement path: Send without holding the lock, or use a non-blocking send that spawns the open directly.

**No connect timeout by default:**
- Problem: `connect` calls `ldap.DialURL` with `Option.Dialer` possibly nil. The context is checked only before dialing, so a blackholed server can block a caller for the OS TCP timeout.
- Files: `connector.go` (lines 134-160)
- Improvement path: Default `Dialer` to a `net.Dialer` with a timeout, and pass `ctx` through to the dial (`net.Dialer.DialContext`).

## Fragile Areas

**`Directory` connection accounting (`numOpen`, `freeConn`, `connRequests`):**
- Files: `directory.go`, `directory_workers.go`, `conn.go`
- Why fragile: `numOpen` is changed in at least six places, including `openNewConnection`, `close`, and `conn`, with inconsistent locking. Recent bugs (double decrement, nil `dir`, wrong `closed` checks) came from this. Each change risks a leak or a deadlock.
- Safe modification: Route every open and close through one helper that updates `numOpen` under `dir.mu` and never takes `dir.mu` from inside another `dir.mu` critical section.
- Test coverage: None. No `*_test.go` files exist.

**`defer` with arguments that are evaluated early:**
- Files: `interface.go` (all operations)
- Why fragile: The pattern looks correct but silently breaks error handling. Any new operation copied from these examples inherits the bug.
- Safe modification: Use `defer func() { ... }()` closures that read the final error, and add a regression test.

**Lock ordering between `conn.Lock` and `dir.mu`:**
- Files: `conn.go` (`close`, `validateConnection`), `directory.go` (`openNewConnection`, `putConn`)
- Why fragile: `close` takes `conn` then `dir.mu`, while other paths take `dir.mu` then call `close`. Only the ordering used in `putConn` avoids deadlock.
- Safe modification: Document the lock hierarchy. Never call `close()` while holding `dir.mu`.

**Concurrent use of the shared `utf16le` encoder:**
- Files: `adhelper/interface.go` (line 11)
- Why fragile: `x/text` transformers carry internal state and are not documented as goroutine-safe. The encoder is package-level and shared across all callers.
- Safe modification: Create a new encoder per call, or guard it with a mutex.

## Scaling Limits

**Connection pool bounds:**
- Current capacity: `MaxOpenConns` and `MaxIdleConns` are unset by default. The idle default is 2. The opener channel holds 10000 pending requests.
- Limit: With `MaxOpenConns` unset, the pool grows without bound. Each LDAP connection holds a server-side slot, and AD limits concurrent connections per client.
- Scaling path: Make `MaxOpenConns` a required or recommended option, and document AD connection limits.

**Single-server concentration:**
- Current capacity: All new connections go to the lowest-latency server (see Performance).
- Limit: One domain controller absorbs most of the load until its latency rises.
- Scaling path: Spread connections across servers using weighted selection.

## Dependencies at Risk

**Old pinned dependencies:**
- Risk: `go.mod` declares `go 1.19` and pins `github.com/go-ldap/ldap/v3 v3.4.6`, `golang.org/x/text v0.14.0`, and indirect `golang.org/x/crypto v0.18.0`. These are from around 2023-2024. Go 1.19 is no longer supported by the Go toolchain.
- Impact: Missing security fixes and newer `x/text` behavior. Build toolchains may warn or refuse the old directive.
- Migration plan: Bump the `go` directive to a supported release, run `go get -u` for each dependency, and run `go mod tidy`. Verify the go-ldap changelog for connection-handling changes.

## Missing Critical Features

**No automated tests:**
- Problem: The repository has no `*_test.go` files, despite the README listing "tests" as a TODO.
- Blocks: Safe refactoring of the connection pool, and verification of the bugs listed above. Tests with a fake LDAP server (or an in-process `ldap` test double) are needed to cover the pool and the error paths.

**No `Close` on `Directory`:** see Tech Debt. Shutdown and resource release are not possible through the public API.

**No Active Directory password-policy handling:** `adhelper.ResetPassword` does not check LDAPS usage, password length, or AD complexity, and returns only encoding errors.

## Test Coverage Gaps

**Connection pool lifecycle:**
- What's not tested: Max open and idle limits, `maxLifetime` and `maxIdleTime` cleanup, `putConn` with error values, waiting on `connRequests`, and the opener deadlock path.
- Files: `directory.go`, `directory_workers.go`, `conn.go`
- Risk: The release-on-error and deadlock bugs above go unnoticed and can hang or leak in production.
- Priority: High

**Password encoding:**
- What's not tested: `adhelper.ResetPassword` output for passwords containing `"`, `\`, non-ASCII, and empty strings.
- Files: `adhelper/interface.go`
- Risk: Silent password mismatch or policy failures in the field.
- Priority: High

**Error classification:**
- What's not tested: `IsBadConn` with `net` errors, EOF, and LDAP result codes.
- Files: `errors.go`
- Risk: Dead servers stay in rotation and dead connections are reused.
- Priority: High

**Concurrency:**
- What's not tested: Race conditions between `decayLatencies`, `updateLatency`, and `connect` under `go test -race`.
- Files: `connector.go`, `directory.go`
- Risk: Data races on server latency and heap order.
- Priority: Medium

---

## database/sql Drift and LDAP Pooling Risks

**Analysis Date:** 2026-10-12

**Scope and method:** The package is a port of the `database/sql` `DB` pool (README links the `go1.19.2` `sql.go`). This section was produced from the repository files only. The Go toolchain on the analysis machine is go1.27.2. `github.com/go-ldap/ldap` v3.4.6, the version pinned in `go.mod`, is not in the local module cache (v3.4.11 through v3.4.15 are). No upstream source was diffed and no network lookups were made. Items marked **Unverified** need a diff against current `database/sql/sql.go` and go-ldap source before anyone acts on them. Items marked **Verified** were confirmed by reading this repository.

### A. Upstream behaviours likely missing from the copy

**Verified (structural gaps in this repo):**
- **No session-reset hook before reuse.** `database/sql` offers optional driver interfaces (`driver.SessionResetter`, `driver.Validator`) that the pool calls before reuse or on return. This package has no equivalent (the interfaces are present upstream; confirm against current source). The reuse path in `dir.conn` (`directory.go`, the `cachedOrNewConn` branch and the `connRequests` wake-up branch) runs only `c.rebind(ctx)`, which is a no-op unless `needRebind` is set and `Option.BindFunc` is non-nil. Nothing resets go-ldap session state. The closest analogue is `validateConnection` (`conn.go`), which issues `WhoAmI` and resets nothing.
- **Bad-connection signal is an LDAP-specific classifier.** `IsBadConn` (`errors.go`) matches only `ldap.ErrorNetwork` and the package's own `ErrBadConn`. `database/sql` uses `driver.ErrBadConn` as the contract between driver and pool. Porting upstream retry logic requires that the LDAP layer produce the bad-conn signal reliably, which it does not (see section F).
- **Context is not passed into the LDAP wire calls.** Upstream passes `ctx` into every driver call that supports it. Here `ctx` is checked only before waiting for a pooled connection (`dir.conn`, `select` on `ctx.Done()`) and before dialing (`connector.connect`). `Search`, `SearchWithPaging`, `Modify`, `ModifyWithResult`, `Del`, and the bind inside `VerifyPassword` take no context. The README (`README.md`, Features) already records this against go-ldap issue #326.

**Unverified (upstream changes after go1.19.2 that may be missing):**
- Upstream may have changed how the final operation error reaches `putConn`/`releaseConn`, or how `Rows`/`Tx` release connections. The local equivalent is the pattern in section B.
- Upstream may have changed the `connRequests` structure to an ordered set for waiter fairness and cancellation. Here `dir.connRequests` is a `map[uint64]chan connRequest` (`directory.go`), and `putConnDirLocked` picks an arbitrary entry with `for ... range` followed by `break`, so waiters are served in map-iteration order rather than arrival order. Confirm the current upstream structure and intent before porting.
- Upstream may have tightened the retry rules around `driver.ErrBadConn` (see section C).

### B. Error-release pattern diverges from `database/sql` and is copied in several places

**Verified:** every operation in `interface.go` uses `defer dc.releaseConn(err)`, and so do `search` and `searchWithPaging`. Go evaluates the arguments of a deferred call when the `defer` statement runs, so `err` is `nil` at release time. This is the root cause of the first Tech Debt item, and it matters for porting:

- `StickyConn` (`interface.go`, line 19) has the same defect. Its `defer dc.releaseConn(err)` captures `err` after `dir.conn` has returned nil, so a `fn` that returns an `IsBadConn` error is released as healthy. `StickyConn` is not retried after `fn` runs, because `retry` wraps only connection acquisition.
- `Modify`, `ModifyWithResult`, `Del`, and `VerifyPassword` declare `err` inside the retry closure and defer with the value that is nil at defer time.

Upstream's pattern is to release through a closure that reads the final error (for example `defer func() { dc.releaseConn(err) }()` with a named result). **Unverified** that this matches the current upstream form exactly; check `sql.go` before copying.

Porting risk: any new method copied from `interface.go` inherits the defect. The fix should be one helper, for example `func (dir *Directory) withConn(ctx, strategy, fn func(*conn) error) error`, that owns acquire, lock, call, and release in one place.

### C. LDAP write retries can re-apply a change

**Files:** `interface.go` (`Modify`, `ModifyWithResult`, `Del`, `VerifyPassword`), `directory.go` (`retry`, `maxBadConnRetries = 2`), `errors.go` (`IsBadConn`).

- **Verified:** `dir.retry` re-runs the whole closure on any `IsBadConn` error: up to two times with `cachedOrNewConn`, then once with `alwaysNewConn`. For `Modify`, `ModifyWithResult`, and `Del`, the closure performs the write. A network error after the request has been sent (for example a reset or read failure while waiting for the response) can therefore lead to a second attempt, and the server may apply the change twice.
- `Del` is the clearest case: a second attempt after a server-side success returns "no such object" and the caller sees a false failure. Non-idempotent modifications are worse.
- **Unverified:** the `database/sql` contract for `driver.ErrBadConn` (whether it guarantees the statement was not sent) and whether go-ldap reports write-side and read-side failures with the same `ErrorNetwork` code. If it does not, the retry decision cannot be made safely in this package.
- Recommendation: retry only before the request is written, or retry only reads. Keep retries on `Search` and `SearchWithPaging`; remove retries from write methods or make them an explicit opt-in.

### D. Connection state leakage between pool users

**Files:** `interface.go` (`StickyConn`, `VerifyPassword`), `conn.go` (`rebind`, `needRebind`), `connector.go` (`connect`, `BindFunc` on dial).

- **Verified:** `needRebind` is set to `true` by `StickyConn` (before `fn`) and by `VerifyPassword` (after the bind, regardless of its result). Nothing in the package sets it back to `false`, so every later use of that connection pays for a `BindFunc` round-trip. This is a performance cost; the more serious consequence is identity.
- **Verified:** if `Option.BindFunc` is nil, `rebind` is a no-op. The next borrower then runs its operations as whichever end user was last bound by `VerifyPassword` or `StickyConn`.
- **Verified:** `VerifyPassword` sets `needRebind` even when `Bind` fails. A failed bind leaves the connection in an unknown bind state. Because the defer bug passes `nil` to `putConn`, that connection is pooled, not discarded.
- **Verified:** `connect` runs `BindFunc` only on freshly dialed connections (`connector.go`). Connections reused from `freeConn` only get `rebind`, so the identity of a pooled connection depends on `needRebind` being set correctly.
- **Verified:** the package sets no server controls and changes no connection options after dial. The only per-connection state it creates is the bind identity and the TLS state. But `StickyConn` hands the raw `*ldap.Conn` to `fn`, so any change `fn` makes (a control, a timeout, a SASL bind) persists for the next borrower, because nothing resets it.
- Recommendations:
  - Treat `StickyConn` as a reason to close the connection on exit rather than pool it, unless `BindFunc` always restores identity.
  - Add a session-reset step that runs in `rebind`, clears `needRebind` only after a successful bind, and returns the error (see Tech Debt, "Errors from rebind are silently dropped").
  - Use a separate pool for end-user credential checks, as already noted under Security Considerations.

### E. Health checking of idle connections and server-side idle timeouts

**Files:** `directory.go` (`conn`, `putConn`), `conn.go` (`validateConnection`), `directory_workers.go` (`connectionCleaner`).

- **Verified:** an idle connection in `freeConn` is handed out with no liveness check. `dir.conn` runs only `rebind`. The only validation is `validateConnection`, which runs at return time.
- **Verified:** `validateConnection` issues `WhoAmI` only when `lastValidated` is older than `minValidationDuration` (14 s). A newly dialed connection has a zero `lastValidated`, so its first return always validates. The call is synchronous in the caller's path and holds `conn.Mutex`.
- **Verified:** `putConn` calls `validateConnection` only when `!IsBadConn(err)`. `err` is always `nil` there because of the defer bug, so validation runs on every release. A failed validation is assigned to `err`, but `IsBadConn(err)` is then checked, and a `WhoAmI` failure that is not `ErrorNetwork` is pooled anyway. The `// TODO might not work` comment at `directory.go` line 410 records this.
- **Verified:** the cleaner (`connectionCleanerRunLocked`) closes idle connections by `maxIdleTime` and `maxLifetime`, but only for connections in `freeConn`. Both timers default to 0 (disabled). `conn()` checks `maxLifetime` on reuse but not `maxIdleTime`, so idle expiry depends on the cleaner waking up, which has a 1-second floor.
- **Unverified (depends on deployment):** the idle disconnect limits of the domain controllers in use. This package cannot read them. Without a `maxIdleTime` below the server limit, the first operation after a quiet period can fail with a network error. That failure is then retried, which is costly for writes (section C).
- Recommendations: set `ConnMaxIdleTime` below the server idle limit once it is known. Consider a cheap liveness probe (`WhoAmI`, or a RootDSE read) for connections idle longer than a threshold, run before the connection is handed out.

### F. Error classification for retry versus discard

**Files:** `errors.go` (`IsBadConn`), `connector.go` (`checkBadConn` in `connect`), `directory.go` (`retry`, `putConn`).

- **Verified:** `IsBadConn` returns true only for `ldap.ErrorNetwork` and `ErrBadConn`.
- **Unverified:** go-ldap may report some connection-level failures under other result codes. Candidates that could leave a connection unusable include the unsolicited Notice of Disconnection (OID `1.3.6.1.4.1.1466.20036`), `unavailable` (52), `busy` (51), `protocolError` (2), and `unwillingToPerform` (53) during server shutdown. Whether go-ldap surfaces the Notice of Disconnection to callers, and under which code, is not determined from this repo.
- **Verified:** `connector.checkBadConn` penalizes a server only for `IsBadConn` errors. Dial errors returned directly by `net` (`*net.OpError`), TLS handshake errors, and `io.EOF` may not be wrapped as `ErrorNetwork`, in which case a dead server keeps its low latency and stays at the top of the heap. **Unverified:** what go-ldap v3.4.6 wraps in `ErrorNetwork`.
- **Verified:** `putConn` discards a connection only on `IsBadConn`. That shape is correct, but because of section B the discard path is effectively never reached from operations.
- Recommendation: define the classification in one function, list the LDAP result codes that mean "discard connection", and test it against a fake server (see Test Coverage Gaps, "Error classification").

### G. Context cancellation on non-context LDAP calls

**Files:** `interface.go` (all operations), `connector.go` (`connect`, `ldap.DialURL`), `conn.go` (`validateConnection`).

- **Verified:** the go-ldap calls this package makes (`Search`, `SearchWithPaging`, `Modify`, `ModifyWithResult`, `Del`, `Bind`, `WhoAmI`, `StartTLS`, `DialURL`) receive no context here. Once an operation starts, `ctx` cannot interrupt it. A caller whose context is cancelled during `Search` still waits for the response or a socket failure.
- **Verified:** the package sets no per-operation deadline. An operation that never receives a response holds `dc.Lock` (in `search`, `searchWithPaging`, and each write method) indefinitely, and every other goroutine waiting on that connection queues behind it. `Option.Dialer` can carry a timeout for dialing only. **Unverified:** whether go-ldap v3.4.6 applies a default read timeout.
- **Verified:** `ctx` is checked before `DialURL` in `connect`, but `DialURL` itself does not observe `ctx`.
- **Abandoned-operation risk:** the common way to honour a context with a non-context API is to run the call in a goroutine and return on `ctx.Done()`. That leaves a response in flight on a connection the caller believes is free. Because go-ldap matches responses by message ID, a late response is normally discarded, but the connection must not return to the pool as healthy until the outstanding operation completes or the connection is closed. **Unverified:** go-ldap's exact behaviour for an abandoned request, which needs the v3.4.x source.
- Recommendations: set a dial deadline through `Option.Dialer` (default one with a timeout), add per-operation read deadlines, and close rather than pool a connection whose call was interrupted.

### H. Referral handling

- **Verified:** the package sets no referral option (a search of the `.go` files finds no referral setting). Referral behaviour is whatever go-ldap defaults to. **Unverified:** that default in v3.4.6.
- If referrals are followed, the library would open a new connection to a host outside the `connector` server list. That connection would bypass `BindFunc`, `StartTLS`, the TLS `ServerName` set in `connect`, and the latency heap. This is a security concern if referrals are followed automatically, since bind credentials could be sent to a host the caller never selected.
- Recommendation: confirm go-ldap's referral default in v3.4.6, disable following explicitly if it is on, and document the decision in `README.md`.

### I. Multi-server failover

**Files:** `connector.go` (`connect`, `updateLatency`, `decayLatencies`, `newSRVConnector`), `directory.go` (`retry`).

- **Verified:** `connect` dials `c.servers[0]` only. It never tries another server within one call. Failover happens only indirectly: an `IsBadConn` error from `checkBadConn` adds `badConnPenalty` (2 s) to that server, which moves it down the heap, and `dir.retry` then calls `connect` again.
- **Verified:** `dir.retry` makes at most three attempts (two `cachedOrNewConn`, one `alwaysNewConn`). If the three attempts hit failing servers, the caller gets an error even when healthy servers exist. Without `Option.Dialer`, a blackholed server can take the OS TCP timeout per attempt.
- **Verified:** the shuffle in `connect` (`rand.Shuffle(i, ...)` over `c.servers`) permutes only the zero-latency prefix. Once a server has a nonzero latency, servers with equal nonzero latency are not reordered among themselves, so the selection order is largely fixed.
- **Verified:** `decayLatencies` runs on every `putConn`, not on a timer, so the decay rate depends on traffic. With no traffic, a penalized server stays penalized.
- Recommendations: try up to `len(servers)` distinct servers inside `connect` before returning an error, filter the candidate set to healthy servers, and decay latency by wall-clock time rather than by call count.

### J. Divergence from `sql.go` that makes porting harder

Each item below is a place where the local code no longer matches the shape of `database/sql`, so an upstream change cannot be copied mechanically:

- **`IsBadConn(ErrBadConn)` in `putConn`** (`directory.go`, line 420): the intent is to test the released error with `errors.Is`. The local version tests a constant, so the expiry branch is dead. Restoring the intent is a one-line change, but any upstream diff touching `putConn` will conflict with it.
- **`closed` flag and `ErrDirClosed`** (`directory.go`, `conn.go`, `errors.go`): no `Close` exists (see Tech Debt), so every `closed` check is unreachable and upstream close-path changes have no counterpart.
- **`closeDBLocked`** (`conn.go`, lines 41-49) is a leftover name from `sql.DB`, is never called, and does not match the lock-held close it implies. Porting upstream code that uses the same name will collide with it.
- **`connector.connect` returns a `conn` with no `dir`** (`connector.go`, lines 180-187), and `dir` is set later in `openNewConnection` and `conn()`. Upstream sets the owner before any path can close a connection. That ordering difference is the cause of the nil-pointer item in Tech Debt.
- **Pool limits are directory-wide.** `maxOpen` applies to the whole directory, not per server, so one slow server can hold all slots. `Directory.maxOpenPerServer` (`directory.go`) is never set from `Option` and never read in `conn()`, so it has no effect.
- **Option names mirror `sql.DB`** (`connector.go`, `Option`: `MaxIdleConns`, `MaxOpenConns`, `ConnMaxLifetime`, `ConnMaxIdleTime`), but the semantics are reimplemented, so a reader who knows `sql.DB` will assume behaviour that is not all present (for example, `ConnMaxIdleTime` is only enforced by the cleaner).
- Recommendation: keep a mapping table in `README.md` from each local name to its `sql.go` counterpart, and avoid renaming functions that have upstream equivalents so that diffs stay readable.

### K. What could not be determined from the code

- Exact current `database/sql` behaviour for each item in section A (no upstream diff performed).
- go-ldap v3.4.6 internals: which errors are wrapped as `ErrorNetwork`, whether a default read timeout applies, the referral default, and abandoned-request handling.
- Server-side idle timeout values for any domain controller.
- Whether the retry rules for `driver.ErrBadConn` changed in any Go release after go1.19.2, and how they apply to LDAP write operations.

---

*Concerns audit: 2026-10-12*
