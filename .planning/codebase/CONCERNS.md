---
last_mapped_commit: 23ccb27db8c4e78284ada38c52c7e913f5fc89b1
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

*Concerns audit: 2026-10-12*
