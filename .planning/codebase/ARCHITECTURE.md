---
last_mapped_commit: 7f4e3b30fdd4733449a679ffc3ab431c184a6a36
last_mapped_at: 2026-10-12
---
<!-- refreshed: 2026-10-12 -->

# Architecture

**Analysis Date:** 2026-10-12

## System Overview

```text
┌─────────────────────────────────────────────────────────────┐
│                  Public API (package ldappool)               │
│  `interface.go`  StickyConn / Search / SearchWithPaging /    │
│                  Modify / ModifyWithResult / Del /           │
│                  VerifyPassword                              │
│  `directory.go`  OpenDirectory, SetMax*, Stats, PrintSRVs    │
└────────────────────────────┬────────────────────────────────┘
                             │ dir.retry(strategy)
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                 Pool core (`directory.go`)                    │
│  freeConn stack · connRequests queue · numOpen accounting    │
│  conn() acquire · putConn() release · retry() bad-conn logic │
└──────┬─────────────────────────────┬────────────────────────┘
       │ openerCh                    │ cleanerCh
       ▼                             ▼
┌──────────────────────────┐  ┌──────────────────────────────┐
│ `directory_workers.go`   │  │ `directory_workers.go`        │
│ connectionOpener goroutine│  │ connectionCleaner goroutine  │
│ → openNewConnection       │  │ → closes idle/expired conns  │
└──────────────┬───────────┘  └──────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────────────────┐
│ Connector (`connector.go`)                                    │
│  SRV discovery · latency-ordered srvHeap · connect()          │
│  TLS / StartTLS / BindFunc · latency decay & penalties        │
└──────────────┬──────────────────────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────────────────┐
│ go-ldap/ldap/v3  (`*ldap.Conn`)  ·  DNS SRV lookup (net)      │
└─────────────────────────────────────────────────────────────┘

Side package: `adhelper/interface.go` (AD `unicodePwd` encoding, used by callers with Modify)
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| Public operations | Wrap LDAP calls in `dir.retry`, acquire a conn, lock it, run the op, release it | `interface.go` |
| Directory / pool | Owns pool state (`freeConn`, `numOpen`, `connRequests`, limits, stats), `OpenDirectory` constructor, acquire/release logic | `directory.go` |
| Background workers | `connectionOpener` (creates conns on demand from `openerCh`), `connectionCleaner` (closes idle/expired conns) | `directory_workers.go` |
| Connector | DNS SRV discovery of `ldap`/`ldaps` targets, latency-based server selection, dialing, StartTLS, custom bind | `connector.go` |
| Server model | `Server` (embeds `*net.SRV`, latency, heap index) and `srvHeap` (`container/heap` ordered by latency) | `server.go` |
| Pooled connection | `conn` wraps `*ldap.Conn` with a mutex, rebind flag, timestamps, in-use flag, validation (`WhoAmI` every >14s) | `conn.go` |
| Error model | Sentinel errors and `IsBadConn` classification (network errors and `ErrBadConn`) | `errors.go` |
| AD helper | `ResetPassword` encodes a password as UTF-16LE into a `unicodePwd` replace on an `ldap.ModifyRequest` | `adhelper/interface.go` |

## Pattern Overview

**Overall:** Single-package connection pool modeled on `database/sql.DB` (see `README.md`), with a pluggable server selector (connector) underneath and thin operation wrappers on top.

**Key Characteristics:**
- Pool logic is a near-direct port of `database/sql` (`conn()`, `putConn()`, `putConnDirLocked()`, `maybeOpenNewConnections()`, `connectionCleaner`), with LDAP specifics swapped in.
- Each public operation follows the same shape: `dir.retry(func(strategy) { acquire; lock; op; unlock; release })`.
- Server selection is latency-driven: a min-heap keyed on `Server.latency`; latency is updated by an exponential-moving-average on successful dials and validations, penalized by `badConnPenalty` (2s) on bad connections, and decayed by `decayRate` (0.98/s) in `decayLatencies`.
- Concurrency: `Directory.mu` guards pool state; `conn` embeds `sync.Mutex` for its `*ldap.Conn`; `connector.mu` guards the server heap. Lock ordering is Directory → conn → connector in most paths, with some exceptions noted below.
- Context support exists at the pool level only (acquire waits, dial checks); the underlying `ldap.Conn` operations are not context-aware.

## Layers

**Public API layer:**
- Purpose: Expose LDAP operations that transparently use pooled connections.
- Location: `interface.go`
- Contains: `StickyConn`, `Search`, `search`, `SearchWithPaging`, `searchWithPaging`, `Modify`, `ModifyWithResult`, `Del`, `VerifyPassword`
- Depends on: pool core (`dir.conn`, `dir.retry`, `conn.releaseConn`), `go-ldap` types
- Used by: external callers importing `github.com/kolatat/go-ldappool`

**Pool core layer:**
- Purpose: Connection lifecycle, limits, retry policy, and statistics.
- Location: `directory.go`
- Contains: `Directory` struct, `OpenDirectory`, `SetMaxOpenConns`, `SetMaxIdleConns`, `SetConnMaxLifetime`, `SetConnMaxIdleTime`, `conn`, `putConn`, `putConnDirLocked`, `maybeOpenNewConnections`, `retry`, `Stats`, `PrintSRVs`, `connReuseStrategy`
- Depends on: connector layer, `conn`, `database/sql` (only for the `DBStats` type alias)
- Used by: public API layer, background workers

**Background worker layer:**
- Purpose: Asynchronous connection creation and reaping.
- Location: `directory_workers.go`
- Contains: `connectionOpener`, `startCleanerLocked`, `connectionCleaner`, `connectionCleanerRunLocked`
- Depends on: `Directory` internals, `connector.connect`
- Used by: started from `OpenDirectory` (opener) and lazily from `startCleanerLocked` (cleaner)

**Connector layer:**
- Purpose: Discover servers and produce raw `*ldap.Conn` values.
- Location: `connector.go`
- Contains: `connector`, `Option`, `newSRVConnector`, `connect`, `updateLatency`, `decayLatencies`
- Depends on: `net.LookupSRV`, `ldap.DialURL`, `Server`/`srvHeap`
- Used by: pool core and workers via `dir.connector`

**Connection and server model:**
- Location: `conn.go` (pooled connection), `server.go` (server + heap)
- Depends on: `go-ldap`, `net`
- Used by: connector and pool core

**Errors:**
- Location: `errors.go`
- Contains: sentinel errors (`ErrNoServerDefined`, `ErrDirClosed`, `ErrDuplicateConnClose`, `ErrConnExpired`, `ErrBadConn`), `adErr` wrapper, `IsBadConn`
- Used by: all layers; `IsBadConn` drives retry decisions

**Helper subpackage:**
- Location: `adhelper/interface.go` (package `adhelper`, import path `github.com/kolatat/go-ldappool/adhelper`)
- Depends on: `go-ldap`, `golang.org/x/text/encoding/unicode`
- Used by: callers that build modify requests; intentionally independent of the pool

## Data Flow

### Primary Request Path (e.g., `Search`)

1. `Directory.Search` calls `dir.retry` with a closure (`interface.go`)
2. `retry` runs the closure up to `maxBadConnRetries` (2) times with `cachedOrNewConn`, retrying only when `IsBadConn` (`directory.go`)
3. `dir.search` calls `dir.conn(ctx, strategy)` to obtain a `*conn` (`directory.go`)
4. `conn()` either pops the last `freeConn` (checking lifetime, then `rebind`), waits on `connRequests` when `maxOpen` is reached, or increments `numOpen` and dials via `connector.connect` (`connector.go`)
5. `connector.connect` picks the lowest-latency server from the heap (shuffling zero-latency entries), dials with `ldap.DialURL`, optionally runs `StartTLS` and `BindFunc`, and updates latency (`connector.go`)
6. The operation runs under `conn.Lock()` on `dc.internal`
7. `conn.releaseConn` → `Directory.putConn` validates (`WhoAmI` if older than 14s), decays latencies, and returns the connection to `freeConn` via `putConnDirLocked`, or closes it on bad/expired/excess (`directory.go`)

### Connection Creation (background)

1. `OpenDirectory` starts `connectionOpener(ctx)` (`directory_workers.go`)
2. `maybeOpenNewConnections` pushes onto `openerCh` for each waiting request that can be satisfied under `maxOpen` (`directory.go`)
3. The opener calls `openNewConnection`, which hands the new connection to a waiting request or to `freeConn` (`directory.go`)

### Connection Reaping (background)

1. `startCleanerLocked` lazily starts `connectionCleaner` when `maxLifetime` or `maxIdleTime` is set and connections exist (`directory_workers.go`)
2. The cleaner closes free connections past `maxIdleTime` (by `returnedAt`) or `maxLifetime` (by `createdAt`), then sleeps until the next expiry (minimum 1s)

### Sticky Path (`StickyConn`)

1. Acquire a connection via `dir.retry`
2. Lock the conn, set `needRebind = true`, and hand the raw `*ldap.Conn` to the caller's callback
3. On the next acquire, `conn.rebind` re-runs `BindFunc` before reuse

**State Management:**
- Pool state lives on `Directory` (`freeConn`, `numOpen`, `connRequests`, limits, counters) guarded by `Directory.mu`
- Per-connection state lives on `conn` (`inUse`, `returnedAt`, `needRebind`, `lastValidated`, `closed`) guarded by `conn`'s mutex or `dir.mu` as commented in `conn.go`
- Server latency state lives on `Server.latency` and is guarded by `connector.mu`

## Key Abstractions

**Directory:**
- Purpose: The pool handle returned by `OpenDirectory`; one per domain
- Examples: `directory.go`
- Pattern: Mirrors `sql.DB`; safe for concurrent use

**connector:**
- Purpose: Server discovery and dialing strategy
- Examples: `connector.go`
- Pattern: Latency-ordered min-heap (`srvHeap` in `server.go`) with exponential smoothing and penalty/decay

**conn:**
- Purpose: Pooled `*ldap.Conn` wrapper with bookkeeping
- Examples: `conn.go`
- Pattern: Embedded mutex, `inUse` flag guarded by the pool lock

**connReuseStrategy:**
- Purpose: Controls whether `retry` may reuse a free connection (`cachedOrNewConn`) or must dial fresh (`alwaysNewConn`)
- Examples: `directory.go`
- Pattern: Enum passed through the closure in `retry`

**Option:**
- Purpose: User configuration for connector and pool limits
- Examples: `connector.go` (struct definition), `directory.go` (applied in `OpenDirectory`)
- Pattern: Variadic optional pointer (`opt ...*Option`)

## Entry Points

**`OpenDirectory`:**
- Location: `directory.go`
- Triggers: caller code creating a pool for a domain
- Responsibilities: runs SRV discovery, constructs `Directory`, starts `connectionOpener`, applies limits from `Option`

**Operation methods on `*Directory`:**
- Location: `interface.go`
- Triggers: caller code (`Search`, `SearchWithPaging`, `Modify`, `ModifyWithResult`, `Del`, `VerifyPassword`, `StickyConn`)
- Responsibilities: retry-wrapped single LDAP operations on a pooled connection

**`adhelper.ResetPassword`:**
- Location: `adhelper/interface.go`
- Triggers: caller code preparing an AD password change
- Responsibilities: fills `unicodePwd` on an `ldap.ModifyRequest`

**No `main` package or binary:** this is a library; there is no executable entry point.

## Architectural Constraints

- **Threading:** Multi-goroutine. One `connectionOpener` goroutine per `Directory`; one `connectionCleaner` goroutine started on demand. Callers' goroutines perform the LDAP operations.
- **Global state:** `connectionRequestQueueSize` package var in `directory.go` (10000). `utf16le` encoder package var in `adhelper/interface.go` (shared encoder; not documented as goroutine-safe, verify before concurrent use).
- **Circular imports:** None. Dependency direction is `interface.go`/`directory_workers.go` → `directory.go` → `connector.go` → `server.go`/`errors.go`; `conn.go` depends on `directory.go` types via `c.dir`.
- **Context:** Only pool acquisition and dial honor `ctx`; `ldap.Conn` calls do not.
- **Module:** `go 1.19` (`go.mod`), uses `atomic.Int64` and `any`.
- **Tests:** No test files exist in the repository (see `README.md` TODO "tests").

## Anti-Patterns

### Deferred release captures a stale error

**What happens:** In `interface.go`, `search`, `searchWithPaging`, `Modify`, `ModifyWithResult`, `Del`, and `VerifyPassword` call `defer dc.releaseConn(err)`. Go evaluates the `err` argument when the `defer` statement runs, which is before the LDAP call sets the result error (`err` is still `nil` from `dir.conn`).
**Why it's wrong:** `putConn` never sees a bad-connection error, so broken connections are returned to `freeConn` rather than closed, and `IsBadConn` retry logic is undermined.
**Do this instead:** Use a closure so the final value is read at return time, e.g. `defer func() { dc.releaseConn(err) }()`, and assign the operation result to the named return `err`.

### Result returned before the call runs

**What happens:** `ModifyWithResult` in `interface.go` is written as `return res, dir.retry(...)`. Go evaluates `res` before `retry` runs, so the caller always receives a nil `*ldap.ModifyResult`.
**Why it's wrong:** The modify result is silently lost.
**Do this instead:** Use named results and `err = dir.retry(...); return res, err`, as `SearchWithPaging` in `interface.go` already does.

### Dead expiry check in `putConn`

**What happens:** `directory.go` `putConn` checks `!IsBadConn(ErrBadConn) && c.expired(...)`. `IsBadConn(ErrBadConn)` is always `true`, so the condition is never met and lifetime expiry at release is never recorded.
**Why it's wrong:** `maxLifetimeClosed` is under-counted and expired connections are not flagged at release. Expiry is still enforced on acquire in `conn()`.
**Do this instead:** Check `c.expired(dir.maxLifetime)` directly, and remove the `ErrBadConn` term.

### Stdout logging from the library

**What happens:** `connector.go` `connect` calls `fmt.Println(err)` on bad connections, and `directory.go` `PrintSRVs` prints to stdout.
**Why it's wrong:** A library writes to the host application's stdout with no way to disable it.
**Do this instead:** Return or surface errors through the caller, or accept a logger in `Option`.

## Error Handling

**Strategy:** Errors are returned up the call chain. Bad connections are classified by `IsBadConn` and cause the connection to be closed and the operation retried.

**Patterns:**
- `dir.retry` retries up to 2 times with cached connections, then once with a fresh dial (`directory.go`)
- `connector.connect` penalizes the server latency by `badConnPenalty` via `checkBadConn` (`connector.go`)
- Sentinel errors in `errors.go` are compared with `errors.Is`/`ldap.IsErrorAnyOf`
- Panics are used for programmer errors only: double close (`conn.close`) and returning a connection that was not checked out (`putConn`)

## Cross-Cutting Concerns

**Logging:** `fmt.Println` in `connector.go` and `directory.go` (`PrintSRVs`). No logger abstraction.
**Validation:** Connection validation via `WhoAmI` in `conn.validateConnection` (`conn.go`), throttled to once per 14 seconds (`minValidationDuration`).
**Authentication:** Caller-supplied `Option.BindFunc` runs after dial and on rebind (`connector.go`, `conn.go`). `VerifyPassword` in `interface.go` binds as the user and marks the connection for rebind.

## database/sql Parity and SQL-vs-LDAP Differences

**Verification scope (2026-10-12):** Everything below was derived by reading the local `.go` files. The upstream `database/sql/sql.go` is not in the repository and was not fetched, so upstream comparisons are by construct name and role only. The README pins `go1.19.2`, the reference the copy was taken from; that reference is old and must be re-diffed against a current Go release before any upstream fix is ported. Items marked **Unverified** could not be confirmed from this repository (go-ldap v3.4.6 internals are not vendored here). Nothing here was executed; the deadlock and nil-dereference paths in D4 are inferred from reading the code.

### Construct mapping

| Local (ldappool) | File | database/sql counterpart | Status |
|------------------|------|--------------------------|--------|
| `Directory` | `directory.go` | `sql.DB` | Mirrored. No `Close()` and no `Ping()` (see D9) |
| `Directory.mu`, `freeConn`, `numOpen`, `connRequests` (map keyed by `uint64`), `nextRequest`, `openerCh`, `maxIdleCount`, `maxOpen`, `maxLifetime`, `maxIdleTime`, `cleanerCh` | `directory.go` | same-named fields on `DB` | Mirrored (go1.19 shapes) |
| `conn` | `conn.go` | `driverConn` | Mirrored, but wraps `*ldap.Conn` directly; no `driver.Conn` interface |
| `connRequest` | `directory.go` | `connRequest` | Mirrored |
| `connReuseStrategy` (`alwaysNewConn`, `cachedOrNewConn`) | `directory.go` | same | Mirrored |
| `retry` (`maxBadConnRetries = 2`, then one `alwaysNewConn` attempt) | `directory.go` | `DB.retry` | Mirrored |
| `conn(ctx, strategy)` | `directory.go` | `DB.conn` | Mirrored. Checks `ctx` at entry and while waiting; does not cancel a dial in progress (D5) |
| `putConn`, `putConnDirLocked` | `directory.go` | `DB.putConn`, `putConnDBLocked` | Mirrored with divergences D1, D2, D6 |
| `maybeOpenNewConnections` | `directory.go` | same | Mirrored |
| `openNewConnection` | `directory.go` | `DB.openNewConnection` | **Diverges** (locking and counter bugs, D4) |
| `connectionOpener` | `directory_workers.go` | same | Mirrored. Goroutine never stops (D9) |
| `startCleanerLocked`, `connectionCleaner`, `connectionCleanerRunLocked` | `directory_workers.go` | same | Mirrored. Closes conns outside `dir.mu`, as upstream does |
| `shortestIdleTimeLocked`, `maxIdleConnsLocked` (default 2) | `directory.go` | same | Mirrored |
| `SetMaxOpenConns`, `SetMaxIdleConns`, `SetConnMaxLifetime`, `SetConnMaxIdleTime` | `directory.go` | same | Mirrored |
| `DBStats` (type alias to `sql.DBStats`), `Stats()` | `directory.go` | same | Reuses the stdlib struct; `numClosed` is commented out |
| `conn.expired`, `conn.close` (panics on double close with `ErrDuplicateConnClose`) | `conn.go` | `driverConn.expired`, `close` | Mirrored. `closeDBLocked` is defined but never called |
| `conn.validateConnection` | `conn.go` | `driver.Validator` / `validateConnection` | **LDAP substitute**: `WhoAmI`, throttled (see L1) |
| `conn.rebind` | `conn.go` | `driver.SessionResetter` / `resetSession` | **LDAP substitute**: `Option.BindFunc`, conditional (see L2) |
| `IsBadConn`, `ErrBadConn`, `ErrConnExpired` (`adErr`) | `errors.go` | `driver.ErrBadConn`, `ErrConnExpired`, `isBadConn` | **Diverges**: classification differs (L4) |
| `ErrDirClosed`, `ErrNoServerDefined`, `ErrDuplicateConnClose` | `errors.go` | `sql` sentinel errors | `ErrDirClosed` is unreachable today (D9) |
| `StickyConn` | `interface.go` | `sql.Conn` (`DB.Conn`) | Analogue. Pins a conn and sets `needRebind`; deferred error is stale (D2) |
| `Search`, `SearchWithPaging`, `Modify`, `ModifyWithResult`, `Del`, `VerifyPassword` | `interface.go` | `DB.Query` / `DB.Exec` | LDAP-specific wrappers. Same retry-and-release shape; same stale-error bug (D2) |

**Not mirrored (no local counterpart):** `Tx` / `Begin`, `Stmt` / `Prepare`, `Rows` / `Row` (results are fully materialized `*ldap.SearchResult`), generic `Exec` / `Query` / `QueryRow`, `Ping` / `PingContext`, `Close`, `driver.NamedValueChecker`, `driver.ErrSkip`, `sql.ErrNoRows`, `sql.ErrConnDone`.

### Divergences from database/sql (defects and gaps found by reading)

**D1. Dead lifetime check in `putConn`.** `directory.go:420` tests `!IsBadConn(ErrBadConn) && c.expired(...)`. `IsBadConn(ErrBadConn)` is always true, so the branch never runs. Lifetime expiry at release is never counted in `maxLifetimeClosed` or turned into `ErrConnExpired`. Expiry is still enforced on acquire in `conn()` (`directory.go:323`, `:370`). Fix: test `c.expired(dir.maxLifetime)` alone.

**D2. Stale deferred error: bad connections are not closed on operation failure.** `StickyConn` (`interface.go:245`), `search` (`:265`), `searchWithPaging` (`:285`), `Modify` (`:298`), `ModifyWithResult` (`:312`), `Del` (`:326`), and `VerifyPassword` (`:340`) all write `defer dc.releaseConn(err)`. Go evaluates `err` when the `defer` statement runs, before the operation, so `putConn` always receives `nil`. Consequences, from reading:
- A connection that fails with `ldap.ErrorNetwork` is returned to `freeConn` rather than closed.
- `dir.retry` then retries. `cachedOrNewConn` pops the most recent free connection (`directory.go:318-321`), so the retry likely gets the same dead connection back. Up to three attempts can hit it before the `alwaysNewConn` dial.
- Release-time detection depends on `validateConnection` (`conn.go:70`), which only pings if `lastValidated` is more than 14s old. A recently used dead connection passes release-time checks.
- Upstream avoids this with a closure that reads the final error at return time. This copy has the bug the existing Anti-Patterns section describes.

**D3. Silently ignored rebind failure.** `conn()` at `directory.go:331` and `:381` handles only `IsBadConn(err)` from `rebind`. A non-bad `BindFunc` error is dropped and the connection is handed to the caller in whatever state the failed bind left it. Upstream treats a failed session reset as a bad connection.

**D4. `openNewConnection` self-deadlocks and double-decrements `numOpen`.** `openNewConnection` (`directory.go:151-175`) holds `dir.mu` via `defer dir.mu.Unlock()`. On the path where `putConnDirLocked` returns `false` for a live connection (`directory.go:171-173`), it calls `c.close()`. `conn.close()` (`conn.go:60`) then locks `c.dir.mu`, the same non-reentrant mutex, so this self-deadlocks. `numOpen` is also decremented twice (`directory.go:172` and `conn.go:61`). `putConnDirLocked` returns `false` when `maxIdle` is full with no waiter or `maxOpen` is exceeded, so this is reachable when an opener-created connection arrives after a release has already satisfied the waiter. A second defect is in the closed branch (`directory.go:155-159`): it calls `c.close()` before `c.dir` is set (`:168`), so `c.dir` is nil and `conn.close` dereferences it. That branch is latent only because `dir.closed` is never set (D9). Upstream releases `db.mu` before closing the connection.

**D5. Context is honored only before and around dials.** `conn()` checks `ctx` at entry (`directory.go:312`) and while waiting (`:349`). `connector.connect` checks it once before dialing (`connector.go:135`). `ldap.DialURL`, `StartTLS`, and every `ldap.Conn` operation are not cancellable, so a caller's deadline does not bound the LDAP round trip. Upstream passes `ctx` into driver methods.

**D6. Validation error path is marked unfinished.** `putConn` (`directory.go:408-410`) carries `// TODO might not work, sql sets err=ErrBadConn`. The code assigns the `WhoAmI` error to `err`. Non-bad LDAP errors (for example a result code from `WhoAmI`) are then kept in the pool, because only `ErrorNetwork` is treated as bad.

**D7. Queue size comment disagrees with value.** `directory.go:46` sets `connectionRequestQueueSize = 10000 // // 1Mil`. The comment says one million. Which one is intended cannot be determined from the code. This value sizes `openerCh` (`directory.go:59`).

**D8. `rebind` never clears `needRebind`.** `conn.rebind` (`conn.go:87-94`) runs `BindFunc` when `needRebind` is set but never resets it. The flag is only set, by `StickyConn` (`interface.go:248`) and `VerifyPassword` (`interface.go:343`). Every later acquisition of that connection therefore re-runs `BindFunc`, one extra bind per acquire. Whether this is intended (rebind on every reuse) or an omission (clear after one rebind) cannot be determined from the code; no test covers it.

**D9. No lifecycle: `Close` is absent and `closed` is never set.** `Directory` has a `stop func()` field (`directory.go:42`) set to a context cancel (`:55-61`), but nothing calls it. `dir.closed` is never assigned `true`. The `connectionOpener` goroutine therefore lives for the life of the process, and `ErrDirClosed` (`directory.go:308`, `:368`) and every `dir.closed` branch are unreachable. `maxOpenPerServer` (`directory.go:33`) is declared and never used. `closeDBLocked` (`conn.go:41`) is never called.

### LDAP-specific differences

**L1. No standard reset or ping: the stand-ins.**
- *Reset (`driver.SessionResetter` analogue):* `conn.rebind` (`conn.go:87`) re-runs `Option.BindFunc` only when `needRebind` is set, and is a no-op when `BindFunc` is nil (`conn.go:90`). Nothing else resets identity.
- *Ping/validate (`driver.Pinger` / `Validator` analogue):* `conn.validateConnection` (`conn.go:66`) calls `WhoAmI(nil)` only if `lastValidated` is more than `minValidationDuration` (14s, `conn.go:13`) old. It runs only at release from `putConn` (`directory.go:408`), never at acquire. A successful `WhoAmI` also feeds the latency update (`conn.go:75`).
- *Fresh connections:* `connector.connect` runs `BindFunc` once, before the connection is returned (`connector.go:173-178`). Fresh connections are not rebound on acquire.

**L2. Per-connection bind state and how it is returned to the pool.**
- A pooled `*ldap.Conn` carries its bound identity. The pool tracks only the `needRebind` boolean (`conn.go:25`), not which identity is bound.
- `VerifyPassword` (`interface.go:334-346`) binds the pooled connection as the end user (`interface.go:342`) and sets `needRebind`. The user's identity stays on the connection after `releaseConn`. The next caller gets it reset only if `Option.BindFunc` is set. With `BindFunc == nil`, the next `Search` or `Modify` on that connection can run as the verified user. This follows from reading `conn.rebind` and `VerifyPassword`; the code does not document it as intended. Treat it as a risk.
- `needRebind` is set before the bind result is checked (`interface.go:343`). A failed bind (invalid credentials) still marks the connection for rebind. This is harmless but not obviously intended.
- `StartTLS` is applied once at dial (`connector.go:165-170`) and is per-connection state. It is not reapplied on rebind.

**L3. No transactions, prepared statements, or streaming rows.**
- Each public method is one LDAP request or one paged search. Grouping operations on one connection is possible only through `StickyConn`, which holds the connection lock for the duration of the caller callback (`interface.go:246-249`).
- `Search` returns a fully materialized `*ldap.SearchResult`. `SearchWithPaging` loops over pages inside `ldap.Conn.SearchWithPaging` while `dc.Lock()` is held (`interface.go:286-288`), so one large paged search blocks other callers of that connection for its whole duration.
- Upstream `Rows` streaming and `Tx` isolation have no analogue here and are not needed for LDAP's request model.

**L4. Bad-connection detection and error classification.**
- `IsBadConn` (`errors.go:124-135`) treats only `ldap.ErrorNetwork` and `ErrBadConn` (including `ErrConnExpired`, which wraps it) as bad. LDAP result codes such as `49` (invalid credentials) are not bad and the connection goes back to the pool.
- Upstream's `driver.ErrBadConn` has no LDAP equivalent. The local sentinel is produced only by `ErrConnExpired` in this repository.
- **Unverified:** whether go-ldap v3.4.6 maps a server-closed idle socket (EOF on read) to `ErrorNetwork`. If it does not, idle drops are never classified as bad, and the connection stays pooled until a `WhoAmI` at release fails with a non-bad error. The go-ldap source is not in the repo.
- Server idle drops are therefore caught only by: (a) `WhoAmI` at release, and only if more than 14s have passed since the last validation (`conn.go:70`); (b) a failed operation, but only if D2 does not hide the error; or (c) the next operation failing and `dir.retry` trying again.
- Latency penalty: `connector.connect` penalizes the chosen server by `badConnPenalty` (2s) when dial, StartTLS, or bind returns a bad error (`connector.go:140-145`, `:161-176`). Errors from operations after dial do not penalize the server at all, so a server that drops connections mid-session is never penalized directly.
- `connector.go:142` calls `fmt.Println(err)` on every bad dial, the same stdout-logging issue noted in the Anti-Patterns section.

**L5. Referrals: not handled.**
- No file in the repository inspects referrals. Operation results are returned as-is, and the referral field on `ldap.SearchResult` (a go-ldap field; confirm the name in v3.4.6) is never read.
- **Unverified:** whether go-ldap follows referrals automatically on any operation. This repository does not show it.
- Callers that need referral chasing must do it themselves, against a server they choose, outside the pool.

**L6. Multiple servers and DNS SRV discovery.**
- `newSRVConnector` (`connector.go:53-103`) runs `net.LookupSRV("ldaps","tcp",domain)` first unless `Option.LDAPsPort != 0` (`:66`), then `net.LookupSRV("ldap","tcp",domain)` (`:84`). An `ldap` record whose target also appears in the `ldaps` list is skipped by hostname (`:90`), regardless of port.
- Both lookups return an error on failure, and the `ldap` lookup error is returned unconditionally (`:84-86`). From reading, `OpenDirectory` therefore fails when the `ldap` SRV lookup has no records, even if `ldaps` records exist. This depends on Go's `net.LookupSRV` returning an error for empty results (stdlib behavior, not checked here).
- With `LDAPsPort` set, the `ldaps` lookup is skipped and `ldap` records are rewritten to `ldaps` on the forced port (`connector.go:152-155`).
- SRV `Priority` and `Weight` are not read anywhere (no references in the repository). Ordering is by measured latency only, through `srvHeap` (`server.go:153-184`). Every server starts at `latency: 1ms` (`connector.go:76`, `:94`), so the initial order is the order `net.LookupSRV` returns, which Go sorts by priority and randomizes by weight.
- SRV records are resolved once, in `OpenDirectory`. There is no re-resolution.
- `connector.connect` picks only `servers[0]`, the heap root (`connector.go:128`). It does not iterate servers within one call. Failover happens across attempts: the penalty (L4) moves the failed server down the heap, and the next `dir.retry` attempt picks a different root.
- Shuffling applies only to servers whose latency has decayed to exactly zero (`connector.go:120-127`). Latencies start at 1ms and decay multiplicatively, so zero is reached only after long decay. This is an edge case of the copied logic.
- Latency update: a success blends the sample as `(latency + 2*dt)/3` (`connector.go:197`); a failure adds `badConnPenalty` (`:194-195`). Decay runs on every release (`putConn`, `directory.go:413`) and scales every server except the one just used (`connector.go:202-226`).

**L7. Context does not reach LDAP operations.** Covered by D5. Relevant here because `StickyConn`'s callback receives a raw `*ldap.Conn` (`interface.go:235`) and cannot be interrupted by `ctx`.

**L8. `adhelper` sits outside the pool.**
- `adhelper.ResetPassword` (`adhelper/interface.go:361-370`) encodes a password as UTF-16LE into a `unicodePwd` `Replace` on an `*ldap.ModifyRequest`. The caller then passes the request to `Directory.Modify` or `Directory.ModifyWithResult`. The pool is not involved in building the request.
- The password is formatted with `fmt.Sprintf("%q", password)` (`adhelper/interface.go:364`). `%q` wraps in double quotes, which AD requires, but it also backslash-escapes embedded `"` and `\` and non-printable characters using Go syntax. AD would store those escapes literally. **Unverified against AD**; a password containing those characters is likely to be stored incorrectly.
- AD requires `unicodePwd` writes over an encrypted channel (LDAPS or StartTLS). Nothing in `adhelper` or in `Directory.Modify` enforces that. The channel is TLS only when `Option.StartTLS` is set or an `ldaps` SRV target is used (`connector.go:165`, `:84`). The caller must ensure it.
- The package-level `utf16le` encoder (`adhelper/interface.go:357-359`) is shared state. Whether a `transform.Encoder` is safe for concurrent `String` calls is **Unverified** here.

### Upstream-sync checklist (for future ports)

1. Re-diff against a current `database/sql/sql.go`. The mapping table is by name; upstream has changed since go1.19 (for example, connection request bookkeeping and the session-reset path).
2. Fix D2 first. The stale-error pattern undermines the `IsBadConn` retry that every other behavior depends on.
3. Fix D4 before any change to `openNewConnection`. Its locking differs from upstream and should be ported, not patched in place.
4. Decide D8 (rebind on every acquire) and the `BindFunc == nil` identity leak (L2) explicitly. They are behavior choices, not parity gaps.
5. Decide D7 (queue size) by intent, not by the comment.

---

*Architecture analysis: 2026-10-12*
