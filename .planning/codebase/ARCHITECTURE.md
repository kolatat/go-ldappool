---
last_mapped_commit: 23ccb27db8c4e78284ada38c52c7e913f5fc89b1
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

---

*Architecture analysis: 2026-10-12*
