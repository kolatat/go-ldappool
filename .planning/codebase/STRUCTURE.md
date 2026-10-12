---
last_mapped_commit: 23ccb27db8c4e78284ada38c52c7e913f5fc89b1
last_mapped_at: 2026-10-12
---
# Codebase Structure

**Analysis Date:** 2026-10-12

## Directory Layout

```
go-ldappool/
├── adhelper/              # Subpackage: Active Directory password encoding helper
│   └── interface.go       # ResetPassword (UTF-16LE unicodePwd)
├── .idea/                 # JetBrains IDE project metadata (not code)
│   ├── .gitignore
│   ├── ldappool.iml
│   ├── modules.xml
│   └── vcs.xml
├── .planning/codebase/    # GSD codebase analysis documents (generated)
├── conn.go                # Pooled connection wrapper (package ldappool)
├── connector.go           # SRV discovery, dialing, latency-based server choice, Option
├── directory.go           # Directory pool, acquire/release, limits, stats
├── directory_workers.go   # Background opener and cleaner goroutines
├── errors.go              # Sentinel errors and IsBadConn
├── go.mod                 # Module github.com/kolatat/go-ldappool (go 1.19)
├── go.sum                 # Dependency checksums
├── interface.go           # Public LDAP operation methods on *Directory
├── server.go              # Server type and srvHeap
├── LICENSE                # Project license
├── LICENSE_go.md          # Go project license (derived code from database/sql)
└── README.md              # Usage, features, and design notes
```

## Directory Purposes

**Root (`/`):**
- Purpose: The `ldappool` Go package. All pool logic lives here as a single flat package.
- Contains: `.go` source files, module files, licenses, README
- Key files: `directory.go` (pool core), `interface.go` (public operations), `connector.go` (dialing)

**`adhelper/`:**
- Purpose: Separate importable package for AD-specific helpers that do not depend on pool state
- Contains: One Go file, `interface.go`, in package `adhelper`
- Key files: `adhelper/interface.go`

**`.idea/`:**
- Purpose: JetBrains IDE configuration
- Contains: IDE module and VCS settings
- Key files: `.idea/ldappool.iml`, `.idea/modules.xml`

**`.planning/codebase/`:**
- Purpose: Output location for codebase mapping documents consumed by GSD commands
- Contains: `ARCHITECTURE.md`, `STRUCTURE.md`, and other focus-area documents
- Generated: Yes
- Committed: Per project policy (orchestrator handles git)

## Key File Locations

**Entry Points:**
- `directory.go`: `OpenDirectory`, the constructor used by callers
- `interface.go`: public operations (`Search`, `Modify`, `VerifyPassword`, etc.)
- `adhelper/interface.go`: `ResetPassword`

**Configuration:**
- `connector.go`: `Option` struct (TLS, StartTLS, `LDAPsPort`, `BindFunc`, `Dialer`, pool limits and lifetimes)
- `go.mod`: module path and dependency versions (`github.com/go-ldap/ldap/v3` v3.4.6, `golang.org/x/text` v0.14.0)

**Core Logic:**
- `directory.go`: pool state machine, `conn()` acquire, `putConn()` release, `retry()`
- `connector.go`: `newSRVConnector`, `connect`, latency update and decay
- `conn.go`: `conn` validation, rebind, close
- `server.go`: `Server`, `srvHeap`
- `directory_workers.go`: `connectionOpener`, `connectionCleaner`

**Error Definitions:**
- `errors.go`: sentinel errors, `adErr`, `IsBadConn`

**Testing:**
- No test files are present. There are no `*_test.go` files at any level.

## Naming Conventions

**Files:**
- Lowercase, single-word or underscore-joined names matching their responsibility: `directory.go`, `directory_workers.go`, `connector.go`
- Generic file name `interface.go` is used for the public API surface in both the root package and `adhelper/`. It does not define Go interfaces. Name new files by responsibility instead.

**Directories:**
- Lowercase, single word: `adhelper/`

**Go identifiers:**
- Exported API: `OpenDirectory`, `Directory`, `Option`, `Server`, `IsBadConn`, `ResetPassword`, `DBStats`
- Unexported internals: `connector`, `conn`, `connRequest`, `connReuseStrategy`, `srvHeap`, `connectionOpener`
- Errors: `Err` prefix for sentinels (`ErrDirClosed`, `ErrBadConn`); the `ad:` prefix appears in messages
- Constants: camelCase unexported (`badConnPenalty`, `decayRate`, `maxBadConnRetries`, `defaultMaxIdleConns`)

## Where to Add New Code

**New LDAP operation (e.g., `Add`, `Compare`):**
- Primary code: `interface.go`, following the existing `dir.retry(func(strategy) { dir.conn; defer release; dc.Lock; op; dc.Unlock })` shape
- Use named return values and a deferred closure for `releaseConn` (see Anti-Patterns in `.planning/codebase/ARCHITECTURE.md`)
- Tests: create `interface_test.go` in the root package (none exist yet)

**New pool behavior (limits, stats, eviction):**
- Primary code: `directory.go` (state and acquire/release), with background work in `directory_workers.go`
- Tests: `directory_test.go` in the root package

**New connection or server selection strategy:**
- Primary code: `connector.go` (dialing and selection), `server.go` (heap ordering)
- Tests: `connector_test.go`

**New error classification:**
- Implementation: `errors.go`

**New AD helper (password, UAC flags, attribute encoding):**
- Implementation: `adhelper/` as a new file, e.g. `adhelper/uac.go`. Keep it free of pool dependencies.

**Utilities:**
- Shared helpers: no `util` package exists. Place small unexported helpers in the file that uses them; if shared across files in the root package, add a new file named for its purpose.

## Special Directories

**`.idea/`:**
- Purpose: JetBrains IDE settings
- Generated: Yes (by IDE)
- Committed: Yes (`.idea/.gitignore` excludes local workspace files)

**`.planning/`:**
- Purpose: GSD planning and codebase analysis artifacts
- Generated: Yes
- Committed: Per project policy

---

*Structure analysis: 2026-10-12*
