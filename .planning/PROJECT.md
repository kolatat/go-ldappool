# go-ldappool

## What This Is

A Go connection-pooling library for [go-ldap](https://github.com/go-ldap/ldap/), written to look and behave like `database/sql`'s pool (`DB`, max open/idle, connection lifetime and idle time, background opener and cleaner, stats). It is concurrency-safe, context-aware at the pool level, and discovers servers through DNS SRV records. It is very Microsoft Active Directory oriented. It is used by the owner's own services, so breaking API changes are acceptable.

## Core Value

A trustworthy LDAP/AD connection pool: connections are never leaked, never carry one caller's state into another's, and failures are retried or discarded correctly — all while staying close enough to `database/sql` that upstream pool bugfixes can be ported cheaply.

## Requirements

### Validated

<!-- Inferred from the existing code (.planning/codebase/). Present, but not yet verified by tests. -->

- ✓ Pool with max open/idle conns, conn max lifetime and idle time, and `DBStats` — existing (`directory.go`, `directory_workers.go`)
- ✓ Multiple servers via DNS SRV discovery (`ldap`/`ldaps`) — existing (`connector.go`, `server.go`)
- ✓ Connection options: StartTLS, TLS config, LDAPS port override, custom dialer, caller-supplied `BindFunc` for rebind — existing (`connector.go`)
- ✓ Pool-level operations (search, modify, delete, WhoAmI, bind variants) with `context.Context` — existing (`interface.go`)
- ✓ Sticky connection for multi-operation sessions — existing (`StickyConn`)
- ✓ Active Directory helper for the `unicodePwd` attribute value (`adhelper/`)

### Active

<!-- Current scope. These are hypotheses until shipped and validated. -->

- [ ] Refresh the pool core against current `database/sql` (`sql.go`), keeping names and structure close to upstream so future bugfixes port easily; document every deliberate divergence
- [ ] Update dependencies (go-ldap, x/crypto, x/text, Go version) to current releases
- [ ] Fix known bugs from the codebase map (see `.planning/codebase/CONCERNS.md`), including: the release call capturing `err` before the operation runs, `openNewConnection` closing under `dir.mu` (deadlock and double-decrement risk, nil `c.dir` dereference), retry of non-idempotent writes, and SRV discovery failing when only `ldaps` records exist
- [ ] Security audit and fixes covering pool state leakage (bind identity crossing between callers, e.g. `VerifyPassword`), injection and escaping (filter/DN escaping, `adhelper`, password handling), transport and TLS (certificate verification, StartTLS/LDAPS defaults, credentials in memory and logs), and dependency vulnerabilities
- [ ] Modern Microsoft AD and LDAP support: secure transport (LDAPS/StartTLS, channel binding and LDAP signing considerations) and AD features (password modify/set, paged searches, and other AD-specific operations)
- [ ] Connection preference when running a query: target a specific DC or AD site, via a pool-level policy (e.g. prefer a site or lowest latency, with failover) that a per-call hint can override
- [ ] Unit tests built on a fake LDAP server/conn, taking inspiration from `database/sql`'s pool tests (fakedb-style), proving pool behavior, bug fixes and LDAP-specific handling

### Out of Scope

- Backward API compatibility — only the owner's own services consume the library, so redesign is allowed where it helps
- Non-AD directory vendor features (OpenLDAP-specific extensions and so on) — the library is AD-oriented; generic LDAP is supported only as far as AD-compatible behavior allows
- Real-AD integration test suites — fake LDAP is the chosen approach; revisit later if needed

## Context

- Brownfield: about 1,100 lines of Go across `conn.go`, `connector.go`, `directory.go`, `directory_workers.go`, `errors.go`, `interface.go`, `server.go` and `adhelper/`. No tests exist. The codebase map is in `.planning/codebase/`.
- The pool is a near-copy of `database/sql` from a very old Go version (the README links go1.19.2). Keeping it a close, portable copy is a design goal: upstream bugfixes should be easy to copy over.
- LDAP is not designed for connection pooling, so `database/sql` assumptions must be adapted rather than copied: per-connection bind/auth state, no standard reset or ping, servers closing idle connections, no transactions or prepared statements, referrals, and multi-server (DC/site) selection. `.planning/codebase/ARCHITECTURE.md` and `CONCERNS.md` hold the SQL-vs-LDAP analysis; items marked Unverified there still need checking.
- The map's findings come from reading the code only; nothing has been executed.

## Constraints

- **Design**: Stay structurally close to current `database/sql` — so upstream pool bugfixes remain easy to port; every divergence is deliberate and documented
- **Platform**: Microsoft Active Directory first — the library's target environment
- **Compatibility**: Breaking API changes allowed — the only consumers are the owner's own services
- **Testing**: Fake LDAP for tests, with `database/sql` pool tests as inspiration — no dependency on a real AD in CI
- **Tech stack**: Go with `go-ldap/ldap/v3`; Go version to be bumped to a current release during the dependency update

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Keep mirroring `database/sql` and refresh from current upstream | Cheap, safe porting of upstream pool bugfixes | — Pending |
| Allow breaking API changes | Only the owner's own services consume the library | — Pending |
| Test with a fake LDAP, using `database/sql` tests as inspiration | Runs anywhere, no AD needed; reuses proven pool test ideas | — Pending |
| Connection preference as pool-level policy plus per-call override | Covers both a default site/DC strategy and one-off targeting | — Pending |
| Security audit covers pool state leakage, injection/escaping, transport/TLS and dependencies | The map found a bind-state leak and retry-on-write risks, and credentials pass through this library | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-10-12 after initialization*
