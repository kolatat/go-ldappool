---
last_mapped_commit: 23ccb27db8c4e78284ada38c52c7e913f5fc89b1
last_mapped_at: 2026-10-12
---
# External Integrations

**Analysis Date:** 2026-10-12

## APIs & External Services

**Directory service (primary integration):**
- Active Directory / generic LDAP servers - the entire purpose of the package
  - SDK/Client: `github.com/go-ldap/ldap/v3` (`ldap.DialURL`, `ldap.DialWithTLSDialer`, `*ldap.Conn`)
  - Operations used (via `dir.conn` + `dc.internal`):
    - `Search`, `SearchWithPaging` - `directory.go`, `conn.go`, `interface.go`
    - `Modify`, `ModifyWithResult` - `interface.go`
    - `Del` - `interface.go`
    - `Bind` (password verification) - `VerifyPassword` in `interface.go`
    - `WhoAmI` (health check, latency sampling) - `validateConnection` in `conn.go`
    - `StartTLS` - `connector.go` when `Option.StartTLS` is set
  - Auth: caller-supplied. Credentials are passed as method arguments (`VerifyPassword(ctx, username, password)`) or through `Option.BindFunc`. The package stores no credentials.

**Active Directory password reset:**
- `adhelper/interface.go` - `ResetPassword(modifyRequest, password)` builds the `unicodePwd` attribute. It wraps the password in double quotes, encodes it as UTF-16LE via `golang.org/x/text/encoding/unicode`, and calls `modifyRequest.Replace("unicodePwd", ...)`. Requires an LDAPS or STARTTLS-protected connection in practice (AD enforces this for `unicodePwd`).
  - References cited in code: `https://gist.github.com/Project0/61c13130563cf7f595e031d54fe55aab` and Microsoft MS-ADTS section 3c5e87db-4728-4f29-b164-01dd7d7391ea.

## Service Discovery

**DNS SRV lookups (Go standard `net` package):**
- `net.LookupSRV("ldaps", "tcp", domain)` - `connector.go` `newSRVConnector`. Skipped when `Option.LDAPsPort != 0`.
- `net.LookupSRV("ldap", "tcp", domain)` - `connector.go` `newSRVConnector`. Entries whose target also appears in the ldaps result are de-duplicated (`hasTLS` map).
- Discovered targets are wrapped as `Server` values (`server.go`) and kept in a latency-ordered heap (`srvHeap`). Initial latency is 1 ms, so new servers are tried first; latency is updated from WhoAmI round-trips.
- Hostnames have a trailing dot trimmed before use.

## Data Storage

**Databases:**
- None. The package does not connect to SQL or NoSQL stores. `database/sql` is imported only for the `sql.DBStats` type alias in `directory.go` and as a design reference (see README).

**File Storage:**
- Not applicable. No file I/O in library code.

**Caching:**
- Not applicable as an external service. The package maintains an in-process connection pool (`connector.go`, `directory.go`, `conn.go`) with idle/open limits and lifetime controls.

## Authentication & Identity

**Auth Provider:**
- Delegated to the LDAP/AD server. The package performs simple binds and passes through whatever bind the caller configures.
  - Password verification: `Directory.VerifyPassword` in `interface.go` performs `dc.internal.Bind(username, password)`, then marks the connection `needRebind = true` so the next user of that pooled connection re-binds via `BindFunc`.
  - Rebind hook: `conn.rebind` in `conn.go` calls `Option.BindFunc(ctx, internal, server)` when `needRebind` is set and `BindFunc` is non-nil.
  - Sticky sessions: `Directory.StickyConn` in `interface.go` pins one connection for a caller-supplied function and sets `needRebind = true` on release.
- NTLM: available through go-ldap's dependency `github.com/Azure/go-ntlmssp` (indirect); no NTLM code is written in this repo.

## TLS

**Transport security:**
- `crypto/tls` config supplied via `Option.TLSConfig` (`connector.go`). Defaults to an empty `tls.Config` if nil.
- `ldaps://` connections: TLS established via `ldap.DialWithTLSDialer` using the configured `TLSConfig` and `Option.Dialer`.
- `ldap://` connections: optionally upgraded with `internal.StartTLS(tc)` when `Option.StartTLS` is true (`connector.go`).
- Certificate verification is governed entirely by the caller's `TLSConfig` (e.g., `InsecureSkipVerify` is not set anywhere in the library).

## Monitoring & Observability

**Error Tracking:**
- None. No external error reporting SDK.

**Logs:**
- None. The library does not log. Errors are returned to the caller; sentinel errors are defined in `errors.go` (`ErrNoServerDefined`, `ErrDirClosed`, `ErrDuplicateConnClose`, and related).

**Stats:**
- `Directory` exposes pool statistics using the `DBStats` type alias (`directory.go`), mirroring `database/sql.DB.Stats()`.

## CI/CD & Deployment

**Hosting:**
- Not applicable. Distributed as a Go module via `go get github.com/kolatat/go-ldappool`.

**CI Pipeline:**
- None detected. No `.github/workflows`, `.gitlab-ci.yml`, or similar files in the repository.

## Environment Configuration

**Required env vars:**
- None read by the library.

**Secrets location:**
- Not applicable to the library itself. Bind credentials are supplied by the consuming application at call time. No `.env` files are present in the repository.

## Webhooks & Callbacks

**Incoming:**
- None.

**Outgoing:**
- None. The only outbound traffic is LDAP/LDAPS/STARTTLS to directory servers and DNS SRV queries.

---

*Integration audit: 2026-10-12*
