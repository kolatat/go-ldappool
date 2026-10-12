---
last_mapped_commit: 23ccb27db8c4e78284ada38c52c7e913f5fc89b1
last_mapped_at: 2026-10-12
---
# Technology Stack

**Analysis Date:** 2026-10-12

## Languages

**Primary:**
- Go 1.19 (minimum, per `go` directive in `go.mod`) - all library code in `conn.go`, `connector.go`, `directory.go`, `directory_workers.go`, `errors.go`, `interface.go`, `server.go`, and `adhelper/interface.go`

**Secondary:**
- Not applicable (no scripts, templates, or other languages detected)

## Runtime

**Environment:**
- Go toolchain, minimum 1.19 (`go 1.19` in `go.mod`). No `toolchain` directive, no `.go-version` or `.nvmrc`-style pin detected.
- The code uses the `any` alias (Go 1.18+) in `server.go`, which is consistent with the 1.19 floor.

**Package Manager:**
- Go modules
- Module path: `github.com/kolatat/go-ldappool` (`go.mod`)
- Lockfile: `go.sum` present (checksums for all direct and indirect dependencies)

## Frameworks

**Core:**
- None. This is a library package (`package ldappool`), not an application or service. It has no `main` package and no HTTP framework.
- `database/sql` is imported in `directory.go` only for its `sql.DBStats` type (aliased as `DBStats`) and its pool design is the model for the package. No SQL driver or database is used.

**Testing:**
- Not detected. No `*_test.go` files exist in the repository. The README lists "tests" under Notes, Goals, TODOs.

**Build/Dev:**
- `go build` / `go test` via the standard toolchain. No Makefile, Taskfile, or build scripts present.
- JetBrains IDE project metadata in `.idea/` (`.idea/ldappool.iml`, `.idea/modules.xml`, `.idea/vcs.xml`). Not used by the build.

## Key Dependencies

**Direct (from `go.mod` `require` block):**
- `github.com/go-ldap/ldap/v3` v3.4.6 - the LDAP client. Every connection, search, modify, delete, bind, and WhoAmI call in the pool goes through `*ldap.Conn`. Imported in `conn.go`, `connector.go`, `directory.go`, `errors.go`, `interface.go`, `adhelper/interface.go`. Critical path.
- `golang.org/x/text` v0.14.0 - provides `encoding/unicode` UTF-16LE encoder used by `adhelper/interface.go` to build the Active Directory `unicodePwd` attribute value. Used only by the `adhelper` subpackage.

**Indirect (from `go.mod` second `require` block):**
- `github.com/go-asn1-ber/asn1-ber` v1.5.5 - ASN.1 BER encoding used internally by go-ldap.
- `github.com/Azure/go-ntlmssp` v0.0.0-20221128193559-754e69321358 - NTLM/SSPI support pulled in by go-ldap for NTLM bind.
- `github.com/google/uuid` v1.6.0 - pulled in transitively by go-ldap.
- `golang.org/x/crypto` v0.18.0 - pulled in transitively (crypto primitives used by go-ldap / ntlmssp).

**Standard library packages used:**
- `context`, `crypto/tls`, `database/sql` (types only), `container/heap`, `math`, `math/rand`, `net`, `net/url`, `strconv`, `strings`, `sync`, `time`, `errors`, `fmt`, `unicode` (via x/text)

## Configuration

**Environment:**
- The library reads no environment variables. Configuration is passed programmatically through the `Option` struct in `connector.go`:
  - `StartTLS bool` - upgrade plain `ldap://` connections with STARTTLS
  - `TLSConfig *tls.Config` - defaults to `new(tls.Config)` when nil
  - `LDAPsPort uint16` - force `ldaps` on the given port instead of using SRV-advertised scheme
  - `BindFunc func(ctx, *ldap.Conn, *Server) error` - caller-supplied bind hook, invoked on rebind
  - `Dialer *net.Dialer` - custom dialer for connection establishment
  - `MaxIdleConns`, `MaxOpenConns`, `ConnMaxLifetime`, `ConnMaxIdleTime` - pool sizing and lifetime limits
- Domain for DNS discovery is a constructor argument (`newSRVConnector(domain string, ...)` in `connector.go`).

**Build:**
- `go.mod`, `go.sum` only. No build-time configuration.

## Platform Requirements

**Development:**
- Go 1.19 or later
- Network access to DNS for SRV lookups when exercising the discovery path
- Windows is the current development platform (per `.idea/` and checkout location); the code itself is platform-neutral Go.

**Production:**
- Not applicable as a standalone deployable. Consumed as a Go module dependency by applications that import `github.com/kolatat/go-ldappool`.
- Runtime requires reachable LDAP/LDAPS servers that advertise `_ldap._tcp.<domain>` and/or `_ldaps._tcp.<domain>` SRV records.

---

*Stack analysis: 2026-10-12*
