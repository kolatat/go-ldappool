# Connection Pooling for LDAP

This `ldappool` package provides connection pooling for [go-ldap](https://github.com/go-ldap/ldap/) in the same style as
Golang's [`database/sql`](https://pkg.go.dev/database/sql) package. It is designed to be safe for concurrent use by
multiple goroutines.

## Usage

Installation

```sh
go get -u github.com/kolatat/go-ldappool
```

Connecting

```go
// TODO
```

Searching

```go
// TODO
```

## Features

* connecting to multiple servers
* concurrent use by multiple goroutines
* support for `context.Context` (in the pool level but not yet in [LDAP](https://github.com/go-ldap/ldap/issues/326))
  operations
* server discovery via DNS SRV records (at the pool level, not [connection](https://github.com/go-ldap/ldap/issues/329))

### Notes, Goals, TODOs

* very much Active Directory oriented
* tests
* preferences towards faster, lower-latency servers
* AD features not supported by standard LDAP calls
    * password modification

## Thanks, sql.DB

* [Source Code](https://cs.opensource.google/go/go/+/refs/tags/go1.19.2:src/database/sql/sql.go)
* [License](LICENSE_go.md)

In fact, this package is virtually a copy of the `database/sql` connection pool, with LDAP connections swapped in place
of SQL connections and other
SQL specifics removed. Many design choices and configuration parameters can be determined by consulting
the `database/sql` documentations.

* [SQL DB Pool `database/sql.DB`](https://pkg.go.dev/database/sql#DB)
* [Managing connections](https://go.dev/doc/database/manage-connections)
