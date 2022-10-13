# Connection Pooling for LDAP

This `ldappool` package provides connection pooling for [go-ldap](https://github.com/go-ldap/ldap/) in the same style as
Golang's [`database/sql`](https://pkg.go.dev/database/sql) package. It is designed to be safe for concurrent use by
multiple goroutines.

