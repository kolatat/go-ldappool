// Package ldaptest is a wire-level, in-process LDAP server used as a test fake.
//
// It is private to this module (it lives under internal/, so consumers such
// as llce cannot import it) and exists only so pool, session and connector
// tests can drive a real go-ldap client against a real socket instead of
// mocking the client interface. Every credential and DN used with it is a
// fictitious test value.
package ldaptest

import (
	"net"
	"testing"

	"github.com/go-ldap/ldap/v3"
	"github.com/vjeantet/ldapserver"
)

// init silences the fake server's logger. ldapserver.Logger is a package
// global that writes every message to stdout by default; assigning it while
// servers are running is a data race, so it is assigned exactly once here.
func init() {
	ldapserver.Logger = ldapserver.DiscardingLogger
}

// NewTCP starts a Fake listening on a loopback TCP port and registers its
// Stop with tb.Cleanup. The listener is bound to 127.0.0.1 only.
func NewTCP(tb testing.TB) *Fake {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("ldaptest: listen on loopback: %v", err)
	}
	f := NewFake(ln)
	f.dial = func() (*ldap.Conn, error) { return ldap.DialURL(f.URL()) }
	tb.Cleanup(f.Stop)
	return f
}

// NewPipe starts a Fake on an in-memory net.Pipe transport and registers its
// Stop with tb.Cleanup. Call it inside a testing/synctest bubble so that the
// fake's goroutines belong to the bubble; inside a bubble also call Stop and
// close the clients explicitly before the bubble function returns. Dial returns
// a go-ldap client on the client end of a fresh pipe. A pipe has no address, so
// Addr and URL return empty strings.
func NewPipe(tb testing.TB) *Fake {
	tb.Helper()
	ln := NewPipeListener()
	f := NewFake(ln)
	f.dial = func() (*ldap.Conn, error) {
		end, err := ln.Dial()
		if err != nil {
			return nil, err
		}
		c := ldap.NewConn(end, false)
		c.Start()
		return c, nil
	}
	tb.Cleanup(f.Stop)
	return f
}
