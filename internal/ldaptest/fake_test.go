package ldaptest

import (
	"testing"

	"github.com/go-ldap/ldap/v3"
	"go.uber.org/goleak"
)

// verifyNoLeaks registers a goroutine-leak check on tb. Call it first in a
// test so that, cleanups running last-in first-out, it runs after the fake's
// own cleanup.
func verifyNoLeaks(tb testing.TB) {
	tb.Helper()
	tb.Cleanup(func() { goleak.VerifyNone(tb) })
}

// TestFakeTracer drives one bind and one search, plus a failing bind, through
// a real go-ldap client against the fake over loopback TCP.
func TestFakeTracer(t *testing.T) {
	verifyNoLeaks(t)
	f := NewTCP(t)
	f.AddUser("cn=admin,dc=example,dc=com", "secret")
	f.AddEntry("cn=alice,dc=example,dc=com", map[string][]string{
		"cn":          {"alice"},
		"objectClass": {"person"},
	})

	c, err := f.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.Bind("cn=admin,dc=example,dc=com", "secret"); err != nil {
		t.Fatalf("bind as admin: %v", err)
	}

	sr, err := c.Search(ldap.NewSearchRequest(
		"dc=example,dc=com", ldap.ScopeWholeSubtree, ldap.NeverDerefAliases,
		0, 0, false, "(objectClass=*)", []string{"cn"}, nil))
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(sr.Entries) != 1 {
		t.Fatalf("search returned %d entries, want 1", len(sr.Entries))
	}
	if got, want := sr.Entries[0].DN, "cn=alice,dc=example,dc=com"; got != want {
		t.Errorf("entry DN = %q, want %q", got, want)
	}
	if got, want := sr.Entries[0].GetAttributeValue("cn"), "alice"; got != want {
		t.Errorf("entry cn = %q, want %q", got, want)
	}

	err = c.Bind("cn=admin,dc=example,dc=com", "wrong")
	if !ldap.IsErrorWithCode(err, ldap.LDAPResultInvalidCredentials) {
		t.Errorf("bind with wrong password: err = %v, want code %d", err, ldap.LDAPResultInvalidCredentials)
	}

	if got := f.Count(OpBind); got != 2 {
		t.Errorf("bind count = %d, want 2", got)
	}
	if got := f.Count(OpSearch); got != 1 {
		t.Errorf("search count = %d, want 1", got)
	}
}
