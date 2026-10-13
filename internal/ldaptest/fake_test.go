package ldaptest

import (
	"bytes"
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

// searchBase returns the entry at dn via a base-scope search.
func searchBase(tb testing.TB, c *ldap.Conn, dn string) *ldap.Entry {
	tb.Helper()
	sr, err := c.Search(ldap.NewSearchRequest(
		dn, ldap.ScopeBaseObject, ldap.NeverDerefAliases,
		0, 0, false, "(objectClass=*)", nil, nil))
	if err != nil {
		tb.Fatalf("search %q: %v", dn, err)
	}
	if len(sr.Entries) != 1 {
		return nil
	}
	return sr.Entries[0]
}

// TestFakeModifyDelete drives modify and delete through a real client and
// checks both the directory state and what the server recorded.
func TestFakeModifyDelete(t *testing.T) {
	verifyNoLeaks(t)
	f := NewTCP(t)
	const alice = "cn=alice,dc=example,dc=com"
	const john = `CN=Doe\, John,OU=Users,DC=corp,DC=example,DC=com`
	f.AddEntry(alice, map[string][]string{
		"cn":   {"alice"},
		"mail": {"a@example.com", "b@example.com"},
		"sn":   {"old"},
	})
	f.AddEntry(john, map[string][]string{"cn": {"John"}})

	c, err := f.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// Replace, add and delete-one-value changes on an existing entry.
	mr := ldap.NewModifyRequest(alice, nil)
	mr.Replace("sn", []string{"new"})
	mr.Add("mail", []string{"c@example.com"})
	mr.Delete("mail", []string{"a@example.com"})
	if err := c.Modify(mr); err != nil {
		t.Fatalf("modify: %v", err)
	}
	e := searchBase(t, c, alice)
	if e == nil {
		t.Fatal("entry missing after modify")
	}
	if got := e.GetAttributeValues("sn"); len(got) != 1 || got[0] != "new" {
		t.Errorf("sn = %q, want [new]", got)
	}
	if got := e.GetAttributeValues("mail"); len(got) != 2 || got[0] != "b@example.com" || got[1] != "c@example.com" {
		t.Errorf("mail = %q, want [b@example.com c@example.com]", got)
	}

	// A binary value and an escaped-comma DN are recorded byte for byte.
	bin := []byte{0x22, 0x00, 'p', 0x00, 0x22}
	mr = ldap.NewModifyRequest(john, nil)
	mr.Replace("unicodePwd", []string{string(bin)})
	if err := c.Modify(mr); err != nil {
		t.Fatalf("modify binary: %v", err)
	}
	log := f.ModifyLog()
	if len(log) != 2 {
		t.Fatalf("ModifyLog has %d records, want 2", len(log))
	}
	if log[1].DN != john {
		t.Errorf("recorded DN = %q, want %q", log[1].DN, john)
	}
	if len(log[1].Changes) != 1 {
		t.Fatalf("recorded %d changes, want 1", len(log[1].Changes))
	}
	ch := log[1].Changes[0]
	if ch.Op != 2 || ch.Attr != "unicodePwd" || len(ch.Vals) != 1 || !bytes.Equal(ch.Vals[0], bin) {
		t.Errorf("recorded change = %+v, want replace unicodePwd %x", ch, bin)
	}
	if len(log[0].Changes) != 3 || log[0].Changes[0].Op != 2 || log[0].Changes[1].Op != 0 || log[0].Changes[2].Op != 1 {
		t.Errorf("first record changes = %+v, want replace, add, delete in order", log[0].Changes)
	}

	// Unknown DN: result code 32, but the server still saw and recorded it.
	ghost := "cn=ghost,dc=example,dc=com"
	mr = ldap.NewModifyRequest(ghost, nil)
	mr.Replace("sn", []string{"x"})
	if err := c.Modify(mr); !ldap.IsErrorWithCode(err, ldap.LDAPResultNoSuchObject) {
		t.Errorf("modify unknown DN: err = %v, want code %d", err, ldap.LDAPResultNoSuchObject)
	}
	if err := c.Del(ldap.NewDelRequest(ghost, nil)); !ldap.IsErrorWithCode(err, ldap.LDAPResultNoSuchObject) {
		t.Errorf("delete unknown DN: err = %v, want code %d", err, ldap.LDAPResultNoSuchObject)
	}

	// Delete of an existing entry removes it.
	if err := c.Del(ldap.NewDelRequest(alice, nil)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := f.Entry(alice); ok {
		t.Error("Entry still reports alice after delete")
	}
	if _, ok := f.Entry(john); !ok {
		t.Error("Entry lost john, which was not deleted")
	}
	sr, err := c.Search(ldap.NewSearchRequest(
		"dc=example,dc=com", ldap.ScopeWholeSubtree, ldap.NeverDerefAliases,
		0, 0, false, "(cn=alice)", nil, nil))
	if err != nil {
		t.Fatalf("search after delete: %v", err)
	}
	for _, en := range sr.Entries {
		if en.DN == alice {
			t.Error("search still returns alice after delete")
		}
	}
	if got := f.Deleted(); len(got) != 2 || got[0] != ghost || got[1] != alice {
		t.Errorf("Deleted = %q, want [%s %s]", got, ghost, alice)
	}

	if got := f.Count(OpModify); got != 3 {
		t.Errorf("modify count = %d, want 3", got)
	}
	if got := f.Count(OpDelete); got != 2 {
		t.Errorf("delete count = %d, want 2", got)
	}
	if got := len(f.ModifyLog()); got != 3 {
		t.Errorf("ModifyLog length = %d, want 3 (unknown DN is recorded)", got)
	}
}
