package ldaptest

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-ldap/ldap/v3"
	msg "github.com/vjeantet/goldap/message"
	"github.com/vjeantet/ldapserver"
	"go.uber.org/goleak"
)

// verifyNoLeaks registers a goroutine-leak check on tb. Call it first in a
// test so that, cleanups running last-in first-out, it runs after the fake's
// own cleanup.
func verifyNoLeaks(tb testing.TB) {
	tb.Helper()
	tb.Cleanup(func() { goleak.VerifyNone(tb) })
}

// eventually polls cond every 2 ms until it returns true, and fails the test
// if timeout passes first. It replaces fixed sleeps for "flips" and "returns to
// zero" assertions.
func eventually(tb testing.TB, timeout time.Duration, cond func() bool) {
	tb.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for !cond() {
		select {
		case <-deadline.C:
			tb.Fatalf("condition not met within %v", timeout)
		case <-tick.C:
		}
	}
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

const (
	adminDN = "cn=admin,dc=example,dc=com"
	bobDN   = "cn=bob,dc=example,dc=com"
)

// whoAmI returns the authzId the server reports for c.
func whoAmI(tb testing.TB, c *ldap.Conn) string {
	tb.Helper()
	w, err := c.WhoAmI(nil)
	if err != nil {
		tb.Fatalf("whoami: %v", err)
	}
	return w.AuthzID
}

func TestFakeWhoAmI(t *testing.T) {
	verifyNoLeaks(t)
	f := NewTCP(t)
	f.AddUser(adminDN, "secret")

	c, err := f.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if got := whoAmI(t, c); got != "" {
		t.Errorf("anonymous authzId = %q, want empty", got)
	}
	if err := c.Bind(adminDN, "secret"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if got, want := whoAmI(t, c), "dn:"+adminDN; got != want {
		t.Errorf("bound authzId = %q, want %q", got, want)
	}
	if got := f.Count(OpWhoAmI); got != 2 {
		t.Errorf("whoami count = %d, want 2", got)
	}
}

func TestFakeFailedBindClearsIdentity(t *testing.T) {
	verifyNoLeaks(t)
	f := NewTCP(t)
	f.AddUser(adminDN, "secret")

	c, err := f.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.Bind(adminDN, "secret"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if got, want := whoAmI(t, c), "dn:"+adminDN; got != want {
		t.Fatalf("authzId after bind = %q, want %q", got, want)
	}
	err = c.Bind(adminDN, "wrong")
	if !ldap.IsErrorWithCode(err, ldap.LDAPResultInvalidCredentials) {
		t.Fatalf("bind with wrong password: err = %v, want code %d", err, ldap.LDAPResultInvalidCredentials)
	}
	if got := whoAmI(t, c); got != "" {
		t.Errorf("authzId after failed bind = %q, want empty", got)
	}
	if got := f.Identities(); len(got) != 1 || got[0] != "" {
		t.Errorf("Identities = %q, want one anonymous connection", got)
	}
}

func TestFakeIndependentIdentities(t *testing.T) {
	verifyNoLeaks(t)
	f := NewTCP(t)
	f.AddUser(adminDN, "secret")
	f.AddUser(bobDN, "hunter2")

	c1, err := f.Dial()
	if err != nil {
		t.Fatalf("dial 1: %v", err)
	}
	defer c1.Close()
	// Wait for the first accept so the accept order is the dial order.
	eventually(t, 2*time.Second, func() bool { return f.NumConns() == 1 })
	c2, err := f.Dial()
	if err != nil {
		t.Fatalf("dial 2: %v", err)
	}
	defer c2.Close()
	eventually(t, 2*time.Second, func() bool { return f.NumConns() == 2 })

	if err := c1.Bind(adminDN, "secret"); err != nil {
		t.Fatalf("bind 1: %v", err)
	}
	if err := c2.Bind(bobDN, "hunter2"); err != nil {
		t.Fatalf("bind 2: %v", err)
	}
	if got, want := whoAmI(t, c1), "dn:"+adminDN; got != want {
		t.Errorf("connection 1 authzId = %q, want %q", got, want)
	}
	if got, want := whoAmI(t, c2), "dn:"+bobDN; got != want {
		t.Errorf("connection 2 authzId = %q, want %q", got, want)
	}
	got := f.Identities()
	if len(got) != 2 || got[0] != adminDN || got[1] != bobDN {
		t.Errorf("Identities = %q, want [%s %s]", got, adminDN, bobDN)
	}
}

func TestFakeCloseConns(t *testing.T) {
	verifyNoLeaks(t)
	f := NewTCP(t)

	var clients []*ldap.Conn
	for i := 0; i < 3; i++ {
		c, err := f.Dial()
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		defer c.Close()
		clients = append(clients, c)
	}
	eventually(t, 2*time.Second, func() bool { return f.NumConns() == len(clients) })

	f.CloseConns()
	for _, c := range clients {
		eventually(t, 2*time.Second, c.IsClosing)
	}
	for _, c := range clients {
		c.Close()
	}
	eventually(t, 2*time.Second, func() bool { return f.NumConns() == 0 })
}

func TestFakeStopTwice(t *testing.T) {
	verifyNoLeaks(t)
	f := NewTCP(t)
	c, err := f.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.Close()
	f.Stop()
	f.Stop() // the cleanup registered by NewTCP calls it a third time
}

func TestFakeConcurrentSearch(t *testing.T) {
	verifyNoLeaks(t)
	f := NewTCP(t)
	f.AddEntry("cn=alice,dc=example,dc=com", map[string][]string{"cn": {"alice"}})

	c, err := f.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	const workers, each = 8, 20
	var wg sync.WaitGroup
	errs := make(chan error, workers*each)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				sr, err := c.Search(ldap.NewSearchRequest(
					"dc=example,dc=com", ldap.ScopeWholeSubtree, ldap.NeverDerefAliases,
					0, 0, false, "(cn=alice)", nil, nil))
				if err != nil {
					errs <- err
					return
				}
				if len(sr.Entries) != 1 {
					errs <- fmt.Errorf("search returned %d entries, want 1", len(sr.Entries))
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := f.Count(OpSearch); got != workers*each {
		t.Errorf("search count = %d, want %d", got, workers*each)
	}
}

// TestFakeConcurrentSearchModify mixes searches and modifies on one client
// connection. The server library handles every request of one client in its own
// goroutine, so a search can run while a modify writes the stored attribute map;
// a search that read that map outside the lock aborted the test binary with a
// concurrent-map fatal error (CR-01).
func TestFakeConcurrentSearchModify(t *testing.T) {
	verifyNoLeaks(t)
	f := NewTCP(t)
	const stableDN = "cn=alice,dc=example,dc=com"
	f.AddEntry(stableDN, map[string][]string{
		"cn":   {"alice"},
		"sn":   {"seed"},
		"mail": {"alice@example.com", "a@example.com"},
	})

	c, err := f.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	c.SetTimeout(10 * time.Second)

	const searchers, modifiers, iterations = 4, 4, 1000
	var wg sync.WaitGroup
	errs := make(chan error, searchers+modifiers)
	for s := 0; s < searchers; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				sr, err := c.Search(ldap.NewSearchRequest(
					"dc=example,dc=com", ldap.ScopeWholeSubtree, ldap.NeverDerefAliases,
					0, 0, false, "(cn=alice)", nil, nil))
				if err != nil {
					errs <- fmt.Errorf("search %d: %w", i, err)
					return
				}
				if len(sr.Entries) != 1 {
					errs <- fmt.Errorf("search %d returned %d entries, want 1", i, len(sr.Entries))
					return
				}
				if got := sr.Entries[0].GetAttributeValue("cn"); got != "alice" {
					errs <- fmt.Errorf("search %d: cn = %q, want alice", i, got)
					return
				}
			}
		}()
	}
	for m := 0; m < modifiers; m++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tmp := fmt.Sprintf("tmp%d", m)
			for i := 0; i < iterations; i++ {
				req := ldap.NewModifyRequest(stableDN, nil)
				req.Replace("sn", []string{fmt.Sprintf("sn-%d-%d", m, i)})
				if i%2 == 0 {
					req.Replace(tmp, []string{"v"})
				} else {
					req.Delete(tmp, nil)
				}
				if err := c.Modify(req); err != nil {
					errs <- fmt.Errorf("modify %d/%d: %w", m, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	if got, want := f.Count(OpSearch), searchers*iterations; got != want {
		t.Errorf("search count = %d, want %d", got, want)
	}
	if got, want := f.Count(OpModify), modifiers*iterations; got != want {
		t.Errorf("modify count = %d, want %d", got, want)
	}
	attrs, ok := f.Entry(stableDN)
	if !ok {
		t.Fatalf("entry %s missing", stableDN)
	}
	if got := attrs["cn"]; len(got) != 1 || got[0] != "alice" {
		t.Errorf("cn = %q, want [alice]", got)
	}
	if got := attrs["mail"]; len(got) != 2 {
		t.Errorf("mail = %q, want two values", got)
	}
	for name := range attrs {
		if strings.HasPrefix(name, "tmp") {
			t.Errorf("attribute %q left behind", name)
		}
	}
}

// TestFixMessageID checks the wire repair of message IDs that the server
// library's encoder gets wrong, across ID widths and length-field boundaries.
func TestFixMessageID(t *testing.T) {
	ids := []int{0, 1, 127, 128, 133, 255, 256, 32767, 32768, 65535, 65536, 8388608}
	for _, id := range ids {
		// Vary the body size around the 127/128 and 255/256 length boundaries.
		for pad := 90; pad < 300; pad += 7 {
			re := ldapserver.NewSearchResultEntry("cn=" + strings.Repeat("x", pad) + ",dc=example,dc=com")
			m := msg.NewLDAPMessageWithProtocolOp(re)
			m.SetMessageID(id)
			b, err := m.Write()
			if err != nil {
				t.Fatalf("write id %d: %v", id, err)
			}
			fixed := fixMessageID(b.Bytes())
			got, err := msg.ReadLDAPMessage(msg.NewBytes(0, fixed))
			if err != nil {
				t.Fatalf("id %d pad %d: fixed message does not parse: %v", id, pad, err)
			}
			if got.MessageID().Int() != id {
				t.Fatalf("id %d pad %d: decoded as %d", id, pad, got.MessageID().Int())
			}
		}
	}
}
