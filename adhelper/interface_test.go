package adhelper_test

import (
	"bytes"
	"testing"

	"github.com/go-ldap/ldap/v3"
	"go.uber.org/goleak"

	"github.com/kolatat/go-ldappool/adhelper"
	"github.com/kolatat/go-ldappool/internal/ldaptest"
)

// TestAdhelperResetPasswordOverWire sends an adhelper-built unicodePwd replace
// for a DN containing an escaped comma through a real go-ldap client and asserts
// on the bytes the fake server received.
//
// Only a simple password is used on purpose: adhelper currently encodes
// passwords containing a double quote or a backslash incorrectly, and that
// encoding is replaced in Phase 6 (AD-01), so this test must neither assert nor
// pin it. The package-level encoder in adhelper is shared and not safe for
// concurrent use, so no test in this package may run in parallel mode.
func TestAdhelperResetPasswordOverWire(t *testing.T) {
	// Registered first so that it runs last, after the fake has been stopped.
	t.Cleanup(func() { goleak.VerifyNone(t) })

	const (
		adminDN = "CN=Admin,OU=Users,DC=corp,DC=example,DC=com"
		userDN  = `CN=Doe\, John,OU=Users,DC=corp,DC=example,DC=com`
	)

	f := ldaptest.NewTCP(t)
	f.AddUser(adminDN, "admin-secret")
	f.AddEntry(userDN, map[string][]string{"cn": {"Doe, John"}})

	c, err := f.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if err := c.Bind(adminDN, "admin-secret"); err != nil {
		t.Fatalf("bind: %v", err)
	}

	if _, err := ldap.ParseDN(userDN); err != nil {
		t.Fatalf("ParseDN rejected the test DN: %v", err)
	}

	req := ldap.NewModifyRequest(userDN, nil)
	if err := adhelper.ResetPassword(req, "Passw0rd"); err != nil {
		t.Fatalf("ResetPassword: %v", err)
	}
	if err := c.Modify(req); err != nil {
		t.Fatalf("modify: %v", err)
	}

	log := f.ModifyLog()
	if len(log) != 1 {
		t.Fatalf("modify records = %d, want 1", len(log))
	}
	rec := log[0]
	if rec.DN != userDN {
		t.Errorf("recorded DN = %q, want %q", rec.DN, userDN)
	}
	if len(rec.Changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(rec.Changes))
	}
	ch := rec.Changes[0]
	if ch.Op != 2 {
		t.Errorf("operation = %d, want 2 (replace)", ch.Op)
	}
	if ch.Attr != "unicodePwd" {
		t.Errorf("attribute = %q, want unicodePwd", ch.Attr)
	}
	// "Passw0rd" in double quotes, UTF-16LE.
	want := []byte{
		0x22, 0x00, 0x50, 0x00, 0x61, 0x00, 0x73, 0x00, 0x73, 0x00,
		0x77, 0x00, 0x30, 0x00, 0x72, 0x00, 0x64, 0x00, 0x22, 0x00,
	}
	if len(ch.Vals) != 1 {
		t.Fatalf("values = %d, want 1", len(ch.Vals))
	}
	if !bytes.Equal(ch.Vals[0], want) {
		t.Errorf("value bytes = % x, want % x", ch.Vals[0], want)
	}
}
