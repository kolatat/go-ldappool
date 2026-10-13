package ldaptest

import (
	"testing"
	"time"

	"github.com/go-ldap/ldap/v3"
)

const faultDN = "cn=alice,dc=example,dc=com"

// faultFake starts a TCP fake holding one entry, so searches and modifies have
// something to act on.
func faultFake(t *testing.T) *Fake {
	t.Helper()
	f := NewTCP(t)
	f.AddEntry(faultDN, map[string][]string{"cn": {"alice"}, "sn": {"old"}})
	return f
}

// faultDial dials the fake and waits until the server has registered the
// connection, so a server-side close cannot miss it. The client is closed on
// test cleanup (Close is safe on an already closing connection).
func faultDial(t *testing.T, f *Fake) *ldap.Conn {
	t.Helper()
	c, err := f.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	eventually(t, 2*time.Second, func() bool { return f.NumConns() >= 1 })
	return c
}

// faultSearch runs one base-scope search for faultDN and returns its error.
func faultSearch(c *ldap.Conn) error {
	_, err := c.Search(ldap.NewSearchRequest(
		faultDN, ldap.ScopeBaseObject, ldap.NeverDerefAliases,
		0, 0, false, "(objectClass=*)", nil, nil))
	return err
}

// netOrPlain reports whether err is one of the two classes a dropped connection
// may produce, and never an LDAP result or success.
func netOrPlain(err error) bool {
	c := Classify(err)
	return c == ClassNetwork || c == ClassPlain
}

// flipLatency waits for c.IsClosing to turn true within 2 seconds and returns
// how long that took (informational only, never asserted).
func flipLatency(t *testing.T, c *ldap.Conn) time.Duration {
	t.Helper()
	start := time.Now()
	eventually(t, 2*time.Second, c.IsClosing)
	return time.Since(start)
}

// TestFaultIdleClose: the server closes an idle connection (FIN). The client's
// IsClosing flips without any request, and the next operation is a coded
// network error.
func TestFaultIdleClose(t *testing.T) {
	verifyNoLeaks(t)
	f := faultFake(t)
	c := faultDial(t, f)
	if c.IsClosing() {
		t.Fatal("IsClosing true before any fault")
	}

	f.CloseConns()
	t.Logf("idle close: IsClosing flipped after %v", flipLatency(t, c))

	if got := Classify(faultSearch(c)); got != ClassNetwork {
		t.Errorf("search after idle close classified %v, want %v", got, ClassNetwork)
	}
}

// TestFaultCloseMidOperation: the server closes the connection on receiving a
// search. The in-flight search fails with a network-ish error and IsClosing
// flips.
func TestFaultCloseMidOperation(t *testing.T) {
	verifyNoLeaks(t)
	f := faultFake(t)
	f.SetBehavior(func(op string) Behavior { return Behavior{CloseBefore: op == OpSearch} })
	c := faultDial(t, f)

	err := faultSearch(c)
	if !netOrPlain(err) {
		t.Errorf("search class = %v, want network or plain", Classify(err))
	}
	flipLatency(t, c)
	if got := f.Count(OpSearch); got != 1 {
		t.Errorf("search count = %d, want 1", got)
	}
}

// TestFaultApplyThenDrop: the server applies a modify and drops the connection
// without answering. The client only sees an error, but the server saw and
// applied exactly one modify.
func TestFaultApplyThenDrop(t *testing.T) {
	verifyNoLeaks(t)
	f := faultFake(t)
	f.SetBehavior(func(op string) Behavior { return Behavior{CloseAfter: op == OpModify} })
	c := faultDial(t, f)

	req := ldap.NewModifyRequest(faultDN, nil)
	req.Replace("sn", []string{"new"})
	err := c.Modify(req)
	if !netOrPlain(err) {
		t.Errorf("modify class = %v, want network or plain", Classify(err))
	}

	if got := f.Count(OpModify); got != 1 {
		t.Errorf("modify count = %d, want 1", got)
	}
	if got := len(f.ModifyLog()); got != 1 {
		t.Errorf("ModifyLog has %d records, want 1", got)
	}
	attrs, ok := f.Entry(faultDN)
	if !ok {
		t.Fatal("entry missing")
	}
	if got := attrs["sn"]; len(got) != 1 || got[0] != "new" {
		t.Errorf("sn = %v, want [new]: the write was not applied", got)
	}
	flipLatency(t, c)
}

// TestFaultNoticeOfDisconnection: stopping the server sends a Notice of
// Disconnection and closes the connection; an idle client sees IsClosing flip.
// A second Stop is harmless.
func TestFaultNoticeOfDisconnection(t *testing.T) {
	verifyNoLeaks(t)
	f := faultFake(t)
	c := faultDial(t, f)

	f.Stop()
	t.Logf("notice of disconnection: IsClosing flipped after %v", flipLatency(t, c))
	f.Stop()

	if got := Classify(faultSearch(c)); got != ClassNetwork {
		t.Errorf("search after server stop classified %v, want %v", got, ClassNetwork)
	}
}

// TestFaultCloseRace: a search issued immediately after a server-side close
// always fails, as a coded network error or a plain error depending on which
// side of the client's reader goroutine wins. The tally is logged.
func TestFaultCloseRace(t *testing.T) {
	verifyNoLeaks(t)
	f := faultFake(t)
	tally := map[ErrClass]int{}
	for i := 0; i < 20; i++ {
		eventually(t, 2*time.Second, func() bool { return f.NumConns() == 0 })
		c, err := f.Dial()
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		eventually(t, 2*time.Second, func() bool { return f.NumConns() == 1 })

		f.CloseConns()
		err = faultSearch(c)
		_ = c.Close()
		if !netOrPlain(err) {
			t.Fatalf("iteration %d: class = %v, want network or plain", i, Classify(err))
		}
		tally[Classify(err)]++
	}
	t.Logf("search right after server close over 20 connections: network=%d plain=%d",
		tally[ClassNetwork], tally[ClassPlain])
}

// TestFaultSlowResponse: a reply slower than the client timeout is a network
// error, the connection stays open, and the late reply does not break later
// requests.
func TestFaultSlowResponse(t *testing.T) {
	verifyNoLeaks(t)
	f := faultFake(t)
	f.SetBehavior(func(op string) Behavior {
		if op == OpSearch {
			return Behavior{Delay: 300 * time.Millisecond}
		}
		return Behavior{}
	})
	c := faultDial(t, f)
	c.SetTimeout(100 * time.Millisecond)

	if got := Classify(faultSearch(c)); got != ClassNetwork {
		t.Errorf("slow search classified %v, want %v", got, ClassNetwork)
	}
	if c.IsClosing() {
		t.Error("connection closing after a request timeout; it should stay open")
	}

	f.SetBehavior(nil)
	// The late reply to the timed-out request is discarded by the client; the
	// same connection serves the next request.
	eventually(t, 2*time.Second, func() bool { return faultSearch(c) == nil })
	if c.IsClosing() {
		t.Error("connection closing after the late reply")
	}
}

// TestFaultSlowResponseAdequateTimeout: a slow reply within the client timeout
// succeeds, and the call really waited for the injected delay.
func TestFaultSlowResponseAdequateTimeout(t *testing.T) {
	verifyNoLeaks(t)
	f := faultFake(t)
	const delay = 300 * time.Millisecond
	f.SetBehavior(func(op string) Behavior {
		if op == OpSearch {
			return Behavior{Delay: delay}
		}
		return Behavior{}
	})
	c := faultDial(t, f)
	c.SetTimeout(2 * time.Second)

	start := time.Now()
	if err := faultSearch(c); err != nil {
		t.Fatalf("search with adequate timeout: %v (class %v)", err, Classify(err))
	}
	if took := time.Since(start); took < delay/2 {
		t.Errorf("search took %v, want at least %v", took, delay/2)
	}
}

// TestFaultHangThenStop: a hung search times out client-side as a network
// error, and Stop still returns, releasing the hung handler.
func TestFaultHangThenStop(t *testing.T) {
	verifyNoLeaks(t)
	f := faultFake(t)
	f.SetBehavior(func(op string) Behavior { return Behavior{Hang: op == OpSearch} })
	c := faultDial(t, f)
	c.SetTimeout(100 * time.Millisecond)

	if got := Classify(faultSearch(c)); got != ClassNetwork {
		t.Errorf("hung search classified %v, want %v", got, ClassNetwork)
	}

	stopped := make(chan struct{})
	go func() {
		f.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return within 5s with a hung handler")
	}
}
