package ldaptest

import (
	"testing"
	"testing/synctest"
	"time"
)

// These tests run the fake on a net.Pipe inside a synctest bubble: time is
// virtual and goroutines settle at synctest.Wait, so nothing here sleeps for
// real. The bubble only ends when every goroutine in it has returned, which is
// the leak check for this transport (so no goleak). The fake is stopped and the
// client closed explicitly inside the bubble body.

// TestFaultPipeServerCloseFlipsAtWait: a server-side close flips the client's
// IsClosing at the next synctest.Wait, with no virtual time passing.
func TestFaultPipeServerCloseFlipsAtWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := NewPipe(t)
		c, err := f.Dial()
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		synctest.Wait()
		if got := f.NumConns(); got != 1 {
			t.Fatalf("server holds %d connections, want 1", got)
		}
		if c.IsClosing() {
			t.Fatal("IsClosing true before any fault")
		}

		start := time.Now()
		f.CloseConns()
		synctest.Wait()
		if !c.IsClosing() {
			t.Error("IsClosing false after CloseConns and synctest.Wait")
		}
		if d := time.Since(start); d != 0 {
			t.Errorf("virtual time advanced by %v, want none", d)
		}
		if got := Classify(faultSearch(c)); got != ClassNetwork {
			t.Errorf("search after close classified %v, want %v", got, ClassNetwork)
		}

		_ = c.Close()
		f.Stop()
	})
}

// TestFaultPipeSlowResponseVirtualTime: a reply slower than the client timeout
// fails as a network error after the timeout in virtual time, and the same
// connection serves a later request once the late reply has been discarded.
func TestFaultPipeSlowResponseVirtualTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := NewPipe(t)
		f.AddEntry(faultDN, map[string][]string{"cn": {"alice"}})
		f.SetBehavior(func(op string) Behavior {
			if op == OpSearch {
				return Behavior{Delay: 300 * time.Millisecond}
			}
			return Behavior{}
		})
		c, err := f.Dial()
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		c.SetTimeout(100 * time.Millisecond)

		start := time.Now()
		err = faultSearch(c)
		took := time.Since(start)
		if got := Classify(err); got != ClassNetwork {
			t.Errorf("slow search classified %v, want %v", got, ClassNetwork)
		}
		if took < 100*time.Millisecond || took >= 300*time.Millisecond {
			t.Errorf("slow search took %v of virtual time, want at least 100ms and under 300ms", took)
		}
		if c.IsClosing() {
			t.Error("connection closing after a request timeout")
		}

		f.SetBehavior(nil)
		time.Sleep(400 * time.Millisecond) // let the delayed reply arrive and be discarded
		synctest.Wait()
		if err := faultSearch(c); err != nil {
			t.Errorf("search after the late reply: %v (class %v)", err, Classify(err))
		}

		_ = c.Close()
		f.Stop()
	})
}

// TestFaultPipeNoticeOfDisconnection: stopping the server with an idle client
// flips IsClosing at the next synctest.Wait.
func TestFaultPipeNoticeOfDisconnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := NewPipe(t)
		c, err := f.Dial()
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		synctest.Wait()
		if c.IsClosing() {
			t.Fatal("IsClosing true before Stop")
		}

		f.Stop()
		synctest.Wait()
		if !c.IsClosing() {
			t.Error("IsClosing false after Stop and synctest.Wait")
		}

		_ = c.Close()
	})
}
