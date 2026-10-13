package ldaptest

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-ldap/ldap/v3"
)

// proxyFake starts a TCP fake and a loopback proxy in front of it. The proxy's
// cleanup is registered after the fake's, so it runs first (cleanups are last in,
// first out) and the fake never waits on a connection the proxy still holds.
func proxyFake(t *testing.T) (*Fake, *Proxy) {
	t.Helper()
	f := faultFake(t)
	p, err := NewProxy(f.Addr())
	if err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	t.Cleanup(p.Close)
	return f, p
}

// proxyDial dials the proxy and waits until the fake has accepted the upstream
// connection. The client is closed on test cleanup, before the proxy.
func proxyDial(t *testing.T, f *Fake, p *Proxy) *ldap.Conn {
	t.Helper()
	c, err := ldap.DialURL("ldap://" + p.Addr())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	eventually(t, 2*time.Second, func() bool { return f.NumConns() >= 1 })
	return c
}

// searchAsync runs a search on c in its own goroutine and delivers its error.
func searchAsync(c *ldap.Conn) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- faultSearch(c) }()
	return ch
}

// waitErr waits up to 5 seconds for a result from ch.
func waitErr(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not return within 5s", what)
		return nil
	}
}

// TestFaultResetIdle: an RST on an idle connection flips IsClosing, and the next
// operation is a coded network error.
func TestFaultResetIdle(t *testing.T) {
	verifyNoLeaks(t)
	f, p := proxyFake(t)
	c := proxyDial(t, f, p)

	p.Reset()
	t.Logf("reset idle: IsClosing flipped after %v", flipLatency(t, c))

	if got := Classify(faultSearch(c)); got != ClassNetwork {
		t.Errorf("search after reset classified %v, want %v", got, ClassNetwork)
	}
}

// TestFaultResetMidOperation: an RST while a search is in flight fails the
// search promptly and flips IsClosing.
func TestFaultResetMidOperation(t *testing.T) {
	verifyNoLeaks(t)
	f, p := proxyFake(t)
	f.SetBehavior(func(op string) Behavior { return Behavior{Hang: op == OpSearch} })
	c := proxyDial(t, f, p)

	res := searchAsync(c)
	eventually(t, 2*time.Second, func() bool { return f.Count(OpSearch) == 1 })
	p.Reset()

	err := waitErr(t, res, "search after reset")
	if !netOrPlain(err) {
		t.Errorf("search class = %v, want network or plain", Classify(err))
	}
	flipLatency(t, c)
}

// TestFaultHalfOpenIdle: a half-open path is invisible to an idle client.
// IsClosing must stay false for a bounded observation window; the one fixed wait
// in these tests.
func TestFaultHalfOpenIdle(t *testing.T) {
	verifyNoLeaks(t)
	f, p := proxyFake(t)
	c := proxyDial(t, f, p)

	p.Pause()
	window := time.NewTimer(700 * time.Millisecond)
	defer window.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-window.C:
			return
		case <-tick.C:
			if c.IsClosing() {
				t.Fatal("IsClosing flipped on a half-open connection")
			}
		}
	}
}

// TestFaultHalfOpenOperation: on a half-open path an operation only fails by
// its client timeout, as a network error, and the connection is left open.
func TestFaultHalfOpenOperation(t *testing.T) {
	verifyNoLeaks(t)
	f, p := proxyFake(t)
	c := proxyDial(t, f, p)
	c.SetTimeout(200 * time.Millisecond)

	p.Pause()
	err := waitErr(t, searchAsync(c), "search on half-open connection")
	if got := Classify(err); got != ClassNetwork {
		t.Errorf("search on half-open connection classified %v, want %v", got, ClassNetwork)
	}
	if c.IsClosing() {
		t.Error("connection closing after a timeout; it should be left open")
	}
}

// TestFaultBlackholeAtConnect: with the proxy swallowing traffic from accept the
// dial still succeeds (go-ldap does no exchange at dial) and the first
// operation times out as a network error.
func TestFaultBlackholeAtConnect(t *testing.T) {
	verifyNoLeaks(t)
	f, p := proxyFake(t)
	p.PauseNew.Store(true)
	c := proxyDial(t, f, p)
	c.SetTimeout(200 * time.Millisecond)

	ch := make(chan error, 1)
	go func() { ch <- c.Bind("cn=admin,dc=example,dc=com", "secret") }()
	err := waitErr(t, ch, "bind through blackhole")
	if got := Classify(err); got != ClassNetwork {
		t.Errorf("bind through blackhole classified %v, want %v", got, ClassNetwork)
	}
	if c.IsClosing() {
		t.Error("connection closing after a timeout; it should be left open")
	}
	if got := f.Count(OpBind); got != 0 {
		t.Errorf("fake received %d binds through the blackhole, want 0", got)
	}
}

// TestFaultCloseWhileHalfOpen: Close on a half-open connection with an operation
// in flight returns promptly and releases the blocked operation.
func TestFaultCloseWhileHalfOpen(t *testing.T) {
	verifyNoLeaks(t)
	f, p := proxyFake(t)
	c := proxyDial(t, f, p)

	p.Pause()
	res := searchAsync(c)

	closed := make(chan struct{})
	go func() {
		_ = c.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2s on a half-open connection")
	}

	err := waitErr(t, res, "blocked search after Close")
	if !netOrPlain(err) {
		t.Errorf("blocked search class = %v, want network or plain", Classify(err))
	}
}

// gatedProxy starts a proxy whose upstream dial blocks until release is called.
// entered receives one value for every dial that starts, so a test can wait until
// the proxy has accepted a connection and is inside its upstream dial. Cleanups
// run last in, first out: release is registered after Close, so it runs first and
// Close never waits on a blocked dial.
func gatedProxy(t *testing.T, f *Fake) (p *Proxy, entered <-chan struct{}, release func()) {
	t.Helper()
	in := make(chan struct{}, 16)
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	hook := func(network, address string) (net.Conn, error) {
		in <- struct{}{}
		<-gate
		return net.Dial(network, address)
	}
	p, err := newProxy(f.Addr(), hook)
	if err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	t.Cleanup(p.Close)
	t.Cleanup(release)
	return p, in, release
}

// waitEntered waits up to 5 seconds for the proxy to enter its upstream dial.
func waitEntered(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("proxy did not start its upstream dial within 5s")
	}
}

// TestProxyResetDuringUpstreamDial: a Reset issued while the proxy is still
// dialing upstream reaches the connection (WR-01); the proxy then forgets the pair
// and leaks nothing.
func TestProxyResetDuringUpstreamDial(t *testing.T) {
	verifyNoLeaks(t)
	f := faultFake(t)
	p, entered, release := gatedProxy(t, f)

	c, err := ldap.DialURL("ldap://" + p.Addr())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	waitEntered(t, entered)

	p.Reset()
	eventually(t, 2*time.Second, c.IsClosing)
	if err := faultSearch(c); !netOrPlain(err) {
		t.Errorf("search after reset class = %v, want network or plain", Classify(err))
	}

	release()
	eventually(t, 2*time.Second, func() bool { return len(p.snapshot()) == 0 })
}

// TestProxyPauseDuringUpstreamDial: a Pause issued while the proxy is still
// dialing upstream holds the connection once the dial finishes (WR-01).
func TestProxyPauseDuringUpstreamDial(t *testing.T) {
	verifyNoLeaks(t)
	f := faultFake(t)
	p, entered, release := gatedProxy(t, f)

	c, err := ldap.DialURL("ldap://" + p.Addr())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	waitEntered(t, entered)

	p.Pause()
	release()
	eventually(t, 2*time.Second, func() bool { return f.NumConns() >= 1 })

	c.SetTimeout(200 * time.Millisecond)
	err = waitErr(t, searchAsync(c), "search on connection paused during dial")
	if got := Classify(err); got != ClassNetwork {
		t.Errorf("search classified %v, want %v", got, ClassNetwork)
	}
	if got := f.Count(OpSearch); got != 0 {
		t.Errorf("fake received %d searches through a paused connection, want 0", got)
	}
	if c.IsClosing() {
		t.Error("connection closing after a timeout; it should be left open")
	}
}

// TestProxyResumeReleasesAndClearsPauseNew: Resume releases data held by PauseNew
// and also clears PauseNew, so a connection accepted afterwards forwards (WR-04).
func TestProxyResumeReleasesAndClearsPauseNew(t *testing.T) {
	verifyNoLeaks(t)
	f, p := proxyFake(t)
	p.PauseNew.Store(true)
	c1 := proxyDial(t, f, p)

	res := searchAsync(c1)
	p.Resume()
	if err := waitErr(t, res, "search held by PauseNew"); err != nil {
		t.Errorf("held search after Resume: %v", err)
	}
	if p.PauseNew.Load() {
		t.Error("PauseNew still set after Resume")
	}

	c2, err := ldap.DialURL("ldap://" + p.Addr())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { _ = c2.Close() })
	eventually(t, 2*time.Second, func() bool { return f.NumConns() >= 2 })
	c2.SetTimeout(2 * time.Second)
	if err := faultSearch(c2); err != nil {
		t.Errorf("search on a connection accepted after Resume: %v", err)
	}
}
