package ldaptest

import (
	"net"
	"sync"
	"sync/atomic"
)

// Proxy is a loopback TCP proxy that sits between a client and a Fake and can
// inject network faults that an in-process close cannot: a connection reset
// (RST), a graceful close (FIN), a half-open path where both peers stay
// established and see only silence, and a blackhole from accept.
//
// It forwards bytes in both directions until told otherwise, stores nothing and
// logs nothing. It listens on the IPv4 loopback address only.
type Proxy struct {
	// PauseNew makes every connection accepted from now on start paused, so the
	// kernel completes the TCP handshake (a dial succeeds) but no byte is ever
	// forwarded: a blackhole from accept. It stays set until Resume clears it or
	// the caller stores false.
	PauseNew atomic.Bool

	ln     net.Listener
	target string
	dial   func(network, address string) (net.Conn, error) // upstream dialer, set before the accept goroutine starts
	wg     sync.WaitGroup                                  // every goroutine the proxy started

	mu     sync.Mutex // guards following
	pairs  map[*pair]struct{}
	wake   chan struct{} // closed and replaced by Resume
	closed bool
}

// pair is one client connection and its upstream connection. The pair is
// registered when the client is accepted, before the upstream dial, so the server
// side is attached later by attach.
type pair struct {
	client *net.TCPConn
	paused bool // guarded by Proxy.mu
	done   chan struct{}
	once   sync.Once

	mu     sync.Mutex // guards following
	server *net.TCPConn
	dead   bool
}

// attach stores the upstream connection. It returns false when the pair was
// already killed, in which case the caller owns the upstream socket and must
// close it. Exactly one of attach and kill closes the upstream socket.
func (pr *pair) attach(server *net.TCPConn) bool {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	if pr.dead {
		return false
	}
	pr.server = server
	return true
}

// kill closes both sockets, once. With reset the sockets are closed with
// SO_LINGER 0, so the peers see an RST instead of a FIN. A pair killed before its
// upstream is attached closes only the client; attach then reports false.
func (pr *pair) kill(reset bool) {
	pr.once.Do(func() {
		if reset {
			_ = pr.client.SetLinger(0)
		}
		_ = pr.client.Close()
		pr.mu.Lock()
		pr.dead = true
		server := pr.server
		pr.mu.Unlock()
		if server != nil {
			if reset {
				_ = server.SetLinger(0)
			}
			_ = server.Close()
		}
		close(pr.done)
	})
}

// NewProxy starts a proxy on 127.0.0.1 with an ephemeral port that forwards
// every accepted connection to target. Call Close to stop it.
func NewProxy(target string) (*Proxy, error) {
	return newProxy(target, net.Dial)
}

// newProxy is NewProxy with an injectable upstream dialer. It is a test-only seam:
// a test passes a dialer that blocks to hold a connection inside its upstream dial.
func newProxy(target string, dial func(network, address string) (net.Conn, error)) (*Proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{
		ln:     ln,
		target: target,
		dial:   dial,
		pairs:  make(map[*pair]struct{}),
		wake:   make(chan struct{}),
	}
	p.wg.Add(1)
	go p.acceptLoop()
	return p, nil
}

// Addr returns the proxy's host:port; clients dial this instead of the target.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// acceptLoop registers each pair under p.mu as soon as the client is accepted and
// only then dials upstream, so Pause, Reset and FIN see the connection for the
// whole dial. The fake counts a connection when the upstream dial lands, which can
// be before the pump goroutines start; registering late would let a fault miss it.
func (p *Proxy) acceptLoop() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		pr := &pair{client: c.(*net.TCPConn), done: make(chan struct{})}

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = c.Close()
			return
		}
		pr.paused = p.PauseNew.Load()
		p.pairs[pr] = struct{}{}
		p.mu.Unlock()

		up, err := p.dial("tcp", p.target)
		if err != nil {
			pr.kill(false)
			p.forget(pr)
			continue
		}
		server := up.(*net.TCPConn)
		if !pr.attach(server) {
			_ = server.Close()
			p.forget(pr)
			continue
		}
		// The accept goroutine still holds a wait-group count, so this cannot race
		// Close's Wait.
		p.wg.Add(2)
		go p.pump(pr, server, pr.client)
		go p.pump(pr, pr.client, server)
	}
}

// forget removes pr from the live pairs.
func (p *Proxy) forget(pr *pair) {
	p.mu.Lock()
	delete(p.pairs, pr)
	p.mu.Unlock()
}

// pump copies src to dst until either side ends, holding each chunk while the
// pair is paused. When it ends it closes both sockets.
func (p *Proxy) pump(pr *pair, dst, src *net.TCPConn) {
	defer p.wg.Done()
	defer func() {
		pr.kill(false)
		p.forget(pr)
	}()
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if !p.waitResumed(pr) {
				return
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// waitResumed blocks while pr is paused. It returns false if the pair was
// closed meanwhile.
func (p *Proxy) waitResumed(pr *pair) bool {
	for {
		p.mu.Lock()
		paused, wake := pr.paused, p.wake
		p.mu.Unlock()
		if !paused {
			return true
		}
		select {
		case <-wake:
		case <-pr.done:
			return false
		}
	}
}

// snapshot returns the live pairs.
func (p *Proxy) snapshot() []*pair {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*pair, 0, len(p.pairs))
	for pr := range p.pairs {
		out = append(out, pr)
	}
	return out
}

// Pause stops forwarding in both directions on every connection the proxy has
// accepted by the time of the call, including one still dialing upstream. Nothing
// is closed or reset: both peers stay established and see silence (half-open).
//
// A connection a client has dialed but the proxy has not yet accepted is not
// affected, which is why tests wait for the fake's NumConns (it only counts after
// the proxy's upstream dial) before injecting a fault. Pause is deliberately not
// sticky: connections accepted after it forward normally. PauseNew covers later
// connections, so a pool test can half-open one connection and still dial a
// healthy replacement.
func (p *Proxy) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for pr := range p.pairs {
		pr.paused = true
	}
}

// Resume releases data held by Pause or PauseNew, clears PauseNew and lets
// forwarding continue, so connections accepted afterwards forward normally.
func (p *Proxy) Resume() {
	p.PauseNew.Store(false)
	p.mu.Lock()
	defer p.mu.Unlock()
	for pr := range p.pairs {
		pr.paused = false
	}
	close(p.wake)
	p.wake = make(chan struct{})
}

// Reset closes every connection the proxy has accepted by the time of the call,
// including one still dialing upstream, with SO_LINGER 0, so both peers see a
// connection reset (RST).
func (p *Proxy) Reset() {
	for _, pr := range p.snapshot() {
		pr.kill(true)
	}
}

// FIN closes every connection the proxy has accepted by the time of the call,
// including one still dialing upstream, normally, so both peers see a graceful
// close.
func (p *Proxy) FIN() {
	for _, pr := range p.snapshot() {
		pr.kill(false)
	}
}

// Close stops accepting, closes every accepted connection including paused ones
// and ones still dialing upstream, and waits for all of the proxy's goroutines. It
// is idempotent. A blocked upstream dial delays Close until the dial returns.
func (p *Proxy) Close() {
	p.mu.Lock()
	first := !p.closed
	p.closed = true
	p.mu.Unlock()
	if first {
		_ = p.ln.Close()
	}
	for _, pr := range p.snapshot() {
		pr.kill(false)
	}
	p.wg.Wait()
}
