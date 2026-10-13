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
	// forwarded: a blackhole from accept.
	PauseNew atomic.Bool

	ln     net.Listener
	target string
	wg     sync.WaitGroup // every goroutine the proxy started

	mu     sync.Mutex // guards following
	pairs  map[*pair]struct{}
	wake   chan struct{} // closed and replaced by Resume
	closed bool
}

// pair is one client connection and its upstream connection.
type pair struct {
	client, server *net.TCPConn
	paused         bool // guarded by Proxy.mu
	done           chan struct{}
	once           sync.Once
}

// kill closes both sockets, once. With reset the sockets are closed with
// SO_LINGER 0, so the peers see an RST instead of a FIN.
func (pr *pair) kill(reset bool) {
	pr.once.Do(func() {
		if reset {
			_ = pr.client.SetLinger(0)
			_ = pr.server.SetLinger(0)
		}
		_ = pr.client.Close()
		_ = pr.server.Close()
		close(pr.done)
	})
}

// NewProxy starts a proxy on 127.0.0.1 with an ephemeral port that forwards
// every accepted connection to target. Call Close to stop it.
func NewProxy(target string) (*Proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{
		ln:     ln,
		target: target,
		pairs:  make(map[*pair]struct{}),
		wake:   make(chan struct{}),
	}
	p.wg.Add(1)
	go p.acceptLoop()
	return p, nil
}

// Addr returns the proxy's host:port; clients dial this instead of the target.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

func (p *Proxy) acceptLoop() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		pr := &pair{client: c.(*net.TCPConn), server: up.(*net.TCPConn), done: make(chan struct{})}

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			pr.kill(false)
			return
		}
		pr.paused = p.PauseNew.Load()
		p.pairs[pr] = struct{}{}
		p.wg.Add(2)
		p.mu.Unlock()

		go p.pump(pr, pr.server, pr.client)
		go p.pump(pr, pr.client, pr.server)
	}
}

// pump copies src to dst until either side ends, holding each chunk while the
// pair is paused. When it ends it closes both sockets.
func (p *Proxy) pump(pr *pair, dst, src *net.TCPConn) {
	defer p.wg.Done()
	defer func() {
		pr.kill(false)
		p.mu.Lock()
		delete(p.pairs, pr)
		p.mu.Unlock()
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

// Pause stops forwarding in both directions on every live connection. Nothing
// is closed or reset: both peers stay established and see silence (half-open).
func (p *Proxy) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for pr := range p.pairs {
		pr.paused = true
	}
}

// Resume releases data held by Pause or PauseNew and lets forwarding continue.
func (p *Proxy) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for pr := range p.pairs {
		pr.paused = false
	}
	close(p.wake)
	p.wake = make(chan struct{})
}

// Reset closes every live connection with SO_LINGER 0, so both peers see a
// connection reset (RST).
func (p *Proxy) Reset() {
	for _, pr := range p.snapshot() {
		pr.kill(true)
	}
}

// FIN closes every live connection normally, so both peers see a graceful close.
func (p *Proxy) FIN() {
	for _, pr := range p.snapshot() {
		pr.kill(false)
	}
}

// Close stops accepting, closes every live socket including paused ones and
// waits for all of the proxy's goroutines. It is idempotent.
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
