package ldaptest

import (
	"net"
	"sync"
)

// PipeListener is a net.Listener whose connections are in-memory net.Pipe pairs.
// Because it involves no kernel sockets it works inside a testing/synctest
// bubble, where time is virtual and a test can settle goroutines with
// synctest.Wait instead of sleeping.
//
// A pipe cannot emulate half-open or blackhole faults: a pipe write blocks until
// the peer reads, so a stalled server stalls the client's writer instead of
// timing out as TCP would. Those faults need the TCP Proxy.
type PipeListener struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

// NewPipeListener returns an open PipeListener.
func NewPipeListener() *PipeListener {
	return &PipeListener{
		conns: make(chan net.Conn),
		done:  make(chan struct{}),
	}
}

// Dial creates a pipe, hands the server end to Accept and returns the client
// end. It fails with net.ErrClosed once the listener is closed.
func (l *PipeListener) Dial() (net.Conn, error) {
	server, client := net.Pipe()
	select {
	case l.conns <- server:
		return client, nil
	case <-l.done:
		_ = server.Close()
		_ = client.Close()
		return nil, net.ErrClosed
	}
}

// Accept returns the server end of the next dialled pipe, or net.ErrClosed after
// Close.
func (l *PipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

// Close stops the listener. It is idempotent.
func (l *PipeListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

// Addr returns a fixed placeholder address.
func (l *PipeListener) Addr() net.Addr { return pipeAddr{} }

// pipeAddr is the address of every pipe listener.
type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }
