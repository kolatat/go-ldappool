package ldappool

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/go-ldap/ldap/v3"
)

const (
	minValidationDuration = 14 * time.Second
)

type conn struct {
	url       url.URL
	id        int
	server    *Server
	dir       *Directory
	createdAt time.Time

	sync.Mutex    // guards following
	internal      *ldap.Conn
	needRebind    bool
	closed        bool
	lastValidated time.Time

	// guarded by dir.mu
	inUse      bool
	returnedAt time.Time
}

func (c *conn) expired(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	return c.createdAt.Add(timeout).Before(time.Now())
}

func (c *conn) closeDBLocked() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ErrDuplicateConnClose
	}
	c.closed = true
	return nil
}

func (c *conn) close() {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		panic(ErrDuplicateConnClose)
	}
	c.closed = true
	c.internal.Close()

	c.dir.mu.Lock()
	c.dir.numOpen--
	c.dir.maybeOpenNewConnections()
	c.dir.mu.Unlock()
}

func (c *conn) validateConnection() error {
	c.Lock()
	defer c.Unlock()

	if time.Since(c.lastValidated) > minValidationDuration {
		start := time.Now()
		_, err := c.internal.WhoAmI(nil)
		if err == nil {
			l := time.Since(start)
			c.dir.connector.updateLatency(c.server, l)
		}
		c.lastValidated = time.Now()
		return err
	}
	return nil
}

func (c *conn) releaseConn(err error) {
	c.dir.putConn(c, err)
}

func (c *conn) rebind(ctx context.Context) error {
	c.Lock()
	defer c.Unlock()
	if !c.needRebind || c.dir.connector.opt.BindFunc == nil {
		return nil
	}
	return c.dir.connector.opt.BindFunc(ctx, c.internal, c.server)
}
