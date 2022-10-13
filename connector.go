package ldappool

import (
	"container/heap"
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-ldap/ldap/v3"
)

const (
	badConnPenalty = 2 * time.Second // penalty added to server latency when they go bad
	decayRate      = 0.98            // in times per second
)

type connector struct {
	servers []*Server

	domain      string
	opt         Option
	nextId      int
	mu          sync.Mutex
	lastDecayed time.Time
}

type Option struct {
	StartTLS  bool
	TLSConfig *tls.Config

	// by default the connector will use the scheme (ldap/ldaps) returned by net.LookupSRV.
	// setting LDAPsPort to a non-zero value will force ldaps on all ldap scheme at the specified port
	LDAPsPort uint16

	BindFunc func(ctx context.Context, conn *ldap.Conn, server *Server) error

	Dialer *net.Dialer

	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

func newSRVConnector(domain string, opts ...*Option) (*connector, error) {
	var opt Option
	if len(opts) > 0 {
		opt = *opts[0]
	}
	if opt.TLSConfig == nil {
		opt.TLSConfig = new(tls.Config)
	}
	hasTLS := make(map[string]struct{})
	c := new(connector)
	c.domain = domain
	c.opt = opt
	i := 0
	if opt.LDAPsPort == 0 {
		_, addrs, err := net.LookupSRV("ldaps", "tcp", domain)
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			addr.Target = strings.Trim(addr.Target, ".")
			c.servers = append(c.servers, &Server{
				SRV:     addr,
				Service: "ldaps",
				latency: 1 * time.Millisecond,
				index:   i,
			})
			i++
			hasTLS[addr.Target] = struct{}{}
		}
	}

	_, addrs, err := net.LookupSRV("ldap", "tcp", domain)
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		addr.Target = strings.Trim(addr.Target, ".")
		if _, ok := hasTLS[addr.Target]; !ok {
			c.servers = append(c.servers, &Server{
				SRV:     addr,
				Service: "ldap",
				latency: 1 * time.Millisecond,
				index:   i,
			})
			i++
		}
	}

	heap.Init((*srvHeap)(&c.servers))
	return c, nil
}

func (c *connector) getNextId() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextId
	c.nextId++
	return id
}

func (c *connector) connect(ctx context.Context) (*conn, error) {
	c.mu.Lock()
	if len(c.servers) == 0 {
		c.mu.Unlock()
		return nil, ErrNoServerDefined
	}

	// if latency==0, then must shuffle
	var i int
	for i = 0; i < len(c.servers); i++ {
		if c.servers[i].latency > 0 {
			break
		}
	}
	rand.Shuffle(i, srvHeap(c.servers).Swap)
	s := c.servers[0]
	c.mu.Unlock()

	var internal *ldap.Conn
	var err error

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	checkBadConn := func(err error) {
		if IsBadConn(err) {
			fmt.Println(err)
			c.updateLatency(s, -1)
		}
	}
	tc := c.opt.TLSConfig.Clone()
	tc.ServerName = s.Target
	addr := url.URL{
		Scheme: s.Service,
	}
	port := s.Port
	if c.opt.LDAPsPort != 0 && s.Service == "ldap" {
		port = c.opt.LDAPsPort
		addr.Scheme = "ldaps"
	}
	addr.Host = s.Target + ":" + strconv.FormatUint(uint64(port), 10)

	id := c.getNextId()
	start := time.Now()
	internal, err = ldap.DialURL(addr.String(), ldap.DialWithTLSDialer(tc, c.opt.Dialer))
	if err != nil {
		checkBadConn(err)
		return nil, err
	}
	if addr.Scheme == "ldap" && c.opt.StartTLS {
		if err = internal.StartTLS(tc); err != nil {
			checkBadConn(err)
			return nil, err
		}
	}
	c.updateLatency(s, time.Since(start))

	if c.opt.BindFunc != nil {
		if err = c.opt.BindFunc(ctx, internal, s); err != nil {
			checkBadConn(err)
			return nil, err
		}
	}

	return &conn{
		id:         id,
		url:        addr,
		server:     s,
		createdAt:  time.Now(),
		returnedAt: time.Now(),
		internal:   internal,
	}, nil
}

func (c *connector) updateLatency(s *Server, dt time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if dt < 0 {
		s.latency += badConnPenalty
	} else {
		s.latency = (s.latency + dt*2) / 3
	}
	heap.Fix((*srvHeap)(&c.servers), s.index)
}

func (c *connector) decayLatencies(except *Server) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lastDecayed.IsZero() {
		c.lastDecayed = time.Now()
		return
	}
	dt := time.Since(c.lastDecayed)
	c.lastDecayed = time.Now()

	if len(c.servers) <= 1 {
		return
	}

	factor := math.Pow(decayRate, float64(dt)/float64(time.Second))

	for _, s := range c.servers {
		if s == except {
			continue
		}
		s.latency = time.Duration(float64(s.latency) * factor)
	}
	heap.Fix((*srvHeap)(&c.servers), except.index)
}
