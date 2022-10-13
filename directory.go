package ldappool

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxBadConnRetries = 2
)

type Directory struct {
	waitDuration atomic.Int64

	Name      string
	connector *connector
	// numClosed

	mu                sync.Mutex
	freeConn          []*conn
	connRequests      map[uint64]chan connRequest
	nextRequest       uint64
	numOpen           int
	openerCh          chan struct{}
	closed            bool
	maxIdleCount      int
	maxOpen           int
	maxOpenPerServer  int
	maxLifetime       time.Duration
	maxIdleTime       time.Duration
	cleanerCh         chan struct{}
	waitCount         int64
	maxIdleClosed     int64
	maxIdleTimeClosed int64
	maxLifetimeClosed int64

	stop func()
}

var (
	connectionRequestQueueSize = 10000 // // 1Mil
)

func OpenDirectory(domain string, opt ...*Option) (*Directory, error) {
	c, err := newSRVConnector(domain, opt...)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	dir := &Directory{
		Name:         domain,
		connector:    c,
		openerCh:     make(chan struct{}, connectionRequestQueueSize),
		connRequests: make(map[uint64]chan connRequest),
		stop:         cancel,
	}

	go dir.connectionOpener(ctx)

	if len(opt) > 0 {
		o := opt[0]
		if o.MaxOpenConns != 0 {
			dir.SetMaxOpenConns(o.MaxOpenConns)
		}
		if o.MaxIdleConns != 0 {
			dir.SetMaxIdleConns(o.MaxIdleConns)
		}
		if o.ConnMaxLifetime != 0 {
			dir.SetConnMaxLifetime(o.ConnMaxLifetime)
		}
		if o.ConnMaxIdleTime != 0 {
			dir.SetConnMaxIdleTime(o.ConnMaxIdleTime)
		}
	}

	return dir, nil
}

func (dir *Directory) SetMaxOpenConns(n int) {
	dir.mu.Lock()
	dir.maxOpen = n
	if n < 0 {
		dir.maxOpen = 0
	}
	syncMaxIdle := dir.maxOpen > 0 && dir.maxIdleConnsLocked() > dir.maxOpen
	dir.mu.Unlock()
	if syncMaxIdle {
		dir.SetMaxIdleConns(n)
	}
}
func (dir *Directory) SetMaxIdleConns(n int) {
	dir.mu.Lock()
	if n > 0 {
		dir.maxIdleCount = n
	} else {
		dir.maxIdleClosed = -1
	}
	if dir.maxOpen > 0 && dir.maxIdleConnsLocked() > dir.maxOpen {
		dir.maxIdleCount = dir.maxOpen
	}
	var closing []*conn
	idleCount := len(dir.freeConn)
	maxIdle := dir.maxIdleConnsLocked()
	if idleCount > maxIdle {
		closing = dir.freeConn[maxIdle:]
		dir.freeConn = dir.freeConn[:maxIdle]
	}
	dir.maxIdleClosed += int64(len(closing))
	dir.mu.Unlock()
	for _, c := range closing {
		c.close()
	}
}
func (dir *Directory) SetConnMaxLifetime(d time.Duration) {
	if d < 0 {
		d = 0
	}
	dir.mu.Lock()
	if d > 0 && d < dir.maxLifetime && dir.cleanerCh != nil {
		select {
		case dir.cleanerCh <- struct{}{}:
		default:
		}
	}
	dir.maxLifetime = d
	dir.startCleanerLocked()
	dir.mu.Unlock()
}
func (dir *Directory) SetConnMaxIdleTime(d time.Duration) {
	if d < 0 {
		d = 0
	}
	dir.mu.Lock()
	defer dir.mu.Unlock()
	if d > 0 && d < dir.maxIdleTime && dir.cleanerCh != nil {
		select {
		case dir.cleanerCh <- struct{}{}:
		default:
		}
	}
	dir.maxIdleTime = d
	dir.startCleanerLocked()
}

func (dir *Directory) openNewConnection(ctx context.Context) {
	c, err := dir.connector.connect(ctx)
	dir.mu.Lock()
	defer dir.mu.Unlock()
	if dir.closed {
		if err == nil {
			c.close()
		}
		dir.numOpen--
		return
	}
	if err != nil {
		dir.numOpen--
		dir.putConnDirLocked(nil, err)
		dir.maybeOpenNewConnections()
		return
	}
	c.dir = dir
	c.createdAt = time.Now()
	c.returnedAt = time.Now()
	if !dir.putConnDirLocked(c, err) {
		dir.numOpen--
		c.close()
	}
}

func (dir *Directory) putConnDirLocked(conn *conn, err error) bool {
	if dir.closed {
		return false
	}
	if dir.maxOpen > 0 && dir.numOpen > dir.maxOpen {
		return false
	}
	if c := len(dir.connRequests); c > 0 {
		var req chan connRequest
		var reqKey uint64
		for reqKey, req = range dir.connRequests {
			break
		}
		delete(dir.connRequests, reqKey)
		if err == nil {
			conn.inUse = true
		}
		req <- connRequest{
			conn: conn,
			err:  err,
		}
		return true
	} else if err == nil && !dir.closed {
		if dir.maxIdleConnsLocked() > len(dir.freeConn) {
			dir.freeConn = append(dir.freeConn, conn)
			dir.startCleanerLocked()
			return true
		}
		dir.maxIdleClosed++
	}
	return false
}

func (dir *Directory) maybeOpenNewConnections() {
	numRequests := len(dir.connRequests)
	if dir.maxOpen > 0 {
		numCanOpen := dir.maxOpen - dir.numOpen
		if numRequests > numCanOpen {
			numRequests = numCanOpen
		}
	}
	for numRequests > 0 {
		dir.numOpen++
		numRequests--
		if dir.closed {
			return
		}
		dir.openerCh <- struct{}{}
	}
}

const defaultMaxIdleConns = 2

func (dir *Directory) maxIdleConnsLocked() int {
	n := dir.maxIdleCount
	switch {
	case n == 0:
		return defaultMaxIdleConns
	case n < 0:
		return 0
	default:
		return n
	}
}

func (dir *Directory) shortestIdleTimeLocked() time.Duration {
	if dir.maxIdleTime <= 0 {
		return dir.maxLifetime
	}
	if dir.maxLifetime <= 0 {
		return dir.maxIdleTime
	}

	min := dir.maxIdleTime
	if min > dir.maxLifetime {
		min = dir.maxLifetime
	}
	return min
}

type connRequest struct {
	conn *conn
	err  error
}

type DBStats = sql.DBStats

func (dir *Directory) Stats() DBStats {
	wait := dir.waitDuration.Load()

	dir.mu.Lock()
	defer dir.mu.Unlock()

	stats := DBStats{
		MaxOpenConnections: dir.maxOpen,

		Idle:            len(dir.freeConn),
		OpenConnections: dir.numOpen,
		InUse:           dir.numOpen - len(dir.freeConn),

		WaitCount:         dir.waitCount,
		WaitDuration:      time.Duration(wait),
		MaxIdleClosed:     dir.maxIdleClosed,
		MaxIdleTimeClosed: dir.maxIdleTimeClosed,
		MaxLifetimeClosed: dir.maxLifetimeClosed,
	}
	return stats
}

type connReuseStrategy uint8

const (
	alwaysNewConn connReuseStrategy = iota
	cachedOrNewConn
)

func (dir *Directory) retry(fn func(strategy connReuseStrategy) error) error {
	for i := int64(0); i < maxBadConnRetries; i++ {
		err := fn(cachedOrNewConn)
		if err == nil || !IsBadConn(err) {
			return err
		}
	}

	return fn(alwaysNewConn)
}

func (dir *Directory) conn(ctx context.Context, strategy connReuseStrategy) (*conn, error) {
	dir.mu.Lock()
	if dir.closed {
		dir.mu.Unlock()
		return nil, ErrDirClosed
	}
	select {
	default:
	case <-ctx.Done():
		dir.mu.Unlock()
		return nil, ctx.Err()
	}
	lifetime := dir.maxLifetime

	last := len(dir.freeConn) - 1
	if strategy == cachedOrNewConn && last >= 0 {
		c := dir.freeConn[last]
		dir.freeConn = dir.freeConn[:last]
		c.inUse = true
		if c.expired(lifetime) {
			dir.maxLifetimeClosed++
			dir.mu.Unlock()
			c.close()
			return nil, ErrConnExpired
		}
		dir.mu.Unlock()

		if err := c.rebind(ctx); IsBadConn(err) {
			c.close()
			return nil, err
		}

		return c, nil
	}

	if dir.maxOpen > 0 && dir.numOpen >= dir.maxOpen {
		req := make(chan connRequest, 1)
		reqKey := dir.nextRequestKeyLocked()
		dir.connRequests[reqKey] = req
		dir.waitCount++
		dir.mu.Unlock()

		waitStart := time.Now()

		select {
		case <-ctx.Done():
			dir.mu.Lock()
			delete(dir.connRequests, reqKey)
			dir.mu.Unlock()

			dir.waitDuration.Add(int64(time.Since(waitStart)))

			select {
			default:
			case ret, ok := <-req:
				if ok && ret.conn != nil {
					dir.putConn(ret.conn, ret.err)
				}
			}
			return nil, ctx.Err()
		case ret, ok := <-req:
			dir.waitDuration.Add(int64(time.Since(waitStart)))

			if !ok {
				return nil, ErrDirClosed
			}
			if strategy == cachedOrNewConn && ret.err == nil && ret.conn.expired(lifetime) {
				dir.mu.Lock()
				dir.maxLifetimeClosed++
				dir.mu.Unlock()
				ret.conn.close()
				return nil, ErrConnExpired
			}
			if ret.conn == nil {
				return nil, ret.err
			}

			if err := ret.conn.rebind(ctx); IsBadConn(err) {
				ret.conn.close()
				return nil, err
			}

			return ret.conn, ret.err
		}
	}

	dir.numOpen++
	dir.mu.Unlock()
	c, err := dir.connector.connect(ctx)
	if err != nil {
		dir.mu.Lock()
		dir.numOpen--
		dir.maybeOpenNewConnections()
		dir.mu.Unlock()
		return nil, err
	}
	// sql locks cuz has deps
	c.dir = dir
	c.inUse = true
	return c, nil
}

func (dir *Directory) putConn(c *conn, err error) {
	if !IsBadConn(err) {
		if err2 := c.validateConnection(); err2 != nil {
			// TODO might not work, sql sets err=ErrBadConn
			err = err2
		}
	}
	dir.connector.decayLatencies(c.server)
	dir.mu.Lock()
	if !c.inUse {
		dir.mu.Unlock()
		panic("ad: connection returned that was never out")
	}

	if !IsBadConn(ErrBadConn) && c.expired(dir.maxLifetime) {
		dir.maxLifetimeClosed++
		err = ErrConnExpired
	}
	c.inUse = false
	c.returnedAt = time.Now()

	if IsBadConn(err) {
		dir.maybeOpenNewConnections()
		dir.mu.Unlock()
		c.close()
		return
	}
	added := dir.putConnDirLocked(c, nil)
	dir.mu.Unlock()

	if !added {
		c.close()
		return
	}
}

func (dir *Directory) nextRequestKeyLocked() uint64 {
	next := dir.nextRequest
	dir.nextRequest++
	return next
}

func (dir *Directory) PrintSRVs() {
	dir.connector.mu.Lock()
	defer dir.connector.mu.Unlock()
	ss := dir.connector.servers
	hs := make([]string, len(ss))
	for i, s := range ss {
		p := strings.Split(s.Target, ".")
		hs[i] = fmt.Sprintf("%s=%s", p[0], s.latency.String())
	}
	fmt.Println("\t" + strings.Join(hs, " "))
}
