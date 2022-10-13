package ldappool

import (
	"context"
	"time"
)

func (dir *Directory) connectionOpener(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-dir.openerCh:
			dir.openNewConnection(ctx)
		}
	}
}

func (dir *Directory) startCleanerLocked() {
	if (dir.maxLifetime > 0 || dir.maxIdleTime > 0) && dir.numOpen > 0 && dir.cleanerCh == nil {
		dir.cleanerCh = make(chan struct{}, 1)
		go dir.connectionCleaner(dir.shortestIdleTimeLocked())
	}
}

func (dir *Directory) connectionCleaner(d time.Duration) {
	const minInterval = time.Second

	if d < minInterval {
		d = minInterval
	}
	t := time.NewTimer(d)

	for {
		select {
		case <-t.C:
		case <-dir.cleanerCh:
		}

		dir.mu.Lock()

		d = dir.shortestIdleTimeLocked()
		if dir.closed || dir.numOpen == 0 || d <= 0 {
			dir.cleanerCh = nil
			dir.mu.Unlock()
			return
		}

		d, closing := dir.connectionCleanerRunLocked(d)
		dir.mu.Unlock()
		for _, c := range closing {
			c.close()
		}

		if d < minInterval {
			d = minInterval
		}

		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		t.Reset(d)
	}
}
func (dir *Directory) connectionCleanerRunLocked(d time.Duration) (time.Duration, []*conn) {
	var idleClosing int64
	var closing []*conn
	if dir.maxIdleTime > 0 {
		idleSince := time.Now().Add(-dir.maxIdleTime)
		for i := 0; i < len(dir.freeConn); i++ {
			c := dir.freeConn[i]
			if c.returnedAt.Before(idleSince) {
				closing = append(closing, c)
				last := len(dir.freeConn) - 1
				dir.freeConn[i] = dir.freeConn[last]
				dir.freeConn = dir.freeConn[:last]
				i--
				continue
			} else if d2 := c.returnedAt.Sub(idleSince); d2 < d {
				d = d2
			}
		}
		idleClosing = int64(len(closing))
		dir.maxIdleTimeClosed += idleClosing
	}

	if dir.maxLifetime > 0 {
		expiredSince := time.Now().Add(-dir.maxLifetime)
		for i := 0; i < len(dir.freeConn); i++ {
			c := dir.freeConn[i]
			if c.createdAt.Before(expiredSince) {
				closing = append(closing, c)
				last := len(dir.freeConn) - 1
				dir.freeConn[i] = dir.freeConn[last]
				dir.freeConn = dir.freeConn[:last]
				i--
			} else if d2 := c.createdAt.Sub(expiredSince); d2 < d {
				d = d2
			}
		}
		dir.maxLifetimeClosed += int64(len(closing)) - idleClosing
	}

	return d, closing
}
