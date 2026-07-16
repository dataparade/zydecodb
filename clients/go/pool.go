package zydecodb

import (
	"context"
	"errors"
	"sync"
	"time"
)

// errPoolClosed is returned by the pool once Close has been called.
var errPoolClosed = errors.New("zydecodb: pool is closed")

// pool is a bounded, concurrency-safe connection pool. Connections are created
// lazily up to maxSize. The sem channel meters total live connections (idle +
// checked out); free holds idle connections ready for reuse. Idle connections
// older than keepaliveIdle are validated with a Ping on checkout.
type pool struct {
	addr          string
	apiKey        string
	timeout       time.Duration
	keepaliveIdle time.Duration

	sem  chan struct{} // capacity = maxSize; one token per live connection
	free chan *conn    // capacity = maxSize; idle connections

	mu     sync.Mutex
	closed bool
}

func newPool(addr, apiKey string, timeout time.Duration, maxSize int) *pool {
	if maxSize < 1 {
		maxSize = 1
	}
	return &pool{
		addr:          addr,
		apiKey:        apiKey,
		timeout:       timeout,
		keepaliveIdle: 30 * time.Second,
		sem:           make(chan struct{}, maxSize),
		free:          make(chan *conn, maxSize),
	}
}

func (p *pool) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// acquire checks out a healthy connection, creating one if capacity allows or
// waiting for one to be returned otherwise. It honors ctx cancellation.
func (p *pool) acquire(ctx context.Context) (*conn, error) {
	for {
		if p.isClosed() {
			return nil, errPoolClosed
		}
		// Fast path: reuse an idle connection if one is immediately available.
		select {
		case c := <-p.free:
			if healthy, c2 := p.validate(ctx, c); healthy {
				return c2, nil
			}
			// Dead connection: its slot was released by validate; loop to retry.
			continue
		default:
		}

		// Either take a slot to create a new connection or wait for a freed one.
		select {
		case p.sem <- struct{}{}:
			c, err := dial(ctx, p.addr, p.timeout, p.apiKey)
			if err != nil {
				<-p.sem
				return nil, err
			}
			return c, nil
		case c := <-p.free:
			if healthy, c2 := p.validate(ctx, c); healthy {
				return c2, nil
			}
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// validate pings a connection that has been idle past keepaliveIdle. If it is
// dead, the connection is discarded (its slot freed) and (false, nil) returned.
func (p *pool) validate(ctx context.Context, c *conn) (bool, *conn) {
	if time.Since(c.lastUsed) <= p.keepaliveIdle {
		return true, c
	}
	if c.ping(ctx) {
		return true, c
	}
	p.discard(c)
	return false, nil
}

// release returns a connection to the pool, or drops it if it died.
func (p *pool) release(c *conn) {
	if c == nil {
		return
	}
	if c.nc == nil { // transport failed mid-request
		p.discard(c)
		return
	}
	if p.isClosed() {
		p.discard(c)
		return
	}
	select {
	case p.free <- c:
	default:
		// free is sized to maxSize and total live <= maxSize, so this should be
		// unreachable; drop defensively rather than block.
		p.discard(c)
	}
}

// discard permanently drops a connection and frees its slot.
func (p *pool) discard(c *conn) {
	if c == nil {
		return
	}
	c.close()
	select {
	case <-p.sem:
	default:
	}
}

func (p *pool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	for {
		select {
		case c := <-p.free:
			c.close()
			select {
			case <-p.sem:
			default:
			}
		default:
			return
		}
	}
}
