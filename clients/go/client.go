package zydecodb

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	mrand "math/rand"
	"time"
)

// Client is a pooled, retrying ZydecoDB connection. It is safe for concurrent
// use by multiple goroutines. Transient transport failures and server
// EngineBusy responses are retried with exponential backoff for operations that
// are safe to repeat; non-idempotent operations (operator updates, deletes) are
// never retried automatically.
type Client struct {
	pool        *pool
	maxRetries  int
	backoffBase time.Duration
	backoffCap  time.Duration
}

// Option configures a Client.
type Option func(*config)

type config struct {
	apiKey      string
	timeout     time.Duration
	poolSize    int
	maxRetries  int
	backoffBase time.Duration
	backoffCap  time.Duration
	tlsConf     *tls.Config
}

// WithAPIKey authenticates each connection with a SessionInit handshake.
func WithAPIKey(key string) Option { return func(c *config) { c.apiKey = key } }

// WithTimeout sets the per-request I/O timeout (default 5s).
func WithTimeout(d time.Duration) Option { return func(c *config) { c.timeout = d } }

// WithPoolSize sets the maximum number of pooled connections (default 8).
func WithPoolSize(n int) Option { return func(c *config) { c.poolSize = n } }

// WithMaxRetries sets how many times idempotent operations are retried on
// transient failures (default 2).
func WithMaxRetries(n int) Option { return func(c *config) { c.maxRetries = n } }

// WithTLS wraps every connection in TLS before the protocol handshake. A nil
// cfg uses sane defaults (system roots). If cfg.ServerName is empty, the SNI
// name is inferred from the dial address, so
// WithTLS(nil) is all a caller needs for a public
// {tenant}.{node}.zydeco.dev endpoint.
func WithTLS(cfg *tls.Config) Option {
	return func(c *config) {
		if cfg == nil {
			cfg = &tls.Config{}
		}
		c.tlsConf = cfg
	}
}

// NewClient connects to a ZydecoDB server at addr (e.g. "127.0.0.1:9470").
func NewClient(addr string, opts ...Option) *Client {
	cfg := config{
		timeout:     5 * time.Second,
		poolSize:    8,
		maxRetries:  2,
		backoffBase: 50 * time.Millisecond,
		backoffCap:  2 * time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.maxRetries < 0 {
		cfg.maxRetries = 0
	}
	return &Client{
		pool:        newPool(addr, cfg.apiKey, cfg.timeout, cfg.poolSize, cfg.tlsConf),
		maxRetries:  cfg.maxRetries,
		backoffBase: cfg.backoffBase,
		backoffCap:  cfg.backoffCap,
	}
}

// Close releases all pooled connections.
func (c *Client) Close() { c.pool.close() }

func (c *Client) backoff(attempt int) time.Duration {
	ceiling := math.Min(float64(c.backoffCap), float64(c.backoffBase)*math.Pow(2, float64(attempt)))
	return time.Duration(mrand.Int63n(int64(ceiling) + 1))
}

// execOptions tunes a single request execution.
type execOptions struct {
	retryable   bool
	notFoundNil bool // map StatusNotFound to (nil, nil)
}

// execute runs one request with pooling and the retry policy. On StatusOK it
// returns the response body. With notFoundNil, StatusNotFound returns (nil, nil).
func (c *Client) execute(ctx context.Context, command byte, payload []byte, op string, eo execOptions) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		conn, err := c.pool.acquire(ctx)
		if err != nil {
			return nil, err
		}
		status, body, err := conn.request(ctx, command, payload)
		if err != nil {
			c.pool.discard(conn)
			lastErr = err
			if eo.retryable && attempt < c.maxRetries {
				if werr := c.wait(ctx, attempt); werr != nil {
					return nil, werr
				}
				continue
			}
			return nil, err
		}
		c.pool.release(conn)

		switch {
		case status == StatusOK:
			return body, nil
		case eo.notFoundNil && status == StatusNotFound:
			return nil, nil
		case status == StatusEngineBusy && eo.retryable && attempt < c.maxRetries:
			lastErr = fromStatus(status, op, body)
			if werr := c.wait(ctx, attempt); werr != nil {
				return nil, werr
			}
			continue
		default:
			return nil, fromStatus(status, op, body)
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("zydecodb: %s: retries exhausted", op)
	}
	return nil, lastErr
}

func (c *Client) wait(ctx context.Context, attempt int) error {
	t := time.NewTimer(c.backoff(attempt))
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// --- health / introspection ---

// Ping verifies the server is reachable and responsive.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.execute(ctx, CmdPing, nil, "Ping", execOptions{retryable: true})
	return err
}

// Stats returns the server's runtime statistics as decoded JSON.
func (c *Client) Stats(ctx context.Context) (map[string]any, error) {
	body, err := c.execute(ctx, CmdStats, nil, "Stats", execOptions{retryable: true})
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("zydecodb: decode stats: %w", err)
	}
	return out, nil
}

// --- raw key/value (idempotent set/get) ---

// Put inserts or replaces a raw KV pair. It returns the engine sequence number.
func (c *Client) Put(ctx context.Context, key, value []byte, expiresAt uint64) (uint64, error) {
	out, err := c.execute(ctx, CmdPut, EncodePut(key, value, expiresAt), "Put", execOptions{retryable: true})
	if err != nil {
		return 0, err
	}
	return decodeU64(out, "Put")
}

// Get fetches a raw KV pair. It returns nil if not found.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	return c.execute(ctx, CmdGet, EncodeKey(key), "Get", execOptions{retryable: true, notFoundNil: true})
}

// Delete removes a raw KV pair, returning whether it existed.
func (c *Client) Delete(ctx context.Context, key []byte) (bool, error) {
	out, err := c.execute(ctx, CmdDel, EncodeKey(key), "Delete", execOptions{retryable: false})
	if err != nil {
		return false, err
	}
	return len(out) > 0 && out[0] != 0, nil
}

// --- document layer (raw bytes; Collection adds JSON ergonomics) ---

// DefineIndex creates a secondary index. With ifNotExists, an existing index
// returns (false, nil) instead of an error.
func (c *Client) DefineIndex(ctx context.Context, collection, index string, fields []string, unique, ifNotExists bool) (bool, error) {
	payload := EncodeIndexDef(collection, index, fields, unique)
	conn, err := c.pool.acquire(ctx)
	if err != nil {
		return false, err
	}
	status, body, err := conn.request(ctx, CmdIndexDef, payload)
	if err != nil {
		c.pool.discard(conn)
		return false, err
	}
	c.pool.release(conn)
	if ifNotExists && status == StatusConflict {
		return false, nil
	}
	if status != StatusOK {
		return false, fromStatus(status, "IndexDef", body)
	}
	return true, nil
}

// PutDocument inserts or replaces a document by id. body is JSON. It returns the
// engine sequence number assigned to the write.
func (c *Client) PutDocument(ctx context.Context, collection, docID string, body []byte, relaxed bool) (uint64, error) {
	out, err := c.execute(ctx, CmdDocPut, EncodeDocPut(collection, []byte(docID), body, relaxed), "DocPut", execOptions{retryable: true})
	if err != nil {
		return 0, err
	}
	return decodeU64(out, "DocPut")
}

// DeleteDocument deletes a document by id, returning whether a document existed.
func (c *Client) DeleteDocument(ctx context.Context, collection, docID string) (bool, error) {
	out, err := c.execute(ctx, CmdDocDel, EncodeDocDel(collection, []byte(docID)), "DocDel", execOptions{retryable: false})
	if err != nil {
		return false, err
	}
	return len(out) > 0 && out[0] != 0, nil
}

// GetDocument fetches one document by id. It returns (nil, nil) if not found.
func (c *Client) GetDocument(ctx context.Context, collection, docID string) ([]byte, error) {
	return c.execute(ctx, CmdQuery, EncodeQueryByID(collection, []byte(docID)), "Query", execOptions{retryable: true, notFoundNil: true})
}

// FindOptions tunes a Find query.
type FindOptions struct {
	Sort       []SortKey
	Projection Projection
	Skip       uint32
	Limit      uint32 // 0 = no limit
	PageSize   uint32 // 0 defaults to 100
}

// Find returns the raw JSON bodies of matching documents, auto-paginating until
// the limit is reached or results are exhausted. filter is opaque JSON bytes
// (nil/empty = match all).
func (c *Client) Find(ctx context.Context, collection string, filter []byte, opts FindOptions) ([][]byte, error) {
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = 100
	}
	var (
		results [][]byte
		cursor  []byte
		skip    = opts.Skip
		yielded uint32
	)
	for {
		want := pageSize
		if opts.Limit != 0 {
			remaining := opts.Limit - yielded
			if remaining <= 0 {
				return results, nil
			}
			if remaining < want {
				want = remaining
			}
		}
		payload := EncodeFind(collection, filter, opts.Sort, opts.Projection, skip, want, cursor)
		body, err := c.execute(ctx, CmdFind, payload, "Find", execOptions{retryable: true})
		if err != nil {
			return nil, err
		}
		rows, next, err := DecodePage(body)
		if err != nil {
			return nil, err
		}
		skip = 0 // applied on the first page; the cursor carries it onward
		for _, row := range rows {
			results = append(results, row.Body)
			yielded++
			if opts.Limit != 0 && yielded >= opts.Limit {
				return results, nil
			}
		}
		if len(next) == 0 {
			return results, nil
		}
		cursor = next
	}
}

// Update applies an update to matching documents and returns the raw JSON
// summary ({"matched":N,"modified":M}). Never retried automatically.
func (c *Client) Update(ctx context.Context, collection string, filter, update []byte, multi, relaxed, upsert bool) ([]byte, error) {
	return c.execute(ctx, CmdUpdate, EncodeUpdate(collection, filter, update, multi, relaxed, upsert), "Update", execOptions{retryable: false})
}

// DeleteByFilter deletes matching documents and returns the deleted count.
// Never retried automatically.
func (c *Client) DeleteByFilter(ctx context.Context, collection string, filter []byte, multi, relaxed bool) (int64, error) {
	body, err := c.execute(ctx, CmdDelete, EncodeDelete(collection, filter, multi, relaxed), "Delete", execOptions{retryable: false})
	if err != nil {
		return 0, err
	}
	var res struct {
		Deleted int64 `json:"deleted"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return 0, fmt.Errorf("zydecodb: decode delete result: %w", err)
	}
	return res.Deleted, nil
}

// Count returns the number of documents matching filter (nil = all).
func (c *Client) Count(ctx context.Context, collection string, filter []byte) (int64, error) {
	body, err := c.execute(ctx, CmdCount, EncodeCount(collection, filter), "Count", execOptions{retryable: true})
	if err != nil {
		return 0, err
	}
	var n int64
	if err := json.Unmarshal(body, &n); err != nil {
		return 0, fmt.Errorf("zydecodb: decode count: %w", err)
	}
	return n, nil
}

// Distinct returns the distinct values of field across matching documents.
func (c *Client) Distinct(ctx context.Context, collection, field string, filter []byte) ([]any, error) {
	body, err := c.execute(ctx, CmdCount, EncodeDistinct(collection, filter, field), "Distinct", execOptions{retryable: true})
	if err != nil {
		return nil, err
	}
	var out []any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("zydecodb: decode distinct: %w", err)
	}
	return out, nil
}

// Collection returns a handle to a named collection with the JSON document API.
func (c *Client) Collection(name string) *Collection {
	return &Collection{client: c, name: name}
}

// GenerateID returns a time-ordered id (UUIDv7-style): a 48-bit millisecond
// timestamp followed by 80 random bits, hex-encoded. It sorts lexicographically
// by creation time.
func GenerateID() string {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli()) & ((1 << 48) - 1)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], ms)
	copy(b[0:6], ts[2:8])
	_, _ = rand.Read(b[6:])
	return hex.EncodeToString(b[:])
}

func decodeU64(body []byte, op string) (uint64, error) {
	if len(body) != 8 {
		return 0, fmt.Errorf("zydecodb: %s: expected 8-byte sequence, got %d", op, len(body))
	}
	return binary.BigEndian.Uint64(body), nil
}
