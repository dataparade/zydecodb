package zydecodb

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// conn is a single TCP connection to a ZydecoDB server. It is NOT safe for
// concurrent use; the pool hands a connection to one caller at a time. Framing
// and the optional auth handshake live here; pooling and retries live above.
type conn struct {
	nc       net.Conn
	timeout  time.Duration
	lastUsed time.Time
}

func dial(ctx context.Context, addr string, timeout time.Duration, apiKey string, tlsConf *tls.Config) (*conn, error) {
	d := net.Dialer{Timeout: timeout}
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, &ConnError{Err: fmt.Errorf("connect to %s failed: %w", addr, err)}
	}
	// Disable Nagle: requests are small and latency-sensitive.
	if tcp, ok := nc.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	if tlsConf != nil {
		cfg := tlsConf.Clone()
		if cfg.ServerName == "" {
			// SNI defaults to the dial host so wildcard certs verify without
			// the caller repeating the hostname.
			if host, _, splitErr := net.SplitHostPort(addr); splitErr == nil {
				cfg.ServerName = host
			} else {
				cfg.ServerName = addr
			}
		}
		tc := tls.Client(nc, cfg)
		hsCtx := ctx
		if _, ok := ctx.Deadline(); !ok && timeout > 0 {
			var cancel context.CancelFunc
			hsCtx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		if err := tc.HandshakeContext(hsCtx); err != nil {
			_ = nc.Close()
			return nil, &ConnError{Err: fmt.Errorf("tls handshake with %s failed: %w", addr, err)}
		}
		nc = tc
	}
	c := &conn{nc: nc, timeout: timeout, lastUsed: time.Now()}
	if apiKey != "" {
		if err := c.sessionInit(ctx, apiKey); err != nil {
			c.close()
			return nil, err
		}
	}
	return c, nil
}

func (c *conn) sessionInit(ctx context.Context, apiKey string) error {
	status, payload, err := c.request(ctx, CmdSessionInit, []byte(apiKey))
	if err != nil {
		return err
	}
	if status != StatusOK {
		return fromStatus(status, "SessionInit", payload)
	}
	return nil
}

func (c *conn) close() {
	if c.nc != nil {
		_ = c.nc.Close()
		c.nc = nil
	}
}

// deadline derives the I/O deadline from the context (if it has one) bounded by
// the connection timeout.
func (c *conn) deadline(ctx context.Context) time.Time {
	d := time.Now().Add(c.timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(d) {
		return ctxDeadline
	}
	return d
}

// request sends one framed request and reads the framed response. Any transport
// failure closes the connection and returns a *ConnError (the pool discards it;
// the client may retry idempotent calls on a fresh connection).
func (c *conn) request(ctx context.Context, command byte, payload []byte) (status byte, body []byte, err error) {
	if c.nc == nil {
		return 0, nil, &ConnError{Err: fmt.Errorf("not connected")}
	}
	if err := c.nc.SetDeadline(c.deadline(ctx)); err != nil {
		c.close()
		return 0, nil, &ConnError{Err: err}
	}
	frame := append(EncodeHeader(command, uint32(len(payload))), payload...)
	if _, err := c.nc.Write(frame); err != nil {
		c.close()
		return 0, nil, &ConnError{Err: fmt.Errorf("write failed: %w", err)}
	}
	status, body, err = c.recv()
	if err != nil {
		c.close()
		return 0, nil, err
	}
	c.lastUsed = time.Now()
	return status, body, nil
}

func (c *conn) recv() (byte, []byte, error) {
	header := make([]byte, HeaderLen)
	if _, err := io.ReadFull(c.nc, header); err != nil {
		return 0, nil, &ConnError{Err: fmt.Errorf("read header failed: %w", err)}
	}
	if header[0] != ProtoVersion {
		return 0, nil, &ConnError{Err: fmt.Errorf("unexpected protocol version 0x%02x", header[0])}
	}
	status := header[1]
	length := binary.BigEndian.Uint32(header[2:])
	if length == 0 {
		return status, nil, nil
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(c.nc, body); err != nil {
		return 0, nil, &ConnError{Err: fmt.Errorf("read body failed: %w", err)}
	}
	return status, body, nil
}

// ping sends a keepalive and reports whether the server answered OK.
func (c *conn) ping(ctx context.Context) bool {
	status, _, err := c.request(ctx, CmdPing, nil)
	return err == nil && status == StatusOK
}
