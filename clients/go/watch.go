package zydecodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/dataparade/zydecodb/clients/go/internal/proto"
)

// ChangeEvent is one durable change from a Watch subscription.
type ChangeEvent struct {
	Op          string // "upsert" or "delete"
	DocID       string
	Document    Document // nil for delete
	ResumeToken string   // base64-encoded opaque bytes
}

// ChangeStream reads Watch events from a dedicated connection.
type ChangeStream struct {
	conn       *conn
	collection string
	opened     bool
}

// Watch opens a dedicated change stream on collection. resumeToken may be nil
// or empty to start after the current durable watermark.
func (c *Client) Watch(ctx context.Context, collection string, resumeToken []byte) (*ChangeStream, error) {
	nc, err := c.pool.openDedicated(ctx)
	if err != nil {
		return nil, err
	}
	cs := &ChangeStream{conn: nc, collection: collection}
	if err := cs.open(ctx, resumeToken); err != nil {
		cs.Close()
		return nil, err
	}
	return cs, nil
}

func (cs *ChangeStream) open(ctx context.Context, resumeToken []byte) error {
	if cs.opened {
		return nil
	}
	status, body, err := cs.conn.request(ctx, proto.CmdWatch, proto.EncodeWatch(cs.collection, resumeToken))
	if err != nil {
		return err
	}
	if status != proto.StatusOK {
		return fromStatus(status, "Watch", body)
	}
	frame, err := proto.DecodeWatchFrame(body)
	if err != nil {
		return err
	}
	if frame.Kind != "ack" {
		return fromStatus(proto.StatusProtocolError, "Watch", []byte("expected initial ack"))
	}
	cs.opened = true
	return nil
}

// Next returns the next change event, skipping heartbeats.
func (cs *ChangeStream) Next(ctx context.Context) (*ChangeEvent, error) {
	if !cs.opened {
		if err := cs.open(ctx, nil); err != nil {
			return nil, err
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		status, body, err := cs.conn.recvCtx(ctx)
		if err != nil {
			return nil, err
		}
		if status != proto.StatusOK {
			return nil, fromStatus(status, "Watch", body)
		}
		frame, err := proto.DecodeWatchFrame(body)
		if err != nil {
			return nil, err
		}
		switch frame.Kind {
		case "ack", "heartbeat":
			continue
		case "event":
			ev, err := changeEventFromFrame(frame)
			if err != nil {
				return nil, err
			}
			return ev, nil
		default:
			return nil, fromStatus(proto.StatusProtocolError, "Watch", []byte("invalid event frame"))
		}
	}
}

func changeEventFromFrame(frame proto.WatchFrame) (*ChangeEvent, error) {
	token := base64.StdEncoding.EncodeToString(frame.ResumeToken)
	docID := string(frame.DocID)
	switch frame.Op {
	case proto.WatchOpUpsert:
		doc := Document{}
		if len(frame.Body) > 0 {
			if err := json.Unmarshal(frame.Body, &doc); err != nil {
				return nil, fmt.Errorf("zydecodb: decode watch document: %w", err)
			}
		}
		return &ChangeEvent{
			Op:          "upsert",
			DocID:       docID,
			Document:    doc,
			ResumeToken: token,
		}, nil
	case proto.WatchOpDelete:
		return &ChangeEvent{
			Op:          "delete",
			DocID:       docID,
			Document:    nil,
			ResumeToken: token,
		}, nil
	default:
		return nil, fromStatus(proto.StatusProtocolError, "Watch", []byte(fmt.Sprintf("unknown op 0x%02x", frame.Op)))
	}
}

// Close closes the dedicated connection.
func (cs *ChangeStream) Close() {
	if cs.conn != nil {
		cs.conn.close()
		cs.conn = nil
	}
}

// Watch opens a change stream on this collection.
func (c *Collection) Watch(ctx context.Context, resumeToken []byte) (*ChangeStream, error) {
	return c.client.Watch(ctx, c.name, resumeToken)
}
