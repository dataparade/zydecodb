package zydecodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// ErrUnknownCommitResult is returned when Commit's transport fails after the
// request may have reached the server. The transaction must not be retried
// blindly; reconcile by re-reading the intended keys.
var ErrUnknownCommitResult = errors.New("zydecodb: commit result unknown (transport failed)")

// Tx is a pinned-connection bounded transaction. All operations use the same
// TCP session; none are retried. After Commit or Rollback the Tx is spent.
type Tx struct {
	client *Client
	conn   *conn
	done   bool
	txID   uint64
}

// BeginTx starts a transaction on an exclusively checked-out connection.
func (c *Client) BeginTx(ctx context.Context) (*Tx, error) {
	conn, err := c.pool.acquire(ctx)
	if err != nil {
		return nil, err
	}
	status, body, err := conn.request(ctx, CmdBegin, nil)
	if err != nil {
		c.pool.discard(conn)
		return nil, err
	}
	if status != StatusOK {
		c.pool.release(conn)
		return nil, fromStatus(status, "Begin", body)
	}
	txID, _, err := DecodeBeginResponse(body)
	if err != nil {
		c.pool.discard(conn)
		return nil, err
	}
	return &Tx{client: c, conn: conn, txID: txID}, nil
}

// WithTransaction runs fn inside a transaction. On success it commits; on
// error or panic it rolls back. Commit transport failures return
// ErrUnknownCommitResult.
func (c *Client) WithTransaction(ctx context.Context, fn func(*Tx) error) (uint64, error) {
	tx, err := c.BeginTx(ctx)
	if err != nil {
		return 0, err
	}
	var committed bool
	defer func() {
		if !committed && !tx.done {
			_ = tx.Rollback(ctx)
		}
	}()
	if err := fn(tx); err != nil {
		return 0, err
	}
	seq, err := tx.Commit(ctx)
	if err != nil {
		return 0, err
	}
	committed = true
	return seq, nil
}

func (tx *Tx) ensureOpen() error {
	if tx.done {
		return fmt.Errorf("zydecodb: transaction already finished")
	}
	return nil
}

func (tx *Tx) request(ctx context.Context, command byte, payload []byte, op string) ([]byte, error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	status, body, err := tx.conn.request(ctx, command, payload)
	if err != nil {
		tx.client.pool.discard(tx.conn)
		tx.conn = nil
		tx.done = true
		return nil, err
	}
	if status != StatusOK {
		if status == StatusNotFound {
			return nil, fromStatus(status, op, body)
		}
		return nil, fromStatus(status, op, body)
	}
	return body, nil
}

// Commit persists all staged operations in one WAL batch.
func (tx *Tx) Commit(ctx context.Context) (uint64, error) {
	if err := tx.ensureOpen(); err != nil {
		return 0, err
	}
	status, body, err := tx.conn.request(ctx, CmdCommit, nil)
	if err != nil {
		tx.client.pool.discard(tx.conn)
		tx.conn = nil
		tx.done = true
		return 0, fmt.Errorf("%w: %v", ErrUnknownCommitResult, err)
	}
	tx.client.pool.release(tx.conn)
	tx.conn = nil
	tx.done = true
	if status != StatusOK {
		return 0, fromStatus(status, "Commit", body)
	}
	return DecodeCommitResponse(body)
}

// Rollback discards staged operations.
func (tx *Tx) Rollback(ctx context.Context) error {
	if tx.done {
		return nil
	}
	if tx.conn == nil {
		tx.done = true
		return nil
	}
	status, body, err := tx.conn.request(ctx, CmdRollback, nil)
	if err != nil {
		tx.client.pool.discard(tx.conn)
		tx.conn = nil
		tx.done = true
		return err
	}
	tx.client.pool.release(tx.conn)
	tx.conn = nil
	tx.done = true
	if status != StatusOK {
		return fromStatus(status, "Rollback", body)
	}
	return nil
}

// Put stages a raw KV put.
func (tx *Tx) Put(ctx context.Context, key, value []byte, expiresAt uint64) error {
	_, err := tx.request(ctx, CmdPut, EncodePut(key, value, expiresAt), "Put")
	return err
}

// Get reads a key with read-your-writes.
func (tx *Tx) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	status, body, err := tx.conn.request(ctx, CmdGet, EncodeKey(key))
	if err != nil {
		tx.client.pool.discard(tx.conn)
		tx.conn = nil
		tx.done = true
		return nil, err
	}
	if status == StatusNotFound {
		return nil, nil
	}
	if status != StatusOK {
		return nil, fromStatus(status, "Get", body)
	}
	return body, nil
}

// Delete stages a raw KV delete.
func (tx *Tx) Delete(ctx context.Context, key []byte) error {
	_, err := tx.request(ctx, CmdDel, EncodeKey(key), "Del")
	return err
}

// PutDocument stages a document replace/insert.
func (tx *Tx) PutDocument(ctx context.Context, collection, docID string, doc any) error {
	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	_, err = tx.request(ctx, CmdDocPut, EncodeDocPut(collection, []byte(docID), body, false, 0), "DocPut")
	return err
}

// PutDocumentIfMatch stages a conditional document replace.
func (tx *Tx) PutDocumentIfMatch(ctx context.Context, collection, docID string, doc any, ifMatch uint64) error {
	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	_, err = tx.request(ctx, CmdDocPutIfMatch, EncodeDocPutIfMatch(collection, []byte(docID), body, false, ifMatch, 0), "DocPutIfMatch")
	return err
}

// DeleteDocument stages a document delete.
func (tx *Tx) DeleteDocument(ctx context.Context, collection, docID string) error {
	_, err := tx.request(ctx, CmdDocDel, EncodeDocDel(collection, []byte(docID)), "DocDel")
	return err
}

// GetDocumentWithRevision reads a document with read-your-writes. Staged
// (uncommitted) bodies return revision 0.
func (tx *Tx) GetDocumentWithRevision(ctx context.Context, collection, docID string) (json.RawMessage, uint64, error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, 0, err
	}
	status, body, err := tx.conn.request(ctx, CmdDocGetRev, EncodeQueryByID(collection, []byte(docID)))
	if err != nil {
		tx.client.pool.discard(tx.conn)
		tx.conn = nil
		tx.done = true
		return nil, 0, err
	}
	if status == StatusNotFound {
		return nil, 0, nil
	}
	if status != StatusOK {
		return nil, 0, fromStatus(status, "DocGetRev", body)
	}
	raw, rev, err := DecodeDocGetRevResponse(body)
	if err != nil {
		return nil, 0, err
	}
	return json.RawMessage(raw), rev, nil
}

// UpdateDocumentIfMatch stages a conditional by-id partial update.
func (tx *Tx) UpdateDocumentIfMatch(ctx context.Context, collection, docID string, update any, ifMatch uint64) error {
	body, err := json.Marshal(update)
	if err != nil {
		return err
	}
	_, err = tx.request(ctx, CmdDocUpdateIfMatch, EncodeDocUpdateIfMatch(collection, []byte(docID), body, false, ifMatch), "DocUpdateIfMatch")
	return err
}
