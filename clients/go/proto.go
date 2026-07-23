// Package zydecodb is the official Go driver for ZydecoDB, a document store
// that speaks a compact binary wire protocol.
//
// This file is the pure codec: command/status constants and the encode/decode
// of payload bodies, with no I/O. It mirrors the Rust definitions in
// crates/zydecodb-engine/src/frame.rs and crates/zydecodb-document/src/wire.rs
// and is verified byte-for-byte against clients/conformance/vectors.json.
package zydecodb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// ProtoVersion is byte 0 of every request and response envelope.
const ProtoVersion byte = 0x01

// HeaderLen is the fixed envelope header size: version + command + u32 length.
const HeaderLen = 6

// Command codes (envelope byte 1).
const (
	CmdPut         byte = 0x01
	CmdGet         byte = 0x02
	CmdDel         byte = 0x03
	CmdQuery       byte = 0x20
	CmdDocPut      byte = 0x21
	CmdDocDel      byte = 0x22
	CmdFind        byte = 0x23
	CmdUpdate      byte = 0x24
	CmdDelete      byte = 0x25
	CmdCount       byte = 0x26
	CmdIndexDef    byte = 0x30
	CmdSessionInit byte = 0x40
	CmdPing        byte = 0xF0
	CmdStats       byte = 0xF1
)

// Query / count sub-command discriminators (first payload byte).
const (
	queryByID         byte = 0x00
	queryIndexRange   byte = 0x01
	countModeCount    byte = 0x00
	countModeDistinct byte = 0x01
)

// Projection modes for Find.
const (
	ProjNone    byte = 0x00
	ProjInclude byte = 0x01
	ProjExclude byte = 0x02
)

// flagRelaxed is bit 0 of the optional trailing flags byte on write payloads:
// when set, the write is acknowledged without waiting for the durability fsync.
// flagUpsert is bit 1: insert one document when an update matches nothing.
const (
	flagRelaxed byte = 0x01
	flagUpsert  byte = 0x02
)

// Status codes (response envelope byte 1).
const (
	StatusOK                byte = 0x00
	StatusNotFound          byte = 0x01
	StatusError             byte = 0x02
	StatusConflict          byte = 0x03
	StatusIOError           byte = 0x04
	StatusInvalidKey        byte = 0x05
	StatusInvalidValue      byte = 0x06
	StatusEngineBusy        byte = 0x07
	StatusProtocolError     byte = 0x08
	StatusPolicyRejected    byte = 0x09
	StatusUnsupportedFormat byte = 0x0A
	StatusUnauthorized      byte = 0x0B
	StatusForbidden         byte = 0x0C
)

func statusName(status byte) string {
	switch status {
	case StatusOK:
		return "Ok"
	case StatusNotFound:
		return "NotFound"
	case StatusError:
		return "Error"
	case StatusConflict:
		return "Conflict"
	case StatusIOError:
		return "IoError"
	case StatusInvalidKey:
		return "InvalidKey"
	case StatusInvalidValue:
		return "InvalidValue"
	case StatusEngineBusy:
		return "EngineBusy"
	case StatusProtocolError:
		return "ProtocolError"
	case StatusPolicyRejected:
		return "PolicyRejected"
	case StatusUnsupportedFormat:
		return "UnsupportedFormat"
	case StatusUnauthorized:
		return "Unauthorized"
	case StatusForbidden:
		return "Forbidden"
	default:
		return fmt.Sprintf("0x%02x", status)
	}
}

// EncodeHeader builds the 6-byte request envelope header.
func EncodeHeader(command byte, payloadLen uint32) []byte {
	h := make([]byte, HeaderLen)
	h[0] = ProtoVersion
	h[1] = command
	binary.BigEndian.PutUint32(h[2:], payloadLen)
	return h
}

func putLP(buf *bytes.Buffer, b []byte) {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(b)))
	buf.Write(l[:])
	buf.Write(b)
}

func putU32(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}

func relaxedByte(relaxed bool) byte {
	if relaxed {
		return flagRelaxed
	}
	return 0
}

func updateFlags(relaxed, upsert bool) byte {
	var f byte
	if relaxed {
		f |= flagRelaxed
	}
	if upsert {
		f |= flagUpsert
	}
	return f
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

// EncodeDocPut builds a DocPut payload: [collection][doc_id][body][flags].
// body is the already-serialized JSON document (the codec treats it as opaque).
func EncodeDocPut(collection string, docID, body []byte, relaxed bool) []byte {
	var buf bytes.Buffer
	putLP(&buf, []byte(collection))
	putLP(&buf, docID)
	putLP(&buf, body)
	buf.WriteByte(relaxedByte(relaxed))
	return buf.Bytes()
}

// EncodeDocDel builds a DocDel payload: [collection][doc_id].
func EncodeDocDel(collection string, docID []byte) []byte {
	var buf bytes.Buffer
	putLP(&buf, []byte(collection))
	putLP(&buf, docID)
	return buf.Bytes()
}

// EncodePut builds a Put payload: [routing 16][txid 8][expires_at 8][klen 4][vlen 4][key][value].
func EncodePut(key, value []byte, expiresAt uint64) []byte {
	var buf bytes.Buffer
	var zeroes [16]byte
	buf.Write(zeroes[:])
	var num [24]byte
	binary.BigEndian.PutUint64(num[0:8], 0) // txid
	binary.BigEndian.PutUint64(num[8:16], expiresAt)
	binary.BigEndian.PutUint32(num[16:20], uint32(len(key)))
	binary.BigEndian.PutUint32(num[20:24], uint32(len(value)))
	buf.Write(num[:])
	buf.Write(key)
	buf.Write(value)
	return buf.Bytes()
}

// EncodeKey builds a Key payload for Get/Del: [routing 16][snapshot_seq 8][klen 4][key].
func EncodeKey(key []byte) []byte {
	var buf bytes.Buffer
	var zeroes [16]byte
	buf.Write(zeroes[:])
	var num [12]byte
	binary.BigEndian.PutUint64(num[0:8], 0) // snapshot_seq
	binary.BigEndian.PutUint32(num[8:12], uint32(len(key)))
	buf.Write(num[:])
	buf.Write(key)
	return buf.Bytes()
}

// EncodeIndexDef builds an IndexDef payload:
// [collection][index_name][unique u8][field_count u32]{[field]}.
func EncodeIndexDef(collection, index string, fields []string, unique bool) []byte {
	var buf bytes.Buffer
	putLP(&buf, []byte(collection))
	putLP(&buf, []byte(index))
	buf.WriteByte(boolByte(unique))
	putU32(&buf, uint32(len(fields)))
	for _, f := range fields {
		putLP(&buf, []byte(f))
	}
	return buf.Bytes()
}

// EncodeQueryByID builds a by-id Query payload: [mode][collection][doc_id].
func EncodeQueryByID(collection string, docID []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(queryByID)
	putLP(&buf, []byte(collection))
	putLP(&buf, docID)
	return buf.Bytes()
}

// EncodeQueryIndexRange builds an index-range Query payload:
// [mode][collection][index][limit u32][lo][hi][cursor]. lo/hi are JSON-array
// bound bytes (empty = unbounded); cursor is an opaque page token.
func EncodeQueryIndexRange(collection, index string, lo, hi, cursor []byte, limit uint32) []byte {
	var buf bytes.Buffer
	buf.WriteByte(queryIndexRange)
	putLP(&buf, []byte(collection))
	putLP(&buf, []byte(index))
	putU32(&buf, limit)
	putLP(&buf, lo)
	putLP(&buf, hi)
	putLP(&buf, cursor)
	return buf.Bytes()
}

// SortKey is one ordering term: a dotted field path and its direction.
type SortKey struct {
	Field     string
	Ascending bool
}

// Projection selects fields to include or exclude. Mode is ProjNone,
// ProjInclude, or ProjExclude; Fields is ignored when Mode is ProjNone.
type Projection struct {
	Mode   byte
	Fields []string
}

// EncodeFind builds a Find payload. filter is opaque JSON bytes (empty = match
// all); cursor is an opaque page token.
func EncodeFind(collection string, filter []byte, sort []SortKey, proj Projection, skip, limit uint32, cursor []byte) []byte {
	var buf bytes.Buffer
	putLP(&buf, []byte(collection))
	putLP(&buf, filter)
	putU32(&buf, uint32(len(sort)))
	for _, s := range sort {
		putLP(&buf, []byte(s.Field))
		buf.WriteByte(boolByte(s.Ascending))
	}
	switch proj.Mode {
	case ProjInclude, ProjExclude:
		buf.WriteByte(proj.Mode)
		putU32(&buf, uint32(len(proj.Fields)))
		for _, f := range proj.Fields {
			putLP(&buf, []byte(f))
		}
	default:
		buf.WriteByte(ProjNone)
	}
	putU32(&buf, skip)
	putU32(&buf, limit)
	putLP(&buf, cursor)
	return buf.Bytes()
}

// EncodeUpdate builds an Update payload:
// [collection][filter][update][multi u8][flags]. filter/update are opaque JSON.
func EncodeUpdate(collection string, filter, update []byte, multi, relaxed, upsert bool) []byte {
	var buf bytes.Buffer
	putLP(&buf, []byte(collection))
	putLP(&buf, filter)
	putLP(&buf, update)
	buf.WriteByte(boolByte(multi))
	buf.WriteByte(updateFlags(relaxed, upsert))
	return buf.Bytes()
}

// EncodeDelete builds a filter-based Delete payload:
// [collection][filter][multi u8][flags].
func EncodeDelete(collection string, filter []byte, multi, relaxed bool) []byte {
	var buf bytes.Buffer
	putLP(&buf, []byte(collection))
	putLP(&buf, filter)
	buf.WriteByte(boolByte(multi))
	buf.WriteByte(relaxedByte(relaxed))
	return buf.Bytes()
}

// EncodeCount builds a Count payload: [mode][collection][filter].
func EncodeCount(collection string, filter []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(countModeCount)
	putLP(&buf, []byte(collection))
	putLP(&buf, filter)
	return buf.Bytes()
}

// EncodeDistinct builds a Distinct payload: [mode][collection][filter][field].
func EncodeDistinct(collection string, filter []byte, field string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(countModeDistinct)
	putLP(&buf, []byte(collection))
	putLP(&buf, filter)
	putLP(&buf, []byte(field))
	return buf.Bytes()
}

// Row is one decoded row from a query/find response page.
type Row struct {
	DocID []byte
	Body  []byte
}

// DecodePage decodes a response page: [row_count u32]{[doc_id][body]}[cursor].
// An empty next cursor (returned as nil) means there are no more pages.
func DecodePage(buf []byte) (rows []Row, cursor []byte, err error) {
	r := &reader{buf: buf}
	count, err := r.u32()
	if err != nil {
		return nil, nil, err
	}
	rows = make([]Row, 0, min(count, 4096))
	for i := uint32(0); i < count; i++ {
		docID, err := r.lp()
		if err != nil {
			return nil, nil, err
		}
		body, err := r.lp()
		if err != nil {
			return nil, nil, err
		}
		rows = append(rows, Row{DocID: docID, Body: body})
	}
	cur, err := r.lp()
	if err != nil {
		return nil, nil, err
	}
	if len(cur) == 0 {
		cur = nil
	}
	return rows, cur, nil
}

type reader struct {
	buf []byte
	pos int
}

func (r *reader) take(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.buf) {
		return nil, fmt.Errorf("zydecodb: payload truncated")
	}
	s := r.buf[r.pos : r.pos+n]
	r.pos += n
	return s, nil
}

func (r *reader) u32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *reader) lp() ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	b, err := r.take(int(n))
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

func min(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}
