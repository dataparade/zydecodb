package zydecodb

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// The vectors are generated from the Rust server encoders (the protocol
// authority). Running the Go codec against them proves it cannot silently drift.
// See clients/conformance/README.md.

type vectorFile struct {
	ProtoVersion byte            `json:"proto_version"`
	Commands     map[string]byte `json:"commands"`
	Statuses     map[string]byte `json:"statuses"`
	Requests     []reqVector     `json:"requests"`
	Responses    []respVector    `json:"responses"`
}

type reqVector struct {
	Name        string          `json:"name"`
	Kind        string          `json:"kind"`
	Command     byte            `json:"command"`
	Input       json.RawMessage `json:"input"`
	PayloadHex  string          `json:"payload_hex"`
	EnvelopeHex string          `json:"envelope_hex"`
}

type respVector struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	BytesHex string `json:"bytes_hex"`
	Decoded  struct {
		Rows []struct {
			DocID    string  `json:"doc_id"`
			BodyJSON string  `json:"body_json"`
			Revision *uint64 `json:"revision"`
		} `json:"rows"`
		NextCursorHex *string `json:"next_cursor_hex"`
		BodyJSON      string  `json:"body_json"`
		Revision      *uint64 `json:"revision"`
		TxID          uint64  `json:"tx_id"`
		SnapshotSeq   uint64  `json:"snapshot_seq"`
		Seq           uint64  `json:"seq"`
		LogicalOps    uint32   `json:"logical_ops"`
		EstimatedKeys uint32   `json:"estimated_keys"`
		RowsJSON      []string `json:"rows_json"`
		ResumeTokenHex string  `json:"resume_token_hex"`
		Op             string  `json:"op"`
		DocID          string  `json:"doc_id"`
	} `json:"decoded"`
}

func loadVectors(t *testing.T) vectorFile {
	t.Helper()
	path := filepath.Join("..", "conformance", "vectors.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var vf vectorFile
	if err := json.Unmarshal(raw, &vf); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	return vf
}

// optBytes returns the raw bytes of a "*_json" field: an opaque pre-serialized
// JSON string the codec must accept verbatim ("" = absent).
func optBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return []byte(s)
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

func encodeRequest(t *testing.T, v reqVector) []byte {
	t.Helper()
	switch v.Kind {
	case "Put":
		var in struct {
			KeyHex    string `json:"key_hex"`
			ValueHex  string `json:"value_hex"`
			ExpiresAt uint64 `json:"expires_at"`
		}
		mustInput(t, v.Input, &in)
		return EncodePut(mustHex(t, in.KeyHex), mustHex(t, in.ValueHex), in.ExpiresAt)
	case "Get":
		var in struct {
			KeyHex string `json:"key_hex"`
		}
		mustInput(t, v.Input, &in)
		return EncodeKey(mustHex(t, in.KeyHex))
	case "Del":
		var in struct {
			KeyHex string `json:"key_hex"`
		}
		mustInput(t, v.Input, &in)
		return EncodeKey(mustHex(t, in.KeyHex))
	case "DocPut":
		var in struct {
			Collection string `json:"collection"`
			DocID      string `json:"doc_id"`
			BodyJSON   string `json:"body_json"`
			Relaxed    bool   `json:"relaxed"`
			ExpiresAt  uint64 `json:"expires_at"`
		}
		mustInput(t, v.Input, &in)
		return EncodeDocPut(in.Collection, []byte(in.DocID), optBytes(in.BodyJSON), in.Relaxed, in.ExpiresAt)
	case "DocDel":
		var in struct {
			Collection string `json:"collection"`
			DocID      string `json:"doc_id"`
		}
		mustInput(t, v.Input, &in)
		return EncodeDocDel(in.Collection, []byte(in.DocID))
	case "IndexDef":
		var in struct {
			Collection         string   `json:"collection"`
			IndexName          string   `json:"index_name"`
			Fields             []string `json:"fields"`
			Unique             bool     `json:"unique"`
			ExpireAfterSeconds uint64   `json:"expire_after_seconds"`
			Directions         []bool   `json:"directions"`
		}
		mustInput(t, v.Input, &in)
		return EncodeIndexDefDirected(in.Collection, in.IndexName, in.Fields, in.Directions, in.Unique, in.ExpireAfterSeconds)
	case "QueryById":
		var in struct {
			Collection string `json:"collection"`
			DocID      string `json:"doc_id"`
		}
		mustInput(t, v.Input, &in)
		return EncodeQueryByID(in.Collection, []byte(in.DocID))
	case "QueryIndexRange":
		var in struct {
			Collection string `json:"collection"`
			IndexName  string `json:"index_name"`
			LoJSON     string `json:"lo_json"`
			HiJSON     string `json:"hi_json"`
			CursorHex  string `json:"cursor_hex"`
			Limit      uint32 `json:"limit"`
		}
		mustInput(t, v.Input, &in)
		return EncodeQueryIndexRange(in.Collection, in.IndexName, optBytes(in.LoJSON), optBytes(in.HiJSON), mustHex(t, in.CursorHex), in.Limit)
	case "Find", "FindRev":
		var in struct {
			Collection string  `json:"collection"`
			FilterJSON string  `json:"filter_json"`
			Sort       [][]any `json:"sort"`
			Projection struct {
				Mode   string   `json:"mode"`
				Fields []string `json:"fields"`
			} `json:"projection"`
			Skip      uint32 `json:"skip"`
			Limit     uint32 `json:"limit"`
			CursorHex string `json:"cursor_hex"`
		}
		mustInput(t, v.Input, &in)
		sort := make([]SortKey, 0, len(in.Sort))
		for _, s := range in.Sort {
			field, _ := s[0].(string)
			asc, _ := s[1].(bool)
			sort = append(sort, SortKey{Field: field, Ascending: asc})
		}
		proj := Projection{Mode: ProjNone}
		switch in.Projection.Mode {
		case "include":
			proj = Projection{Mode: ProjInclude, Fields: in.Projection.Fields}
		case "exclude":
			proj = Projection{Mode: ProjExclude, Fields: in.Projection.Fields}
		}
		return EncodeFind(in.Collection, optBytes(in.FilterJSON), sort, proj, in.Skip, in.Limit, mustHex(t, in.CursorHex))
	case "DocGetRev":
		var in struct {
			Collection string `json:"collection"`
			DocID      string `json:"doc_id"`
		}
		mustInput(t, v.Input, &in)
		return EncodeQueryByID(in.Collection, []byte(in.DocID))
	case "DocPutIfMatch":
		var in struct {
			Collection string `json:"collection"`
			DocID      string `json:"doc_id"`
			BodyJSON   string `json:"body_json"`
			Relaxed    bool   `json:"relaxed"`
			IfMatch    uint64 `json:"if_match"`
			ExpiresAt  uint64 `json:"expires_at"`
		}
		mustInput(t, v.Input, &in)
		return EncodeDocPutIfMatch(in.Collection, []byte(in.DocID), optBytes(in.BodyJSON), in.Relaxed, in.IfMatch, in.ExpiresAt)
	case "DocUpdateIfMatch":
		var in struct {
			Collection string `json:"collection"`
			DocID      string `json:"doc_id"`
			UpdateJSON string `json:"update_json"`
			Relaxed    bool   `json:"relaxed"`
			IfMatch    uint64 `json:"if_match"`
		}
		mustInput(t, v.Input, &in)
		return EncodeDocUpdateIfMatch(in.Collection, []byte(in.DocID), optBytes(in.UpdateJSON), in.Relaxed, in.IfMatch)
	case "Begin", "Commit", "Rollback":
		return nil
	case "Update":
		var in struct {
			Collection string `json:"collection"`
			FilterJSON string `json:"filter_json"`
			UpdateJSON string `json:"update_json"`
			Multi      bool   `json:"multi"`
			Relaxed    bool   `json:"relaxed"`
			Upsert     bool   `json:"upsert"`
		}
		mustInput(t, v.Input, &in)
		return EncodeUpdate(in.Collection, optBytes(in.FilterJSON), optBytes(in.UpdateJSON), in.Multi, in.Relaxed, in.Upsert)
	case "Delete":
		var in struct {
			Collection string `json:"collection"`
			FilterJSON string `json:"filter_json"`
			Multi      bool   `json:"multi"`
			Relaxed    bool   `json:"relaxed"`
		}
		mustInput(t, v.Input, &in)
		return EncodeDelete(in.Collection, optBytes(in.FilterJSON), in.Multi, in.Relaxed)
	case "Count":
		var in struct {
			Collection string `json:"collection"`
			FilterJSON string `json:"filter_json"`
		}
		mustInput(t, v.Input, &in)
		return EncodeCount(in.Collection, optBytes(in.FilterJSON))
	case "Distinct":
		var in struct {
			Collection string `json:"collection"`
			FilterJSON string `json:"filter_json"`
			Field      string `json:"field"`
		}
		mustInput(t, v.Input, &in)
		return EncodeDistinct(in.Collection, optBytes(in.FilterJSON), in.Field)
	case "Aggregate":
		var in struct {
			Collection   string `json:"collection"`
			PipelineJSON string `json:"pipeline_json"`
		}
		mustInput(t, v.Input, &in)
		return EncodeAggregate(in.Collection, optBytes(in.PipelineJSON))
	case "Watch":
		var in struct {
			Collection     string `json:"collection"`
			ResumeTokenHex string `json:"resume_token_hex"`
		}
		mustInput(t, v.Input, &in)
		return EncodeWatch(in.Collection, mustHex(t, in.ResumeTokenHex))
	case "SessionInit":
		var in struct {
			APIKey string `json:"api_key"`
		}
		mustInput(t, v.Input, &in)
		return []byte(in.APIKey)
	case "Ping":
		return nil
	default:
		t.Fatalf("unhandled request kind: %s", v.Kind)
		return nil
	}
}

func mustInput(t *testing.T, raw json.RawMessage, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("decode input: %v", err)
	}
}

func TestRequestVectors(t *testing.T) {
	vf := loadVectors(t)
	if vf.ProtoVersion != ProtoVersion {
		t.Fatalf("proto version mismatch: vectors=%d go=%d", vf.ProtoVersion, ProtoVersion)
	}
	for _, v := range vf.Requests {
		t.Run(v.Name, func(t *testing.T) {
			payload := encodeRequest(t, v)
			if got := hex.EncodeToString(payload); got != v.PayloadHex {
				t.Fatalf("payload mismatch\n got: %s\nwant: %s", got, v.PayloadHex)
			}
			envelope := append(EncodeHeader(v.Command, uint32(len(payload))), payload...)
			if got := hex.EncodeToString(envelope); got != v.EnvelopeHex {
				t.Fatalf("envelope mismatch\n got: %s\nwant: %s", got, v.EnvelopeHex)
			}
		})
	}
}

func TestResponseVectors(t *testing.T) {
	vf := loadVectors(t)
	for _, v := range vf.Responses {
		t.Run(v.Name, func(t *testing.T) {
			switch v.Kind {
			case "QueryPage":
				rows, cursor, err := DecodePage(mustHex(t, v.BytesHex))
				if err != nil {
					t.Fatalf("decode page: %v", err)
				}
				if len(rows) != len(v.Decoded.Rows) {
					t.Fatalf("row count: got %d want %d", len(rows), len(v.Decoded.Rows))
				}
				for i, exp := range v.Decoded.Rows {
					if string(rows[i].DocID) != exp.DocID {
						t.Errorf("row %d doc_id: got %q want %q", i, rows[i].DocID, exp.DocID)
					}
					if string(rows[i].Body) != exp.BodyJSON {
						t.Errorf("row %d body: got %q want %q", i, rows[i].Body, exp.BodyJSON)
					}
				}
				if v.Decoded.NextCursorHex == nil {
					if cursor != nil {
						t.Errorf("expected nil cursor, got %x", cursor)
					}
				} else if got := hex.EncodeToString(cursor); got != *v.Decoded.NextCursorHex {
					t.Errorf("cursor: got %s want %s", got, *v.Decoded.NextCursorHex)
				}
			case "QueryPageRev":
				rows, cursor, err := DecodePageWithRevision(mustHex(t, v.BytesHex))
				if err != nil {
					t.Fatalf("decode page: %v", err)
				}
				if len(rows) != len(v.Decoded.Rows) {
					t.Fatalf("row count: got %d want %d", len(rows), len(v.Decoded.Rows))
				}
				for i, exp := range v.Decoded.Rows {
					if string(rows[i].DocID) != exp.DocID {
						t.Errorf("row %d doc_id: got %q want %q", i, rows[i].DocID, exp.DocID)
					}
					if string(rows[i].Body) != exp.BodyJSON {
						t.Errorf("row %d body: got %q want %q", i, rows[i].Body, exp.BodyJSON)
					}
					if exp.Revision == nil || rows[i].Revision != *exp.Revision {
						t.Errorf("row %d revision: got %d want %v", i, rows[i].Revision, exp.Revision)
					}
				}
				if cursor != nil {
					t.Errorf("expected nil cursor, got %x", cursor)
				}
			case "DocGetRevResponse":
				body, rev, err := DecodeDocGetRevResponse(mustHex(t, v.BytesHex))
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				if string(body) != v.Decoded.BodyJSON {
					t.Errorf("body: got %q want %q", body, v.Decoded.BodyJSON)
				}
				if v.Decoded.Revision == nil || rev != *v.Decoded.Revision {
					t.Errorf("revision: got %d want %v", rev, v.Decoded.Revision)
				}
			case "BeginResponse":
				txID, snap, err := DecodeBeginResponse(mustHex(t, v.BytesHex))
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				if txID != uint64(v.Decoded.TxID) || snap != uint64(v.Decoded.SnapshotSeq) {
					t.Errorf("begin: got (%d,%d) want (%d,%d)", txID, snap, v.Decoded.TxID, v.Decoded.SnapshotSeq)
				}
			case "CommitResponse":
				seq, err := DecodeCommitResponse(mustHex(t, v.BytesHex))
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				if seq != uint64(v.Decoded.Seq) {
					t.Errorf("commit seq: got %d want %d", seq, v.Decoded.Seq)
				}
			case "StageAck":
				ops, keys, err := DecodeStageAck(mustHex(t, v.BytesHex))
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				if ops != uint32(v.Decoded.LogicalOps) || keys != uint32(v.Decoded.EstimatedKeys) {
					t.Errorf("stage ack: got (%d,%d) want (%d,%d)", ops, keys, v.Decoded.LogicalOps, v.Decoded.EstimatedKeys)
				}
			case "AggregateResponse":
				rows, err := DecodeAggregateResponse(mustHex(t, v.BytesHex))
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				if len(rows) != len(v.Decoded.RowsJSON) {
					t.Fatalf("row count: got %d want %d", len(rows), len(v.Decoded.RowsJSON))
				}
				for i, exp := range v.Decoded.RowsJSON {
					if string(rows[i]) != exp {
						t.Errorf("row %d: got %q want %q", i, rows[i], exp)
					}
				}
			case "WatchFrameAck", "WatchFrameHeartbeat":
				frame, err := DecodeWatchFrame(mustHex(t, v.BytesHex))
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				wantKind := map[string]string{
					"WatchFrameAck":       "ack",
					"WatchFrameHeartbeat": "heartbeat",
				}[v.Kind]
				if frame.Kind != wantKind {
					t.Errorf("kind: got %q want %q", frame.Kind, wantKind)
				}
				if got := hex.EncodeToString(frame.ResumeToken); got != v.Decoded.ResumeTokenHex {
					t.Errorf("resume token: got %s want %s", got, v.Decoded.ResumeTokenHex)
				}
			case "WatchFrameEvent":
				frame, err := DecodeWatchFrame(mustHex(t, v.BytesHex))
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				if frame.Kind != "event" {
					t.Fatalf("kind: got %q want event", frame.Kind)
				}
				if got := hex.EncodeToString(frame.ResumeToken); got != v.Decoded.ResumeTokenHex {
					t.Errorf("resume token: got %s want %s", got, v.Decoded.ResumeTokenHex)
				}
				wantOp := map[string]byte{"upsert": WatchOpUpsert, "delete": WatchOpDelete}[v.Decoded.Op]
				if frame.Op != wantOp {
					t.Errorf("op: got 0x%02x want 0x%02x", frame.Op, wantOp)
				}
				if string(frame.DocID) != v.Decoded.DocID {
					t.Errorf("doc_id: got %q want %q", frame.DocID, v.Decoded.DocID)
				}
				if string(frame.Body) != v.Decoded.BodyJSON {
					t.Errorf("body: got %q want %q", frame.Body, v.Decoded.BodyJSON)
				}
			default:
				t.Fatalf("unhandled response kind: %s", v.Kind)
			}
		})
	}
}

func TestCommandAndStatusCodes(t *testing.T) {
	vf := loadVectors(t)
	checks := map[string]byte{
		"DocPut":           CmdDocPut,
		"Find":             CmdFind,
		"Update":           CmdUpdate,
		"Delete":           CmdDelete,
		"Count":            CmdCount,
		"DocGetRev":        CmdDocGetRev,
		"FindRev":          CmdFindRev,
		"DocPutIfMatch":    CmdDocPutIfMatch,
		"DocUpdateIfMatch": CmdDocUpdateIfMatch,
		"Aggregate":        CmdAggregate,
		"Watch":            CmdWatch,
		"Begin":            CmdBegin,
		"Commit":           CmdCommit,
		"Rollback":         CmdRollback,
		"IndexDef":         CmdIndexDef,
		"SessionInit":      CmdSessionInit,
	}
	for name, want := range checks {
		if vf.Commands[name] != want {
			t.Errorf("command %s: vectors=%d go=%d", name, vf.Commands[name], want)
		}
	}
	statusChecks := map[string]byte{
		"Ok":                StatusOK,
		"EngineBusy":        StatusEngineBusy,
		"PolicyRejected":    StatusPolicyRejected,
		"UnsupportedFormat": StatusUnsupportedFormat,
		"Unauthorized":      StatusUnauthorized,
		"Forbidden":         StatusForbidden,
	}
	for name, want := range statusChecks {
		if vf.Statuses[name] != want {
			t.Errorf("status %s: vectors=%d go=%d", name, vf.Statuses[name], want)
		}
	}
}
