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
			DocID    string `json:"doc_id"`
			BodyJSON string `json:"body_json"`
		} `json:"rows"`
		NextCursorHex *string `json:"next_cursor_hex"`
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
		}
		mustInput(t, v.Input, &in)
		return EncodeIndexDef(in.Collection, in.IndexName, in.Fields, in.Unique, in.ExpireAfterSeconds)
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
	case "Find":
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
			if v.Kind != "QueryPage" {
				t.Fatalf("unhandled response kind: %s", v.Kind)
			}
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
		})
	}
}

func TestCommandAndStatusCodes(t *testing.T) {
	vf := loadVectors(t)
	checks := map[string]byte{
		"DocPut":      CmdDocPut,
		"Find":        CmdFind,
		"Update":      CmdUpdate,
		"Delete":      CmdDelete,
		"Count":       CmdCount,
		"IndexDef":    CmdIndexDef,
		"SessionInit": CmdSessionInit,
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
