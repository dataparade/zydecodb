package zydecodb

import "github.com/dataparade/zydecodb/clients/go/internal/proto"

// Re-exported public types used by Collection / Client option structs.
// Wire codecs and opcode constants live in internal/proto and are not part of
// the 1.x driver contract.

type SortKey = proto.SortKey
type Projection = proto.Projection
type Row = proto.Row

const (
	ProjNone    = proto.ProjNone
	ProjInclude = proto.ProjInclude
	ProjExclude = proto.ProjExclude
)
