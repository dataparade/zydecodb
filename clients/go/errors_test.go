package zydecodb

import (
	"testing"

	"github.com/dataparade/zydecodb/clients/go/internal/proto"
)

func TestStatusHelpers(t *testing.T) {
	policy := fromStatus(proto.StatusPolicyRejected, "Put", []byte("quota"))
	if !IsPolicyRejected(policy) {
		t.Fatalf("expected IsPolicyRejected")
	}
	if IsBusy(policy) || IsConflict(policy) {
		t.Fatalf("policy must not match other helpers")
	}
	fmt := fromStatus(proto.StatusUnsupportedFormat, "Open", nil)
	if !IsUnsupportedFormat(fmt) {
		t.Fatalf("expected IsUnsupportedFormat")
	}
}
