package zydecodb

import "testing"

func TestStatusHelpers(t *testing.T) {
	policy := fromStatus(StatusPolicyRejected, "Put", []byte("quota"))
	if !IsPolicyRejected(policy) {
		t.Fatalf("expected IsPolicyRejected")
	}
	if IsBusy(policy) || IsConflict(policy) {
		t.Fatalf("policy must not match other helpers")
	}
	fmt := fromStatus(StatusUnsupportedFormat, "Open", nil)
	if !IsUnsupportedFormat(fmt) {
		t.Fatalf("expected IsUnsupportedFormat")
	}
}
