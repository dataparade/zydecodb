package zydecodb

import (
	"errors"
	"fmt"
)

// ConnError is a transport-level failure (dial/write/read). It is safe to retry
// for idempotent operations; the client does this automatically.
type ConnError struct {
	Err error
}

func (e *ConnError) Error() string { return "zydecodb: connection error: " + e.Err.Error() }
func (e *ConnError) Unwrap() error { return e.Err }

// ServerError is a non-OK response from the server. Status is the wire status
// byte so callers can branch on the failure class with the Is* helpers below.
type ServerError struct {
	Op     string
	Status byte
	Detail string
}

func (e *ServerError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("zydecodb: %s failed: %s (%s)", e.Op, statusName(e.Status), e.Detail)
	}
	return fmt.Sprintf("zydecodb: %s failed: %s", e.Op, statusName(e.Status))
}

func fromStatus(status byte, op string, payload []byte) *ServerError {
	return &ServerError{Op: op, Status: status, Detail: string(payload)}
}

func serverStatus(err error) (byte, bool) {
	var se *ServerError
	if errors.As(err, &se) {
		return se.Status, true
	}
	return 0, false
}

// IsConflict reports whether err is a constraint conflict (e.g. a unique-index
// violation, status 0x03).
func IsConflict(err error) bool {
	s, ok := serverStatus(err)
	return ok && s == StatusConflict
}

// IsAuth reports whether err is an authentication/authorization failure
// (status 0x0B / 0x0C).
func IsAuth(err error) bool {
	s, ok := serverStatus(err)
	return ok && (s == StatusUnauthorized || s == StatusForbidden)
}

// IsBusy reports whether the server is shedding load (status 0x07). The client
// retries this automatically for idempotent operations.
func IsBusy(err error) bool {
	s, ok := serverStatus(err)
	return ok && s == StatusEngineBusy
}

// IsInvalidRequest reports whether the server rejected the request as malformed
// (protocol / invalid-key / invalid-value).
func IsInvalidRequest(err error) bool {
	s, ok := serverStatus(err)
	return ok && (s == StatusProtocolError || s == StatusInvalidKey || s == StatusInvalidValue)
}

// IsPolicyRejected reports whether the server rejected the request under an
// admission/write policy (quota, status 0x09).
func IsPolicyRejected(err error) bool {
	s, ok := serverStatus(err)
	return ok && s == StatusPolicyRejected
}

// IsUnsupportedFormat reports whether the server refused an on-disk artifact
// whose format version it cannot read (status 0x0A).
func IsUnsupportedFormat(err error) bool {
	s, ok := serverStatus(err)
	return ok && s == StatusUnsupportedFormat
}
