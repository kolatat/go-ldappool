package ldaptest

import (
	"errors"

	"github.com/go-ldap/ldap/v3"
)

// ErrClass is how a client-side error from go-ldap is classified. Tests assert
// on the class, never on error text, because the text differs between
// platforms (for example a Windows and a Linux connection reset).
type ErrClass int

const (
	// ClassNone is a nil error.
	ClassNone ErrClass = iota
	// ClassNetwork is an *ldap.Error carrying the ldap.ErrorNetwork result code
	// (connection closed, response channel closed, or request timed out).
	ClassNetwork
	// ClassLDAPResult is an *ldap.Error carrying any other result code, that is,
	// an answer from the server.
	ClassLDAPResult
	// ClassPlain is any other non-nil error.
	ClassPlain
)

// String returns the name of the class.
func (c ErrClass) String() string {
	switch c {
	case ClassNone:
		return "none"
	case ClassNetwork:
		return "network"
	case ClassLDAPResult:
		return "ldap-result"
	case ClassPlain:
		return "plain"
	}
	return "unknown"
}

// Classify returns the class of err.
//
// Mid-operation failures frequently arrive as ClassPlain, not ClassNetwork: when
// the read side fails while a request is in flight go-ldap returns a plain
// "unable to read LDAP response packet" error, and only operations issued after
// the connection is known to be closing get the coded ErrorNetwork. A retry
// policy that looks only for ErrorNetwork therefore misses a large share of
// dropped connections (the SESS-02 consequence recorded in the Phase 1 research).
func Classify(err error) ErrClass {
	if err == nil {
		return ClassNone
	}
	var le *ldap.Error
	if errors.As(err, &le) {
		if le.ResultCode == ldap.ErrorNetwork {
			return ClassNetwork
		}
		return ClassLDAPResult
	}
	return ClassPlain
}
