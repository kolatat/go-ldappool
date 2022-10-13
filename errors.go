package ldappool

import (
	"errors"

	"github.com/go-ldap/ldap/v3"
)

var (
	ErrNoServerDefined    = errors.New("ad: no server defined")
	ErrDirClosed          = errors.New("ad: directory closed")
	ErrDuplicateConnClose = errors.New("ad: duplicate conn close")
	ErrConnExpired        = adErr{ErrBadConn, "ad: connection expired"}
	ErrBadConn            = errors.New("ad: bad conn")
)

type adErr struct {
	parent error
	msg    string
}

func (err adErr) Error() string {
	return err.msg
}

func (err adErr) Unwrap() error {
	return err.parent
}

func IsBadConn(err error) bool {
	switch {
	case err == nil:
		return false
	case ldap.IsErrorAnyOf(err, ldap.ErrorNetwork):
		return true
	case errors.Is(err, ErrBadConn):
		return true
	default:
		return false
	}
}
