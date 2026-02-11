package msg

import "errors"

var (
	ErrNoSpace     = errors.New("db: no space")
	ErrBadArgument = errors.New("db: bad argument")
	ErrClosed      = errors.New("db: closed")
)
