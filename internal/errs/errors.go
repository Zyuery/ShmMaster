package errs

import "errors"

var (
	ErrNoSpace     = errors.New("db: no space")
	ErrBadArgument = errors.New("db: bad argument")
	ErrClosed      = errors.New("db: closed")
	ErrCorrupt     = errors.New("db: corrupt")
)
