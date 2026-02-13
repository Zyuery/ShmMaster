//go:build windows

package mmap

import "errors"

var ErrNotSupported = errors.New("mmap not supported on windows")

func Map(fd uintptr, size int) ([]byte, error) {
	return nil, ErrNotSupported
}

func Sync(data []byte) error {
	return ErrNotSupported
}

func Unmap(data []byte) error {
	return nil
}
