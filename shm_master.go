package shm_master

import (
	"shm_master/internal/engine"
	"shm_master/internal/errs"
)

// 对外暴露的 sentinel errors，便于调用方 errors.Is。
var (
	ErrNoSpace     = errs.ErrNoSpace
	ErrBadArgument = errs.ErrBadArgument
	ErrClosed      = errs.ErrClosed
	ErrCorrupt     = errs.ErrCorrupt
)

type DB struct {
	e *engine.DB
}

// Open 打开或创建 DB。base 为数据文件路径前缀，segSize 为单段大小（字节）。
func Open(base string, segSize int64) (*DB, error) {
	e, err := engine.Open(base, segSize)
	if err != nil {
		return nil, err
	}
	return &DB{e: e}, nil
}

func (db *DB) Close() error {
	if db == nil || db.e == nil {
		return nil
	}
	return db.e.Close()
}

func (db *DB) Get(key string) ([]byte, bool, error) {
	if db == nil || db.e == nil {
		return nil, false, nil
	}
	return db.e.Get(key)
}
func (db *DB) Set(key string, value []byte) error {
	if db == nil || db.e == nil {
		return nil
	}
	return db.e.Set(key, value)
}

func (db *DB) Del(key string) error {
	if db == nil || db.e == nil {
		return nil
	}
	return db.e.Del(key)
}
