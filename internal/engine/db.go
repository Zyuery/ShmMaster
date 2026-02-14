package engine

import (
	"shm_master/consts"
	"shm_master/internal/index"
	"shm_master/internal/segment"
	"sync"
)

type DB struct {
	lifeMu  sync.RWMutex
	writeMu sync.Mutex

	base    string
	segSize int64

	segMgr *segment.Manager
	idx    index.Index
}

func NewDB(base string, segSize int64, shardN int) *DB {
	return &DB{
		base:    base,
		segSize: segSize,
		segMgr:  segment.NewManager(base, segSize),
		idx:     index.NewSharded(shardN),
	}
}

// Open 打开或创建 DB
func Open(base string, segSize int64) (*DB, error) {
	db := NewDB(base, segSize, consts.ShardSize)
	if err := db.segMgr.OpenBase(); err != nil {
		return nil, err
	}
	if err := db.segMgr.EnsureOne(); err != nil {
		return nil, err
	}
	if err := db.Recover(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// Close 关闭所有段并清空索引。
func (db *DB) Close() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.lifeMu.Lock()
	defer db.lifeMu.Unlock()
	return db.segMgr.Close()
}
