package index

import (
	"md_master/msg"
	"md_master/util"
	"sync"
)

type shard struct {
	rw  sync.RWMutex
	idx map[string]Entry
}

// Sharded 分片 map 实现的 Index。
type Sharded struct {
	shards []shard
}

// NewSharded 创建 shardNum 个分片。
func NewSharded(shardNum int) *Sharded {
	shards := make([]shard, shardNum)
	for i := range shards {
		shards[i].idx = make(map[string]Entry)
	}
	return &Sharded{shards: shards}
}

func (s *Sharded) shard(key string) *shard {
	i := util.Str2Int(key, msg.ShardSize)
	return &s.shards[i]
}

func (s *Sharded) Get(key string) (Entry, bool) {
	sh := s.shard(key)
	sh.rw.RLock()
	e, ok := sh.idx[key]
	sh.rw.RUnlock()
	return e, ok
}

func (s *Sharded) Set(key string, e Entry) {
	sh := s.shard(key)
	sh.rw.Lock()
	sh.idx[key] = e
	sh.rw.Unlock()
}

func (s *Sharded) Del(key string) {
	sh := s.shard(key)
	sh.rw.Lock()
	delete(sh.idx, key)
	sh.rw.Unlock()
}

func (s *Sharded) Clear() {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.rw.Lock()
		for k := range sh.idx {
			delete(sh.idx, k)
		}
		sh.rw.Unlock()
	}
}
