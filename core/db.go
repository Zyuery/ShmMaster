package core

import (
	"encoding/binary"
	"md_master/msg"
	"md_master/util"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

type Entry struct {
	ValOff uint64
	ValLen uint32
}
type shard struct {
	rw  sync.RWMutex
	idx map[string]Entry
}

type DB struct {
	f    *os.File
	data []byte

	endOff uint64
	mu     sync.Mutex
	shards []shard
}

func NewDB(f *os.File, data []byte, shardN int64) *DB {
	shards := make([]shard, shardN)
	for i := range shards {
		shards[i].idx = make(map[string]Entry)
		shards[i].rw = sync.RWMutex{}
	}
	return &DB{f: f, data: data, mu: sync.Mutex{}, shards: shards}
}

// Open 打开/创建文件 + mmap + Recover （必须单线程访问，全局共享实例！！！！！！）
func Open(path string, size int64) (*DB, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	err = f.Truncate(size)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	//初始化DB
	db := NewDB(f, data, msg.ShardSize)
	if err := db.Recover(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// Close 关闭文件 + 取消映射
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.data != nil {
		_ = unix.Munmap(db.data)
		db.data = nil
	}
	if db.f != nil {
		_ = db.f.Close()
		db.f = nil
	}
	return nil
}

// Recover 恢复索引，更新endOff
func (db *DB) Recover() error {
	var off uint64 = 0
	limit := uint64(len(db.data))
	for {
		//不够一个记录头
		if off+msg.HeaderSize > limit {
			break
		}
		h := decodeHeader(db.data[off : off+msg.HeaderSize])
		//校验头
		if h.Magic != msg.Magic {
			break
		}
		if h.Ver != msg.Version {
			break
		}
		//长度校验
		if h.KeyLen == 0 {
			break
		}
		recordLen := uint64(msg.HeaderSize + uint32(h.KeyLen) + h.ValLen)
		if off+recordLen > limit {
			break
		}
		//校验CRC
		keyStart := off + msg.HeaderSize
		valStart := keyStart + uint64(h.KeyLen)
		keyBytes := db.data[keyStart : keyStart+uint64(h.KeyLen)]
		valBytes := db.data[valStart : valStart+uint64(h.ValLen)]
		if calcCRC(h.Flags, h.KeyLen, h.ValLen, keyBytes, valBytes) != h.CRC32 {
			break
		}
		// 校验通过，更新索引
		k := string(keyBytes)
		switch h.Flags {
		case msg.FlagPut:
			db.shards[util.Str2Int(k, msg.ShardSize)].idx[k] = Entry{ValOff: valStart, ValLen: h.ValLen}
		case msg.FlagDel:
			delete(db.shards[util.Str2Int(k, msg.ShardSize)].idx, k)
		default:
			return nil //未知flag
		}
		off += recordLen
	}
	db.endOff = off
	return nil
}

func (db *DB) Set(key string, value []byte) error {
	if len(key) == 0 || len(key) > int(^uint16(0)) {
		return msg.ErrBadArgument
	}
	keyLen := len(key)
	valueLen := len(value)
	if keyLen > int(^uint16(0)) || valueLen > int(^uint32(0)) {
		return msg.ErrBadArgument
	}
	recTotal := uint64(msg.HeaderSize) + uint64(keyLen) + uint64(valueLen)

	//写data & endOff
	db.mu.Lock()
	if db.endOff+recTotal > uint64(len(db.data)) {
		db.mu.Unlock()
		return msg.ErrNoSpace
	}
	off := db.endOff
	//写记录头
	h := header{
		Magic:  msg.Magic,
		Ver:    msg.Version,
		Flags:  msg.FlagPut,
		KeyLen: uint16(keyLen),
		ValLen: uint32(valueLen),
		CRC32:  0,
	}
	encodeHeader(db.data[off:off+msg.HeaderSize], h)
	//写kv
	keyStart := off + msg.HeaderSize
	valStart := keyStart + uint64(h.KeyLen)
	copy(db.data[keyStart:keyStart+uint64(h.KeyLen)], key)
	copy(db.data[valStart:valStart+uint64(h.ValLen)], value)
	//回写crc
	crc := calcCRC(msg.FlagPut, uint16(keyLen), uint32(valueLen), db.data[keyStart:valStart], db.data[valStart:valStart+uint64(valueLen)])
	binary.LittleEndian.PutUint32(db.data[off+16:off+20], crc)
	// （可选）为了 crash 测试更稳定，可以强刷一下
	// _ = unix.Msync(db.data[off:off+recTotal], unix.MS_SYNC)
	db.endOff += recTotal
	db.mu.Unlock()

	//存索引
	sid := util.Str2Int(key, msg.ShardSize)
	shard := &db.shards[sid]
	shard.rw.Lock()
	defer shard.rw.Unlock()

	db.shards[sid].idx[key] = Entry{ValOff: valStart, ValLen: uint32(valueLen)}
	return nil
}

func (db *DB) Get(key string) ([]byte, bool, error) {
	sid := util.Str2Int(key, msg.ShardSize)
	shard := &db.shards[sid]
	shard.rw.RLock()
	defer shard.rw.RUnlock()

	e, ok := shard.idx[key]
	if !ok {
		return nil, false, nil
	}
	start := e.ValOff
	end := start + uint64(e.ValLen)
	if end > uint64(len(db.data)) {
		return nil, false, msg.ErrNoSpace
	}
	//拷贝返回，避免调用方误改 mmap 区
	v := append([]byte(nil), db.data[start:end]...)
	return v, true, nil
}

func (db *DB) Del(key string) error {
	if len(key) == 0 || len(key) > int(^uint16(0)) {
		return msg.ErrBadArgument
	}
	keyLen := len(key)
	valueLen := 0
	if keyLen > int(^uint16(0)) || valueLen > int(^uint32(0)) {
		return msg.ErrBadArgument
	}
	recTotal := uint64(msg.HeaderSize) + uint64(keyLen) + uint64(valueLen)

	//写 data & endOff
	db.mu.Lock()
	if db.endOff+recTotal > uint64(len(db.data)) {
		db.mu.Unlock()
		return msg.ErrNoSpace
	}
	off := db.endOff
	//记录头
	h := header{
		Magic:  msg.Magic,
		Ver:    msg.Version,
		Flags:  msg.FlagDel,
		KeyLen: uint16(len(key)),
		ValLen: 0,
		CRC32:  0,
	}
	encodeHeader(db.data[off:off+msg.HeaderSize], h)
	//写kv
	keyStart := off + msg.HeaderSize
	copy(db.data[keyStart:keyStart+uint64(h.KeyLen)], key)
	keyBytes := db.data[keyStart : keyStart+uint64(h.KeyLen)]
	crc := calcCRC(msg.FlagDel, uint16(len(key)), 0, keyBytes, nil)
	binary.LittleEndian.PutUint32(db.data[off+16:off+20], crc)
	//（可选）为了 crash 测试更稳定，可以强刷一下
	//_ = unix.Msync(db.data[off:off+recTotal], unix.MS_SYNC)
	db.endOff += recTotal
	db.mu.Unlock()

	//删除索引
	sid := util.Str2Int(key, msg.ShardSize)
	shard := &db.shards[sid]
	shard.rw.Lock()
	defer shard.rw.Unlock()

	delete(db.shards[sid].idx, key)
	return nil
}
