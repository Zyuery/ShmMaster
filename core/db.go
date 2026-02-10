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

	logEnd uint64
	valEnd uint64

	mu     sync.Mutex
	shards []shard

	// allocator freelist：按 size class 存 offset
	free map[uint32][]uint64
}

func NewDB(f *os.File, data []byte, shardN int64) *DB {
	shards := make([]shard, shardN)
	for i := range shards {
		shards[i].idx = make(map[string]Entry)
		shards[i].rw = sync.RWMutex{}
	}
	return &DB{
		f:      f,
		data:   data,
		mu:     sync.Mutex{},
		shards: shards,
		free:   make(map[uint32][]uint64),
		logEnd: 0,
		valEnd: uint64(len(data)),
	}
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

// Recover 恢复索引，更新logEnd、valEnd
func (db *DB) Recover() error {
	var off uint64 = 0
	fileLimit := uint64(len(db.data))
	minValOff := fileLimit
	for {
		//不够一个记录头
		if off+msg.HeaderSize > minValOff {
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
		recordLen := uint64(msg.HeaderSize) + uint64(h.KeyLen)
		if off+recordLen > minValOff {
			break
		}
		//校验CRC
		keyStart := off + msg.HeaderSize
		keyBytes := db.data[keyStart : keyStart+uint64(h.KeyLen)]
		var valBytes []byte
		if h.Flags == msg.FlagPut {
			if h.ValOff > fileLimit || uint64(h.ValLen) > fileLimit || h.ValOff+uint64(h.ValLen) > fileLimit {
				break
			}
			if h.ValOff < off+recordLen {
				break
			}
			valBytes = db.data[h.ValOff : h.ValOff+uint64(h.ValLen)]
			if h.ValOff < minValOff {
				minValOff = h.ValOff
			}
		}
		if calcCRC(h.Flags, h.KeyLen, h.ValLen, h.ValOff, keyBytes, valBytes) != h.CRC32 {
			break
		}
		// 校验通过，更新索引
		k := string(keyBytes)
		sid := util.Str2Int(k, msg.ShardSize)
		shard := &db.shards[sid]
		//oldEntity, hadOld := shard.idx[k]
		//if hadOld {
		//	db.FreeBlock(oldEntity.ValOff, oldEntity.ValLen)
		//}
		switch h.Flags {
		case msg.FlagPut:
			shard.idx[k] = Entry{ValOff: h.ValOff, ValLen: h.ValLen}
		case msg.FlagDel:
			delete(shard.idx, k)
		default:
			return nil //未知flag
		}
		off += recordLen
	}
	db.logEnd = off
	db.valEnd = minValOff
	return nil
}

func (db *DB) Set(key string, value []byte) error {
	if len(key) == 0 || len(key) > int(^uint16(0)) {
		return msg.ErrBadArgument
	}
	keyLen := len(key)
	valueLen := uint32(len(value))
	if keyLen > int(^uint16(0)) || valueLen > ^uint32(0) {
		return msg.ErrBadArgument
	}
	recTotal := uint64(msg.HeaderSize) + uint64(keyLen)

	//写value入value区
	db.mu.Lock()
	var valOff uint64
	off, ok := db.Alloc(valueLen)
	if !ok {
		db.mu.Unlock()
		return msg.ErrNoSpace
	}
	valOff = off
	copy(db.data[valOff:valOff+uint64(valueLen)], value)

	//追加log record进log区
	if db.logEnd+recTotal > db.valEnd {
		db.mu.Unlock()
		return msg.ErrNoSpace
	}
	off = db.logEnd
	//写log Header
	h := header{
		Magic:  msg.Magic,
		Ver:    msg.Version,
		Flags:  msg.FlagPut,
		KeyLen: uint16(keyLen),
		ValLen: valueLen,
		ValOff: valOff,
		CRC32:  0,
	}
	encodeHeader(db.data[off:off+msg.HeaderSize], h)
	//写keyBytes
	keyStart := off + msg.HeaderSize
	keyEnd := keyStart + uint64(h.KeyLen)
	copy(db.data[keyStart:keyStart+uint64(h.KeyLen)], key)
	//回写crc
	crc := calcCRC(
		msg.FlagPut,
		uint16(keyLen),
		valueLen,
		valOff,
		db.data[keyStart:keyEnd],
		db.data[valOff:valOff+uint64(valueLen)])
	binary.LittleEndian.PutUint32(db.data[off+24:off+28], crc)
	// （可选）为了 crash 测试更稳定，可以强刷一下
	// _ = unix.Msync(db.data[off:off+recTotal], unix.MS_SYNC)
	db.logEnd += recTotal

	//更新idx
	sid := util.Str2Int(key, msg.ShardSize)
	shard := &db.shards[sid]
	shard.rw.Lock()
	oldEntity, hadOld := shard.idx[key]
	if hadOld { //释放该key旧value的内存
		db.FreeBlock(oldEntity.ValOff, oldEntity.ValLen)
	}
	shard.idx[key] = Entry{ValOff: valOff, ValLen: valueLen}
	shard.rw.Unlock()
	db.mu.Unlock()

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
	//if end > uint64(len(db.data)) {
	//	return nil, false, msg.ErrNoSpace
	//}
	//拷贝返回，避免调用方误改 mmap 区
	v := append([]byte(nil), db.data[start:end]...)
	return v, true, nil
}

func (db *DB) Del(key string) error {
	if len(key) == 0 || len(key) > int(^uint16(0)) {
		return msg.ErrBadArgument
	}
	keyLen := len(key)
	if keyLen > int(^uint16(0)) {
		return msg.ErrBadArgument
	}
	recTotal := uint64(msg.HeaderSize) + uint64(keyLen)

	db.mu.Lock()
	if db.logEnd+recTotal > db.valEnd {
		db.mu.Unlock()
		return msg.ErrNoSpace
	}

	//追加log record 进log区
	off := db.logEnd
	//写log Header
	h := header{
		Magic:  msg.Magic,
		Ver:    msg.Version,
		Flags:  msg.FlagDel,
		KeyLen: uint16(len(key)),
		ValLen: 0,
		ValOff: 0,
		CRC32:  0,
	}
	keyStart := off + msg.HeaderSize
	encodeHeader(db.data[off:keyStart], h)
	//写keyBytes
	copy(db.data[keyStart:keyStart+uint64(h.KeyLen)], key)
	keyBytes := db.data[keyStart : keyStart+uint64(h.KeyLen)]
	valBytes := db.data[keyStart+uint64(h.ValLen):]
	crc := calcCRC(
		msg.FlagDel,
		h.KeyLen,
		0,
		0,
		keyBytes,
		valBytes)
	binary.LittleEndian.PutUint32(db.data[off+24:off+28], crc)
	//（可选）为了 crash 测试更稳定，可以强刷一下
	//_ = unix.Msync(db.data[off:off+recTotal], unix.MS_SYNC)
	db.logEnd += recTotal

	//释放旧value内存
	sid := util.Str2Int(key, msg.ShardSize)
	shard := &db.shards[sid]
	shard.rw.RLock()
	oldEntity, hadOld := shard.idx[key]
	if hadOld { //释放该key旧value的内存
		db.FreeBlock(oldEntity.ValOff, oldEntity.ValLen)
	}
	shard.rw.RUnlock()

	db.mu.Unlock()

	//删除索引
	shard.rw.Lock()
	defer shard.rw.Unlock()

	delete(db.shards[sid].idx, key)
	return nil
}

// Alloc 分配内存，不命中空闲区则更新valEnd，必须在db.mu.Lock()保护下调用
func (db *DB) Alloc(n uint32) (off uint64, ok bool) {
	c := util.SizeClass(n)
	if c == 0 {
		return 0, true
	}
	//复用freelist空闲值区
	if ptrList, ok := db.free[c]; ok && len(ptrList) > 0 {
		freeValOff := ptrList[len(ptrList)-1]
		db.free[c] = ptrList[:len(ptrList)-1]
		return freeValOff, true
	}
	//无空闲，则分配新区
	need := uint64(c)
	if db.valEnd < need || db.valEnd-need < db.logEnd {
		return 0, false
	}
	db.valEnd -= need
	return db.valEnd, true
}

// FreeBlock 释放块内存，加入空闲区，必须在db.mu.Lock()保护下调用
func (db *DB) FreeBlock(off uint64, n uint32) {
	c := util.SizeClass(n)
	if c == 0 {
		return
	}
	db.free[c] = append(db.free[c], off)
}
