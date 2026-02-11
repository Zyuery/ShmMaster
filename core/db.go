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

	mu     sync.RWMutex
	shards []shard

	free  map[uint32][]uint64 // size class -> offsets
	truth map[uint64]uint32   // offset -> size class
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
		mu:     sync.RWMutex{},
		shards: shards,
		free:   make(map[uint32][]uint64),
		truth:  make(map[uint64]uint32),
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

	var err error
	if db.data != nil {
		if e := unix.Msync(db.data, unix.MS_SYNC); err == nil {
			err = e
		}
		if e := unix.Munmap(db.data); err == nil {
			err = e
		}
		db.data = nil
	}
	if db.f != nil {
		if e := db.f.Close(); err == nil {
			err = e
		}
		db.f = nil
	}

	db.free = nil
	db.truth = nil
	db.shards = nil
	return err
}

// Recover 恢复索引，更新logEnd、valEnd
func (db *DB) Recover() error {
	if db.free == nil {
		db.free = make(map[uint32][]uint64)
	} else {
		for k := range db.free {
			delete(db.free, k)
		}
	}
	if db.truth == nil {
		db.truth = make(map[uint64]uint32)
	} else {
		for k := range db.truth {
			delete(db.truth, k)
		}
	}
	var off uint64 = 0
	fileLimit := uint64(len(db.data))
	minValOff := fileLimit
Loop:
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
		if h.Flags == msg.FlagPut {
			if h.ValOff > fileLimit || uint64(h.ValLen) > fileLimit || h.ValOff+uint64(h.ValLen) > fileLimit {
				break
			}
			if h.ValOff < off+recordLen {
				break
			}
		}
		if calcCRC(h.Flags, h.KeyLen, h.ValLen, h.ValOff, keyBytes) != h.CRC32 {
			break
		}
		if h.Flags == msg.FlagPut && h.ValOff < minValOff {
			minValOff = h.ValOff
		}
		// 校验通过，更新索引
		k := string(keyBytes)
		sid := util.Str2Int(k, msg.ShardSize)
		shard := &db.shards[sid]
		oldEntity, hadOld := shard.idx[k]
		switch h.Flags {
		case msg.FlagPut:
			if hadOld {
				db.FreeBlock(oldEntity.ValOff, oldEntity.ValLen)
			}
			db.MarkUsed(h.ValOff) //标记占用真相
			shard.idx[k] = Entry{ValOff: h.ValOff, ValLen: h.ValLen}
		case msg.FlagDel:
			if hadOld {
				db.FreeBlock(oldEntity.ValOff, oldEntity.ValLen)
			}
			delete(shard.idx, k)
		default:
			break Loop //未知flag
		}
		off += recordLen
	}
	db.logEnd = off
	db.valEnd = minValOff
	return nil
}

func (db *DB) Set(key string, value []byte) error {
	if db.data == nil {
		return msg.ErrClosed
	}
	if len(key) == 0 || len(key) > int(^uint16(0)) {
		return msg.ErrBadArgument
	}
	keyLen := len(key)
	valueLen := uint32(len(value))
	if keyLen > int(^uint16(0)) || len(value) > int(^uint32(0)) {
		return msg.ErrBadArgument
	}
	if valueLen == 0 {
		return msg.ErrBadArgument
	}
	recTotal := uint64(msg.HeaderSize) + uint64(keyLen)

	//写value入value区
	db.mu.Lock()
	var valOff uint64
	off, ok := db.Alloc(valueLen, recTotal)
	if !ok {
		db.mu.Unlock()
		return msg.ErrNoSpace
	}
	valOff = off
	copy(db.data[valOff:valOff+uint64(valueLen)], value)

	//追加log record进log区
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
		db.data[keyStart:keyEnd])
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
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.data == nil {
		return nil, false, msg.ErrClosed
	}
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
	if end > uint64(len(db.data)) || start < db.valEnd {
		return nil, false, msg.ErrNoSpace // 或 ErrCorrupt
	}
	//拷贝返回，避免调用方误改 mmap 区
	v := append([]byte(nil), db.data[start:end]...)
	return v, true, nil
}

func (db *DB) Del(key string) error {
	if db.data == nil {
		return msg.ErrClosed
	}
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
	crc := calcCRC(
		msg.FlagDel,
		h.KeyLen,
		0,
		0,
		keyBytes)
	binary.LittleEndian.PutUint32(db.data[off+24:off+28], crc)
	//（可选）为了 crash 测试更稳定，可以强刷一下
	//_ = unix.Msync(db.data[off:off+recTotal], unix.MS_SYNC)
	db.logEnd += recTotal

	//释放旧value内存
	sid := util.Str2Int(key, msg.ShardSize)
	shard := &db.shards[sid]
	shard.rw.Lock()
	oldEntity, hadOld := shard.idx[key]
	if hadOld { //释放该key旧value的内存
		db.FreeBlock(oldEntity.ValOff, oldEntity.ValLen)
	}
	//删除索引
	delete(shard.idx, key)
	shard.rw.Unlock()

	db.mu.Unlock()
	return nil
}

// Alloc 分配内存，不命中空闲区则更新valEnd，必须在db.mu.Lock()保护下或单线程调用
func (db *DB) Alloc(n uint32, logNeed uint64) (off uint64, ok bool) {
	c := util.SizeClass(n)
	if c == 0 {
		return 0, false
	}
	// 1)查freelist
	if db.logEnd+logNeed <= db.valEnd { //有空闲区
		stack := db.free[c]
		for len(stack) > 0 {
			off = stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if cls, ok := db.truth[off]; ok && cls == c {
				delete(db.truth, off)
				db.free[c] = stack
				return off, true
			}
		}
		db.free[c] = stack
	} else {
		return 0, false
	}
	// tail 分配
	need := uint64(c)
	if db.valEnd < need {
		return 0, false
	}
	newValEnd := db.valEnd - need
	if db.logEnd+logNeed > newValEnd {
		return 0, false
	}
	db.valEnd = newValEnd
	return db.valEnd, true
}

// FreeBlock 释放块内存，加入空闲区，必须在db.mu.Lock()保护下或单线程调用
func (db *DB) FreeBlock(off uint64, n uint32) {
	c := util.SizeClass(n)
	if c == 0 {
		return
	}
	if cls, ok := db.truth[off]; ok && cls == c {
		return // 已经 free 过了，拒绝 double-free
	}
	db.truth[off] = c
	db.free[c] = append(db.free[c], off)
}

// MarkUsed 标记内存已使用，必须在db.mu.Lock()保护下或单线程调用
func (db *DB) MarkUsed(off uint64) {
	delete(db.truth, off)
}
