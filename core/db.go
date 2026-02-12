package core

import (
	"encoding/binary"
	"md_master/msg"
	"md_master/util"
	"os"
	"sync"
)

type Entry struct {
	SegID  uint32
	ValOff uint64
	ValLen uint32
}
type shard struct {
	rw  sync.RWMutex
	idx map[string]Entry
}

type DB struct {
	lifeMu  sync.RWMutex // 保护 segs｜Mmap 生命周期，Get 持 RLock，Close 持 Lock
	writeMu sync.Mutex   // 串行化 Set/Del/Recover/Close

	base    string
	segSize int64

	segs   []*Segment
	shards []shard
}

func NewDB(base string, segSize int64, shardN int) *DB {
	shards := make([]shard, shardN)
	for i := range shards {
		shards[i].idx = make(map[string]Entry)
	}
	return &DB{
		base:    base,
		segSize: segSize,
		segs:    make([]*Segment, 0, 4),
		shards:  shards,
	}
}

func Open(base string, segSize int64) (*DB, error) {
	db := NewDB(base, segSize, msg.ShardSize)
	for id := uint32(0); ; id++ {
		p := util.SegPath(base, id)
		// 发现已有 segment
		if _, err := os.Stat(p); err != nil {
			if os.IsNotExist(err) {
				break
			}
			return nil, err
		}
		// 打开 segment
		seg, err := OpenSegment(p, id, segSize, false)
		if err != nil {
			return nil, err
		}
		db.segs = append(db.segs, seg)
	}
	// 没有 segment 时，创建第一个
	if len(db.segs) == 0 {
		p := util.SegPath(base, 0)
		seg, err := OpenSegment(p, 0, segSize, true)
		if err != nil {
			return nil, err
		}
		db.segs = append(db.segs, seg)
	}

	if err := db.Recover(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// Close 关闭文件 + 取消映射
func (db *DB) Close() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.lifeMu.Lock()
	defer db.lifeMu.Unlock()
	var firstErr error
	for _, seg := range db.segs {
		if seg == nil {
			continue
		}
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	db.segs = nil
	db.shards = nil
	return firstErr
}

// ApnSeg 追加 segment,只在写路径里触发：必须拿 lifeMu 写锁
func (db *DB) ApnSeg(base string, segSize int64) (*Segment, error) {
	id := uint32(len(db.segs))
	p := util.SegPath(base, id)
	seg, err := OpenSegment(p, id, segSize, true)
	if err != nil {
		return nil, err
	}
	db.segs = append(db.segs, seg)
	return seg, nil
}

// lastSeg 返回最后一个 segment，拿writeMu或者lifeMu读锁
func (db *DB) lastSeg() *Segment {
	if len(db.segs) == 0 {
		return nil
	}
	return db.segs[len(db.segs)-1]
}

func (db *DB) Recover() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	// 清空 idx（Recover 后 idx 以磁盘为准）
	for i := range db.shards {
		sh := &db.shards[i]
		sh.rw.Lock()
		for k := range sh.idx {
			delete(sh.idx, k)
		}
		sh.rw.Unlock()
	}
	if len(db.segs) == 0 {
		return nil
	}
	lastID := db.segs[len(db.segs)-1].id
	// 逐段扫
	for _, seg := range db.segs {
		// 只有最后段需要 freelist/truth
		if seg.id == lastID {
			// 清 allocator 状态
			for k := range seg.free {
				delete(seg.free, k)
			}
			for k := range seg.truth {
				delete(seg.truth, k)
			}
		}
		if err := db.recoverOne(seg, lastID); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) recoverOne(seg *Segment, lastID uint32) error {
	off := uint64(0)
	fileLimit := uint64(len(seg.data))
	minValOff := fileLimit
Loop:
	for {
		if off+msg.HeaderSize > minValOff {
			break
		}
		h := decodeHeader(seg.data[off : off+msg.HeaderSize])
		if h.Magic != msg.Magic || h.Ver != msg.Version || h.KeyLen == 0 {
			break
		}
		recLen := uint64(msg.HeaderSize) + uint64(h.KeyLen)
		if off+recLen > minValOff {
			break
		}
		keyStart := off + msg.HeaderSize
		keyBytes := seg.data[keyStart : keyStart+uint64(h.KeyLen)]
		if h.Flags == msg.FlagPut {
			if h.ValOff > fileLimit || uint64(h.ValLen) > fileLimit || h.ValOff+uint64(h.ValLen) > fileLimit {
				break
			}
			if h.ValOff < off+recLen { // value 不能落在 log 记录内部
				break
			}
		}
		if calcCRC(h.Flags, h.KeyLen, h.ValLen, h.ValOff, keyBytes) != h.CRC32 {
			break
		}
		if h.Flags == msg.FlagPut && h.ValOff < minValOff {
			minValOff = h.ValOff
		}
		k := string(keyBytes)
		sid := util.Str2Int(k, msg.ShardSize)
		sh := &db.shards[sid]
		old, hadOld := sh.idx[k]
		switch h.Flags {
		// 只回收“最后段内部”的旧值
		case msg.FlagPut:
			if seg.id == lastID && hadOld && old.SegID == lastID {
				seg.FreeBlock(old.ValOff, old.ValLen)
			}
			if seg.id == lastID {
				seg.MarkUsed(h.ValOff)
			}
			sh.idx[k] = Entry{SegID: seg.id, ValOff: h.ValOff, ValLen: h.ValLen}
		case msg.FlagDel:
			if seg.id == lastID && hadOld && old.SegID == lastID {
				seg.FreeBlock(old.ValOff, old.ValLen)
			}
			delete(sh.idx, k)
		default:
			break Loop
		}
		off += recLen
	}
	seg.logEnd = off
	seg.valEnd = minValOff
	return nil
}

func (db *DB) Set(key string, value []byte) error {
	if len(key) == 0 || len(key) > int(^uint16(0)) {
		return msg.ErrBadArgument
	}
	if len(value) == 0 || len(value) > int(^uint32(0)) {
		return msg.ErrBadArgument
	}
	keyLen := len(key)
	valLen := uint32(len(value))
	recTotal := uint64(msg.HeaderSize) + uint64(keyLen)

	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	//写value
	seg := db.lastSeg()
	if seg == nil || seg.data == nil {
		return msg.ErrClosed
	}
	valOff, ok := seg.Alloc(valLen, recTotal)
	if !ok {
		// 扩容：append 新段（需要 lifeMu.Lock，保护 segs slice 给 Get 用）
		db.lifeMu.Lock()
		newSeg, err := db.ApnSeg(db.base, db.segSize)
		db.lifeMu.Unlock()
		if err != nil {
			return err
		}
		seg = newSeg
		valOff, ok = seg.Alloc(valLen, recTotal)
		if !ok {
			return msg.ErrNoSpace
		}
	}
	copy(seg.data[valOff:valOff+uint64(valLen)], value)
	// 追加 log（写在同一个 seg 的 log 区）
	off := seg.logEnd
	h := header{
		Magic:  msg.Magic,
		Ver:    msg.Version,
		Flags:  msg.FlagPut,
		KeyLen: uint16(keyLen),
		ValLen: valLen,
		ValOff: valOff,
		CRC32:  0,
	}
	encodeHeader(seg.data[off:off+msg.HeaderSize], h)
	keyStart := off + msg.HeaderSize
	keyEnd := keyStart + uint64(h.KeyLen)
	copy(seg.data[keyStart:keyEnd], key)

	crc := calcCRC(msg.FlagPut, uint16(keyLen), valLen, valOff, seg.data[keyStart:keyEnd])
	binary.LittleEndian.PutUint32(seg.data[off+24:off+28], crc)
	seg.logEnd += recTotal

	// 更新 idx（覆盖旧值只回收本段）
	sid := util.Str2Int(key, msg.ShardSize)
	sh := &db.shards[sid]
	sh.rw.Lock()
	old, hadOld := sh.idx[key]
	if hadOld && old.SegID == seg.id {
		seg.FreeBlock(old.ValOff, old.ValLen)
	}
	sh.idx[key] = Entry{SegID: seg.id, ValOff: valOff, ValLen: valLen}
	sh.rw.Unlock()
	return nil
}

func (db *DB) Get(key string) ([]byte, bool, error) {
	db.lifeMu.RLock()
	defer db.lifeMu.RUnlock()
	if len(db.segs) == 0 {
		return nil, false, msg.ErrClosed
	}

	sid := util.Str2Int(key, msg.ShardSize)
	shard := &db.shards[sid]
	shard.rw.RLock()
	e, ok := shard.idx[key]
	shard.rw.RUnlock()
	if !ok {
		return nil, false, nil
	}
	if int(e.SegID) >= len(db.segs) {
		return nil, false, msg.ErrCorrupt
	}
	//取kv所在segment
	seg := db.segs[e.SegID]
	if seg == nil || seg.data == nil {
		return nil, false, msg.ErrClosed
	}
	start := e.ValOff
	end := start + uint64(e.ValLen)
	if end > uint64(len(seg.data)) {
		return nil, false, msg.ErrCorrupt
	}
	//分配堆内存，拷贝传出
	v := append([]byte(nil), seg.data[start:end]...)
	return v, true, nil
}

func (db *DB) Del(key string) error {
	if len(key) == 0 || len(key) > int(^uint16(0)) {
		return msg.ErrBadArgument
	}
	recTotal := uint64(msg.HeaderSize) + uint64(len(key))

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	seg := db.lastSeg()
	if seg == nil || seg.data == nil {
		return msg.ErrClosed
	}

	// del 只需要 log 空间；不够就扩容
	if seg.logEnd+recTotal > seg.valEnd {
		db.lifeMu.Lock()
		newSeg, err := db.ApnSeg(db.base, db.segSize)
		db.lifeMu.Unlock()
		if err != nil {
			return err
		}
		seg = newSeg
	}
	off := seg.logEnd
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
	encodeHeader(seg.data[off:keyStart], h)
	copy(seg.data[keyStart:keyStart+uint64(h.KeyLen)], key)
	keyBytes := seg.data[keyStart : keyStart+uint64(h.KeyLen)]
	crc := calcCRC(msg.FlagDel, h.KeyLen, 0, 0, keyBytes)
	binary.LittleEndian.PutUint32(seg.data[off+24:off+28], crc)
	seg.logEnd += recTotal

	sid := util.Str2Int(key, msg.ShardSize)
	sh := &db.shards[sid]
	sh.rw.Lock()
	old, hadOld := sh.idx[key]
	if hadOld && old.SegID == seg.id {
		seg.FreeBlock(old.ValOff, old.ValLen)
	}
	delete(sh.idx, key)
	sh.rw.Unlock()

	return nil
}
