package engine

import (
	"encoding/binary"
	"md_master/consts"
	"md_master/internal/errs"
	"md_master/internal/index"
	"md_master/internal/record"
	"md_master/internal/segment"
)

func (db *DB) lastSeg() *segment.Segment {
	return db.segMgr.Last()
}

func (db *DB) Set(key string, value []byte) error {
	if len(key) == 0 || len(key) > int(^uint16(0)) {
		return errs.ErrBadArgument
	}
	if len(value) == 0 || len(value) > int(^uint32(0)) {
		return errs.ErrBadArgument
	}
	keyLen := len(key)
	valLen := uint32(len(value))
	recTotal := uint64(consts.HeaderSize) + uint64(keyLen)

	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	seg := db.lastSeg()
	if seg == nil || seg.GetData() == nil {
		return errs.ErrClosed
	}
	valOff, ok := seg.Alloc(valLen, recTotal)
	if !ok {
		db.lifeMu.Lock()
		newSeg, err := db.segMgr.ApnSeg()
		db.lifeMu.Unlock()
		if err != nil {
			return err
		}
		seg = newSeg
		valOff, ok = seg.Alloc(valLen, recTotal)
		if !ok {
			return errs.ErrNoSpace
		}
	}
	data := seg.GetData()
	copy(data[valOff:valOff+uint64(valLen)], value)
	off := seg.LogEnd()
	h := record.Header{
		Magic:  consts.Magic,
		Ver:    consts.Version,
		Flags:  consts.FlagPut,
		KeyLen: uint16(keyLen),
		ValLen: valLen,
		ValOff: valOff,
		CRC32:  0,
	}
	record.EncodeHeader(data[off:off+consts.HeaderSize], h)
	keyStart := off + consts.HeaderSize
	keyEnd := keyStart + uint64(h.KeyLen)
	copy(data[keyStart:keyEnd], key)
	crc := record.CalcCRC(consts.FlagPut, uint16(keyLen), valLen, valOff, data[keyStart:keyEnd])
	binary.LittleEndian.PutUint32(data[off+24:off+28], crc)
	seg.SetLogEnd(off + recTotal)

	old, hadOld := db.idx.Get(key)
	if hadOld && old.SegID == seg.ID() {
		seg.FreeBlock(old.ValOff, old.ValLen)
	}
	db.idx.Set(key, index.Entry{SegID: seg.ID(), ValOff: valOff, ValLen: valLen})
	return nil
}

func (db *DB) Get(key string) ([]byte, bool, error) {
	db.lifeMu.RLock()
	defer db.lifeMu.RUnlock()
	segs := db.segMgr.Segments()
	if len(segs) == 0 {
		return nil, false, errs.ErrClosed
	}
	e, ok := db.idx.Get(key)
	if !ok {
		return nil, false, nil
	}
	if int(e.SegID) >= len(segs) {
		return nil, false, errs.ErrCorrupt
	}
	seg := segs[e.SegID]
	if seg == nil || seg.GetData() == nil {
		return nil, false, errs.ErrClosed
	}
	data := seg.GetData()
	start := e.ValOff
	end := start + uint64(e.ValLen)
	if end > uint64(len(data)) {
		return nil, false, errs.ErrCorrupt
	}
	return append([]byte(nil), data[start:end]...), true, nil
}

func (db *DB) Del(key string) error {
	if len(key) == 0 || len(key) > int(^uint16(0)) {
		return errs.ErrBadArgument
	}
	recTotal := uint64(consts.HeaderSize) + uint64(len(key))

	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	seg := db.lastSeg()
	if seg == nil || seg.GetData() == nil {
		return errs.ErrClosed
	}
	if seg.LogEnd()+recTotal > seg.ValEnd() {
		db.lifeMu.Lock()
		newSeg, err := db.segMgr.ApnSeg()
		db.lifeMu.Unlock()
		if err != nil {
			return err
		}
		seg = newSeg
	}
	data := seg.GetData()
	off := seg.LogEnd()
	h := record.Header{
		Magic:  consts.Magic,
		Ver:    consts.Version,
		Flags:  consts.FlagDel,
		KeyLen: uint16(len(key)),
		ValLen: 0,
		ValOff: 0,
		CRC32:  0,
	}
	keyStart := off + consts.HeaderSize
	record.EncodeHeader(data[off:keyStart], h)
	copy(data[keyStart:keyStart+uint64(h.KeyLen)], key)
	keyBytes := data[keyStart : keyStart+uint64(h.KeyLen)]
	crc := record.CalcCRC(consts.FlagDel, h.KeyLen, 0, 0, keyBytes)
	binary.LittleEndian.PutUint32(data[off+24:off+28], crc)
	seg.SetLogEnd(off + recTotal)

	old, hadOld := db.idx.Get(key)
	if hadOld && old.SegID == seg.ID() {
		seg.FreeBlock(old.ValOff, old.ValLen)
	}
	db.idx.Del(key)
	return nil
}
