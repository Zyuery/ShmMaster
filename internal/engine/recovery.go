package engine

import (
	"md_master/consts"
	"md_master/internal/index"
	"md_master/internal/record"
	"md_master/internal/segment"
)

// Recover 重放所有段的 log，重建 index 并更新每段的 logEnd/valEnd。
func (db *DB) Recover() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	db.idx.Clear()
	segs := db.segMgr.Segments()
	if len(segs) == 0 {
		return nil
	}
	lastID := segs[len(segs)-1].ID()
	for _, seg := range segs {
		if seg.ID() == lastID {
			seg.ResetFreeTruth()
		}
		if err := db.recoverOne(seg, lastID); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) recoverOne(seg *segment.Segment, lastID uint32) error {
	data := seg.GetData()
	off := uint64(0)
	fileLimit := uint64(len(data))
	minValOff := fileLimit
Loop:
	for {
		if off+consts.HeaderSize > minValOff {
			break
		}
		h := record.DecodeHeader(data[off : off+consts.HeaderSize])
		if h.Magic != consts.Magic || h.Ver != consts.Version || h.KeyLen == 0 {
			break
		}
		recLen := uint64(consts.HeaderSize) + uint64(h.KeyLen)
		if off+recLen > minValOff {
			break
		}
		keyStart := off + consts.HeaderSize
		keyBytes := data[keyStart : keyStart+uint64(h.KeyLen)]
		if h.Flags == consts.FlagPut {
			if h.ValOff > fileLimit || uint64(h.ValLen) > fileLimit || h.ValOff+uint64(h.ValLen) > fileLimit {
				break
			}
			if h.ValOff < off+recLen {
				break
			}
		}
		if record.CalcCRC(h.Flags, h.KeyLen, h.ValLen, h.ValOff, keyBytes) != h.CRC32 {
			break
		}
		if h.Flags == consts.FlagPut && h.ValOff < minValOff {
			minValOff = h.ValOff
		}
		k := string(keyBytes)
		old, hadOld := db.idx.Get(k)
		switch h.Flags {
		case consts.FlagPut:
			if seg.ID() == lastID && hadOld && old.SegID == lastID {
				seg.FreeBlock(old.ValOff, old.ValLen)
			}
			if seg.ID() == lastID {
				seg.MarkUsed(h.ValOff)
			}
			db.idx.Set(k, index.Entry{SegID: seg.ID(), ValOff: h.ValOff, ValLen: h.ValLen})
		case consts.FlagDel:
			if seg.ID() == lastID && hadOld && old.SegID == lastID {
				seg.FreeBlock(old.ValOff, old.ValLen)
			}
			db.idx.Del(k)
		default:
			break Loop
		}
		off += recLen
	}
	seg.SetLogEnd(off)
	seg.SetValEnd(minValOff)
	return nil
}
