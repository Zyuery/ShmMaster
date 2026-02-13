package segment

import (
	"fmt"
	"md_master/internal/mmap"
	"os"
)

// Segment 单段：mmap 文件、log/value 边界、freelist。
type Segment struct {
	id     uint32
	path   string
	f      *os.File
	data   []byte
	logEnd uint64
	valEnd uint64
	free   map[uint32][]uint64
	truth  map[uint64]uint32
}

// ID 返回段 id。
func (s *Segment) ID() uint32 { return s.id }

// GetData 返回 mmap 切片（供 engine 读写），Close 后勿用。
func (s *Segment) GetData() []byte { return s.data }

// LogEnd 返回 log 区当前末尾。
func (s *Segment) LogEnd() uint64 { return s.logEnd }

// SetLogEnd 设置 log 区末尾（Recover 用）。
func (s *Segment) SetLogEnd(v uint64) { s.logEnd = v }

// ValEnd 返回 value 区当前末尾。
func (s *Segment) ValEnd() uint64 { return s.valEnd }

// SetValEnd 设置 value 区末尾（Recover 用）。
func (s *Segment) SetValEnd(v uint64) { s.valEnd = v }

// DataLen 返回 mmap 长度，data 为 nil 时返回 0。
func (s *Segment) DataLen() int {
	if s.data == nil {
		return 0
	}
	return len(s.data)
}

// OpenSegment 打开或创建 segment 文件。
func OpenSegment(path string, id uint32, segSize int64, create bool) (*Segment, error) {
	flag := os.O_RDWR
	if create {
		flag |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, err
	}
	if create {
		if err := f.Truncate(segSize); err != nil {
			_ = f.Close()
			return nil, err
		}
	} else {
		st, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		if st.Size() != segSize {
			_ = f.Close()
			return nil, fmt.Errorf("segment size mismatch: %s", path)
		}
	}
	data, err := mmap.Map(f.Fd(), int(segSize))
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Segment{
		id:     id,
		path:   path,
		f:      f,
		data:   data,
		logEnd: 0,
		valEnd: uint64(len(data)),
		free:   make(map[uint32][]uint64),
		truth:  make(map[uint64]uint32),
	}, nil
}

// Alloc 分配 value 区块，不命中 freelist 则从尾部分配。
func (s *Segment) Alloc(n uint32, logNeed uint64) (off uint64, ok bool) {
	c := SizeClass(n)
	if c == 0 {
		return 0, false
	}
	if s.logEnd+logNeed <= s.valEnd {
		stack := s.free[c]
		for len(stack) > 0 {
			off = stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if cls, ok := s.truth[off]; ok && cls == c {
				delete(s.truth, off)
				s.free[c] = stack
				return off, true
			}
		}
		s.free[c] = stack
	} else {
		return 0, false
	}
	need := uint64(c)
	if s.valEnd < need {
		return 0, false
	}
	newValEnd := s.valEnd - need
	if s.logEnd+logNeed > newValEnd {
		return 0, false
	}
	s.valEnd = newValEnd
	return s.valEnd, true
}

// FreeBlock 释放块并加入 freelist；double-free 忽略。
func (s *Segment) FreeBlock(off uint64, n uint32) {
	c := SizeClass(n)
	if c == 0 {
		return
	}
	if cls, ok := s.truth[off]; ok && cls == c {
		return
	}
	s.truth[off] = c
	s.free[c] = append(s.free[c], off)
}

// MarkUsed 标记 offset 已占用（Recover 用）。
func (s *Segment) MarkUsed(off uint64) {
	delete(s.truth, off)
}

// ResetFreeTruth 清空 freelist 与 truth（Recover 前对最后段调用）。
func (s *Segment) ResetFreeTruth() {
	for c := range s.free {
		delete(s.free, c)
	}
	for off := range s.truth {
		delete(s.truth, off)
	}
}

// Close 刷盘、解除映射、关闭文件。
func (s *Segment) Close() error {
	if s.data != nil {
		if err := mmap.Sync(s.data); err != nil {
			return err
		}
		if err := mmap.Unmap(s.data); err != nil {
			return err
		}
		s.data = nil
	}
	if s.f != nil {
		if err := s.f.Close(); err != nil {
			return err
		}
		s.f = nil
	}
	s.free = nil
	s.truth = nil
	return nil
}
