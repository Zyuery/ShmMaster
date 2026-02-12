package core

import (
	"fmt"
	"md_master/util"
	"os"

	"golang.org/x/sys/unix"
)

type Segment struct {
	id   uint32
	path string
	f    *os.File
	data []byte

	logEnd uint64
	valEnd uint64

	// 只需要在“当前写段”复用空间：也可以所有段都保留，简单起见全保留
	free  map[uint32][]uint64
	truth map[uint64]uint32
}

// ValEnd 返回 value 区当前末尾（供测试断言）
func (seg *Segment) ValEnd() uint64 { return seg.valEnd }

// LogEnd 返回 log 区当前末尾（供测试断言）
func (seg *Segment) LogEnd() uint64 { return seg.logEnd }

// DataLen 返回 mmap 长度，data 为 nil 时返回 0（供测试断言）
func (seg *Segment) DataLen() int {
	if seg.data == nil {
		return 0
	}
	return len(seg.data)
}

// openSegment 打开或创建一个 segment 文件
func OpenSegment(path string, id uint32, segSize int64, create bool) (*Segment, error) {
	flag := os.O_RDWR
	if create {
		flag |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, err
	}
	//创建新文件才截断
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
	//Mmap映射
	data, err := unix.Mmap(int(f.Fd()), 0, int(segSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
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

// Alloc 分配内存，不命中空闲区则更新valEnd
func (seg *Segment) Alloc(n uint32, logNeed uint64) (off uint64, ok bool) {
	c := util.SizeClass(n)
	if c == 0 {
		return 0, false
	}
	// 1)查freelist
	if seg.logEnd+logNeed <= seg.valEnd { //有空闲区
		stack := seg.free[c]
		for len(stack) > 0 {
			off = stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if cls, ok := seg.truth[off]; ok && cls == c {
				delete(seg.truth, off)
				seg.free[c] = stack
				return off, true
			}
		}
		seg.free[c] = stack
	} else {
		return 0, false
	}
	// tail 分配
	need := uint64(c)
	if seg.valEnd < need {
		return 0, false
	}
	newValEnd := seg.valEnd - need
	if seg.logEnd+logNeed > newValEnd {
		return 0, false
	}
	seg.valEnd = newValEnd
	return seg.valEnd, true
}

// FreeBlock 释放块内存，加入空闲区
func (seg *Segment) FreeBlock(off uint64, n uint32) {
	c := util.SizeClass(n)
	if c == 0 {
		return
	}
	if cls, ok := seg.truth[off]; ok && cls == c {
		return // 已经 free 过了，拒绝 double-free
	}
	seg.truth[off] = c
	seg.free[c] = append(seg.free[c], off)
}

// MarkUsed 标记内存已使用
func (seg *Segment) MarkUsed(off uint64) {
	delete(seg.truth, off)
}

// Close 关闭 segment 文件
func (seg *Segment) Close() error {
	if seg.data != nil {
		if err := unix.Msync(seg.data, unix.MS_SYNC); err != nil {
			return err
		}
		if err := unix.Munmap(seg.data); err != nil {
			return err
		}
		seg.data = nil
	}
	if seg.f != nil {
		if err := seg.f.Close(); err != nil {
			return err
		}
		seg.f = nil
	}
	seg.free = nil
	seg.truth = nil
	return nil
}
