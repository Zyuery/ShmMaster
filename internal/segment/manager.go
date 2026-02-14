package segment

import (
	"os"
	"shm_master/internal/fs"
)

// Manager 管理多段：按 base 扫描已有段、追加新段。
type Manager struct {
	base    string
	segSize int64
	segs    []*Segment
}

// NewManager 创建 manager，不打开文件。
func NewManager(base string, segSize int64) *Manager {
	return &Manager{base: base, segSize: segSize, segs: make([]*Segment, 0, 4)}
}

// OpenBase 扫描 base.000, base.001, ... 打开已存在的 segment。
func (m *Manager) OpenBase() error {
	for id := uint32(0); ; id++ {
		p := fs.SegPath(m.base, id)
		if _, err := os.Stat(p); err != nil {
			if os.IsNotExist(err) {
				break
			}
			return err
		}
		seg, err := OpenSegment(p, id, m.segSize, false)
		if err != nil {
			return err
		}
		m.segs = append(m.segs, seg)
	}
	return nil
}

// EnsureOne 若尚无段则创建 base.000。
func (m *Manager) EnsureOne() error {
	if len(m.segs) > 0 {
		return nil
	}
	p := fs.SegPath(m.base, 0)
	seg, err := OpenSegment(p, 0, m.segSize, true)
	if err != nil {
		return err
	}
	m.segs = append(m.segs, seg)
	return nil
}

// Segments 返回当前段列表（只读）。
func (m *Manager) Segments() []*Segment {
	return m.segs
}

// Last 返回最后一个段。
func (m *Manager) Last() *Segment {
	if len(m.segs) == 0 {
		return nil
	}
	return m.segs[len(m.segs)-1]
}

// ApnSeg 追加一个新段。
func (m *Manager) ApnSeg() (*Segment, error) {
	id := uint32(len(m.segs))
	p := fs.SegPath(m.base, id)
	seg, err := OpenSegment(p, id, m.segSize, true)
	if err != nil {
		return nil, err
	}
	m.segs = append(m.segs, seg)
	return seg, nil
}

// Close 关闭所有段。
func (m *Manager) Close() error {
	var firstErr error
	for _, seg := range m.segs {
		if seg != nil {
			if err := seg.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	m.segs = nil
	return firstErr
}
