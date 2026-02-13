package segment

import (
	"os"
	"path/filepath"
	"testing"
)

const testSegSize = 64 << 10

func TestOpenSegmentCreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.000")
	seg, err := OpenSegment(path, 0, testSegSize, true)
	if err != nil {
		t.Fatalf("openSegment create: %v", err)
	}
	defer seg.Close()
	st, _ := os.Stat(path)
	if st == nil {
		t.Fatal("segment file not created")
	}
	if st.Size() != testSegSize {
		t.Errorf("segment size: got %d want %d", st.Size(), testSegSize)
	}
	if seg.ValEnd() != uint64(testSegSize) || seg.LogEnd() != 0 || seg.DataLen() != testSegSize {
		t.Errorf("ValEnd=%d LogEnd=%d DataLen=%d", seg.ValEnd(), seg.LogEnd(), seg.DataLen())
	}
}

func TestOpenSegmentExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.001")
	seg1, _ := OpenSegment(path, 0, testSegSize, true)
	seg1.Close()
	seg2, err := OpenSegment(path, 0, testSegSize, false)
	if err != nil {
		t.Fatalf("open existing: %v", err)
	}
	defer seg2.Close()
	if seg2.ValEnd() != uint64(testSegSize) || seg2.LogEnd() != 0 {
		t.Errorf("ValEnd=%d LogEnd=%d", seg2.ValEnd(), seg2.LogEnd())
	}
}

func TestOpenSegmentSizeMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.002")
	seg, _ := OpenSegment(path, 0, testSegSize, true)
	seg.Close()
	_, err := OpenSegment(path, 0, testSegSize/2, false)
	if err == nil {
		t.Fatal("expected error for size mismatch")
	}
}

func TestSegmentAlloc(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.003")
	seg, _ := OpenSegment(path, 0, testSegSize, true)
	defer seg.Close()
	_, ok := seg.Alloc(0, 0)
	if ok {
		t.Error("Alloc(0) should return false")
	}
	logNeed := uint64(64)
	off1, ok := seg.Alloc(16, logNeed)
	if !ok {
		t.Fatal("Alloc(16) failed")
	}
	if off1 >= seg.ValEnd()+16 || off1+16 > uint64(testSegSize) {
		t.Errorf("Alloc offset %d out of range", off1)
	}
	valEndAfterFirst := seg.ValEnd()
	seg.Alloc(32, logNeed)
	if seg.ValEnd() >= valEndAfterFirst {
		t.Error("valEnd should decrease after tail alloc")
	}
}

func TestSegmentAllocNoSpace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.004")
	seg, _ := OpenSegment(path, 0, testSegSize, true)
	defer seg.Close()
	logNeed := uint64(256)
	for {
		_, ok := seg.Alloc(16, logNeed)
		if !ok {
			break
		}
	}
	_, ok := seg.Alloc(16, logNeed)
	if ok {
		t.Error("expected Alloc to fail when no space")
	}
}

func TestSegmentFreeBlockAndReuse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.005")
	seg, _ := OpenSegment(path, 0, testSegSize, true)
	defer seg.Close()
	logNeed := uint64(128)
	off1, _ := seg.Alloc(16, logNeed)
	seg.FreeBlock(off1, 16)
	off2, ok := seg.Alloc(16, logNeed)
	if !ok || off1 != off2 {
		t.Errorf("expected reuse %d got %d ok=%v", off1, off2, ok)
	}
}

func TestSegmentFreeBlockDoubleFree(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.006")
	seg, _ := OpenSegment(path, 0, testSegSize, true)
	defer seg.Close()
	logNeed := uint64(128)
	off, _ := seg.Alloc(16, logNeed)
	seg.FreeBlock(off, 16)
	seg.FreeBlock(off, 16)
	off2, ok := seg.Alloc(16, logNeed)
	if !ok || off != off2 {
		t.Errorf("after double-free alloc: off=%d off2=%d ok=%v", off, off2, ok)
	}
}

func TestSegmentClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.008")
	seg, _ := OpenSegment(path, 0, testSegSize, true)
	if err := seg.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if seg.DataLen() != 0 {
		t.Error("Close should set data to nil")
	}
	_ = seg.Close()
}
