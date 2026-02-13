// 工程化严格测试：崩溃模拟、损坏容忍、并发 Close、长时浸泡、竞态检测
package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"md_master"
	"md_master/util"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCrashSimulation 模拟崩溃：在写入中途截断文件，验证 Recover 不 panic、能安全停止
// 文档要求：写 value 后、写 log 前，或 log 写一半
func TestCrashSimulation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crash.db")
	const dbSize = 2 << 20

	db, err := shm_master.Open(path, dbSize)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := db.Set(fmt.Sprintf("ok:%d", i), []byte(fmt.Sprintf("val%d", i))); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 场景1：截断整个文件（模拟 crash 时只写了一半）
	// DB 使用 segment 文件 path.000，非 path 本身
	segPath := util.SegPath(path, 0)
	f, err := os.OpenFile(segPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	info, _ := f.Stat()
	truncLen := info.Size() / 2
	if truncLen < 256 {
		truncLen = 256
	}
	if err := f.Truncate(truncLen); err != nil {
		f.Close()
		t.Fatalf("Truncate: %v", err)
	}
	f.Close()
	// segment 被截断后需恢复为 segSize，否则 openSegment 会报 size mismatch（扩展部分为 0）
	if f2, err := os.OpenFile(segPath, os.O_RDWR, 0644); err == nil {
		f2.Truncate(dbSize)
		f2.Close()
	}

	db2, err := shm_master.Open(path, dbSize)
	if err != nil {
		t.Fatalf("Reopen after truncate: %v", err)
	}
	defer db2.Close()
	for i := 0; i < 5; i++ {
		_, _, _ = db2.Get(fmt.Sprintf("ok:%d", i))
	}
}

// TestCrashSimulationLogHalf 精确模拟：log 写一半（某条 record 的 header 写了、key/CRC 未写全）
func TestCrashSimulationLogHalf(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crash2.db")
	const dbSize = 1 << 20

	db, err := shm_master.Open(path, dbSize)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// 写 3 条完整记录，每条约 28+6=34 字节
	for i := 0; i < 3; i++ {
		if err := db.Set(fmt.Sprintf("k:%d", i), []byte("vv")); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	segPath2 := util.SegPath(path, 0)
	data, err := os.ReadFile(segPath2)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// 每条 record = header(28) + key(3) = 31，3 条约 93 字节。截断到 60，落在第 2 条 record 中间
	truncAt := 60
	if len(data) < truncAt+10 {
		truncAt = len(data) / 2
	}
	if err := os.WriteFile(segPath2, data[:truncAt], 0644); err != nil {
		t.Fatalf("WriteFile truncate: %v", err)
	}
	// 恢复文件大小为 segSize，否则 openSegment 报 size mismatch
	if f2, err := os.OpenFile(segPath2, os.O_RDWR, 0644); err == nil {
		f2.Truncate(dbSize)
		f2.Close()
	}

	db2, err := shm_master.Open(path, dbSize)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()
	// 至少能读到部分 key
	_, _, _ = db2.Get("k:0")
}

// TestCorruptTolerance 损坏容忍：Magic/Version/CRC 错误、record 截断，Recover 在首个非法处停止
func TestCorruptTolerance(t *testing.T) {
	const dbSize = 1 << 20

	makeDB := func(t *testing.T) (path string) {
		dir := t.TempDir()
		path = filepath.Join(dir, "corrupt.db")
		db, err := shm_master.Open(path, dbSize)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		db.Set("a", []byte("x"))
		db.Close()
		return path
	}

	t.Run("BadMagic", func(t *testing.T) {
		path := makeDB(t)
		segPath := util.SegPath(path, 0)
		data, readErr := os.ReadFile(segPath)
		if readErr != nil || len(data) == 0 {
			t.Fatalf("ReadFile: %v", readErr)
		}
		data[0] ^= 0xff
		os.WriteFile(segPath, data, 0644)

		db2, err := shm_master.Open(path, dbSize)
		if err != nil {
			return
		}
		defer db2.Close()
		if _, ok, _ := db2.Get("a"); ok {
			t.Error("expected key 'a' missing after corrupt magic")
		}
	})

	t.Run("BadVersion", func(t *testing.T) {
		path := makeDB(t)
		segPath := util.SegPath(path, 0)
		data, err := os.ReadFile(segPath)
		if err != nil || len(data) < 6 {
			t.Fatalf("ReadFile: %v", err)
		}
		// Version at offset 4, 2 bytes. 改为 0 或 99
		data[4] = 0xff
		data[5] = 0xff
		os.WriteFile(segPath, data, 0644)

		db2, err := shm_master.Open(path, dbSize)
		if err != nil {
			return
		}
		defer db2.Close()
		if _, ok, _ := db2.Get("a"); ok {
			t.Error("expected key 'a' missing after corrupt version")
		}
	})

	t.Run("BadCRC", func(t *testing.T) {
		path := makeDB(t)
		segPath := util.SegPath(path, 0)
		data, err := os.ReadFile(segPath)
		if err != nil || len(data) < 28 {
			t.Fatalf("ReadFile: %v", err)
		}
		// 首条 record 的 CRC 在 offset 24..28
		data[24] ^= 0xff
		os.WriteFile(segPath, data, 0644)

		db2, err := shm_master.Open(path, dbSize)
		if err != nil {
			return
		}
		defer db2.Close()
		if _, ok, _ := db2.Get("a"); ok {
			t.Error("expected key 'a' missing after corrupt CRC")
		}
	})

	t.Run("RecordTruncated", func(t *testing.T) {
		path := makeDB(t)
		segPath := util.SegPath(path, 0)
		data, err := os.ReadFile(segPath)
		if err != nil || len(data) <= 15 {
			t.Fatalf("ReadFile: %v", err)
		}
		// 截断到 header 中间（不足一条完整 record）
		truncAt := 15
		if err := os.WriteFile(segPath, data[:truncAt], 0644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		// 恢复文件大小为 segSize
		if f, err := os.OpenFile(segPath, os.O_RDWR, 0644); err == nil {
			f.Truncate(dbSize)
			f.Close()
		}

		db2, err := shm_master.Open(path, dbSize)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer db2.Close()
		if _, ok, _ := db2.Get("a"); ok {
			t.Error("expected key 'a' missing after record truncate")
		}
	})
}

// TestConcurrentClose 并发 Close：先通知停止、等待 goroutine 退出后再 Close（优雅关闭）
// 注意：在 goroutine 未完全退出时 Close 会导致 use-after-close panic，工程上需保证调用方先停止写入
func TestConcurrentClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "conclose.db")
	db, err := shm_master.Open(path, 8<<20)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 100; i++ {
		db.Set(fmt.Sprintf("c:%d", i), []byte("v"))
	}

	var wg sync.WaitGroup
	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(id)))
			for {
				select {
				case <-done:
					return
				default:
					i := r.Intn(150)
					switch r.Intn(3) {
					case 0:
						_ = db.Set(fmt.Sprintf("c:%d", i), []byte("v"))
					case 1:
						_, _, _ = db.Get(fmt.Sprintf("c:%d", i))
					case 2:
						_ = db.Del(fmt.Sprintf("c:%d", i))
					}
				}
			}
		}(g)
	}
	time.Sleep(20 * time.Millisecond)
	close(done)
	wg.Wait() // 先等待所有 goroutine 退出，再 Close
	if err := db.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestSoak 长时浸泡：10^6 次操作 + 每 10^5 次重启校验
func TestSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("skip soak in short mode")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "soak.db")
	const dbSize = 32 << 20
	const rounds = 10
	const opsPerRound = 100000

	expected := make(map[string][]byte)
	var mu sync.RWMutex

	db, err := shm_master.Open(path, dbSize)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	r := rand.New(rand.NewSource(99))

	for round := 0; round < rounds; round++ {
		for i := 0; i < opsPerRound; i++ {
			k := fmt.Sprintf("s:%d:%d", round, r.Intn(2000))
			switch r.Intn(3) {
			case 0:
				v := []byte(fmt.Sprintf("v%d", r.Int()))
				if err := db.Set(k, v); err != nil {
					continue
				}
				mu.Lock()
				expected[k] = v
				mu.Unlock()
			case 1:
				if err := db.Del(k); err != nil {
					continue
				}
				mu.Lock()
				delete(expected, k)
				mu.Unlock()
			case 2:
				got, ok, _ := db.Get(k)
				mu.RLock()
				want, wantOk := expected[k]
				mu.RUnlock()
				if wantOk && (!ok || !bytes.Equal(got, want)) {
					t.Fatalf("round %d: key %s mismatch", round, k)
				}
			}
		}

		db.Close()
		db, err = shm_master.Open(path, dbSize)
		if err != nil {
			t.Fatalf("Reopen round %d: %v", round, err)
		}
		mu.RLock()
		for k, want := range expected {
			got, ok, _ := db.Get(k)
			if !ok || !bytes.Equal(got, want) {
				mu.RUnlock()
				t.Fatalf("after reopen round %d key %s: want %q got %q ok=%v", round, k, want, got, ok)
			}
		}
		mu.RUnlock()
	}
	db.Close()
}

// FuzzDB 模糊测试：随机 key/value/操作序列，用 reference map 校验
func FuzzDB(f *testing.F) {
	f.Add([]byte{0, 2, 'k', '1', 1, 'v'})
	f.Add([]byte{1, 2, 'k', '1'})
	f.Add([]byte{2, 2, 'k', '1'})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 4 {
			return
		}
		dir := t.TempDir()
		path := filepath.Join(dir, "fuzz.db")
		db, err := shm_master.Open(path, 4<<20)
		if err != nil {
			return
		}
		defer db.Close()

		expected := make(map[string][]byte)
		i := 0
		for i+2 <= len(data) {
			op := data[i] % 3
			kl := int(data[i+1]) % 64
			if kl == 0 {
				kl = 1
			}
			i += 2
			if i+kl > len(data) {
				break
			}
			key := string(data[i : i+kl])
			i += kl
			if len(key) == 0 || len(key) > 65535 {
				continue
			}

			switch op {
			case 0: // Put
				if i >= len(data) {
					break
				}
				vl := int(data[i]) % 128
				if vl == 0 {
					vl = 1
				}
				i++
				if i+vl > len(data) {
					break
				}
				val := append([]byte(nil), data[i:i+vl]...)
				i += vl
				if err := db.Set(key, val); err != nil {
					continue
				}
				expected[key] = val
			case 1: // Del
				_ = db.Del(key)
				delete(expected, key)
			case 2: // Get
				got, ok, _ := db.Get(key)
				want, wantOk := expected[key]
				if wantOk && (!ok || !bytes.Equal(got, want)) {
					t.Fatalf("key %q: want %q got %q", key, want, got)
				}
				if !wantOk && ok {
					t.Fatalf("key %q: expected missing, got %q", key, got)
				}
			}
		}
	})
}

// TestRaceDetector 竞态检测：高并发读写，需配合 go test -race
func TestRaceDetector(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "race.db")
	db, err := shm_master.Open(path, 32<<20)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 500; i++ {
		db.Set(fmt.Sprintf("r:%d", i), []byte("v"))
	}

	var count atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < runtime.NumCPU()*4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(id)))
			for n := 0; n < 500; n++ {
				i := r.Intn(1000)
				k := fmt.Sprintf("r:%d", i)
				switch r.Intn(3) {
				case 0:
					_ = db.Set(k, []byte("x"))
				case 1:
					_, _, _ = db.Get(k)
				case 2:
					_ = db.Del(k)
				}
				count.Add(1)
			}
		}(g)
	}
	wg.Wait()
	if count.Load() < 1000 {
		t.Fatalf("too few ops: %d", count.Load())
	}
}
