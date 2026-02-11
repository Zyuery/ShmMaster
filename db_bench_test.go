package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"md_master/core"
	"sync"
	"testing"
)

func mustOpenBenchDB(b *testing.B, path string, size int64) *core.DB {
	b.Helper()
	db, err := core.Open(path, size)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	return db
}

func warmupKeys(b *testing.B, db *core.DB, n int) {
	b.Helper()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("player:%d", i)
		if err := db.Set(k, []byte("hello")); err != nil {
			b.Fatalf("warmup set: %v", err)
		}
	}
}

func BenchmarkGetParallel(b *testing.B) {
	db := mustOpenBenchDB(b, "bench.data", 128<<20)
	defer db.Close()

	//warmupKeys(b, db, 100_000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(1)) // 每个 goroutine 自己的随机源
		for pb.Next() {
			key := fmt.Sprintf("player:%d", r.Intn(100_000))
			_, _, _ = db.Get(key)
		}
	})
}

func BenchmarkMix9010Parallel(b *testing.B) {
	db := mustOpenBenchDB(b, "bench.data", 128<<20)
	defer db.Close()

	//warmupKeys(b, db, 100_000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(1)) // 每个 goroutine 自己的随机源
		for pb.Next() {
			i := r.Intn(100_000)
			if r.Intn(100) < 10 { // 10%s 写
				_ = db.Set(fmt.Sprintf("player:%d", i), []byte("world"))
			} else { // 90% 读s
				_, _, _ = db.Get(fmt.Sprintf("player:%d", i))
			}
		}
	})
}

func BenchmarkMix5050Parallel(b *testing.B) {
	db := mustOpenBenchDB(b, "bench.data", 128<<20)
	defer db.Close()

	warmupKeys(b, db, 100_000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(1)) // 每个 goroutine 自己的随机源
		for pb.Next() {
			i := r.Intn(100_000)
			if r.Intn(100) < 50 { // 50%s 写
				_ = db.Set(fmt.Sprintf("player:%d", i), []byte("world"))
			} else { // 50% 读
				_, _, _ = db.Get(fmt.Sprintf("player:%d", i))
			}
		}
	})
}

func BenchmarkSetParallel(b *testing.B) {
	db := mustOpenBenchDB(b, "bench.data", 128<<20)
	defer db.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(1))
		for pb.Next() {
			i := r.Int()
			_ = db.Set(fmt.Sprintf("k:%d", i), []byte("v"))
		}
	})
}

// TestDataCorrectness 校验 Put/Del/Get 后存储数据是否正确，含重启后再次校验
func TestDataCorrectness(t *testing.T) {
	const keyCount = 2000
	const opsPerRound = 5000
	const dbSize = 32 << 20

	path := t.TempDir() + "/correctness.data"
	db, err := core.Open(path, dbSize)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// 期望表：key -> value，不存在则 key 不在 map 里
	expected := make(map[string][]byte)
	var mu sync.RWMutex

	// 随机 Put/Del/Get，并校验每次 Get
	r := rand.New(rand.NewSource(42))
	for round := 0; round < opsPerRound; round++ {
		i := r.Intn(keyCount)
		key := fmt.Sprintf("k:%d", i)
		switch r.Intn(3) {
		case 0: // Put
			val := []byte(fmt.Sprintf("v:%d:%d", i, round))
			if err := db.Set(key, val); err != nil {
				t.Fatalf("Set %q: %v", key, err)
			}
			mu.Lock()
			expected[key] = val
			mu.Unlock()

		case 1: // Del
			if err := db.Del(key); err != nil {
				t.Fatalf("Del %q: %v", key, err)
			}
			mu.Lock()
			delete(expected, key)
			mu.Unlock()

		case 2: // Get 并校验
			got, ok, err := db.Get(key)
			if err != nil {
				t.Fatalf("Get %q: %v", key, err)
			}
			mu.RLock()
			wantVal, wantOk := expected[key]
			mu.RUnlock()
			if wantOk {
				if !ok {
					t.Errorf("key %q: expected present, got missing", key)
				} else if !bytes.Equal(got, wantVal) {
					t.Errorf("key %q: value mismatch: got %q want %q", key, got, wantVal)
				}
			} else {
				if ok {
					t.Errorf("key %q: expected missing, got value %q", key, got)
				}
			}
		}
	}

	// 全量扫描：对每个可能存在的 key 做一次 Get，与 expected 一致
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("k:%d", i)
		got, ok, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get %q: %v", key, err)
		}
		mu.RLock()
		wantVal, wantOk := expected[key]
		mu.RUnlock()
		if wantOk != ok {
			t.Errorf("key %q: ok mismatch want=%v got=%v", key, wantOk, ok)
		}
		if wantOk && !bytes.Equal(got, wantVal) {
			t.Errorf("key %q: value mismatch: got %q want %q", key, got, wantVal)
		}
	}

	// 关闭后重新打开，依赖 Recover 重建索引，再全量校验一遍
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db2, err := core.Open(path, dbSize)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	mu.RLock()
	for key, wantVal := range expected {
		got, ok, err := db2.Get(key)
		if err != nil {
			mu.RUnlock()
			t.Fatalf("after reopen Get %q: %v", key, err)
		}
		if !ok {
			t.Errorf("after reopen key %q: expected present, got missing", key)
		} else if !bytes.Equal(got, wantVal) {
			t.Errorf("after reopen key %q: got %q want %q", key, got, wantVal)
		}
	}
	mu.RUnlock()

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("k:%d", i)
		mu.RLock()
		_, inExpected := expected[key]
		mu.RUnlock()
		if inExpected {
			continue
		}
		got, ok, _ := db2.Get(key)
		if ok {
			t.Errorf("after reopen key %q: expected missing, got %q", key, got)
		}
	}
}
