package main

import (
	"fmt"
	"math/rand"
	"md_master/core"
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
	db := mustOpenBenchDB(b, "bench.data", 1<<30)
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
	db := mustOpenBenchDB(b, "bench.data", 1<<30)
	defer db.Close()

	warmupKeys(b, db, 100_000)

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
	db := mustOpenBenchDB(b, "bench.data", 1<<30)
	defer db.Close()

	//warmupKeys(b, db, 100_000)

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
	db := mustOpenBenchDB(b, "bench.data", 1<<30)
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
