package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"md_master"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// acceptanceReport 验收测试报告
type acceptanceReport struct {
	Timestamp time.Time
	Phase     string // "stage-1-acceptance"
	Results   []testResult
	Summary   summary
}

type testResult struct {
	Category   string // 测试类别
	Name       string // 用例名
	Passed     bool
	DurationMs int64
	Error      string
}

type summary struct {
	Total   int
	Passed  int
	Failed  int
	Skipped int
}

// testCase 定义单个验收用例
type testCase struct {
	Category string
	Name     string
	Fn       func(t *testing.T)
}

// runAcceptance 运行全部验收测试并收集报告
func runAcceptance(t *testing.T, report *acceptanceReport) {
	report.Timestamp = time.Now()
	report.Phase = "stage-1-acceptance"
	report.Results = nil

	cases := []testCase{
		{"BasicCRUD", "SetGetDel", testBasicCRUD},
		{"ArgumentValidation", "EmptyKey", testEmptyKey},
		{"ArgumentValidation", "EmptyValue", testEmptyValue},
		{"ArgumentValidation", "KeyTooLong", testKeyTooLong},
		{"ArgumentValidation", "ValueMaxLen", testValueMaxLen},
		{"BoundaryValues", "SingleByteKeyValue", testSingleByteKeyValue},
		{"BoundaryValues", "MaxKeyLen", testMaxKeyLen},
		{"BoundaryValues", "LargeValue", testLargeValue},
		{"BoundaryValues", "BinaryValue", testBinaryValue},
		{"BoundaryValues", "GetNonExistent", testGetNonExistent},
		{"OverwriteAndDelete", "OverwriteSameKey", testOverwriteSameKey},
		{"OverwriteAndDelete", "DeleteNonExistent", testDeleteNonExistent},
		{"OverwriteAndDelete", "DelThenGet", testDelThenGet},
		{"Recovery", "ReopenAfterSet", testReopenAfterSet},
		{"Recovery", "ReopenAfterDel", testReopenAfterDel},
		{"Recovery", "RecoverFromEmpty", testRecoverFromEmpty},
		{"Recovery", "ReopenVerifyThenFreeReuseThenCloseVerify", testReopenVerifyFreeReuseCloseVerify},
		{"Recovery", "MultiCycleReopenVerify", testMultiCycleReopenVerify},
		{"SpaceExhaustion", "ErrNoSpace", testErrNoSpace},
		{"SpaceExhaustion", "DelFreesSpace", testDelFreesSpace},
		{"Fragmentation", "PutDelReuse", testPutDelReuse},
		{"Concurrency", "ParallelGetSet", testParallelGetSet},
		{"Concurrency", "ParallelMixOps", testParallelMixOps},
		{"Stress", "HighVolumeWrite", testHighVolumeWrite},
		{"Stress", "SustainedMix", testSustainedMix},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.Category+"/"+tc.Name, func(t *testing.T) {
			start := time.Now()
			tr := testResult{Category: tc.Category, Name: tc.Name}
			defer func() {
				tr.DurationMs = time.Since(start).Milliseconds()
				if e := recover(); e != nil {
					tr.Passed = false
					tr.Error = fmt.Sprintf("panic: %v", e)
				} else {
					tr.Passed = !t.Failed()
				}
				report.Results = append(report.Results, tr)
			}()
			tc.Fn(t)
		})
	}

	// 汇总
	report.Summary.Total = len(report.Results)
	for _, r := range report.Results {
		if r.Passed {
			report.Summary.Passed++
		} else {
			report.Summary.Failed++
		}
	}
}

// 辅助：打开临时 DB
func tempDB(t *testing.T, size int64) (path string, db *shm_master.DB) {
	t.Helper()
	dir := t.TempDir()
	path = filepath.Join(dir, "test.db")
	d, err := shm_master.Open(path, size)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return path, d
}

func testBasicCRUD(t *testing.T) {
	_, db := tempDB(t, 32<<20)
	defer db.Close()

	if err := db.Set("k1", []byte("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	v, ok, err := db.Get("k1")
	if err != nil || !ok || !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("Get: ok=%v err=%v want v1 got %q", ok, err, v)
	}
	if err := db.Del("k1"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	v, ok, _ = db.Get("k1")
	if ok {
		t.Fatalf("Get after Del: expected missing, got %q", v)
	}
}

func testEmptyKey(t *testing.T) {
	_, db := tempDB(t, 32<<20)
	defer db.Close()
	if err := db.Set("", []byte("x")); err != shm_master.ErrBadArgument {
		t.Fatalf("Set empty key: want ErrBadArgument got %v", err)
	}
	if err := db.Del(""); err != shm_master.ErrBadArgument {
		t.Fatalf("Del empty key: want ErrBadArgument got %v", err)
	}
}

func testEmptyValue(t *testing.T) {
	_, db := tempDB(t, 32<<20)
	defer db.Close()
	if err := db.Set("k", nil); err != shm_master.ErrBadArgument {
		t.Fatalf("Set nil value: want ErrBadArgument got %v", err)
	}
	if err := db.Set("k", []byte{}); err != shm_master.ErrBadArgument {
		t.Fatalf("Set empty value: want ErrBadArgument got %v", err)
	}
}

func testKeyTooLong(t *testing.T) {
	_, db := tempDB(t, 32<<20)
	defer db.Close()
	// 65536 超过 uint16
	key := string(make([]byte, 65536))
	if err := db.Set(key, []byte("v")); err != shm_master.ErrBadArgument {
		t.Fatalf("Set key len 65536: want ErrBadArgument got %v", err)
	}
}

func testValueMaxLen(t *testing.T) {
	// 不实际分配超大 value，只验证接口允许合理大 value
	_, db := tempDB(t, 512<<20)
	defer db.Close()
	large := make([]byte, 1<<20) // 1MB
	for i := range large {
		large[i] = byte(i)
	}
	if err := db.Set("big", large); err != nil {
		t.Fatalf("Set 1MB value: %v", err)
	}
	got, ok, err := db.Get("big")
	if err != nil || !ok || !bytes.Equal(got, large) {
		t.Fatalf("Get big: ok=%v err=%v len=%d", ok, err, len(got))
	}
}

func testSingleByteKeyValue(t *testing.T) {
	_, db := tempDB(t, 32<<20)
	defer db.Close()
	if err := db.Set("a", []byte("b")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	v, ok, _ := db.Get("a")
	if !ok || !bytes.Equal(v, []byte("b")) {
		t.Fatalf("Get: want b got %q", v)
	}
}

func testMaxKeyLen(t *testing.T) {
	_, db := tempDB(t, 32<<20)
	defer db.Close()
	key := string(make([]byte, 65535))
	if err := db.Set(key, []byte("v")); err != nil {
		t.Fatalf("Set max key len: %v", err)
	}
	v, ok, _ := db.Get(key)
	if !ok || !bytes.Equal(v, []byte("v")) {
		t.Fatalf("Get max key: want v got %q", v)
	}
}

func testLargeValue(t *testing.T) {
	_, db := tempDB(t, 64<<20)
	defer db.Close()
	val := bytes.Repeat([]byte("x"), 1024*1024) // 1MB
	if err := db.Set("k", val); err != nil {
		t.Fatalf("Set 1MB: %v", err)
	}
	got, ok, _ := db.Get("k")
	if !ok || !bytes.Equal(got, val) {
		t.Fatalf("Get: len want %d got %d", len(val), len(got))
	}
}

func testBinaryValue(t *testing.T) {
	_, db := tempDB(t, 32<<20)
	defer db.Close()
	// 含 \x00、0xff 等特殊字节
	val := []byte{0, 1, 0xff, 0xfe, 'a', 0, 'b'}
	if err := db.Set("bin", val); err != nil {
		t.Fatalf("Set binary: %v", err)
	}
	got, ok, _ := db.Get("bin")
	if !ok || !bytes.Equal(got, val) {
		t.Fatalf("Get binary: want %x got %x", val, got)
	}
}

func testGetNonExistent(t *testing.T) {
	_, db := tempDB(t, 32<<20)
	defer db.Close()
	v, ok, err := db.Get("never_exists")
	if err != nil {
		t.Fatalf("Get non-existent: err=%v (should be nil)", err)
	}
	if ok || v != nil {
		t.Fatalf("Get non-existent: want (nil, false) got (ok=%v, v=%v)", ok, v)
	}
}

func testOverwriteSameKey(t *testing.T) {
	_, db := tempDB(t, 32<<20)
	defer db.Close()
	db.Set("k", []byte("v1"))
	db.Set("k", []byte("v2"))
	v, ok, _ := db.Get("k")
	if !ok || !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("overwrite: want v2 got %q", v)
	}
}

func testDeleteNonExistent(t *testing.T) {
	_, db := tempDB(t, 32<<20)
	defer db.Close()
	if err := db.Del("nonexistent"); err != nil {
		t.Fatalf("Del non-existent: %v (should succeed)", err)
	}
}

func testDelThenGet(t *testing.T) {
	_, db := tempDB(t, 32<<20)
	defer db.Close()
	db.Set("k", []byte("v"))
	db.Del("k")
	v, ok, _ := db.Get("k")
	if ok {
		t.Fatalf("Get after Del: expected missing got %q", v)
	}
}

func testReopenAfterSet(t *testing.T) {
	path, db := tempDB(t, 32<<20)
	db.Set("rk", []byte("rv"))
	db.Close()

	db2, err := shm_master.Open(path, 32<<20)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()
	v, ok, _ := db2.Get("rk")
	if !ok || !bytes.Equal(v, []byte("rv")) {
		t.Fatalf("after reopen Get: want rv got %q", v)
	}
}

func testReopenAfterDel(t *testing.T) {
	path, db := tempDB(t, 32<<20)
	db.Set("rk", []byte("rv"))
	db.Del("rk")
	db.Close()

	db2, err := shm_master.Open(path, 32<<20)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()
	v, ok, _ := db2.Get("rk")
	if ok {
		t.Fatalf("after reopen Get deleted: expected missing got %q", v)
	}
}

func testRecoverFromEmpty(t *testing.T) {
	path, db := tempDB(t, 32<<20)
	db.Close()

	db2, err := shm_master.Open(path, 32<<20)
	if err != nil {
		t.Fatalf("Open empty: %v", err)
	}
	defer db2.Close()
	_, ok, _ := db2.Get("x")
	if ok {
		t.Fatalf("empty db Get: should be missing")
	}
}

// testReopenVerifyFreeReuseCloseVerify 变态流程：持久化->恢复校验->利用 free 区写入->Close->再次恢复校验
func testReopenVerifyFreeReuseCloseVerify(t *testing.T) {
	const dbSize = 4 << 20 // 4MB，预留足够 log 空间（Del/Set 都需写 log）
	const fillKeys = 4000  // 写满至接近饱和
	const valLen = 64
	path, db := tempDB(t, dbSize)

	// 1. 大量写入
	written := make(map[string][]byte)
	for i := 0; i < fillKeys; i++ {
		k := fmt.Sprintf("fill:%d", i)
		v := bytes.Repeat([]byte{byte(i & 0xff)}, valLen)
		if err := db.Set(k, v); err != nil {
			if err == shm_master.ErrNoSpace {
				break
			}
			t.Fatalf("Set: %v", err)
		}
		written[k] = v
	}
	if len(written) < 500 {
		t.Fatalf("expected to write at least 500 keys, wrote %d", len(written))
	}

	// 2. 删除约一半（按索引，保证确定性），产生 free 块
	delCount := len(written) / 2
	for j := 0; j < delCount; j++ {
		k := fmt.Sprintf("fill:%d", j)
		if _, ok := written[k]; !ok {
			continue
		}
		if err := db.Del(k); err != nil {
			t.Fatalf("Del %s: %v", k, err)
		}
		delete(written, k)
	}

	// 3. Close
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 4. Reopen，恢复
	db2, err := shm_master.Open(path, dbSize)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}

	// 5. 校验恢复后剩余 key 的正确性
	for k, want := range written {
		got, ok, err := db2.Get(k)
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		if !ok {
			t.Fatalf("key %s: expected present after recover, got missing", k)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("key %s: value mismatch after recover: got %x want %x", k, got, want)
		}
	}

	// 6. 利用 free 区写入新 key（复用 Del 释放的块）
	newKeys := make(map[string][]byte)
	for j := 0; j < delCount; j++ {
		k := fmt.Sprintf("reuse:%d", j)
		v := []byte(fmt.Sprintf("reused_val_%d", j))
		if err := db2.Set(k, v); err != nil {
			t.Fatalf("Set %s after recover (free reuse): %v", k, err)
		}
		newKeys[k] = v
	}

	// 7. 校验旧+新数据
	for k, want := range written {
		got, ok, _ := db2.Get(k)
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("key %s after free reuse: want %x got %x ok=%v", k, want, got, ok)
		}
	}
	for k, want := range newKeys {
		got, ok, _ := db2.Get(k)
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("new key %s: want %x got %x ok=%v", k, want, got, ok)
		}
	}

	// 8. 再次 Close
	if err := db2.Close(); err != nil {
		t.Fatalf("Close db2: %v", err)
	}

	// 9. 再次 Reopen
	db3, err := shm_master.Open(path, dbSize)
	if err != nil {
		t.Fatalf("Reopen again: %v", err)
	}
	defer db3.Close()

	// 10. 最终校验：所有 key（含 free 区复用的）都正确
	for k, want := range written {
		got, ok, _ := db3.Get(k)
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("final key %s: want %x got %x ok=%v", k, want, got, ok)
		}
	}
	for k, want := range newKeys {
		got, ok, _ := db3.Get(k)
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("final new key %s: want %x got %x ok=%v", k, want, got, ok)
		}
	}
}

// testMultiCycleReopenVerify 多轮：写 -> Close -> Reopen -> 校验 -> 再写 -> Close -> Reopen -> 校验（重复多轮）
func testMultiCycleReopenVerify(t *testing.T) {
	const dbSize = 8 << 20
	dir := t.TempDir()
	path := filepath.Join(dir, "multi.db")
	db, err := shm_master.Open(path, dbSize)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	expected := make(map[string][]byte)
	const rounds = 5
	const keysPerRound = 200

	for round := 0; round < rounds; round++ {
		// 本轮写入
		for i := 0; i < keysPerRound; i++ {
			k := fmt.Sprintf("r%d:k%d", round, i)
			v := []byte(fmt.Sprintf("round%d_val_%d", round, i))
			if err := db.Set(k, v); err != nil {
				_ = db.Close()
				t.Fatalf("round %d Set %s: %v", round, k, err)
			}
			expected[k] = v
		}
		// 每轮删除上一轮部分 key（制造 free 区）
		if round > 0 {
			for i := 0; i < keysPerRound/2; i++ {
				k := fmt.Sprintf("r%d:k%d", round-1, i)
				if err := db.Del(k); err != nil {
					_ = db.Close()
					t.Fatalf("round %d Del %s: %v", round, k, err)
				}
				delete(expected, k)
			}
		}

		if err := db.Close(); err != nil {
			t.Fatalf("round %d Close: %v", round, err)
		}

		db, err = shm_master.Open(path, dbSize)
		if err != nil {
			t.Fatalf("round %d Reopen: %v", round, err)
		}

		for k, want := range expected {
			got, ok, err := db.Get(k)
			if err != nil {
				_ = db.Close()
				t.Fatalf("round %d Get %s: %v", round, k, err)
			}
			if !ok {
				t.Fatalf("round %d key %s: expected present, got missing", round, k)
			}
			if !bytes.Equal(got, want) {
				_ = db.Close()
				t.Fatalf("round %d key %s: want %q got %q", round, k, want, got)
			}
		}
	}

	_ = db.Close()
}

func testErrNoSpace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tiny.db")
	// 极小文件：header(28)+key(1)=29 且 value 至少 16 字节，总需 45+，40 字节不足
	size := int64(40)
	d, err := shm_master.Open(path, size)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()
	err = d.Set("k", []byte("value"))
	if err != shm_master.ErrNoSpace {
		t.Fatalf("Set on tiny file: want ErrNoSpace got %v", err)
	}
}

func testDelFreesSpace(t *testing.T) {
	_, db := tempDB(t, 4<<20) // 4MB
	defer db.Close()

	// 写满到接近空间耗尽
	var i int
	for ; i < 10000; i++ {
		if err := db.Set(fmt.Sprintf("k:%d", i), []byte("abcd")); err != nil {
			break
		}
	}
	if i < 100 {
		t.Skip("file too small to fill")
	}
	lastErr := db.Set("overflow", []byte("x"))
	if lastErr != shm_master.ErrNoSpace {
		t.Logf("note: may not hit ErrNoSpace if space sufficient: %v", lastErr)
	}
	// 删除一半后应能再写
	for j := 0; j < i/2; j++ {
		db.Del(fmt.Sprintf("k:%d", j))
	}
	if err := db.Set("after_del", []byte("ok")); err != nil {
		t.Fatalf("Set after Del: %v (space should be freed)", err)
	}
}

func testPutDelReuse(t *testing.T) {
	_, db := tempDB(t, 8<<20)
	defer db.Close()

	// 反复 Put/Del 同一批 key，验证 value 区域重用
	for round := 0; round < 50; round++ {
		for i := 0; i < 100; i++ {
			k := fmt.Sprintf("frag:%d", i)
			v := []byte(fmt.Sprintf("val:%d:%d", i, round))
			if err := db.Set(k, v); err != nil {
				t.Fatalf("round %d Set %s: %v", round, k, err)
			}
		}
		for i := 0; i < 100; i++ {
			if err := db.Del(fmt.Sprintf("frag:%d", i)); err != nil {
				t.Fatalf("round %d Del: %v", round, err)
			}
		}
	}
	// 最后再写一批，验证能成功
	for i := 0; i < 100; i++ {
		if err := db.Set(fmt.Sprintf("final:%d", i), []byte("final")); err != nil {
			t.Fatalf("final Set: %v", err)
		}
	}
	for i := 0; i < 100; i++ {
		v, ok, _ := db.Get(fmt.Sprintf("final:%d", i))
		if !ok || !bytes.Equal(v, []byte("final")) {
			t.Fatalf("final Get: want final got %q", v)
		}
	}
}

func testParallelGetSet(t *testing.T) {
	_, db := tempDB(t, 64<<20)
	defer db.Close()

	// warmup
	for i := 0; i < 1000; i++ {
		db.Set(fmt.Sprintf("p:%d", i), []byte("v"))
	}

	var wg sync.WaitGroup
	for g := 0; g < runtime.NumCPU()*2; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < 500; i++ {
				k := fmt.Sprintf("p:%d", r.Intn(1000))
				if r.Intn(2) == 0 {
					_ = db.Set(k, []byte(fmt.Sprintf("v%d", id)))
				} else {
					_, _, _ = db.Get(k)
				}
			}
		}(g)
	}
	wg.Wait()
}

func testParallelMixOps(t *testing.T) {
	_, db := tempDB(t, 64<<20)
	defer db.Close()

	var wg sync.WaitGroup
	n := 500
	for g := 0; g < runtime.NumCPU()*2; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(id + 1000)))
			for i := 0; i < n; i++ {
				k := fmt.Sprintf("mix:%d:%d", id, r.Intn(100))
				switch r.Intn(3) {
				case 0:
					_ = db.Set(k, []byte("v"))
				case 1:
					_, _, _ = db.Get(k)
				case 2:
					_ = db.Del(k)
				}
			}
		}(g)
	}
	wg.Wait()
}

func testHighVolumeWrite(t *testing.T) {
	_, db := tempDB(t, 128<<20)
	defer db.Close()

	n := 50000
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("hv:%d", i)
		v := []byte(fmt.Sprintf("value_%d", i))
		if err := db.Set(k, v); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("hv:%d", i)
		v, ok, _ := db.Get(k)
		if !ok || !bytes.Equal(v, []byte(fmt.Sprintf("value_%d", i))) {
			t.Fatalf("Get %d: ok=%v got %q", i, ok, v)
		}
	}
}

func testSustainedMix(t *testing.T) {
	_, db := tempDB(t, 128<<20)
	defer db.Close()

	keys := 1000
	for i := 0; i < keys; i++ {
		db.Set(fmt.Sprintf("sm:%d", i), []byte("init"))
	}

	ops := 10000
	var done atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(id + 2000)))
			for j := 0; j < ops/8; j++ {
				k := fmt.Sprintf("sm:%d", r.Intn(keys))
				switch r.Intn(3) {
				case 0:
					_ = db.Set(k, []byte(fmt.Sprintf("v%d", j)))
				case 1:
					_, _, _ = db.Get(k)
				case 2:
					_ = db.Del(k)
				}
				done.Add(1)
			}
		}(g)
	}
	wg.Wait()
	if done.Load() < int64(ops/2) {
		t.Fatalf("sustained mix: completed %d ops", done.Load())
	}
}

// TestAcceptance 运行全部验收测试并输出报告
func TestAcceptance(t *testing.T) {
	report := &acceptanceReport{}
	runAcceptance(t, report)
	writeReport(report)
}

func writeReport(r *acceptanceReport) {
	// 文本报告
	if err := writeTextReport(r, "acceptance_report.txt"); err != nil {
		fmt.Printf("cannot write text report: %v\n", err)
	}
	// JSON 报告（便于 CI/脚本解析）
	if err := writeJSONReport(r, "acceptance_report.json"); err != nil {
		fmt.Printf("cannot write json report: %v\n", err)
	}
}

func writeTextReport(r *acceptanceReport, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "=== MD Master 验收测试报告 ===\n")
	fmt.Fprintf(f, "时间: %s\n", r.Timestamp.Format(time.RFC3339))
	fmt.Fprintf(f, "阶段: %s\n\n", r.Phase)

	byCat := make(map[string][]testResult)
	for _, tr := range r.Results {
		byCat[tr.Category] = append(byCat[tr.Category], tr)
	}

	for cat, list := range byCat {
		fmt.Fprintf(f, "--- %s ---\n", cat)
		for _, tr := range list {
			status := "PASS"
			if !tr.Passed {
				status = "FAIL"
			}
			fmt.Fprintf(f, "  [%s] %s (%dms)", status, tr.Name, tr.DurationMs)
			if tr.Error != "" {
				fmt.Fprintf(f, " %s", tr.Error)
			}
			fmt.Fprintln(f)
		}
		fmt.Fprintln(f)
	}

	fmt.Fprintf(f, "--- 汇总 ---\n")
	fmt.Fprintf(f, "  总计: %d  通过: %d  失败: %d  通过率: %.1f%%\n",
		r.Summary.Total, r.Summary.Passed, r.Summary.Failed,
		float64(r.Summary.Passed)/float64(max(1, r.Summary.Total))*100)
	fmt.Fprintf(f, "=== 报告结束 ===\n")
	fmt.Printf("验收报告已写入 %s\n", path)
	return nil
}

func writeJSONReport(r *acceptanceReport, path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}
