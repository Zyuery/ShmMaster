.PHONY: test benchmark bench test-all

# 仅执行 test 包下所有普通测试（不跑 benchmark）
test:
	go test -v -run '^Test|^Fuzz' ./test/...

# 仅执行 test 包下所有 benchmark
benchmark:
	go test -bench=. -benchmem ./test/...

# 简写
bench: benchmark

# 普通测试 + benchmark 都跑（先 test 再 benchmark）
test-all: test benchmark
