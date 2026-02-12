package _interface

type Allocator interface {
	Alloc(n uint32, logNeed uint64) (off uint64, ok bool)
	FreeBlock(off uint64, n uint32)
}
