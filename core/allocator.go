package core

type Allocator interface {
	Alloc(n uint32) (off uint64, ok bool)
	FreeBlock(off uint64, n uint64)
}
