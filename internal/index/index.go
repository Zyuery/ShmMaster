package index

// Entry 索引项：key 对应 value 所在 segment 与偏移。
type Entry struct {
	SegID  uint32
	ValOff uint64
	ValLen uint32
}

// Index 键值索引接口：Get/Set/Del。
type Index interface {
	Get(key string) (Entry, bool)
	Set(key string, e Entry)
	Del(key string)
	Clear()
}
