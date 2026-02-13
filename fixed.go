package shm_master

import "md_master/internal/fixed"

// SetFixed 将无指针类型 T 的实例序列化写入 db。
func SetFixed[T any](db *DB, key string, v *T) error {
	if db == nil || db.e == nil {
		return nil
	}
	return fixed.SetFixed(db, key, v)
}

// GetFixed 从 db 读出并反序列化为 *T。
func GetFixed[T any](db *DB, key string) (*T, bool, error) {
	if db == nil || db.e == nil {
		return nil, false, nil
	}
	return fixed.GetFixed[T](db, key)
}
