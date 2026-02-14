package segment

import "shm_master/consts"

// SizeClass 把 n 向上取整到某个档位，比如 16/32/64/.../64K。
func SizeClass(n uint32) uint32 {
	if n == 0 {
		return 0
	}
	return (n + consts.Align - 1) / consts.Align * consts.Align
}
