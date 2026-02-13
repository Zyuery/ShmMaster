package fs

import "fmt"

// SegPath 返回 base 对应 id 的 segment 文件路径。
func SegPath(base string, id uint32) string {
	return fmt.Sprintf("%s.%03d", base, id)
}
