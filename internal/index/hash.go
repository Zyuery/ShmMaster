package index

import (
	"hash/fnv"
)

func Str2Int(s string, modNum uint32) int {
	h := fnv.New32a()            // 创建 FNV-1a 32位哈希器（也可使用64位：fnv.New64a()）
	_, err := h.Write([]byte(s)) // 将字符串写入哈希器
	if err != nil {
		return 0 // 实际场景中可根据需求处理错误，此处简化为返回0
	}
	return int(h.Sum32() % modNum) // 将哈希值转换为int（32位哈希值转int无溢出风险）
}
