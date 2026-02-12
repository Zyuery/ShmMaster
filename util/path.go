package util

import "fmt"

func SegPath(base string, id uint32) string {
	return fmt.Sprintf("%s.%03d", base, id)
}
