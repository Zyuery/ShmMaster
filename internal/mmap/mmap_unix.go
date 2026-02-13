//go:build unix

package mmap

import (
	"golang.org/x/sys/unix"
)

// Map 将文件 fd 的 [0, size) 映射为可读写共享内存。
func Map(fd uintptr, size int) ([]byte, error) {
	return unix.Mmap(int(fd), 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
}

// Sync 将映射区刷回磁盘。
func Sync(data []byte) error {
	return unix.Msync(data, unix.MS_SYNC)
}

// Unmap 解除映射。
func Unmap(data []byte) error {
	return unix.Munmap(data)
}
