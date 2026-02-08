package core

import (
	"encoding/binary"
	"hash/crc32"
)

type header struct {
	Magic  uint32
	Ver    uint16
	Flags  uint16
	KeyLen uint16
	_      uint16 // reserved/pad
	ValLen uint32
	CRC32  uint32
}

func decodeHeader(data []byte) header {
	return header{
		Magic:  binary.LittleEndian.Uint32(data[0:4]),
		Ver:    binary.LittleEndian.Uint16(data[4:6]),
		Flags:  binary.LittleEndian.Uint16(data[6:8]),
		KeyLen: binary.LittleEndian.Uint16(data[8:10]),
		ValLen: binary.LittleEndian.Uint32(data[12:16]),
		CRC32:  binary.LittleEndian.Uint32(data[16:20]),
	}
}

func encodeHeader(b []byte, h header) {
	binary.LittleEndian.PutUint32(b[0:4], h.Magic)
	binary.LittleEndian.PutUint16(b[4:6], h.Ver)
	binary.LittleEndian.PutUint16(b[6:8], h.Flags)
	binary.LittleEndian.PutUint16(b[8:10], h.KeyLen)
	binary.LittleEndian.PutUint16(b[10:12], 0)
	binary.LittleEndian.PutUint32(b[12:16], h.ValLen)
	binary.LittleEndian.PutUint32(b[16:20], h.CRC32)
}

func calcCRC(flags uint16, keyLen uint16, valLen uint32, key, val []byte) uint32 {
	// 把一部分头字段 + key + val 做校验 ；注意：不要把 Magic/Ver/CRC 自己算进去也行，但必须读写一致
	var tmp [2 + 2 + 4]byte
	binary.LittleEndian.PutUint16(tmp[0:2], flags)
	binary.LittleEndian.PutUint16(tmp[2:4], keyLen)
	binary.LittleEndian.PutUint32(tmp[4:8], valLen)

	c := crc32.NewIEEE()
	_, _ = c.Write(tmp[:])
	_, _ = c.Write(key)
	_, _ = c.Write(val)
	return c.Sum32()
}
