package record

import (
	"encoding/binary"
	"hash/crc32"
)

// Header 记录头（magic/version/type/len/crc）
type Header struct {
	Magic  uint32
	Ver    uint16
	Flags  uint16
	KeyLen uint16
	_      uint16
	ValLen uint32
	ValOff uint64
	CRC32  uint32
}

// DecodeHeader 从 data 解码一条记录头。
func DecodeHeader(data []byte) Header {
	return Header{
		Magic:  binary.LittleEndian.Uint32(data[0:4]),
		Ver:    binary.LittleEndian.Uint16(data[4:6]),
		Flags:  binary.LittleEndian.Uint16(data[6:8]),
		KeyLen: binary.LittleEndian.Uint16(data[8:10]),
		ValLen: binary.LittleEndian.Uint32(data[12:16]),
		ValOff: binary.LittleEndian.Uint64(data[16:24]),
		CRC32:  binary.LittleEndian.Uint32(data[24:28]),
	}
}

// EncodeHeader 将 h 编码到 b（至少 HeaderSize 字节）。
func EncodeHeader(b []byte, h Header) {
	binary.LittleEndian.PutUint32(b[0:4], h.Magic)
	binary.LittleEndian.PutUint16(b[4:6], h.Ver)
	binary.LittleEndian.PutUint16(b[6:8], h.Flags)
	binary.LittleEndian.PutUint16(b[8:10], h.KeyLen)
	binary.LittleEndian.PutUint16(b[10:12], 0)
	binary.LittleEndian.PutUint32(b[12:16], h.ValLen)
	binary.LittleEndian.PutUint64(b[16:24], h.ValOff)
	binary.LittleEndian.PutUint32(b[24:28], h.CRC32)
}

// CalcCRC 计算记录 CRC（与 DecodeHeader 约定一致）。
func CalcCRC(flags uint16, keyLen uint16, valLen uint32, valOff uint64, key []byte) uint32 {
	var tmp [2 + 2 + 4 + 8]byte
	binary.LittleEndian.PutUint16(tmp[0:2], flags)
	binary.LittleEndian.PutUint16(tmp[2:4], keyLen)
	binary.LittleEndian.PutUint32(tmp[4:8], valLen)
	binary.LittleEndian.PutUint64(tmp[8:16], valOff)
	c := crc32.NewIEEE()
	_, _ = c.Write(tmp[:])
	_, _ = c.Write(key)
	return c.Sum32()
}
