package msg

// Header Const
const (
	Magic      = uint32(0x4B564C47) // 'KVLG' 随便选
	Version    = uint16(1)
	FlagPut    = uint16(1)
	FlagDel    = uint16(2)
	HeaderSize = 4 + 2 + 2 + 2 + 2 + 4 + 4 // 20 bytes（含 reserved）
)

const (
	ShardSize = 32
)
