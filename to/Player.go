package to

type Player struct {
	ID   uint64
	HP   uint32
	MP   uint32
	Name [32]byte
}

func NewPlayer(id uint64, hp, mp uint32, name string) *Player {
	p := Player{ID: id, HP: hp, MP: mp}
	copy(p.Name[:], []byte(name))
	return &p
}
