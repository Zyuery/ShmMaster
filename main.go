package main

import (
	"fmt"
	"md_master/client"
	"md_master/core"
)

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

func main() {
	db, _ := core.Open("./kv.data", 1<<10)
	defer db.Close()
	//
	//var wg sync.WaitGroup
	//addPlayer := func() {
	//	for i := 0; i < 100; i++ {
	//		_ = client.SetFixed(db, fmt.Sprintf("player:%d", i), NewPlayer(uint64(i), uint32(i), uint32(i), fmt.Sprintf("player%d", i)))
	//
	//	}
	//	wg.Done()
	//}
	//addMaster := func() {
	//	for i := 0; i < 100; i++ {
	//		_ = client.SetFixed(db, fmt.Sprintf("master:%d", i), NewPlayer(uint64(i), uint32(i), uint32(i), fmt.Sprintf("master%d", i)))
	//
	//	}
	//	wg.Done()
	//}
	//wg.Add(2)
	//go addPlayer()
	//go addMaster()
	//wg.Wait()

	for i := 0; i < 100; i++ {
		got, ok, _ := client.GetFixed[Player](db, fmt.Sprintf("player:%d", i))
		if !ok {
			break
		}
		fmt.Println(ok, got.ID, got.HP, string(got.Name[:]))
	}

	for i := 0; i < 100; i++ {
		got, ok, _ := client.GetFixed[Player](db, fmt.Sprintf("master:%d", i))
		if !ok {
			break
		}
		fmt.Println(ok, got.ID, got.HP, string(got.Name[:]))
	}
}
