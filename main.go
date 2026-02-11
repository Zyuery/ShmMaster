package main

import (
	"fmt"
	"md_master/core"
	"md_master/to"
	"sync"
)

func main() {
	db, _ := core.Open("./struct.data", 1<<20)
	defer db.Close()

	var wg sync.WaitGroup
	addPlayer := func() {
		for i := 0; i < 1000; i++ {
			_ = core.SetFixed(db, fmt.Sprintf("player:%d", i), to.NewPlayer(uint64(i), uint32(i), uint32(i), fmt.Sprintf("player%d", i)))

		}
		wg.Done()
	}
	addMaster := func() {
		for i := 0; i < 1000; i++ {
			_ = core.SetFixed(db, fmt.Sprintf("master:%d", i), to.NewPlayer(uint64(i), uint32(i), uint32(i), fmt.Sprintf("master%d", i)))

		}
		wg.Done()
	}
	wg.Add(2)
	go addPlayer()
	go addMaster()
	wg.Wait()

	for i := 0; i < 1000; i++ {
		got, ok, _ := core.GetFixed[to.Player](db, fmt.Sprintf("player:%d", i))
		if !ok {
			break
		}
		fmt.Println(ok, got.ID, got.HP, string(got.Name[:]))
	}

	for i := 0; i < 1000; i++ {
		got, ok, _ := core.GetFixed[to.Player](db, fmt.Sprintf("master:%d", i))
		if !ok {
			break
		}
		fmt.Println(ok, got.ID, got.HP, string(got.Name[:]))
	}
}
