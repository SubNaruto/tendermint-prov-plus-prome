package util

import "github.com/tendermint/tendermint/memcask/option"

func CalLevelSize(level int) int {
	var size int
	for i := 0; i <= level; i++ {
		if size == 0 {
			size = option.Level0Size
		} else {
			size *= 10
		}
	}
	return size
}
