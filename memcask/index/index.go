package index

import "github.com/tendermint/tendermint/memcask/position"

type MemoryIndex interface {
	Put(key []byte, pos *position.Position)
	Get(key []byte) *position.Position
	Delete(key []byte)
	Len() int
}
