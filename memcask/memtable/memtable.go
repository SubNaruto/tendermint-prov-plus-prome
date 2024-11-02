package memtable

import (
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/iterator"
)

type MemTable interface {
	Put(data *data.Data)
	Get(key []byte) ([]byte, bool)
	GetRange(minKey, maxKey []byte) ([]*data.Data, bool)
	Len() int
	Size() int
	GetIterator() iterator.Iterator
}
