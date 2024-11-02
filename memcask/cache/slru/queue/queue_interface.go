package queue

import (
	"container/list"
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/iterator"
)

type QueueInterface interface {
	Add(key, value []byte)
	Pop() *data.Data
	Exceed() bool
	RemoveElem(elem *list.Element)
	GetElemFromMap(key string) (*list.Element, bool)
	MoveElemToFront(e *list.Element)
	Iterator() iterator.Iterator
}
