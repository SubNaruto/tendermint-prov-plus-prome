package iterator

import (
	"container/list"
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/iterator"
)

type Iterator struct {
	items  []*data.Data
	cursor int
}

var _ iterator.Iterator = (*Iterator)(nil)

func NewIterator(l *list.List) iterator.Iterator {
	items := make([]*data.Data, 0, l.Len())
	for e := l.Front(); e != nil; e = e.Next() {
		i := e.Value.(*data.Data)
		items = append(items, i)
	}
	return &Iterator{
		items:  items,
		cursor: 0,
	}
}

func (i *Iterator) Valid() bool {
	return i.cursor < len(i.items)
}

func (i *Iterator) Value() *data.Data {
	if !i.Valid() {
		return nil
	}
	return i.items[i.cursor]
}

func (i *Iterator) Next() {
	i.cursor++
}

func (i *Iterator) Rollback() {
	i.cursor = 0
}
