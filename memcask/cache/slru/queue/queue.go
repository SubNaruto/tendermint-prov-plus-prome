package queue

import (
	"container/list"
	"github.com/tendermint/tendermint/memcask/cache/slru/iterator"
	"github.com/tendermint/tendermint/memcask/data"
	iterator2 "github.com/tendermint/tendermint/memcask/iterator"
)

type Queue struct {
	queue   *list.List
	elemMap map[string]*list.Element
	size    int
	limit   int
}

var _ QueueInterface = (*Queue)(nil)

func NewQueue(limit int) *Queue {
	return &Queue{
		queue:   list.New(),
		elemMap: make(map[string]*list.Element),
		limit:   limit,
	}
}

func (q *Queue) Add(key, value []byte) {
	e := &data.Data{
		Key:   key,
		Value: value,
	}
	k := string(key)
	elem, ok := q.elemMap[k]
	if ok { // 已经存在
		elem.Value = value
		q.queue.MoveToFront(elem)
	} else { // 不存在
		q.elemMap[k] = q.queue.PushFront(e)
		q.size += len(key) + len(value) + 1
	}
}

func (q *Queue) Pop() *data.Data {
	if q.queue.Len() > 0 {
		i := q.queue.Remove(q.queue.Back()).(*data.Data)
		q.size -= len(i.Key) + len(i.Value) + 1
		delete(q.elemMap, string(i.Key))
		return i
	}
	return nil
}

func (q *Queue) RemoveElem(elem *list.Element) {
	q.queue.Remove(elem)
	i := elem.Value.(*data.Data)
	q.size -= len(i.Key) + len(i.Value) + 1
	delete(q.elemMap, string(i.Key))
}

func (q *Queue) Exceed() bool {
	return q.size > q.limit
}

func (q *Queue) GetElemFromMap(key string) (*list.Element, bool) {
	e, ok := q.elemMap[key]
	return e, ok
}

func (q *Queue) MoveElemToFront(e *list.Element) {
	q.queue.MoveToFront(e)
}

func (q *Queue) Iterator() iterator2.Iterator {
	return iterator.NewIterator(q.queue)
}
