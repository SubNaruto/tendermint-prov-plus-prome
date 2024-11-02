package slru

import (
	"github.com/tendermint/tendermint/memcask/cache"
	"github.com/tendermint/tendermint/memcask/cache/slru/queue"
	"github.com/tendermint/tendermint/memcask/data"
	"sync"
)

type S3Cache struct {
	mu              sync.Mutex
	probationQueue  *queue.Queue
	protectionQueue *queue.Queue
}

var _ cache.CacheInterface = (*S3Cache)(nil)

func NewS3Cache(probationQueueLimit, protectionQueueLimit int) *S3Cache {
	return &S3Cache{
		probationQueue:  queue.NewQueue(probationQueueLimit),
		protectionQueue: queue.NewQueue(protectionQueueLimit),
	}
}

func (sc *S3Cache) Put(key, value []byte) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	e0, ok := sc.probationQueue.GetElemFromMap(string(key))
	if ok { // 如果已经存在于probation，则将其移动到protection头部

		sc.probationQueue.RemoveElem(e0) // 删除probation中的elem

		sc.protectionQueue.Add(key, value) // 将数据加入到protection的头部

		for sc.protectionQueue.Exceed() { // 如果protection超限，将其末尾数据淘汰到probation的头部
			e1 := sc.protectionQueue.Pop()
			sc.probationQueue.Add(e1.Key, e1.Value)
		}
		for sc.probationQueue.Exceed() { // 如果probation超限，将其末尾数据淘汰
			sc.probationQueue.Pop()
		}
		return
	}

	e1, ok := sc.protectionQueue.GetElemFromMap(string(key))
	if ok { // 已经存在于protection，移动到头部
		e1.Value = &data.Data{
			Key:   key,
			Value: value, // 新的value
		}
		sc.protectionQueue.MoveElemToFront(e1)
		return
	}

	// 都不存在，直接加入到probationQueue的头部
	sc.probationQueue.Add(key, value)
	for sc.probationQueue.Exceed() {
		sc.probationQueue.Pop()
	}
}

func (sc *S3Cache) Get(key []byte) ([]byte, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	e0, ok := sc.probationQueue.GetElemFromMap(string(key))
	if ok { // 如果已经存在于probation，则将其移动到protection头部
		sc.probationQueue.RemoveElem(e0) // 删除probation中的elem

		i := e0.Value.(*data.Data)

		sc.protectionQueue.Add(key, i.Value)

		for sc.protectionQueue.Exceed() { // 如果protection超限，将其末尾数据淘汰到probation的头部
			e1 := sc.protectionQueue.Pop()
			sc.probationQueue.Add(e1.Key, e1.Value)
		}
		for sc.probationQueue.Exceed() { // 如果probation超限，将其末尾数据淘汰
			sc.probationQueue.Pop()
		}
		return i.Value, true
	}

	e1, ok := sc.protectionQueue.GetElemFromMap(string(key))
	if ok { // 已经存在于protection，只需要移动到头部即可
		sc.protectionQueue.MoveElemToFront(e1)
		return e1.Value.(*data.Data).Value, true
	}

	// 都不存在，返回空值即可
	return nil, false
}
