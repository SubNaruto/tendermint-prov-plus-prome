package index

import (
	"bytes"
	"github.com/google/btree"
	"github.com/tendermint/tendermint/memcask/position"
	"sync"
)

// BTreeIndex B树内存索引
type BTreeIndex struct {
	btree *btree.BTree
	mu    sync.RWMutex
}

// BTreeIndex实现了MemoryIndex接口
var _ MemoryIndex = (*BTreeIndex)(nil)

// Item BTreeIndex存储的元素,包含数据的key以及数据的位置信息
type Item struct {
	key      []byte
	position *position.Position
}

// Less 定义了Item在BTreeIndex的排序方式，这里是按照key的字典序从小到大
func (item *Item) Less(other btree.Item) bool {
	if bytes.Compare(item.key, other.(*Item).key) == -1 {
		return true
	}
	return false
}

var _ btree.Item = (*Item)(nil)

/*	NewBTreeIndex
 *  @Description: 创建一个BTreeIndex实例
 *  @param m: BTree的阶数
 *  @return MemoryIndex: BTreeIndex实例
 */
func NewBTreeIndex(m int) MemoryIndex {
	return &BTreeIndex{
		btree: btree.New(m),
	}
}

/*	Put
 *  @Description: 将key和pos写入内存索引
 *  @param key: 数据的key
 *  @param dataPos: 数据的位置索引信息
 */
func (bti *BTreeIndex) Put(key []byte, dataPos *position.Position) {
	bti.mu.Lock()
	defer bti.mu.Unlock()
	item := &Item{
		key:      key,
		position: dataPos,
	}
	bti.btree.ReplaceOrInsert(item)
}

/*	Get
 *  @Description: 根据key获取数据的位置索引信息
 *  @param key: 数据的key
 *  @return *position.Position: 数据的位置索引信息
 */
func (bti *BTreeIndex) Get(key []byte) *position.Position {
	bti.mu.RLock()
	defer bti.mu.RUnlock()
	item := &Item{
		key: key,
	}
	i := bti.btree.Get(item)
	if i == nil {
		return nil
	}
	return i.(*Item).position
}

/*	Delete
 *  @Description: 根据key删除对应的位置索引
 *  @param key: 数据的key
 */
func (bti *BTreeIndex) Delete(key []byte) {
	bti.mu.Lock()
	defer bti.mu.Unlock()
	item := &Item{
		key: key,
	}
	bti.btree.Delete(item)
}

/*	Len
 *  @Description: 获取内存索引的元素个数
 *  @return int: 数据个数
 */
func (bti *BTreeIndex) Len() int {
	return bti.btree.Len()
}
