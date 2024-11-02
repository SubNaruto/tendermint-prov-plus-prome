package memtable

import (
	"bytes"
	"github.com/google/btree"
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/iterator"
	"sync"
)

// BTreeMemTable 用于在内存暂存写入的数据
type BTreeMemTable struct {
	btree *btree.BTree
	mu    sync.RWMutex
	size  int
}

// BTreeMemTable实现了MemTable接口
var _ MemTable = (*BTreeMemTable)(nil)

// Item BTreeMemTable存储的元素,包含一个kv数据
type Item struct {
	data *data.Data
}

func (i *Item) Less(other btree.Item) bool {
	if bytes.Compare(i.data.Key, other.(*Item).data.Key) == -1 {
		return true
	}
	return false
}

var _ btree.Item = (*Item)(nil)

/*	NewBTreeMemTable
 *  @Description: 创建一个BTreeMemTable实例
 *  @param m: BTree的阶数
 *  @return MemTable: MemTable实例
 */
func NewBTreeMemTable(m int) MemTable {
	return &BTreeMemTable{
		btree: btree.New(m),
	}
}

/*	Put
 *  @Description: 向MemTable放入一个数据
 *  @param data: 放入的数据
 */
func (btm *BTreeMemTable) Put(data *data.Data) {
	btm.mu.Lock()
	defer btm.mu.Unlock()
	btm.btree.ReplaceOrInsert(&Item{
		data: data,
	})
	btm.size += len(data.Key) + len(data.Value) + 1
}

/*	Get
 *  @Description: 根据key从MemTable中读取一个数据
 *  @param key: 数据的key
 *  @return []byte: 数据的value
 *  @return bool: 数据是否查找到
 */
func (btm *BTreeMemTable) Get(key []byte) ([]byte, bool) {
	btm.mu.RLock()
	defer btm.mu.RUnlock()
	i := btm.btree.Get(&Item{
		data: &data.Data{
			Key: key,
		},
	})
	if i == nil {
		return nil, false
	}
	d := i.(*Item).data
	return d.Value, true
}

func (btm *BTreeMemTable) GetRange(minKey, maxKey []byte) ([]*data.Data, bool) {
	btm.mu.RLock()
	defer btm.mu.RUnlock()

	values := make([]*data.Data, 0, btm.btree.Len())
	saveValue := func(i btree.Item) bool {
		values = append(values, i.(*Item).data)
		return true
	}
	btm.btree.AscendRange(
		&Item{
			data: &data.Data{Key: minKey},
		}, &Item{
			data: &data.Data{Key: maxKey},
		}, saveValue)

	return values, len(values) != 0
}

/*	Len
 *  @Description: 获取MemTable中数据的个数
 *  @return int: 数据的个数
 */
func (btm *BTreeMemTable) Len() int {
	btm.mu.RLock()
	defer btm.mu.RUnlock()
	return btm.btree.Len()
}

/*	Size
 *  @Description: 获取MemTable中数据的总大小(B)
 *  @return int: 数据的总大小(B)
 */
func (btm *BTreeMemTable) Size() int {
	btm.mu.RLock()
	defer btm.mu.RUnlock()
	return btm.size
}

/*	GetIterator
 *  @Description: 获取MemTable的迭代器
 *  @return iterator.Iterator: MemTable的迭代器实例
 */
func (btm *BTreeMemTable) GetIterator() iterator.Iterator {
	btm.mu.RLock()
	defer btm.mu.RUnlock()
	return NewBTreeIterator(btm.btree)
}
