package memtable

import (
	"github.com/google/btree"
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/iterator"
)

// BTreeIterator BTreeIndex的迭代器，用于遍历索引中的所有数据
type BTreeIterator struct {
	Cursor int     // 读指针
	Values []*Item // 数据数组
}

// BTreeIterator实现了Iterator接口
var _ iterator.Iterator = (*BTreeIterator)(nil)

/*	NewBTreeIterator
 *  @Description: 创建一个BTree迭代器
 *  @param t: BTree实例
 *  @return iterator.Iterator: BTree迭代器实例
 */
func NewBTreeIterator(t *btree.BTree) iterator.Iterator {
	var idx int
	values := make([]*Item, t.Len())
	saveValue := func(i btree.Item) bool {
		values[idx] = i.(*Item)
		idx++
		return true
	}
	t.Ascend(saveValue)
	return &BTreeIterator{
		Cursor: 0,
		Values: values,
	}
}

/*	Value
 *  @Description: 获取迭代器当前指针指向的数据
 *  @return *data.Data: 指向的数据
 */
func (it *BTreeIterator) Value() *data.Data {
	if it.Valid() {
		return it.Values[it.Cursor].data
	}
	return nil
}

/*	Next
 *  @Description: 迭代器指针后移
 */
func (it *BTreeIterator) Next() {
	it.Cursor++
}

/*	Rollback
 *  @Description: 迭代器指针回到起始位置
 */
func (it *BTreeIterator) Rollback() {
	it.Cursor = 0
}

/*	Valid
 *  @Description: 迭代器指针是否有效
 *  @return bool: 如果有效返回true
 */
func (it *BTreeIterator) Valid() bool {
	return it.Cursor < len(it.Values)
}
