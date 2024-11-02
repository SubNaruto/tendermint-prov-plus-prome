package memtable

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tendermint/tendermint/memcask/data"
	"sort"
	"strconv"
	"testing"
)

func TestBTreeMemTable(t *testing.T) {
	memtable := NewBTreeMemTable(32)

	var keys [][]byte

	for i := 0; i <= 1000; i++ {
		memtable.Put(&data.Data{
			Key:      []byte(strconv.Itoa(i)),
			Value:    []byte(strconv.Itoa(i)),
			DataType: data.Normal,
		})
		keys = append(keys, []byte(strconv.Itoa(i)))
	}
	assert.Equal(t, memtable.Len(), 1001)

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	it := memtable.GetIterator()
	for it.Valid() {
		d := it.Value()
		assert.NotNil(t, d)
		fmt.Println(string(d.Key), string(d.Value), d.DataType)
		it.Next()
	}

	fmt.Println(memtable.Size())
}
