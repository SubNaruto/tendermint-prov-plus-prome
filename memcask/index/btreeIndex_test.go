package index

import (
	"github.com/stretchr/testify/assert"
	"github.com/tendermint/tendermint/memcask/position"
	"strconv"
	"testing"
)

func TestBTreeIndex(t *testing.T) {
	bti := NewBTreeIndex(32)

	for i := 0; i < 100; i++ {
		bti.Put([]byte(strconv.Itoa(i)), &position.Position{
			Filename: strconv.Itoa(i),
			Offset:   i,
		})
	}
	assert.Equal(t, bti.Len(), 100)

	for i := 0; i < 100; i++ {
		pos := bti.Get([]byte(strconv.Itoa(i)))
		assert.NotNil(t, pos)
		assert.Equal(t, pos.Offset, i)
	}

	for i := 0; i < 100; i++ {
		bti.Delete([]byte(strconv.Itoa(i)))
	}
	assert.Equal(t, bti.Len(), 0)
}
