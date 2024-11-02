package wal

import (
	"github.com/stretchr/testify/assert"
	"github.com/tendermint/tendermint/memcask/data"
	"strconv"
	"testing"
)

func TestWAL(t *testing.T) {
	fp := "/tmp/wal.data"

	wal, err := OpenWAL(fp)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10_0000; i++ {
		s := []byte(strconv.Itoa(i))
		d := &data.Data{
			Key:      s,
			Value:    s,
			DataType: data.Normal,
		}
		err = wal.WriteData(d)
		assert.Nil(t, err)
		if err != nil {
			panic(err)
		}
	}
	wal.Close()

	wal, err = OpenWAL(fp)
	if err != nil {
		panic(err)
	}
	defer wal.Close()
	dataList, err := wal.Replay()
	if err != nil {
		panic(err)
	}
	assert.Equal(t, 10_0000, len(dataList))
	for i, d := range dataList {
		s := strconv.Itoa(i)
		assert.Equal(t, s, string(d.Key))
		assert.Equal(t, s, string(d.Value))
	}
}
