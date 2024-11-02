package diskFile

import (
	"github.com/stretchr/testify/assert"
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/option"
	"strconv"
	"testing"
)

func TestDataFileList(t *testing.T) {
	dataFileList := NewDataFileList("/tmp/data", ".data", option.MB, false, 0, 0)
	for i := 0; i < 1000; i++ {
		str := strconv.Itoa(i)
		_, err := dataFileList.WriteData(&data.Data{
			Key:      []byte(str),
			Value:    []byte(str),
			DataType: data.Normal,
		})
		assert.Nil(t, err)
		if err != nil {
			panic(err)
		}
	}
}
