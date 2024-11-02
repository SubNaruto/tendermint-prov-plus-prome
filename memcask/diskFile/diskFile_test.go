package diskFile

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/option"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDiskFile(t *testing.T) {
	file, err := NewDiskFile("/tmp", "test.data")
	assert.Nil(t, err)
	defer func() {
		_ = file.Close()
	}()
	for i := 0; i < 100; i++ {
		str := strconv.Itoa(i)
		_, err := file.WriteData(&data.Data{
			Key:      []byte(str),
			Value:    []byte(str),
			DataType: data.Normal,
		})
		assert.Nil(t, err)
	}
	dataList, err := file.ReadAll()
	assert.Nil(t, err)
	if err != nil {
		fmt.Println(err.Error())
	}
	for i := 0; i < len(dataList); i++ {
		fmt.Println(string(dataList[i].Key), string(dataList[i].Value))
	}
}

func TestDiskFileRead0(t *testing.T) {
	file, err := NewDiskFile("/tmp", "test.data")
	assert.Nil(t, err)
	defer func() {
		_ = file.Close()
	}()
	for i := 0; i < 1024*4; i++ {
		var s strings.Builder
		for s.Len() < 64*option.KB {
			s.WriteString(time.Now().String())
		}
		_, err = file.WriteData(&data.Data{
			Key:      []byte("hello:world:520"),
			Value:    []byte(s.String()),
			DataType: data.Normal,
		})
		assert.Nil(t, err)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("ReadAll:")
	for i := 0; i < 10; i++ {
		_, err = file.ReadAll()
		assert.Nil(t, err)
		if err != nil {
			panic(err)
		}
		_, err = file.readAllTempTest()
		assert.Nil(t, err)
		if err != nil {
			panic(err)
		}
		fmt.Println()
	}

	fmt.Println("ReadAllPosition:")
	for i := 0; i < 10; i++ {
		_, _, err = file.ReadAllPosition()
		assert.Nil(t, err)
		if err != nil {
			panic(err)
		}
		_, _, err = file.readAllPositionTempTest()
		assert.Nil(t, err)
		if err != nil {
			panic(err)
		}
		fmt.Println()
	}
}
