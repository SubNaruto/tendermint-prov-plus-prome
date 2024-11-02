package data

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestData(t *testing.T) {
	d := &Data{
		Key:      []byte("name"),
		Value:    []byte("xiaoming"),
		DataType: Normal,
	}

	enc := d.Encode()

	fmt.Println(len(enc))

	dec, _ := DecodeHeader(enc[:DataHeaderSize])

	fmt.Printf("%+v", dec)
}

func TestDecodeBufToDataList(t *testing.T) {
	var dataList []*Data
	var buf []byte
	for i := 0; i < 100; i++ {
		d := &Data{
			Key:      []byte(strconv.Itoa(i)),
			Value:    []byte(strconv.Itoa(i)),
			DataType: Normal,
		}
		dataList = append(dataList, d)
		buf = append(buf, d.Encode()...)
	}
	d, err := DecodeBufToDataList(buf)
	assert.Nil(t, err)
	for i := 0; i < 100; i++ {
		assert.Equal(t, strconv.Itoa(i), string(d[i].Key))
		assert.Equal(t, strconv.Itoa(i), string(d[i].Value))
		assert.Equal(t, Normal, d[i].DataType)
		fmt.Printf("%+v\n", d[i])
	}
}
