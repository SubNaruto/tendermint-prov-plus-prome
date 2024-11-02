package data

import (
	"encoding/binary"
	"github.com/tendermint/tendermint/memcask/errors"
	"hash/crc32"
)

type DataType byte

// 数据类型
const (
	Normal    DataType = iota // 非区块数据
	Delete                    // 删除数据
	BlockPart                 // 区块分片数据
	BlockData                 // 区块数据
	ProvData                  // 溯源数据

	DataHeaderSize = 5 + 5 + 5 + 1 // Data编码后的头部长度
)

type Data struct {
	Key      []byte   // 数据的key
	Value    []byte   // 数据的value
	DataType DataType // 数据的类型
}

type DataHeader struct {
	Crc      uint32   // 校验值
	KeySize  int      // key的长度
	ValSize  int      // value的长度
	DataType DataType // 数据类型
}

/*
crc keySize valueSize type
 5     5		5      1
*/

/*	Encode
 *  @Description: 将data进行编码
 *  @return []byte: 返回编码后的字符串
 */
func (data *Data) Encode() []byte {
	keySize := len(data.Key)
	valueSize := len(data.Value)

	offset := 0
	headerWithoutCrc := make([]byte, DataHeaderSize)

	n := binary.PutVarint(headerWithoutCrc[offset:], int64(keySize))
	offset += n

	n = binary.PutVarint(headerWithoutCrc[offset:], int64(valueSize))
	offset += n

	headerWithoutCrc[offset] = byte(data.DataType)
	offset += 1

	buf := make([]byte, 5, 5+offset+len(data.Key)+len(data.Value))
	buf = append(buf, headerWithoutCrc[:offset]...)
	buf = append(buf, data.Key...)
	buf = append(buf, data.Value...)

	crc := crc32.ChecksumIEEE(buf[5:])
	binary.LittleEndian.PutUint32(buf[:5], crc)

	return buf
}

/*	DecodeHeader
 *  @Description: 将编码字符串解码为DataHeader
 *  @param buf: 编码后的dataHeader头部
 *  @return *DataHeader: 解码得到的*DataHeader实例
 *  @return int: 编码字符串实际有效的长度
 */
func DecodeHeader(buf []byte) (*DataHeader, int) {
	offset := 5
	crc := binary.LittleEndian.Uint32(buf[:offset])

	keySize, n := binary.Varint(buf[offset:])
	offset += n

	valueSize, n := binary.Varint(buf[offset:])
	offset += n

	dataType := buf[offset]
	offset++

	return &DataHeader{
		Crc:      crc,
		KeySize:  int(keySize),
		ValSize:  int(valueSize),
		DataType: DataType(dataType),
	}, offset
}

/*	DecodeBufToDataList
 *  @Description: 将编码字符串解码为一系列Data
 *  @param buf: 若干Data编码组成的字符串
 *  @return []*Data: 解码得到的一系列Data实例
 *  @return error: 错误信息
 */
func DecodeBufToDataList(buf []byte) ([]*Data, error) {
	var offset int
	var dataList []*Data
	for offset < len(buf) {
		idx := offset
		dataHeaderFrom := offset

		crc1 := binary.LittleEndian.Uint32(buf[idx : idx+5])
		idx += 5

		keySize, n := binary.Varint(buf[idx:])
		idx += n

		valueSize, n := binary.Varint(buf[idx:])
		idx += n

		t := buf[idx]
		idx++

		// headerWithoutCrc := buf[offset+5 : idx]

		keyFrom := idx
		valueFrom := keyFrom + int(keySize)
		valueTo := valueFrom + int(valueSize)

		crc0 := crc32.ChecksumIEEE(buf[dataHeaderFrom+5 : valueTo])
		// crc0 := crc32.ChecksumIEEE(append(headerWithoutCrc, buf[keyFrom:valueTo]...))

		if crc0 != crc1 {
			return nil, errors.ErrCrcNotValid
		}

		dataList = append(dataList, &Data{
			Key:      buf[keyFrom:valueFrom],
			Value:    buf[valueFrom:valueTo],
			DataType: DataType(t),
		})

		idx += int(keySize)
		idx += int(valueSize)

		offset = idx
	}
	return dataList, nil
}
