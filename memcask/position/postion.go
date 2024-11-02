package position

import "encoding/binary"

// Position 表示一个数据在磁盘中的存储位置
type Position struct {
	Filename string // 数据所在的文件名
	Offset   int    // 数据在文件中的偏移量
}

// offset
//   5

/*	Encode
 *  @Description: 将position进行编码
 *  @return []byte: 编码后的字符串
 */
func (pos *Position) Encode() []byte {
	header := make([]byte, binary.MaxVarintLen64)
	var index int
	index += binary.PutVarint(header, int64(pos.Offset))
	buf := append(header[:index], pos.Filename...)
	return buf
}

/*	DecodePosition
 *  @Description: 将字符串解码为position
 *  @param buf: 编码的字符串
 *  @return *Position: 解码得到的position实例
 */
func DecodePosition(buf []byte) *Position {
	offset, n := binary.Varint(buf)
	filename := buf[n:]
	return &Position{
		Filename: string(filename),
		Offset:   int(offset),
	}
}
