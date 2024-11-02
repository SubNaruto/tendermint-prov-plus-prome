package diskFile

import (
	"fmt"
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/errors"
	"github.com/tendermint/tendermint/memcask/position"
	"github.com/tendermint/tendermint/memcask/util"
	"golang.org/x/exp/mmap"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
)

// DiskFile 是对文件指针的高级封装
type DiskFile struct {
	f        *os.File
	Filename string
	Offset   int
	mf       *mmap.ReaderAt
}

/*	NewDiskFile
 *  @Description: 创建一个新的DiskFile实例
 *  @param dir: 文件所在目录
 *  @param filename: 文件名
 *  @return *DiskFile: 创建的DiskFile实例
 *  @return error: 错误信息
 */
func NewDiskFile(dir, filename string) (*DiskFile, error) {
	_, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) { // 如果目录不存在，创建目录
			err = os.MkdirAll(dir, 0777)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	fp := filepath.Join(dir, filename)                                 // 获取文件的绝对路径
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777) // 打开文件
	if err != nil {
		return nil, err
	}
	return &DiskFile{ // 封装成DiskFile实例
		f:        f,
		Filename: filename,
		Offset:   0,
	}, nil
}

/*	Close
 *  @Description: 关闭DiskFile
 *  @return error: 错误信息
 */
func (df *DiskFile) Close() error {
	if df.mf != nil {
		err := df.mf.Close()
		if err != nil {
			return err
		}
	}
	return df.f.Close()
}

func (df *DiskFile) ReadAt(buf []byte, offset int64) (int, error) {
	if df.mf != nil {
		return df.mf.ReadAt(buf, offset)
	}
	return df.f.ReadAt(buf, offset)
}

/*	WriteData
 *  @Description: 向DiskFile写入数据
 *  @param data: 需要写入的数据
 *  @return int: 写入的数据大小
 *  @return error: 错误信息
 */
func (df *DiskFile) WriteData(data *data.Data) (int, error) {
	enc := data.Encode()

	n, err := df.f.Write(enc)
	df.Offset += n
	return n, err
}

/*
crc keySize valueSize type
 5     5		5      1
*/

/*	ReadData
 *  @Description: 根据offset读取一个完整的数据
 *  @param offset: 数据偏移量
 *  @return *data.Data: 读取的数据
 *  @return int: 数据在文件中的大小
 *  @return error: 错误信息
 */
func (df *DiskFile) ReadData(offset int) (*data.Data, int, error) {
	fileSize, err := df.GetFileSize()
	if err != nil {
		return nil, 0, err
	}
	if offset >= fileSize {
		return nil, 0, io.EOF
	}

	var l int

	var headerSize int

	if offset+data.DataHeaderSize > fileSize {
		headerSize = fileSize - offset
	} else {
		headerSize = data.DataHeaderSize
	}

	headerBuf := make([]byte, headerSize)

	_, err = df.ReadAt(headerBuf, int64(offset))
	if err != nil {
		return nil, 0, err
	}

	dataHeader, n := data.DecodeHeader(headerBuf)
	l += n

	dataSize := dataHeader.KeySize + dataHeader.ValSize
	l += dataSize

	dataBuf := make([]byte, dataSize)
	_, err = df.ReadAt(dataBuf, int64(offset+n))
	if err != nil {
		return nil, 0, err
	}

	crc := crc32.ChecksumIEEE(append(headerBuf[5:n], dataBuf...))

	if crc != dataHeader.Crc {
		return nil, 0, errors.ErrCrcNotValid
	}

	return &data.Data{
		Key:      dataBuf[:dataHeader.KeySize],
		Value:    dataBuf[dataHeader.KeySize:],
		DataType: dataHeader.DataType,
	}, l, nil
}

/*	ReadAll
 *  @Description: 读取一个DiskFile的所有内容并转化为对应的数据
 *  @return []*data.Data: 解码得到的所有数据
 *  @return error: 错误信息
 */
func (df *DiskFile) ReadAll() ([]*data.Data, error) {
	fileSize, err := df.GetFileSize()
	if err != nil {
		return nil, err
	}

	//const blockSize = 4 * option.MB
	//buf := make([]byte, fileSize)
	//blockNum := fileSize / blockSize
	//lastSize := fileSize & (blockSize - 1)
	//for i := 0; i < blockNum; i++ {
	//	of := i * blockSize
	//	_, err = df.ReadAt(buf[of:of+blockSize], int64(of))
	//	if err != nil {
	//		return nil, err
	//	}
	//}
	//if lastSize > 0 {
	//	of := blockNum * blockSize
	//	_, err = df.ReadAt(buf[of:of+lastSize], int64(of))
	//	if err != nil {
	//		return nil, err
	//	}
	//}

	buf := make([]byte, fileSize)
	_, err = df.ReadAt(buf, 0)
	if err != nil {
		return nil, err
	}

	var dataList []*data.Data

	var offset int
	for offset < len(buf) {
		var headerSize int

		dataHeaderFrom := offset

		if offset+data.DataHeaderSize <= len(buf) {
			headerSize = data.DataHeaderSize
		} else {
			headerSize = len(buf) - offset
		}

		headerBuf := make([]byte, headerSize)
		copy(headerBuf, buf[offset:])

		dataHeader, n := data.DecodeHeader(headerBuf)
		offset += n

		keyFrom := offset
		offset += dataHeader.KeySize

		valueFrom := offset
		offset += dataHeader.ValSize
		valueTo := offset

		crc := crc32.ChecksumIEEE(buf[dataHeaderFrom+5 : valueTo])

		//crc := crc32.ChecksumIEEE(append(headerBuf[5:n], buf[keyFrom:valueTo]...))

		if crc != dataHeader.Crc {
			return nil, errors.ErrCrcNotValid
		}

		dataList = append(dataList, &data.Data{
			Key:      buf[keyFrom:valueFrom],
			Value:    buf[valueFrom:valueTo],
			DataType: dataHeader.DataType,
		})
	}
	return dataList, nil
}

func (df *DiskFile) readAllTempTest() ([]*data.Data, error) {
	fileSize, err := df.GetFileSize()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, fileSize)
	_, err = df.ReadAt(buf, 0)
	if err != nil {
		return nil, err
	}

	var dataList []*data.Data

	st := time.Now()

	var offset int
	for offset < len(buf) {
		var headerSize int

		if offset+data.DataHeaderSize <= len(buf) {
			headerSize = data.DataHeaderSize
		} else {
			headerSize = len(buf) - offset
		}
		headerBuf := make([]byte, headerSize)
		copy(headerBuf, buf[offset:])

		dataHeader, n := data.DecodeHeader(headerBuf)
		offset += n

		key := make([]byte, dataHeader.KeySize)
		copy(key, buf[offset:])
		offset += dataHeader.KeySize

		value := make([]byte, dataHeader.ValSize)
		copy(value, buf[offset:])
		offset += dataHeader.ValSize

		crc := crc32.ChecksumIEEE(append(headerBuf[5:n], append(key, value...)...))

		if crc != dataHeader.Crc {
			return nil, errors.ErrCrcNotValid
		}

		dataList = append(dataList, &data.Data{
			Key:      key,
			Value:    value,
			DataType: dataHeader.DataType,
		})
	}

	fmt.Println(time.Since(st).Milliseconds())

	return dataList, nil
}

/*	ReadAllPosition
 *  @Description: 读取一个DiskFile的所有内容并转化为对应的数据以及位置索引信息
 *  @return []*data.Data: 解码得到的所有数据
 *  @return []*position.Position: 解码得到的所有数据的位置索引信息
 *  @return error: 错误信息
 */
func (df *DiskFile) ReadAllPosition() ([]*data.Data, []*position.Position, error) {
	fileSize, err := df.GetFileSize()
	if err != nil {
		return nil, nil, err
	}

	buf := make([]byte, fileSize)

	_, err = df.ReadAt(buf, 0)
	if err != nil {
		return nil, nil, err
	}

	df.Offset = len(buf)

	var posList []*position.Position
	var dataList []*data.Data

	var offset int
	for offset < len(buf) {
		var headerSize int
		dataHeaderFrom := offset

		if offset+data.DataHeaderSize <= len(buf) {
			headerSize = data.DataHeaderSize
		} else {
			headerSize = len(buf) - offset
		}
		headerBuf := make([]byte, headerSize)
		copy(headerBuf, buf[offset:])

		pos := &position.Position{
			Filename: df.Filename,
			Offset:   offset,
		}

		dataHeader, n := data.DecodeHeader(headerBuf)
		offset += n

		keyFrom := offset
		offset += dataHeader.KeySize
		keyTo := offset

		offset += dataHeader.ValSize
		valueTo := offset

		crc := crc32.ChecksumIEEE(buf[dataHeaderFrom+5 : valueTo])

		if crc != dataHeader.Crc {
			return nil, nil, errors.ErrCrcNotValid
		}

		dataList = append(dataList, &data.Data{
			Key:      buf[keyFrom:keyTo],
			DataType: dataHeader.DataType,
		})

		posList = append(posList, pos)
	}

	return dataList, posList, nil
}

func (df *DiskFile) readAllPositionTempTest() ([]*data.Data, []*position.Position, error) {
	fileSize, err := df.GetFileSize()
	if err != nil {
		return nil, nil, err
	}

	buf := make([]byte, fileSize)

	_, err = df.ReadAt(buf, 0)
	if err != nil {
		return nil, nil, err
	}

	df.Offset = len(buf)

	var posList []*position.Position
	var dataList []*data.Data

	s := time.Now()

	var offset int
	for offset < len(buf) {
		var headerSize int

		if offset+data.DataHeaderSize <= len(buf) {
			headerSize = data.DataHeaderSize
		} else {
			headerSize = len(buf) - offset
		}
		headerBuf := make([]byte, headerSize)
		copy(headerBuf, buf[offset:])

		pos := &position.Position{
			Filename: df.Filename,
			Offset:   offset,
		}

		dataHeader, n := data.DecodeHeader(headerBuf)
		offset += n

		key := make([]byte, dataHeader.KeySize)
		copy(key, buf[offset:])
		offset += dataHeader.KeySize

		value := make([]byte, dataHeader.ValSize)
		copy(value, buf[offset:])
		offset += dataHeader.ValSize

		crc := crc32.ChecksumIEEE(append(headerBuf[5:n], append(key, value...)...))

		if crc != dataHeader.Crc {
			return nil, nil, errors.ErrCrcNotValid
		}

		dataList = append(dataList, &data.Data{
			Key:      key,
			DataType: dataHeader.DataType,
		})

		posList = append(posList, pos)
	}

	fmt.Println(time.Since(s).Milliseconds())

	return dataList, posList, nil
}

func (df *DiskFile) GetFileSize() (int, error) {
	if df.mf != nil {
		return df.mf.Len(), nil
	}
	size, err := util.GetFileSize(df.f)
	if err != nil {
		return 0, err
	}
	return size, nil
}
