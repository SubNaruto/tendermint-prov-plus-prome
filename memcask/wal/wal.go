package wal

import (
	"github.com/sirupsen/logrus"
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/errors"
	"github.com/tendermint/tendermint/memcask/util"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WAL 表示预写日志
type WAL struct {
	Filepath string       // wal的路径
	mu       sync.RWMutex // 读写锁
	f        *os.File     // wal文件指针
}

/*	OpenWAL
 *  @Description: 根据文件路径打开wal文件
 *  @param filepath: wal文件的路径
 *  @return *WAL: wal实例
 *  @return error: 错误信息
 */
func OpenWAL(filepath string) (*WAL, error) {
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		return nil, err
	}
	wal := &WAL{
		Filepath: filepath,
		f:        f,
	}
	return wal, nil
}

/*	WriteData
 *  @Description: 将数据写入WAL实例
 *  @param data: 需要写入的数据
 *  @return error: 错误信息
 */
func (wal *WAL) WriteData(data *data.Data) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	enc := data.Encode()

	_, err := wal.f.Write(enc)
	return err
}

/*	Replay
 *  @Description: 重放并解码获取wal文件中的所有数据
 *  @return []*data.Data: 一系列数据
 *  @return error: 错误信息
 */
func (wal *WAL) Replay() ([]*data.Data, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	fileSize, err := util.GetFileSize(wal.f)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, fileSize)

	_, err = wal.f.ReadAt(buf, 0)
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

		//key := make([]byte, dataHeader.KeySize)
		//copy(key, buf[offset:])
		//offset += dataHeader.KeySize
		//
		//value := make([]byte, dataHeader.ValSize)
		//copy(value, buf[offset:])
		//offset += dataHeader.ValSize
		crc := crc32.ChecksumIEEE(buf[dataHeaderFrom+5 : valueTo])
		// crc := crc32.ChecksumIEEE(append(headerBuf[5:n], append(key, value...)...))

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

/*	Reset
 *  @Description: 清空wal文件
 *  @return error: 错误信息
 */
func (wal *WAL) Reset() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	tempFilePath := filepath.Dir(wal.Filepath) + "/" + time.Now().String() + ".data"
	err := os.Rename(wal.Filepath, tempFilePath)
	if err != nil {
		return err
	}

	logrus.Info("wal rename success")

	go func() {
		err := os.Remove(tempFilePath)
		if err == nil {
			logrus.Info("old wal delete success")
		}

	}()

	f, err := os.OpenFile(wal.Filepath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		return err
	}
	wal.f = f

	return nil
}

/*	Close
 *  @Description: 关闭wal文件
 */
func (wal *WAL) Close() {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	_ = wal.f.Close()
}
