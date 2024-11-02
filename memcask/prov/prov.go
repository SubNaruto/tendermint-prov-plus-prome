package prov

import (
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/errors"
	"github.com/tendermint/tendermint/memcask/util"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type ProvFileMap struct {
	files  map[string]*os.File
	mu     sync.RWMutex
	dir    string
	suffix string
}

/*	NewProvFileMap
 *  @Description: 初始化一个ProvFileMap
 *  @param dir: 溯源数据所在的目录
 *  @param suffix: 溯源数据文件的后缀
 *  @return *ProvFileMap: 初始化的ProvFileMap实例
 *  @return error: 错误信息
 */
func NewProvFileMap(dir, suffix string) (*ProvFileMap, error) {
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

	return &ProvFileMap{
		files:  make(map[string]*os.File),
		dir:    dir,
		suffix: suffix,
	}, nil
}

/*	AddFile
 *  @Description: 向ProvFileMap添加文件句柄，DB初始化时使用
 *  @param entityID: 实体ID
 *  @param f: 文件句柄实例
 */
func (pfm *ProvFileMap) AddFile(entityID string, f *os.File) {
	pfm.files[entityID] = f
}

/*	Put
 *  @Description: 向ProvFileMap添加一条溯源记录
 *  @param elem: 溯源数据
 *  @return error: 错误信息
 */
func (pfm *ProvFileMap) WriteData(elem *data.Data) error {
	entityID := string(elem.Key)
	filename := entityID + pfm.suffix
	enc := elem.Encode()

	pfm.mu.Lock()
	defer pfm.mu.Unlock()
	f, ok := pfm.files[entityID]
	if !ok {
		var err error
		f, err = os.OpenFile(filepath.Join(pfm.dir, filename), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0777)
		if err != nil {
			return err
		}
		pfm.files[entityID] = f
		_, err = f.Write(enc)
		if err != nil {
			return err
		}
	} else {
		_, err := f.Write(enc)
		if err != nil {
			return err
		}
	}
	return nil
}

/*	Get
 *  @Description: 根据实体ID获取对应的所有溯源数据
 *  @param entityID: 实体ID
 *  @return [][]byte: 解码得到的溯源数据
 *  @return error: 错误信息
 */
func (pfm *ProvFileMap) Get(entityID string) ([][]byte, error) {
	pfm.mu.RLock()
	defer pfm.mu.RUnlock()
	f, ok := pfm.files[entityID]
	if !ok {
		return nil, nil
	} else {
		fileSize, err := util.GetFileSize(f)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, fileSize)
		_, err = f.ReadAt(buf, 0)

		var dataList [][]byte
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

			// keyFrom := offset
			offset += dataHeader.KeySize

			valueFrom := offset
			offset += dataHeader.ValSize
			valueTo := offset

			crc := crc32.ChecksumIEEE(buf[dataHeaderFrom+5 : valueTo])

			if crc != dataHeader.Crc {
				return nil, errors.ErrCrcNotValid
			}

			dataList = append(dataList, buf[valueFrom:valueTo])
		}
		return dataList, err
	}
}

/*	Delete
 *  @Description: 根据实体ID删除对应的溯源数据文件
 *  @param entityID: 实体ID
 *  @return error: 错误信息
 */
func (pfm *ProvFileMap) Delete(entityID string) error {
	fp := pfm.dir + "/" + entityID + pfm.suffix
	tempFilePath := pfm.dir + "/" + time.Now().String() + ".prov"

	pfm.mu.Lock()
	defer pfm.mu.Unlock()

	if _, ok := pfm.files[entityID]; !ok {
		return nil
	}

	delete(pfm.files, entityID)

	err := os.Rename(fp, tempFilePath)
	if err != nil {
		return err
	}

	go func() {
		_ = os.Remove(tempFilePath)
	}()

	return nil
}

/*	Close
 *  @Description: 关闭ProvFileMap
 */
func (pfm *ProvFileMap) Close() {
	pfm.mu.Lock()
	defer pfm.mu.Unlock()
	for _, f := range pfm.files {
		_ = f.Close()
	}
}
