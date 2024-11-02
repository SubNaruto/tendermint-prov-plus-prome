package diskFile

import (
	"fmt"
	"github.com/tendermint/tendermint/memcask/cache"
	"github.com/tendermint/tendermint/memcask/cache/slru"
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/errors"
	"github.com/tendermint/tendermint/memcask/position"
	"github.com/tendermint/tendermint/memcask/util"
	"golang.org/x/exp/mmap"
	"path/filepath"
	"sync"
)

// DataFileList 一个活跃数据文件和若干旧数据文件，缓存了所有文件的指针
type DataFileList struct {
	FileNumber   int64                // 活跃数据文件的编号
	OlderFileMap map[int64]*DiskFile  // 旧数据文件
	ActiveFile   *DiskFile            // 活跃数据文件
	Mu           sync.RWMutex         // 读写锁
	dir          string               // 文件所在目录
	suffix       string               // 文件后缀
	cache        cache.CacheInterface // 缓存
	Limit        int                  // 数据文件的阈值
}

/*	NewDataFileList
 *  @Description: 创建一个新的DataFileList实例
 *  @param dir: 文件所在目录(创建文件时使用)
 *  @param suffix: 文件后缀(创建文件时使用)
 *  @return *DataFileList: 创建的DataFileList实例
 */
func NewDataFileList(dir, suffix string, limit int, enableCache bool, probationLimit, protectionLimit int) *DataFileList {
	var c cache.CacheInterface
	if enableCache {
		c = slru.NewS3Cache(probationLimit, protectionLimit)
	}
	return &DataFileList{
		FileNumber:   0,
		OlderFileMap: make(map[int64]*DiskFile),
		dir:          dir,
		suffix:       suffix,
		cache:        c,
		Limit:        limit,
	}
}

/*	GetDiskFileByFilename
 *  @Description: 通过文件名获取文件对象
 *  @param filename: 文件名
 *  @return *DiskFile: 创建的DataFile实例
 *  @return error: 错误信息
 */
func (dfl *DataFileList) GetDiskFileByFilename(filename string) (*DiskFile, error) {
	dfl.Mu.RLock()
	defer dfl.Mu.RUnlock()

	if dfl.ActiveFile.Filename == filename {
		return dfl.ActiveFile, nil
	}

	fileId, err := util.GetFileNumByFilename(filename)
	if err != nil {
		return nil, err
	}

	df := dfl.OlderFileMap[fileId]
	if df == nil {
		return nil, errors.ErrDiskFileNotFound
	}
	return df, nil
}

/*	WriteData
 *  @Description: 向DataFileList的文件写入数据
 *  @param data: 要写入的数据
 *  @return *position.Position: 该数据对应的位置索引信息
 *  @return error: 错误信息
 */
func (dfl *DataFileList) WriteData(data *data.Data) (*position.Position, error) {
	var err error
	dfl.Mu.Lock()
	defer dfl.Mu.Unlock()

	var pos *position.Position

	// 如果活跃文件不存在
	if dfl.ActiveFile == nil {
		dfl.ActiveFile, err = dfl.CreateNewActiveFile()
		if err != nil {
			return nil, err
		}
	}

	// 如果活跃文件超载
	if dfl.ActiveFile.Offset >= dfl.Limit {
		err = dfl.TransferFileToOlder(dfl.FileNumber, dfl.ActiveFile)
		if err != nil {
			return nil, err
		}

		dfl.OlderFileMap[dfl.FileNumber] = dfl.ActiveFile

		dfl.ActiveFile, err = dfl.CreateNewActiveFile()
		if err != nil {
			return nil, err
		}
	}

	pos = &position.Position{
		Filename: dfl.ActiveFile.Filename,
		Offset:   dfl.ActiveFile.Offset,
	}
	_, err = dfl.ActiveFile.WriteData(data)
	if err != nil {
		return nil, err
	}
	return pos, nil
}

func (dfl *DataFileList) TransferFileToOlder(fileNumber int64, file *DiskFile) error {
	var err error
	file.mf, err = mmap.Open(filepath.Join(dfl.dir, file.Filename))
	if err != nil {
		return err
	}
	dfl.OlderFileMap[fileNumber] = file
	return nil
}

/*	CreateNewActiveFile
 *  @Description: 创建一个新的活跃数据文件
 *  @return *DiskFile: 创建的活跃数据文件实例
 *  @return error: 错误信息
 */
func (dfl *DataFileList) CreateNewActiveFile() (*DiskFile, error) {
	dfl.FileNumber++
	var err error
	dfl.ActiveFile, err = NewDiskFile(dfl.dir, fmt.Sprintf("%08d%s", dfl.FileNumber, dfl.suffix))
	return dfl.ActiveFile, err
}

/*	Close
 *  @Description: 关闭DataFileList的所有文件
 */
func (dfl *DataFileList) Close() {
	dfl.Mu.Lock()
	defer dfl.Mu.Unlock()
	if dfl.ActiveFile != nil {
		_ = dfl.ActiveFile.f.Close()
	}
	for _, olderFile := range dfl.OlderFileMap {
		_ = olderFile.Close()
	}
}

/*	GetDataFromCache
 *  @Description: 根据key从cache获取数据
 *  @param key: 数据的key
 *  @return []byte: 数据的value
 *  @return bool: 数据是否查找到
 */
func (dfl *DataFileList) GetDataFromCache(key []byte) ([]byte, bool) {
	return dfl.cache.Get(key)
}

/*	PutDataInCache
 *  @Description: 将数据存入cache
 *  @param key: 数据的key
 *  @param value: 数据的value
 */
func (dfl *DataFileList) PutDataInCache(key, value []byte) {
	dfl.cache.Put(key, value)
}
