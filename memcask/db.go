package db

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/diskFile"
	"github.com/tendermint/tendermint/memcask/errors"
	"github.com/tendermint/tendermint/memcask/index"
	"github.com/tendermint/tendermint/memcask/memtable"
	"github.com/tendermint/tendermint/memcask/option"
	"github.com/tendermint/tendermint/memcask/position"
	"github.com/tendermint/tendermint/memcask/prov"
	"github.com/tendermint/tendermint/memcask/util"
	"github.com/tendermint/tendermint/memcask/wal"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	mu sync.RWMutex // 读写锁

	memIndex []index.MemoryIndex // 内存索引

	memTable          memtable.MemTable // 内存MemTable
	immutableMemTable memtable.MemTable // 不可变的MemTable

	option *option.Option // 配置选项

	blockDataFiles  *diskFile.DataFileList // 存储区块的非分片数据
	blockPartFiles  *diskFile.DataFileList // 存储区块的分片数据
	normalDataFiles *diskFile.DataFileList // 存储非区块数据

	genImmutableMemTableChan   chan struct{} // 通知生成immutableMemTable的通道
	flushImmutableMemTableChan chan struct{} // 能够进行immutableMemTable flush的临界资源

	ProvFileMap   *prov.ProvFileMap // entityID -> 文件句柄
	ProvWriteChan chan *data.Data

	closed bool // db是否关闭

	wal *wal.WAL // 预写日志

	merging bool // 是否正在merge
}

/*	NewDBWithOption
 *  @Description: 初始化一个DB实例
 *  @param opt: 配置选项
 *  @return *DB: 初始化的DB实例
 */
func NewDBWithOption(opt *option.Option) *DB {
	if _, err := os.Stat(opt.DataDir); os.IsNotExist(err) {
		err = os.MkdirAll(opt.DataDir, 0777)
		if err != nil {
			panic(err)
		}
	}

	db := &DB{
		memTable: memtable.NewBTreeMemTable(opt.BTreeDegreeForMemTable),
		option:   opt,

		blockDataFiles: diskFile.NewDataFileList(
			opt.DataDir+"/"+option.BlockDataDir,
			option.BlockDataFileSuffix,
			opt.DiskFileSize,
			opt.EnableBlockDataCache,
			opt.ProbationLimit,
			opt.ProtectionLimit,
		),

		blockPartFiles: diskFile.NewDataFileList(
			opt.DataDir+"/"+option.BlockPartDir,
			option.BlockPartFileSuffix,
			opt.DiskFileSize,
			false,
			0, 0,
		),

		normalDataFiles: diskFile.NewDataFileList(
			opt.DataDir+"/"+option.DataFileDir,
			option.DataFileSuffix,
			opt.DiskFileSize,
			false,
			0, 0,
		),

		genImmutableMemTableChan: make(chan struct{}),

		flushImmutableMemTableChan: make(chan struct{}, 1),
	}

	// 是否支持存储溯源数据
	if opt.EnableProv {
		provFileMap, err := prov.NewProvFileMap(opt.DataDir+"/"+option.ProvenanceDir, option.ProvenanceSuffix)
		if err != nil {
			panic(err)
		}

		db.ProvFileMap = provFileMap
		db.ProvWriteChan = make(chan *data.Data, opt.ProvWriteChanSize)

		err = db.loadProvFiles()

		go func() {
			for d := range db.ProvWriteChan {
				err := db.ProvFileMap.WriteData(d)
				if err != nil {
					panic(err)
				}
			}
		}()

		if err != nil {
			panic(err)
		}
	}

	// 是否支持预写日志
	if !opt.WalDisabled {
		walFile, err := wal.OpenWAL(opt.WalFilePath)
		if err != nil {
			panic(err)
		}
		db.wal = walFile
	}

	db.memIndex = make([]index.MemoryIndex, opt.MemIndexNum)
	for i := 0; i < opt.MemIndexNum; i++ {
		db.memIndex[i] = index.NewBTreeIndex(opt.BTreeDegreeForIndex)
	}

	db.flushImmutableMemTableChan <- struct{}{}

	err := db.loadBlockDataFromDisk()
	if err != nil {
		panic(err)
	}

	err = db.transferMergeFiles()
	if err != nil {
		panic(err)
	}

	err = db.loadNormalDataAndIndex()
	if err != nil {
		panic(err)
	}

	// 将ImmutableMemTable的数据刷新到磁盘中
	logrus.Info("开启协程监测immutableMemTable...")
	go db.listenMemTableSignal()

	return db
}

func (db *DB) PutPos(key []byte, pos *position.Position) {
	i := util.CalKeyIndex(key, &db.option.MemIndexNum)
	db.memIndex[i].Put(key, pos)
}

func (db *DB) GetPos(key []byte) *position.Position {
	i := util.CalKeyIndex(key, &db.option.MemIndexNum)
	return db.memIndex[i].Get(key)
}

func (db *DB) DeletePos(key []byte) {
	i := util.CalKeyIndex(key, &db.option.MemIndexNum)
	db.memIndex[i].Delete(key)
}

/*	listenMemTableSignal
 *  @Description: 监听db.genImmutableMemTableChan通道的协程，用于flush immutableMemTable
 */
func (db *DB) listenMemTableSignal() {
	for range db.genImmutableMemTableChan {
		func() {
			logrus.Info("收到信号，将immutableMemTable中的数据刷新到磁盘中...")

			it := db.immutableMemTable.GetIterator()

			for it.Valid() {
				elem := it.Value()
				var pos *position.Position
				var err error
				if elem.DataType == data.Delete {
					db.DeletePos(elem.Key)
				} else if elem.DataType == data.Normal {
					// 如果是非区块数据,写入normalDataFiles
					pos, err = db.normalDataFiles.WriteData(elem)
					if err != nil {
						panic(err)
					}
				} else if elem.DataType == data.BlockData {
					// 如果是区块的非分片数据,写入BlockDataFiles
					pos, err = db.blockDataFiles.WriteData(elem)
					if err != nil {
						panic(err)
					}

				} else if elem.DataType == data.BlockPart {
					// 如果是区块的分片数据,写入BlockPartFiles
					pos, err = db.blockPartFiles.WriteData(elem)
					if err != nil {
						panic(err)
					}
				}
				if pos != nil {
					// 更新内存索引
					db.PutPos(elem.Key, pos)
				}
				it.Next()
			}

			// 确保已经刷盘
			db.immutableMemTable = nil
			logrus.Info("flush完成,将immutableMemTable置空")

			db.flushImmutableMemTableChan <- struct{}{}
		}()
	}
}

// GetBlockParts P:height:0 -> P:height:total-1
// GetBlockParts 根据分片的第一个key和最后一个key获取完整的分片数据(P:height:index)

/*	GetBlockParts
 *  @Description: 根据height和分片数获取聚合的分片
 *  @param height: 区块高度
 *  @param total: 分片数
 *  @return [][]byte: 所有的分片
 *  @return error: 错误信息
 */
func (db *DB) GetBlockParts(height, total int) ([][]byte, error) {
	posList := make([]*position.Position, 0, total)
	parts := make([][]byte, total)
	keys := make([][]byte, 0, total)

	// inCache := make([]bool, total)

	for index := 0; index < total; index++ {
		// 根据height和index构造分片的key
		key := []byte(fmt.Sprintf("P:%v:%v", height, index))
		keys = append(keys, key)

		// 去memTable中查找
		if db.memTable != nil {
			val, ok := db.memTable.Get(key) // 找到了
			if ok {
				parts[index] = val
				continue
			}
		}

		// 去immutableMemTable中查找
		if db.immutableMemTable != nil {
			val, ok := db.immutableMemTable.Get(key) // 找到了
			if ok {
				parts[index] = val
				continue
			}
		}

		// 通过内存索引去磁盘查找
		pos := db.GetPos(key)
		if pos != nil {
			posList = append(posList, pos) // 暂存位置索引信息
		}

		//if pos != nil {
		//	fmt.Println(string(key), pos.Filename, pos.Offset)
		//}
		//if height == 106 && index == 10 {
		//	fmt.Println(pos.Filename, pos.Offset)
		//}
	}

	// Very Important Operation
	// immutableMemTable在flush时，是按照key的字典序来写入磁盘的，也就是说key=[P:10:20]在key=[P:10:9]的前面先写入磁盘
	// 因此读取pos时，其实同一个file的pos的offset并不是有序的，我们需要按offset排序后，先针对一个文件进行一次的区间读取
	sort.Slice(posList, func(i, j int) bool {
		if posList[i].Filename != posList[j].Filename {
			return posList[i].Filename < posList[j].Filename
		}
		return posList[i].Offset < posList[j].Offset
	})

	// 查询磁盘中的分片
	var filename string
	offset0, offset1 := -1, -1 // 分别记录同一个文件中第一个分片的偏移量和最后一个分片的偏移量
	// blockPartDir := db.option.DataDir + "/" + option.BlockPartDir // 分片数据文件所在的目录
	for _, pos := range posList {
		if filename == "" { // 遇到第一个文件,初始化偏移量
			filename = pos.Filename
			offset0 = pos.Offset
		} else if filename != pos.Filename { // 查找到新的文件，则需要先处理上一个文件

			// 打开文件
			df, err := db.blockPartFiles.GetDiskFileByFilename(filename)
			if err != nil {
				return nil, err
			}

			var buf []byte

			if offset1 == -1 {
				// 上一个文件只有一个分片(因为offset1没有更新)
				fileSize, err := df.GetFileSize()
				if err != nil {
					return nil, err
				}
				buf = make([]byte, fileSize-offset0)
			} else {
				// 上一个文件有多个分片
				buf = make([]byte, offset1-offset0)
			}

			_, err = df.ReadAt(buf, int64(offset0)) // 从offset0开始读取len(buf)的数据
			if err != nil {
				return nil, err
			}

			// 将buf数据解码为多个data
			dataList, err := data.DecodeBufToDataList(buf)
			if err != nil {
				return nil, err
			}
			for _, d := range dataList {
				idx, err := util.GetPartIndexFromKey(string(d.Key))
				if err != nil {
					return nil, err
				}
				parts[idx] = d.Value // 获取data的分片数据
			}

			if offset1 != -1 { // 如果offset1不等于-1，则还需要读取最后一个分片
				d, _, err := df.ReadData(offset1)
				if err != nil {
					return nil, err
				}
				idx, err := util.GetPartIndexFromKey(string(d.Key))
				if err != nil {
					return nil, err
				}
				parts[idx] = d.Value // 获取data的分片数据
			}

			filename = pos.Filename
			offset0 = pos.Offset
			offset1 = -1
		} else { // 还是同一个文件，只更新最新的偏移量
			offset1 = pos.Offset
		}
	}
	if filename != "" { // 读取最后一个文件的分片

		df, err := db.blockPartFiles.GetDiskFileByFilename(filename)
		if err != nil {
			return nil, err
		}

		var buf []byte

		if offset1 == -1 { // 文件只有一个分片
			fileSize, err := df.GetFileSize()
			if err != nil {
				return nil, err
			}
			buf = make([]byte, fileSize-offset0)
		} else { // 上一个文件有多个分片
			buf = make([]byte, offset1-offset0)
		}

		_, err = df.ReadAt(buf, int64(offset0)) // 一次性读取文件的所有分片
		if err != nil {
			return nil, err
		}

		dataList, err := data.DecodeBufToDataList(buf)
		if err != nil {
			return nil, err
		}
		for _, d := range dataList {
			idx, err := util.GetPartIndexFromKey(string(d.Key))
			if err != nil {
				return nil, err
			}
			parts[idx] = d.Value
		}

		if offset1 != -1 { // 还需要读取最后一个分片
			d, _, err := df.ReadData(offset1)
			if err != nil {
				return nil, err
			}
			idx, err := util.GetPartIndexFromKey(string(d.Key))
			if err != nil {
				return nil, err
			}
			parts[idx] = d.Value
		}
	}
	return parts, nil
}

func (db *DB) GetBlockPart(height, index int) ([]byte, error) {
	key := []byte(fmt.Sprintf("P:%v:%v", height, index))

	// 去memTable中查找
	if db.memTable != nil {
		part, ok := db.memTable.Get(key) // 找到了
		if ok {
			return part, nil
		}
	}

	// 去immutableMemTable中查找
	if db.immutableMemTable != nil {
		part, ok := db.immutableMemTable.Get(key) // 找到了
		if ok {
			return part, nil
		}
	}

	// 通过内存索引去磁盘查找
	pos := db.GetPos(key)

	if pos != nil { // 存在于磁盘
		df, err := db.blockPartFiles.GetDiskFileByFilename(pos.Filename)
		if err != nil {
			return nil, err
		}

		d, _, err := df.ReadData(pos.Offset)
		if err != nil {
			return nil, err
		}

		return d.Value, nil
	}
	return nil, nil
}

/*	transferMumTableToImmutableAsync
 *  @Description: 将MemTable转化为ImmutableMemTable，开启flush，immutableMemTable正在flush时，不会阻塞
 */
func (db *DB) transferMumTableToImmutableAsync() {
	select {
	case <-db.flushImmutableMemTableChan: // immutableMemTable正在flush时，这里会阻塞，此时会走default分支
		logrus.Info("将memTable转化为immutableMemTable")

		db.immutableMemTable = db.memTable
		db.memTable = memtable.NewBTreeMemTable(db.option.BTreeDegreeForMemTable)

		if !db.option.WalDisabled {
			_ = db.wal.Reset()
		}

		db.genImmutableMemTableChan <- struct{}{} // 向flush协程发送信号
	default: // immutableMemTable正在flush，直接返回。如果没有default分支， immutableMemTable在flush时put会阻塞等待
	}
}

/*	transferMumTableToImmutableSync
 *  @Description: 将MemTable转化为ImmutableMemTable，开启flush，immutableMemTable正在flush时，会阻塞等待
 */
func (db *DB) transferMumTableToImmutableSync() {
	<-db.flushImmutableMemTableChan // immutableMemTable正在flush时，这里会阻塞等待
	logrus.Info("将memTable转化为immutableMemTable")

	db.immutableMemTable = db.memTable
	db.memTable = memtable.NewBTreeMemTable(db.option.BTreeDegreeForMemTable)

	if !db.option.WalDisabled {
		_ = db.wal.Reset()
	}

	db.genImmutableMemTableChan <- struct{}{} // 向flush协程发送信号
}

/*	Put
 *  @Description: 向DB写入数据
 *  @param key: 数据的key
 *  @param value: 数据的value
 *  @param dataType: 数据的类型
 */
func (db *DB) Put(key, value []byte, dataType data.DataType) {
	d := &data.Data{
		Key:      key,
		Value:    value,
		DataType: dataType,
	}

	if dataType == data.ProvData {
		db.ProvWriteChan <- d
		return
	}

	if !db.option.WalDisabled {
		_ = db.wal.WriteData(d)
	}

	db.memTable.Put(d)

	if db.memTable.Size() >= db.option.MemTableSize {
		db.transferMumTableToImmutableAsync()
	}
}

/*	Get
 *  @Description: 根据key和dataType从DB查询非分片数据的value
 *  @param key: 数据的key
 *  @param dataType: 数据的类型
 *  @return []byte: 数据的value
 */
func (db *DB) Get(key []byte, dataType data.DataType) []byte {
	if db.memTable != nil {
		val, ok := db.memTable.Get(key)
		if ok { // 存在于memTable
			return val
		}
	}

	if db.immutableMemTable != nil {
		val, ok := db.immutableMemTable.Get(key)
		if ok { // 存在于immutableMemTable
			return val
		}
	}

	pos := db.GetPos(key)

	if pos != nil { // 存在于磁盘
		var df *diskFile.DiskFile
		var err error
		if dataType == data.Normal {
			df, err = db.normalDataFiles.GetDiskFileByFilename(pos.Filename)
		} else if dataType == data.BlockData {
			if db.option.EnableBlockDataCache {
				val, ok := db.blockDataFiles.GetDataFromCache(key) // 先去缓存中获取
				if ok {
					return val
				}
			}

			df, err = db.blockDataFiles.GetDiskFileByFilename(pos.Filename)
		}
		if err != nil {
			panic(err)
		}

		d, _, err := df.ReadData(pos.Offset)
		if err != nil {
			panic(err)
		}

		if dataType == data.BlockData {
			if db.option.EnableBlockDataCache {
				db.blockDataFiles.PutDataInCache(key, d.Value)
			}
		}

		return d.Value
	}

	return nil
}

/*	GetProvData
 *  @Description: 根据实体ID获取所有json化的溯源数据
 *  @param entityID: 实体ID
 *  @param [][]byte: 一系列json化的溯源数据
 *  @return error: 错误信息
 */
func (db *DB) GetProvData(entityID string) ([][]byte, error) {
	return db.ProvFileMap.Get(entityID)
}

/*	DeleteProvData
 *  @Description: 根据实体ID删除对应的溯源文件
 *  @param entityID: 实体ID
 *  @return error: 错误信息
 */
func (db *DB) DeleteProvData(entityID string) error {
	return db.ProvFileMap.Delete(entityID)
}

/*	Delete
 *  @Description: 删除非区块数据
 *  @param key: 数据的key
 */
func (db *DB) Delete(key []byte) {
	db.Put(key, nil, data.Delete)
}

/*	loadBlockDataFromDisk
 *  @Description: 加载磁盘文件的Block数据，并生成内存索引
 *  @return error: 错误信息
 */
func (db *DB) loadBlockDataFromDisk() error {
	subDirs := []string{option.BlockDataDir, option.BlockPartDir}
	suffix := []string{option.BlockDataFileSuffix, option.BlockPartFileSuffix}

	if !db.option.WalDisabled {
		// wal重放
		dataList, err := db.wal.Replay()
		if err != nil {
			return err
		}
		for _, d := range dataList {
			db.memTable.Put(d)
		}
	}

	// 遍历数据目录
	for i, subDir := range subDirs {
		dir := db.option.DataDir + "/" + subDir
		dirEntries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			} else {
				return err
			}
		}

		fileNames := make([]string, 0, len(dirEntries))

		// 遍历当前目录的所有目录项
		for _, dirEntry := range dirEntries {
			if !dirEntry.IsDir() && strings.HasSuffix(dirEntry.Name(), suffix[i]) {
				fileNames = append(fileNames, dirEntry.Name())
			}
		}

		// 将文件名排序
		sort.Slice(fileNames, func(i, j int) bool {
			return fileNames[i] < fileNames[j]
		})

		// 遍历所有文件名，打开对应的文件
		for j, fileName := range fileNames {

			// 创建DiskFile
			file, err := diskFile.NewDiskFile(dir, fileName)
			if err != nil {
				return err
			}

			// 将diskFile示例加入到DiskFileList中
			if suffix[i] == option.BlockDataFileSuffix {
				fileNumber, err := util.GetFileNumByFilename(fileName)
				if err != nil {
					return err
				}
				if j == len(dirEntries)-1 { // 最后一个是活跃数据文件
					db.blockDataFiles.ActiveFile = file
				} else {
					err = db.blockDataFiles.TransferFileToOlder(fileNumber, file)
					if err != nil {
						return err
					}
					// db.blockDataFiles.OlderFileMap[fileNumber] = file
				}
				db.blockDataFiles.FileNumber = fileNumber
			} else if suffix[i] == option.BlockPartFileSuffix {
				fileNumber, err := util.GetFileNumByFilename(fileName)
				if err != nil {
					return err
				}
				if j == len(dirEntries)-1 { // 最后一个是活跃数据文件
					db.blockPartFiles.ActiveFile = file
				} else {
					err = db.blockPartFiles.TransferFileToOlder(fileNumber, file)
					if err != nil {
						return err
					}
				}
				db.blockPartFiles.FileNumber = fileNumber
			}

			// 获取当前DiskFile中的所有数据以及对应的位置信息
			dataList, posList, err := file.ReadAllPosition()
			if err != nil {
				return err
			}

			// 修改内存索引
			for k, pos := range posList {
				if dataList[k].DataType == data.Delete {
					db.DeletePos(dataList[k].Key)
				} else {
					db.PutPos(dataList[k].Key, pos)
				}
			}
		}
	}
	return nil
}

/*	loadProvFiles
 *  @Description: 加载磁盘文件的溯源数据文件句柄
 *  @return error: 错误信息
 */
func (db *DB) loadProvFiles() error {
	if db.option.EnableProv {
		dir := db.option.DataDir + "/" + option.ProvenanceDir
		dirEntries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		for _, dirEntry := range dirEntries {
			if !dirEntry.IsDir() && strings.HasSuffix(dirEntry.Name(), option.ProvenanceSuffix) {
				f, err := os.OpenFile(filepath.Join(dir, dirEntry.Name()), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0777)
				if err != nil {
					return err
				}
				parts := strings.Split(dirEntry.Name(), ".")
				if len(parts) != 2 {
					return errors.ErrInvalidProvFileName
				}
				entityID := parts[0]
				db.ProvFileMap.AddFile(entityID, f)
			}
		}
	}
	return nil
}

/*	transferMergeFiles
 *  @Description: 处理merge文件并移动到数据目录下
 *  @return error: 错误信息
 */
func (db *DB) transferMergeFiles() error {
	// merge目录不存在的话直接返回
	_, err := os.Stat(db.option.MergeDir)
	if err != nil {
		if os.IsNotExist(err) {
			logrus.Info("merge目录不存在")
			return nil
		} else {
			return err
		}
	}

	defer func() {
		_ = os.RemoveAll(db.option.MergeDir)
	}()

	// 读取merge目录的所有文件
	logrus.Info("读取merge目录的所有文件")
	dirEntries, err := os.ReadDir(db.option.MergeDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// 查找标识 merge 完成的文件，判断 merge 是否处理完了
	var mergeFinished bool
	mergeFileNames := make([]string, 0, len(dirEntries))

	for _, entry := range dirEntries {
		if entry.Name() == db.option.MergeFinishFileName {
			logrus.Info("merge-finish is ok")
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// 没有 merge 完成则直接返回
	if !mergeFinished {
		return nil
	}

	// 获取非merge的最小fileId
	nonMergeFileId, err := db.getNonMergeFileNum(db.option.MergeDir)
	if err != nil {
		return err
	}

	dataFileDir := db.option.DataDir + "/" + option.DataFileDir

	// 删除数据目录下的所有旧数据文件
	logrus.Info("删除数据目录下的所有旧数据文件")
	for fileId := int64(1); fileId < nonMergeFileId; fileId++ {
		fp := filepath.Join(dataFileDir, fmt.Sprintf("%08d%s", fileId, option.DataFileSuffix))
		fmt.Println(fp)
		if _, err := os.Stat(fp); err == nil {
			if err := os.Remove(fp); err != nil {
				return err
			}
		}
	}

	_ = os.Remove(filepath.Join(dataFileDir, db.option.MergeFinishFileName))
	_ = os.Remove(filepath.Join(dataFileDir, db.option.MergeHintFilename))

	// 将merge后的数据文件移到数据目录下
	logrus.Info("将merge后的数据文件移到数据目录下")
	for _, fileName := range mergeFileNames {
		fmt.Println(fileName)
		srcPath := filepath.Join(db.option.MergeDir, fileName)
		destPath := filepath.Join(dataFileDir, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}

	return nil
}

/*	loadNormalDataAndIndex
 *  @Description: 加载磁盘文件的非Block数据，生成内存索引
 *  @return error: 错误信息
 */
func (db *DB) loadNormalDataAndIndex() error {
	// 非区块数据所在的文件目录
	dataFileDir := db.option.DataDir + "/" + option.DataFileDir

	// 从Hint加载文件索引
	logrus.Info("从Hint加载文件索引")
	err := db.loadIndexFromHintFile(dataFileDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	// 没有参与merge的最小fileId
	nonMergeFileId, err := db.getNonMergeFileNum(dataFileDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	// 读取更新后的数据目录下的所有文件
	logrus.Info("读取更新后的数据目录下的所有文件")
	dirEntries, err := os.ReadDir(dataFileDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		} else {
			return err
		}
	}

	var dataFileEntries []os.DirEntry

	for _, dirEntry := range dirEntries {
		if !dirEntry.IsDir() && strings.HasSuffix(dirEntry.Name(), option.DataFileSuffix) {
			// 排除掉merge-finish和merge-hint文件
			dataFileEntries = append(dataFileEntries, dirEntry)
		}
	}

	for i, dataFileEntry := range dataFileEntries {

		// 获取文件编号
		fileNum, err := util.GetFileNumByFilename(dataFileEntry.Name())
		if err != nil {
			return err
		}

		// 初始化DiskFile
		df, err := diskFile.NewDiskFile(dataFileDir, dataFileEntry.Name())
		if err != nil {
			return err
		}

		// 将DiskFile加入到DataFileList中
		if i == len(dataFileEntries)-1 {
			db.normalDataFiles.ActiveFile = df
		} else {
			err = db.normalDataFiles.TransferFileToOlder(fileNum, df)
			if err != nil {
				return err
			}
		}

		// 需要读取文件索引
		if fileNum >= nonMergeFileId {

			// 获取当前DiskFile中的所有数据以及对应的位置信息
			dataList, posList, err := df.ReadAllPosition()
			if err != nil {
				panic(err)
			}

			// 修改内存索引
			for k, pos := range posList {
				if dataList[k].DataType == data.Delete {
					db.DeletePos(dataList[k].Key)
				} else {
					db.PutPos(dataList[k].Key, pos)
				}
			}
		}
	}

	return nil
}

/*	getNonMergeFileNum
 *  @Description: 获取最小的未合并的FileId
 *  @param dir: nonMergeFile所在的目录
 *  @return int64: 未参与merge的FileId
 *  @return error: 错误信息
 */
func (db *DB) getNonMergeFileNum(dir string) (int64, error) {
	f, err := os.OpenFile(dir+"/"+db.option.MergeFinishFileName, os.O_RDONLY, 0777)
	if err != nil {
		return 0, err
	}
	buf, err := io.ReadAll(f)
	if err != nil {
		return 0, err
	}
	fileId, err := strconv.ParseInt(string(buf), 10, 64)
	if err != nil {
		return 0, err
	}
	return fileId, nil
}

/*	loadIndexFromHintFile
 *  @Description: 从hintFile文件中加载索引信息
 *  @param dir: hintFile所在的目录
 *  @return error: 错误信息
 */
func (db *DB) loadIndexFromHintFile(dir string) error {
	// hintFile记录merge后的数据的索引信息，用于重放索引，这样就不必再扫描整个dataFiles，节省时间
	hintFile, err := os.OpenFile(filepath.Join(dir, db.option.MergeHintFilename), os.O_RDONLY, 0777)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	buf, err := io.ReadAll(hintFile)
	if err != nil {
		return err
	}

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

		dataHeader, n := data.DecodeHeader(buf[offset:])
		offset += n

		keyFrom := offset
		//key := make([]byte, dataHeader.KeySize)
		//copy(key, buf[offset:])
		offset += dataHeader.KeySize

		valueFrom := offset
		//value := make([]byte, dataHeader.ValSize)
		//copy(value, buf[offset:])
		offset += dataHeader.ValSize
		valueTo := offset

		crc := crc32.ChecksumIEEE(buf[dataHeaderFrom+5 : valueTo])
		//crc := crc32.ChecksumIEEE(append(headerBuf[5:n], append(key, value...)...))

		if crc != dataHeader.Crc {
			return errors.ErrCrcNotValid
		}

		key := buf[keyFrom:valueFrom]
		pos := position.DecodePosition(buf[valueFrom:valueTo])

		db.PutPos(key, pos)
	}

	return nil
}

/*	Close
 *  @Description: 关闭DB
 */
func (db *DB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return
	}
	db.closed = true

	if db.memTable != nil { // 同步等待，持久化MemTable的数据
		if db.memTable.Size() > 0 {
			db.transferMumTableToImmutableSync()
		}
	}

	if db.flushImmutableMemTableChan != nil { // flush完成后，才会归还flushImmutableMemTableChan的临界资源
		<-db.flushImmutableMemTableChan
	}

	if db.ProvWriteChan != nil {
		for len(db.ProvWriteChan) > 0 { // 等待后台协程将该通道的prov全部flush

		}
	}

	for {
		if db.immutableMemTable == nil {

			if db.normalDataFiles != nil {
				db.normalDataFiles.Close()
			}

			if db.blockDataFiles != nil {
				db.blockDataFiles.Close()
			}

			if db.blockPartFiles != nil {
				db.blockPartFiles.Close()
			}

			if db.ProvFileMap != nil {
				db.ProvFileMap.Close()
			}

			if db.wal != nil {
				db.wal.Close()
			}

			if db.genImmutableMemTableChan != nil {
				close(db.genImmutableMemTableChan)
			}

			if db.flushImmutableMemTableChan != nil {
				close(db.flushImmutableMemTableChan)
			}

			db.memIndex = nil
			db.memTable = nil
			return
		}
	}
}

/*	Merge
 *  @Description: 压缩合并数据，去除过期数据
 *  @return error: 错误信息
 */
func (db *DB) Merge() error {
	db.mu.Lock()
	if db.merging { // 如果db正在merge，则直接返回
		db.mu.Unlock()
		return nil
	}
	db.merging = true
	db.mu.Unlock()

	if db.memTable != nil {
		if db.memTable.Size() > 0 {
			db.transferMumTableToImmutableSync()
		}
	}

	if db.flushImmutableMemTableChan != nil {
		<-db.flushImmutableMemTableChan
		db.flushImmutableMemTableChan <- struct{}{}
	}

	defer func() {
		db.merging = false
	}()

	_ = os.RemoveAll(db.option.MergeDir)

	// 将活跃数据文件转化为旧数据文件
	logrus.Info("将活跃数据文件转化为旧数据文件")
	db.normalDataFiles.Mu.Lock()
	if db.normalDataFiles.ActiveFile != nil {
		err := db.normalDataFiles.TransferFileToOlder(db.normalDataFiles.FileNumber, db.normalDataFiles.ActiveFile)
		if err != nil {
			return err
		}
		//db.normalDataFiles.OlderFileMap[db.normalDataFiles.FileNumber] = db.normalDataFiles.ActiveFile
		f, err := db.normalDataFiles.CreateNewActiveFile()
		db.normalDataFiles.ActiveFile = f
		if err != nil {
			db.normalDataFiles.Mu.Unlock()
			return err
		}
	}

	nonMergeFileId := db.normalDataFiles.FileNumber

	var fileIds []int64

	// 获取所有旧数据文件的文件编号
	logrus.Info("获取所有旧数据文件的文件编号")
	for fileId, _ := range db.normalDataFiles.OlderFileMap {
		fileIds = append(fileIds, fileId)
	}

	sort.Slice(fileIds, func(i, j int) bool {
		return fileIds[i] < fileIds[j]
	})

	// 如果mergeDir不存在，则创建该目录
	if _, err := os.Stat(db.option.MergeDir); err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(db.option.MergeDir, 0777)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	// 用于merge的临时DB
	tempDB := &DB{
		normalDataFiles: diskFile.NewDataFileList(
			db.option.MergeDir,
			option.DataFileSuffix,
			db.option.DiskFileSize,
			false,
			0,
			0,
		),
		option: option.DefaultOption(),
	}

	// hintFile记录merge后的数据的索引信息，用于重放索引，这样就不必再扫描整个dataFiles，节省时间
	hintFile, err := os.OpenFile(filepath.Join(db.option.MergeDir, db.option.MergeHintFilename), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		return err
	}

	// 扫描所有旧数据文件，去除过期数据
	logrus.Info("扫描所有旧数据文件，去除过期数据")
	for _, fileId := range fileIds {
		file := db.normalDataFiles.OlderFileMap[fileId]
		var offset int
		for {
			d, n, err := file.ReadData(offset)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					return err
				}
			}
			indexPos := db.GetPos(d.Key)
			if d.DataType != data.Delete && indexPos != nil && indexPos.Filename == file.Filename && indexPos.Offset == offset { // 有效的数据

				// 将有效数据写入mergeDB的dataFile
				pos, err := tempDB.normalDataFiles.WriteData(d) // 新的pos
				if err != nil {
					return err
				}

				posData := &data.Data{
					Key:   d.Key,
					Value: pos.Encode(),
				}

				// 将pos作为value打包成data写入hintFile
				_, err = hintFile.Write(posData.Encode())
				if err != nil {
					return err
				}
			}
			offset += n
		}
	}

	db.normalDataFiles.Mu.Unlock()

	mergeFinishFile, err := os.OpenFile(filepath.Join(db.option.MergeDir, db.option.MergeFinishFileName), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		return err
	}
	_, err = mergeFinishFile.Write([]byte(strconv.Itoa(int(nonMergeFileId))))
	if err != nil {
		return err
	}
	tempDB.Close()
	return nil
}
