package lsm

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/diskFile"
	"github.com/tendermint/tendermint/memcask/index"
	"github.com/tendermint/tendermint/memcask/memtable"
	"github.com/tendermint/tendermint/memcask/position"
	"github.com/tendermint/tendermint/memcask/util"
	"os"
	"path/filepath"
)

type LevelTree struct {
	levels         []*Level
	dir            string
	suffix         string
	compactChannel chan int
}

func (lvt *LevelTree) Close() {
	for _, level := range lvt.levels {
		func() {
			level.mu.Lock()
			defer level.mu.Unlock()
			for _, sstable := range level.Sstables {
				sstable.Close()
			}
		}()
	}
}

func NewLevelTree(levelLen int, dir, suffix string, size int) *LevelTree {
	levels := make([]*Level, levelLen)
	for i := 0; i < levelLen; i++ {
		levels[i] = new(Level)
	}
	lvt := &LevelTree{
		dir:            dir,
		suffix:         suffix,
		compactChannel: make(chan int, size),
		levels:         levels,
	}
	return lvt
}

// AddSSTable 将sstable文件加入到level的末尾
func (lvt *LevelTree) AddSSTable(sstable *diskFile.DiskFile, level int) error {
	logrus.Infof("将sstable加入第%d层", level)

	lvt.levels[level].mu.Lock()
	defer lvt.levels[level].mu.Unlock()

	lvt.levels[level].Sstables = append(lvt.levels[level].Sstables, sstable)

	size, err := sstable.GetFileSize()
	if err != nil {
		return err
	}
	lvt.levels[level].totalSize += size

	// level超载
	if lvt.levels[level].totalSize > util.CalLevelSize(level) {
		logrus.Infof("第%d层超载，传递信号", level)
		lvt.compactChannel <- level
	}

	return nil
}

// WriteDataList 将immutableMemTable的dataList写入levelTree的第一层(level 0)
func (lvt *LevelTree) WriteDataList(memIndex index.MemoryIndex, lsmDataList []*data.Data, level int) error {
	filename := fmt.Sprintf("level%d-%d%s", level, len(lvt.levels[level].Sstables), lvt.suffix)
	sstable, err := diskFile.NewDiskFile(lvt.dir, filename)
	if err != nil {
		return err
	}

	var offset int
	for _, lsmData := range lsmDataList {
		// 将data写入sstable中
		n, err := sstable.WriteData(lsmData)
		if err != nil {
			return err
		}

		if lsmData.DataType != data.Delete {
			pos := &position.Position{
				Filename: filename,
				Offset:   offset,
			}
			memIndex.Put(lsmData.Key, pos)
			offset += n
		} else {
			memIndex.Delete(lsmData.Key)
		}
	}

	// 将sstable加入到level0的末尾
	err = lvt.AddSSTable(sstable, level)
	if err != nil {
		return err
	}

	return nil
}

func (lvt *LevelTree) readAndRemoveLevel(temp memtable.MemTable, level int, memIndex index.MemoryIndex) {
	lvt.levels[level].mu.Lock()
	defer lvt.levels[level].mu.Unlock()

	// 读取该level层所有的sstable
	for i := 0; i < len(lvt.levels[level].Sstables); i++ {
		sstable := lvt.levels[level].Sstables[i]

		// 读取该sstable的所有data（一次IO）
		dataList0, err := sstable.ReadAll()
		if err != nil {
			panic(err)
		}

		for _, d := range dataList0 {
			memIndex.Delete(d.Key)
			temp.Put(d)
		}

		_ = os.Remove(filepath.Join(lvt.dir, sstable.Filename))
	}

	lvt.levels[level].totalSize = 0
	lvt.levels[level].Sstables = nil

	logrus.Infof("第%d层已经清空...", level)
}

func (lvt *LevelTree) CheckLevelCompaction(memIndex index.MemoryIndex) {
	for level := range lvt.compactChannel {
		logrus.Infof("收到超载信号，第%d层超载，需要进行压缩...", level)

		// 新sstable文件的名称
		filename := fmt.Sprintf("level%d-%d%s", level+1, len(lvt.levels[level].Sstables), lvt.suffix)

		// 用于存放merge后的数据的临时memtable
		temp := memtable.NewBTreeMemTable(32)

		// 创建新的sstable文件
		newSstable, err := diskFile.NewDiskFile(lvt.dir, filename)
		if err != nil {
			panic(err)
		}

		// 删除第level层并将数据放入临时memtable中
		lvt.readAndRemoveLevel(temp, level, memIndex)

		// insert k0  --  delete k0
		// insert k0  --  insert k0 ...
		// delete k0  --  insert k0
		// 将merge后的数据写入到新的sstable并更新内存索引
		var offset int
		iter := temp.GetIterator()
		for iter.Valid() {
			elem := iter.Value()
			if elem.DataType != data.Delete {
				n, err := newSstable.WriteData(elem)
				if err != nil {
					panic(err)
				}
				memIndex.Put(elem.Key, &position.Position{
					Filename: newSstable.Filename,
					Offset:   offset,
				})
				offset += n
			}
			iter.Next()
		}

		// 将新的sstable加到下一层的level
		err = lvt.AddSSTable(newSstable, level+1)
		if err != nil {
			panic(err)
		}
	}
}
