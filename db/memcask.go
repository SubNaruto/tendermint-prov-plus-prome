package db

import (
	"fmt"
	mc "github.com/tendermint/tendermint/memcask"
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/option"
	tmdb "github.com/tendermint/tm-db"
)

type MemCaskDB struct {
	db *mc.DB
}

var _ DB = (*MemCaskDB)(nil)

func NewMemCaskDB(dataDir, mergeDir string, enableProv bool) (*MemCaskDB, error) {
	opt := option.DefaultOption()

	opt.DataDir = dataDir
	opt.MergeDir = mergeDir
	opt.WalFilePath = dataDir + "/wal.log"

	//opt.MemTableSize = memSize
	//opt.DiskFileSize = dataFileSize

	opt.EnableProv = enableProv

	db := mc.NewDBWithOption(opt)

	return &MemCaskDB{
		db: db,
	}, nil
}

func (db *MemCaskDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key empty error")
	}
	val := db.db.Get(key, data.BlockData)
	return val, nil
}

func (db *MemCaskDB) GetBlockParts(height, total int) ([][]byte, error) {
	return db.db.GetBlockParts(height, total)
}

func (db *MemCaskDB) Has(key []byte) (bool, error) {
	bytes, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return bytes != nil, nil
}

func (db *MemCaskDB) Set(key []byte, value []byte) error {
	db.db.Put(key, value, data.BlockData)
	return nil
}

func (db *MemCaskDB) SetBlockPart(key []byte, value []byte) {
	db.db.Put(key, value, data.BlockPart)
}

func (db *MemCaskDB) GetBlockPart(height, index int) ([]byte, error) {
	return db.db.GetBlockPart(height, index)
}

func (db *MemCaskDB) SetSync(key []byte, value []byte) error {
	return nil
}

func (db *MemCaskDB) Delete(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key empty error")
	}
	db.db.Delete(key)
	return nil
}

func (db *MemCaskDB) DeleteSync(key []byte) error {
	return db.Delete(key)
}

func (db *MemCaskDB) Close() error {
	db.db.Close()
	return nil
}

func (db *MemCaskDB) Print() error {
	return nil
}

func (db *MemCaskDB) Stats() map[string]string {
	return nil
}

func (db *MemCaskDB) NewBatch() tmdb.Batch {
	return nil
}

func (db *MemCaskDB) Iterator(start, end []byte) (tmdb.Iterator, error) {
	return nil, nil
}

func (db *MemCaskDB) ReverseIterator(start, end []byte) (tmdb.Iterator, error) {
	return nil, nil
}

func (db *MemCaskDB) SetProvData(entityId []byte, val []byte) {
	db.db.Put(entityId, val, data.ProvData)
}

func (db *MemCaskDB) GetProvData(entityId string) ([][]byte, error) {
	return db.db.GetProvData(entityId)
}
