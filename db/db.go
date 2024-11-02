package db

import (
	tmdb "github.com/tendermint/tm-db"
)

type DB interface {
	// Get 查询区块的非分片数据(如blockMeta等)
	Get([]byte) ([]byte, error)

	Has(key []byte) (bool, error)

	// Set 存储区块的非分片数据
	Set([]byte, []byte) error

	SetSync([]byte, []byte) error

	Delete([]byte) error

	DeleteSync([]byte) error

	Close() error

	Print() error

	Stats() map[string]string

	// GetBlockParts 输入height和total，获取所有分片
	GetBlockParts(int, int) ([][]byte, error)

	// SetBlockPart 输入key和part，保存一个分片
	SetBlockPart([]byte, []byte)

	// GetBlockPart 输入height和index，获取一个分片
	GetBlockPart(int, int) ([]byte, error)

	// SetProvData 存储溯源数据
	SetProvData([]byte, []byte)

	// GetProvData 根据entityId获取所有溯源数据
	GetProvData(string) ([][]byte, error)

	NewBatch() tmdb.Batch

	Iterator(start, end []byte) (tmdb.Iterator, error)

	ReverseIterator(start, end []byte) (tmdb.Iterator, error)
}
