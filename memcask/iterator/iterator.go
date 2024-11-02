package iterator

import "github.com/tendermint/tendermint/memcask/data"

type Iterator interface {
	Value() *data.Data
	Next()
	Rollback()
	Valid() bool
}
