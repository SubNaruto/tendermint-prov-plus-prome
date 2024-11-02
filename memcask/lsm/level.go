package lsm

import (
	"github.com/tendermint/tendermint/memcask/diskFile"
	"sync"
)

type Level struct {
	Sstables  []*diskFile.DiskFile
	mu        sync.RWMutex
	totalSize int
}
