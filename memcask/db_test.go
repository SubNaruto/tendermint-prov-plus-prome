package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tendermint/tendermint/memcask/data"
	"github.com/tendermint/tendermint/memcask/option"
	"golang.org/x/exp/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

type OperationType byte

const (
	Insert OperationType = iota
	Delete
	Update
	Select
)

type Record struct {
	Hash  string `json:"hash"`
	Value RecordBody
}

type RecordBody struct {
	OpType    OperationType `json:"op_type"`
	Timestamp string        `json:"timestamp"`
	UserID    string        `json:"user_id"`
	EntityID  string        `json:"entity_id"`
	Height    int           `json:"height"`
	Num       int           `json:"num"`
}

func TestDB1(t *testing.T) {
	db := NewDBWithOption(option.DefaultOption())

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		// PUT
		for i := 0; i < 100_0000; i++ {
			s := strconv.Itoa(i)
			db.Put([]byte(s), []byte(s), data.Normal)
		}
	}()

	wg.Wait()

	//time.Sleep(time.Second * 2)

	// GET
	for i := 0; i < 100_0000; i++ {
		s := strconv.Itoa(i)
		val := db.Get([]byte(s), data.Normal)
		if !assert.Equal(t, s, string(val)) {
			os.Exit(1)
		}
	}
}

func TestDB2(t *testing.T) {
	db := NewDBWithOption(option.DefaultOption())

	for i := 0; i < 100_0000; i++ {
		s := strconv.Itoa(i)

		db.Put([]byte(s), []byte(s), data.Normal)

		if i > 1000 {
			for j := 0; j < 10; j++ {
				randKey := strconv.Itoa(rand.Intn(i))
				val := db.Get([]byte(randKey), data.Normal)
				if !assert.Equal(t, randKey, string(val)) {
					os.Exit(1)
				}
			}
		}

		db.Put([]byte("name"), nil, data.Normal)
		val := db.Get([]byte("name"), data.Normal)
		if !assert.Equal(t, "", string(val)) {
			os.Exit(1)
		}
	}
}

func TestDB3(t *testing.T) {
	db := NewDBWithOption(option.DefaultOption())

	for i := 0; i < 500_000; i++ {
		var dt data.DataType
		if i%3 == 0 {
			dt = data.Normal
		} else if i%3 == 1 {
			dt = data.BlockData
		} else {
			dt = data.BlockPart
		}
		s := strconv.Itoa(i)

		db.Put([]byte(s), []byte(s), dt)
	}

	db.Close()

	db = NewDBWithOption(option.DefaultOption())
	for i := 0; i < 500_000; i++ {
		var dt data.DataType
		if i%3 == 0 {
			dt = data.Normal
		} else if i%3 == 1 {
			dt = data.BlockData
		} else {
			dt = data.BlockPart
		}

		if dt != data.BlockPart {
			s := strconv.Itoa(i)
			val := db.Get([]byte(s), dt)
			if !assert.Equal(t, s, string(val)) {
				fmt.Println(s)
				os.Exit(1)
			}
		}
	}
	db.Close()
}

func TestDB4(t *testing.T) {
	db := NewDBWithOption(option.DefaultOption())

	for i := 0; i < 100_0000; i++ {
		s := strconv.Itoa(i)

		db.Put([]byte(s), []byte(s), data.Normal)
	}

	db.Close()
}

func TestDB5(t *testing.T) {
	db := NewDBWithOption(option.DefaultOption())

	// PUT
	for i := 0; i < 100_0000; i++ {
		s := strconv.Itoa(i)
		db.Put([]byte(s), []byte(s), data.Normal)
		if i > 0 && i%20_0000 == 0 {
			st := strconv.Itoa(i)
			val := db.Get([]byte(st), data.Normal)
			if !assert.Equal(t, st, string(val)) {
				fmt.Println("testdb5", st, string(val))
				os.Exit(1)
			}
		}
	}
}

func TestGetBlockParts(t *testing.T) {
	db := NewDBWithOption(option.DefaultOption())
	defer db.Close()

	const partNum = 50

	for height := 100; height <= 200; height++ {

		parts := make([]byte, 0, partNum*64*option.KB)

		fmt.Println(height)

		// PUT
		for i := 0; i < partNum; i++ {
			part := make([]byte, 0, 64*option.KB)
			for len(part) <= 64*option.KB {
				part = append(part, time.Now().String()...)
			}
			parts = append(parts, part...)
			db.Put([]byte(fmt.Sprintf("P:%v:%v", height, i)), part, data.BlockPart)
		}

		// GET
		val, err := db.GetBlockParts(height, partNum)
		assert.Nil(t, err)
		res := bytes.Join(val, []byte(""))
		if !assert.Equal(t, string(parts), string(res)) {
			fmt.Println(height)
			os.Exit(1)
		}
	}
}

func TestMerge(t *testing.T) {
	db := NewDBWithOption(option.DefaultOption())
	defer db.Close()

	// PUT
	for i := 0; i < 10_0000; i++ {
		s := strconv.Itoa(i)
		db.Put([]byte(s), []byte(s), data.Normal)
	}

	// DELETE
	for i := 0; i < 10_0000; i++ {
		if i&15 == 0 {
			s := strconv.Itoa(i)
			db.Delete([]byte(s))
		}
	}

	err := db.Merge()
	assert.Nil(t, err)
}

func TestOpenDB(t *testing.T) {
	db := NewDBWithOption(option.DefaultOption())
	defer db.Close()

	// Get
	for i := 0; i < 10_0000; i++ {
		s := strconv.Itoa(i)
		val := db.Get([]byte(s), data.Normal)
		if i&15 == 0 { // deleted
			if !assert.Nil(t, val) {
				fmt.Println(s)
				os.Exit(1)
			}
		} else { // not deleted
			if !assert.Equal(t, s, string(val)) {
				fmt.Println(s)
				os.Exit(1)
			}
		}
	}
}

func TestLoadData(t *testing.T) {
	db := NewDBWithOption(option.DefaultOption())

	// PUT
	for i := 0; i < 100_0000; i++ {
		s := strconv.Itoa(i)
		db.Put([]byte(s), []byte(s), data.BlockData)
	}

	db.Close()

	db = NewDBWithOption(option.DefaultOption())
	defer db.Close()

	// Get
	for i := 0; i < 100_0000; i++ {
		s := strconv.Itoa(i)
		val := db.Get([]byte(s), data.BlockData)
		if !assert.Equal(t, s, string(val)) {
			fmt.Println(s)
			os.Exit(1)
		}
	}
}

func TestGetBlockParts2(t *testing.T) {
	db := NewDBWithOption(option.DefaultOption())
	defer db.Close()

	const total = 100
	const h = 1000
	var parts []byte

	// PUT
	for i := 0; i < total; i++ {
		s := time.Now().String()
		parts = append(parts, s...)
		db.Put([]byte(fmt.Sprintf("P:%v:%v", h, i)), []byte(s), data.BlockPart)
	}

	for i := 0; i < 10_0000; i++ {
		// GET
		res, err := db.GetBlockParts(h, total)
		assert.Nil(t, err)
		parts = bytes.Join(res, []byte(""))
		if !assert.Equal(t, string(parts), string(parts)) {
			fmt.Println(h)
			os.Exit(1)
		}
	}
}

func TestWAL(t *testing.T) {
	opt := option.DefaultOption()
	opt.WalDisabled = false
	db := NewDBWithOption(option.DefaultOption())

	// PUT
	for i := 0; i < 100_0000; i++ {
		s := strconv.Itoa(i)
		db.Put([]byte(s), []byte(s), data.Normal)
		if i%20_0000 == 0 {
			time.Sleep(time.Millisecond * 200)
		}
	}

	db.Close()
}

func TestProv(t *testing.T) {
	opt := option.DefaultOption()
	opt.EnableProv = true
	db := NewDBWithOption(opt)
	assert.NotNil(t, db)
	defer db.Close()

	for entityID := 1; entityID <= 1000; entityID++ {
		s := []byte(strconv.Itoa(entityID))
		for i := 0; i <= 1000; i++ {
			db.Put(s, []byte(strconv.Itoa(i)), data.ProvData)
		}
	}

	for entityID := 1; entityID <= 1000; entityID++ {
		s := strconv.Itoa(entityID)
		values, err := db.GetProvData(s)
		assert.Nil(t, err)
		if err != nil {
			fmt.Println(entityID)
			panic(err)
		}
		for i, v := range values {
			assert.Equal(t, strconv.Itoa(i), string(v))
		}
	}
}

func TestProv2(t *testing.T) {
	opt := option.DefaultOption()
	opt.EnableProv = true
	db := NewDBWithOption(opt)

	for entityID := 1; entityID <= 100; entityID++ {
		for height := 1; height <= 100; height++ {
			for num := 1; num <= 100; num++ {
				s := strconv.Itoa(entityID)
				rb := &RecordBody{
					OpType:    Update,
					Timestamp: time.Now().Format("2006-01-02 15"),
					UserID:    "001",
					EntityID:  s,
					Height:    height,
					Num:       num,
				}
				js, err := json.Marshal(rb)
				assert.Nil(t, err)
				db.Put([]byte(s), js, data.ProvData)
			}
		}
	}

	db.Close()

	db = NewDBWithOption(opt)
	defer db.Close()
	for entityID := 1; entityID <= 100; entityID++ {
		str := strconv.Itoa(entityID)
		values, err := db.GetProvData(str)
		assert.Nil(t, err)
		if err != nil {
			panic(err)
		}
		i := 0
		for height := 1; height <= 100; height++ {
			for num := 1; num <= 100; num++ {
				var rb RecordBody
				err = json.Unmarshal(values[i], &rb)
				assert.Nil(t, err)
				assert.Equal(t, Update, rb.OpType)
				assert.Equal(t, time.Now().Format("2006-01-02 15"), rb.Timestamp)
				assert.Equal(t, "001", rb.UserID)
				assert.Equal(t, str, rb.EntityID)
				assert.Equal(t, height, rb.Height)
				assert.Equal(t, num, rb.Num)
				i++
			}
		}
	}
}
