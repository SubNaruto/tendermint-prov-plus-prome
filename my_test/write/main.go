package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	"github.com/tendermint/tendermint/my_test/option"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
	"github.com/tendermint/tendermint/rpc/client/http"
	"math/rand"
	"strconv"
	"time"
)

func initRedis() (*redis.Client, error) {
	r := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
	})
	_, err := r.Ping().Result()
	if err != nil {
		return nil, err
	}
	return r, nil
}

func main() {
	rand.Seed(time.Now().Unix())

	var height int
	flag.IntVar(&height, "h", 0, "the number of blocks")
	flag.Parse()
	if height == 0 {
		fmt.Println("height must be greater than zero..")
		return
	}

	r, err := initRedis()
	if err != nil {
		panic(err)
	}
	defer r.Close()

	//cli, err := http.New("http://10.46.120.20:26657", "/websocket")
	cli, err := http.New("http://localhost:26657", "/websocket")
	if err != nil {
		panic(err)
	}

	for h := 1; h <= height; h++ {

		for i := 0; i < option.TxNum; i++ {
			entityId := rand.Intn(option.EntityNum)

			strEntityId := strconv.Itoa(entityId)
			version, err := r.Incr(strEntityId).Result()
			if err != nil {
				panic(err)
			}

			tx := &tmproto.Tx{
				Key: fmt.Sprintf("%s@%d", strEntityId, version),
				TxBody: &tmproto.TxBody{
					OpType:    tmproto.OperationType_SELECT,
					Timestamp: time.Now().Format("2006-01-02 15:04:05"),
					UserId:    "001",
					EntityId:  strEntityId,
					Version:   uint32(version),
				},
			}

			pb, err := proto.Marshal(tx)
			if err != nil {
				panic(err)
			}

			_, err = cli.BroadcastTxAsync(context.Background(), pb)
			if err != nil {
				panic(err)
			}
		}

		time.Sleep(time.Millisecond * 100)
	}
}
