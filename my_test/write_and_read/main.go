package main

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
	"github.com/tendermint/tendermint/rpc/client/http"
	"sync"
	"time"
)

func main() {
	cli, err := http.New("http://127.0.0.1:26657", "/websocket")
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	const entityId = "001"

	wg.Add(1)
	go func() {
		defer wg.Done()
		var version uint32

		for i := 0; i < 10; i++ {
			tx := &tmproto.Tx{
				Key: fmt.Sprintf("%s@%d", entityId, version),
				TxBody: &tmproto.TxBody{
					OpType:    tmproto.OperationType_SELECT,
					Timestamp: time.Now().Format("2006-01-02 15:04:05"),
					UserId:    "001",
					EntityId:  entityId,
					Version:   version,
				},
			}
			version++

			pb, err := proto.Marshal(tx)
			if err != nil {
				panic(err)
			}

			_, err = cli.BroadcastTxAsync(context.Background(), pb)
			if err != nil {
				panic(err)
			}
		}
	}()

	wg.Wait()
	time.Sleep(time.Second * 10)

	r, err := cli.ProvQuery(context.Background(), entityId)
	if err != nil {
		panic(err)
	}

	for i := 0; i < len(r.Heights); i++ {
		fmt.Print(r.Heights[i], ":", r.Versions[i], "   ")
	}
	fmt.Println("success!	")
}
