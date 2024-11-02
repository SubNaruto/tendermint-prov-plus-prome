package main

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tendermint/tendermint/my_test/option"
	"github.com/tendermint/tendermint/rpc/client/http"
	"os"
	"time"
)

func main() {
	dir := "./log"

	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		err = os.Mkdir(dir, 0777)
		if err != nil {
			panic(err)
		}
	}

	file, err := os.OpenFile(dir+"/log.txt", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	logrus.SetOutput(file)

	cli, err := http.New("http://127.0.0.1:26657", "/websocket")
	if err != nil {
		panic(err)
	}

	start := time.Now()

	res, err := cli.ProvQuery(context.Background(), option.TargetEntityId)
	if err != nil {
		panic(err)
	}

	for i := 0; i < len(res.Heights); i++ {
		fmt.Print(res.Heights[i], ":", res.Versions[i], "   ")
	}

	logrus.Info("success! ths cost is ", time.Since(start).Microseconds(), "us")
}
