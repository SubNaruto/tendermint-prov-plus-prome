package main

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	"github.com/tendermint/tendermint/my_test/util"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
	hp "github.com/tendermint/tendermint/rpc/client/http"
	"time"
)

var cli *hp.HTTP
var r *redis.Client

func init() {
	r = redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		Password:     "",
		MinIdleConns: 100,
		IdleTimeout:  -1,
	})
	_, err := r.Ping().Result()
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v\n", r.Options())

	cli, err = hp.New("http://127.0.0.1:26657", "/websocket")
	if err != nil {
		panic(err)
	}
}

func main() {
	engine := gin.Default()

	engine.GET("/prov", func(c *gin.Context) {
		entityId := c.Query("entityId")

		provKey := util.GenProvKey(entityId)       // 溯源数据的key
		provResKey := util.GenProvResKey(entityId) // 溯源结果的key

		provRes, err := r.Get(provResKey).Result()
		if err != nil && err != redis.Nil {
			c.JSON(200, gin.H{
				"code":    1,
				"message": err.Error(),
			})
			return
		}
		if provRes == "true" { // 溯源结果ok
			c.JSON(200, gin.H{
				"code": 0,
			})
			return
		}

		buf, err := r.Get(provKey).Result() // 获取溯源数据
		if err != nil && err != redis.Nil {
			c.JSON(200, gin.H{
				"code":    1,
				"message": err.Error(),
			})
			return
		}

		if buf != "" { // 溯源数据存在
			var provDataList tmproto.ProvDataList
			err = proto.Unmarshal([]byte(buf), &provDataList) // 反序列化
			if err != nil {
				c.JSON(200, gin.H{
					"code":    1,
					"message": err.Error(),
				})
				return
			}

			err = util.VerifyProvDataList(&provDataList) // 进行验证
			if err != nil {
				c.JSON(200, gin.H{
					"code":    1,
					"message": err.Error(),
				})
				return
			}
		} else { // 溯源数据不存在
			res, err := cli.ProvQuery(context.TODO(), entityId) // 从tendermint中获取溯源数据
			if err != nil {
				c.JSON(200, gin.H{
					"code":    1,
					"message": err.Error(),
				})
				return
			}
			err = util.VerifyProvDataList(res.ProvDataList) // 验证溯源数据
			if err != nil {
				c.JSON(200, gin.H{
					"code":    1,
					"message": err.Error(),
				})
				return
			}

			pb, err := proto.Marshal(res.ProvDataList) // 序列化
			if err != nil {
				c.JSON(200, gin.H{
					"code":    1,
					"message": err.Error(),
				})
				return
			}

			r.Set(provKey, pb, time.Minute) // 存入缓存
		}

		r.Set(provResKey, "true", time.Minute) // 验证正确则记入溯源结果

		c.JSON(200, gin.H{
			"code": 0,
		})
	})

	if err := engine.Run(":9090"); err != nil {
		panic(err)
	}
}
