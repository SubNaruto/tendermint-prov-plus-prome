package main

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tendermint/tendermint/my_test/util"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
	hp "github.com/tendermint/tendermint/rpc/client/http"
	"sync"
	"time"
)

// Prometheus Metrics
var PcdQrs = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "namespace",
	Subsystem: "state",
	Name:      "num_qrs",
	Help:      "Number of processed queries.",
})

var TotalQrs = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "namespace",
	Subsystem: "state",
	Name:      "total_qrs",
	Help:      "Total number of queries.",
})

var AvgQps = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "namespace",
	Subsystem: "state",
	Name:      "latest_average_qps",
	Help:      "The latest second qps.",
})

var cli *hp.HTTP
var r *redis.Client
var mu sync.Mutex // Mutex to protect access to the counter
var requestCount int

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
	// Register Prometheus metrics
	prometheus.MustRegister(PcdQrs)
	prometheus.MustRegister(TotalQrs)
	prometheus.MustRegister(AvgQps)
}

func main() {
	PcdQrs.Set(float64(0.0))
	TotalQrs.Set(float64(0.0))
	AvgQps.Set(float64(0.0))
	// Initialize ticker for QPS calculation every second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Start the QPS calculation in a goroutine
	go func() {
		for {
			select {
			case <-ticker.C:
				// Lock mutex to update QPS
				mu.Lock()

				// Calculate QPS based on the request count in the last second
				qps := float64(requestCount)
				AvgQps.Set(qps) // Set the latest QPS value

				// Reset request count for the next second
				requestCount = 0

				// Unlock mutex
				mu.Unlock()
			}
		}
	}()

	engine := gin.Default()
	// Metrics endpoint for Prometheus scraping
	engine.GET("/metrics", gin.WrapH(promhttp.Handler()))

	engine.GET("/prov", func(c *gin.Context) {
		startTime := time.Now() // Start time for QPS calculation
		entityId := c.Query("entityId")

		provKey := util.GenProvKey(entityId)       // 溯源数据的key
		provResKey := util.GenProvResKey(entityId) // 溯源结果的key
		TotalQrs.Add(1)

		provRes, err := r.Get(provResKey).Result()
		if err != nil && err != redis.Nil {
			c.JSON(200, gin.H{
				"code":    1,
				"message": err.Error(),
			})
			return
		}
		if provRes == "true" { // 溯源结果ok
			//更新PcdQrs
			PcdQrs.Add(1)

			// Increment the request count for QPS calculation
			mu.Lock()
			requestCount++
			mu.Unlock()

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
		// Update PcdQrs as a processed query
		PcdQrs.Add(1)
		// Increment the request count for QPS calculation
		mu.Lock()
		requestCount++
		mu.Unlock()
		// Calculate QPS and update AvgQps
		elapsed := time.Since(startTime).Seconds() // Calculate the time taken for the query
		qps := 1 / elapsed                         // Calculate the QPS
		AvgQps.Set(qps)                            // Set the latest QPS

		c.JSON(200, gin.H{
			"code": 0,
		})
	})

	if err := engine.Run(":26661"); err != nil {
		panic(err)
	}
}
