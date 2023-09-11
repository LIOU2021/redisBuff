package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/LIOU2021/redisBuff"
	redis "github.com/redis/go-redis/v9"
)

var rdb = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
})

func init() {
	redisBuff.InitRedisClient(rdb)
}

func main() {
	c1 := &redisBuff.Config{
		SendBuff:       100,
		MsgBatch:       5,
		RunnerInterval: 5 * time.Second,
		CacheName:      "message-test",
		LockDuration:   3 * time.Second,
		ClearMsgFunc: func(msg []string) {
			fmt.Println(msg)
		},
	}

	s1 := redisBuff.New(c1)
	closeRunner := s1.SendMsgRunner()

	go testAdd(s1)

	select {
	case <-time.After(5 * time.Second):
		closeRunner <- true
	}
}

func testAdd(b *redisBuff.Buff) {
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(index int, wgg *sync.WaitGroup) {
			b.Add(index)
			wgg.Done()
		}(i, &wg)
	}
	wg.Wait()

}
