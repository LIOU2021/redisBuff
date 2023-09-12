package main

import (
	"flag"
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

var flagvar int

func init() {
	redisBuff.InitRedisClient(rdb)
	flag.IntVar(&flagvar, "flag", 999, "help message for flag")
}

func main() {
	flag.Parse()

	c1 := &redisBuff.Config{
		SendBuff:       100,
		MsgBatch:       5,
		RunnerInterval: 5 * time.Second,
		CacheName:      "message-test",
		LockDuration:   3 * time.Second,
		ClearMsgFunc: func(msg []string) {
			fmt.Println(msg)
		},
		Debug: true,
	}

	s1 := redisBuff.New(c1)
	closeRunner := s1.SendMsgRunner()

	go testAdd(s1)
	go loopAdd(s1)

	c2 := *c1
	c2.CacheName += "-2"
	s2 := redisBuff.New(&c2)
	closeRunner2 := s2.SendMsgRunner()

	go testAdd(s2)
	go loopAdd(s2)

	select {
	case <-time.After(10 * time.Second):
		fmt.Println("send closeRunner signal")
		closeRunner <- true
		closeRunner2 <- true
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

func loopAdd(b *redisBuff.Buff) {
	for {
		b.Add(flagvar)
		time.Sleep(200 * time.Millisecond)
	}
}
