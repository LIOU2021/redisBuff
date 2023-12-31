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

	c1 := newConfig("message-test-1")
	s1 := redisBuff.New(c1)
	closeRunner := s1.SendMsgRunner()

	go testAdd(s1)
	go loopAdd(s1)
	go testReadList(s1)

	c2 := newConfig("message-test-2")
	s2 := redisBuff.New(c2)
	closeRunner2 := s2.SendMsgRunner()

	go testAdd(s2)
	go loopAdd(s2)
	go testReadList(s2)

	select {
	case <-time.After(10 * time.Second):
		fmt.Println("send closeRunner signal")
		closeRunner <- true
		closeRunner2 <- true
		time.Sleep(50 * time.Millisecond)
	}
}

func newConfig(name string) *redisBuff.Config {
	return &redisBuff.Config{
		SendBuff:       100,
		MsgBatch:       5,
		RunnerInterval: 5 * time.Second,
		CacheName:      name,
		LockDuration:   3 * time.Second,
		RLockDuration:  3 * time.Second,
		ClearMsgFunc: func(msg []string) {
			fmt.Println("from client: ", msg)
		},
		// Debug: true,
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

func testReadList(b *redisBuff.Buff) {
	for i := 0; i < 10; i++ {
		go func() {
			fmt.Println("from testReadList-1: ", b.List())
		}()
	}
}

func loopAdd(b *redisBuff.Buff) {
	for {
		b.Add(flagvar)
		time.Sleep(200 * time.Millisecond)
	}
}
