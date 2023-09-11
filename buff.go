package redisBuff

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
)

var rdb redis.Cmdable
var rdbOnce sync.Once
var ctx = context.Background()

func InitRedisClient(client redis.Cmdable) {
	rdbOnce.Do(func() {
		rdb = client
	})
}

func New(c *Config) *Buff {
	b := new(Buff)
	b.send = make(chan interface{}, c.SendBuff)
	b.msgBatch = c.MsgBatch
	b.runnerInterval = c.RunnerInterval
	b.cacheName = c.CacheName
	b.lockDuration = c.LockDuration
	b.clearMsgFunc = c.ClearMsgFunc
	return b
}

type Config struct {
	SendBuff       int
	MsgBatch       int64 // 讯息达到N则时发送
	RunnerInterval time.Duration
	CacheName      string             // 缓存命名
	LockDuration   time.Duration      // 分布式锁上锁时间
	ClearMsgFunc   func(msg []string) // 清除讯息时要执行的
}

type Buff struct {
	send           chan interface{}
	msgBatch       int64
	runnerInterval time.Duration
	cacheName      string
	lockDuration   time.Duration
	clearMsgFunc   func(msg []string)
}

// execute runner
func (b *Buff) SendMsgRunner() chan<- bool {
	done := make(chan bool)
	ticker := time.NewTicker(b.runnerInterval)
	go func() {
		defer ticker.Stop()

		for {
			select {
			case msg := <-b.send:
				b.PushMsgWithLock(msg)
			case <-done:
				fmt.Println("redisBuff SendMsgRunner close")
				return
			case <-ticker.C:
				b.ClearMsgWithLock()
			}
		}
	}()

	return done
}

// add msg
// sendBuff满时，将堵塞
func (b *Buff) Add(data interface{}) {
	b.send <- data
}

func (b *Buff) lock() func() {
	key := fmt.Sprintf("redisBuff-lock-%s", b.cacheName)
	for {
		ok := rdb.SetNX(ctx, key, 1, b.lockDuration).Val()
		if ok {
			break
		}
		time.Sleep(time.Millisecond * 50)
	}

	return func() {
		rdb.Del(ctx, key)
	}
}

func (b *Buff) ClearMsgWithLock() {
	unlock := b.lock()
	defer unlock()
	b.clearMsg()
}

func (b *Buff) clearMsg() {
	key := b.cacheName
	totalMsg := []string{}
	var lenList int64

	for {
		lenList = rdb.LLen(ctx, key).Val()
		if lenList <= 0 {
			break
		}
		cursor := rdb.LPop(ctx, key).Val()
		totalMsg = append(totalMsg, cursor)
	}

	if len(totalMsg) > 0 {
		b.clearMsgFunc(totalMsg)
	}
}

func (b *Buff) PushMsgWithLock(msg interface{}) {
	unlock := b.lock()
	defer unlock()
	b.pushMsg(msg)
}

func (b *Buff) pushMsg(msg interface{}) {
	key := b.cacheName
	if err := rdb.RPush(ctx, key, msg).Err(); err != nil {
		log.Fatalf("[push fail] - %v", err)
	}
	len := rdb.LLen(ctx, key).Val()
	if len >= b.msgBatch { // 大于5则讯息则推送
		b.clearMsg()
	}
}
