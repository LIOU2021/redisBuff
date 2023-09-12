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

	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(fmt.Sprintf("[InitRedisClient fail] - %v", err))
	}
}

func New(c *Config) *Buff {
	b := new(Buff)

	sendBuff := c.SendBuff
	if sendBuff < 1 {
		sendBuff = 100
	}
	b.send = make(chan interface{}, sendBuff)

	msgBatch := c.MsgBatch
	if msgBatch < 1 {
		msgBatch = 5
	}
	b.msgBatch = msgBatch

	runnerInterval := c.RunnerInterval
	if runnerInterval < 1 {
		runnerInterval = time.Millisecond * 5000
	}
	b.runnerInterval = runnerInterval

	if c.CacheName == "" {
		panic(fmt.Sprintf("[New fail] - %s", "c.CacheName wail empty"))
	}
	b.cacheName = c.CacheName

	lockDuration := c.LockDuration
	if lockDuration < 1 {
		lockDuration = time.Second * 3
	}
	b.lockDuration = lockDuration

	clearFunc := c.ClearMsgFunc
	if clearFunc == nil {
		panic(fmt.Sprintf("[New fail] - %s", "c.ClearMsgFunc wail empty"))
	}
	b.clearMsgFunc = clearFunc
	return b
}

type Config struct {
	SendBuff       int                // 决定发送讯息时使用的chan的buff大小
	MsgBatch       int64              // 讯息达到N则时发送
	RunnerInterval time.Duration      // 每N时间排程执行一次讯息处理
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
				b.pushMsgWithLock(msg)
			case <-done:
				close(b.send)
				s := make(chan bool)
				go func() { // 等chan上的讯息处理完后才能关闭
					for len(b.send) > 0 {
						time.Sleep(200 * time.Second)
					}
					s <- true
					close(s)
				}()
				<-s
				fmt.Println("redisBuff SendMsgRunner close")
				return
			case <-ticker.C:
				if ok := b.getIntervalLock(); !ok {
					continue
				}
				b.clearMsgWithLock()
			}
		}
	}()

	return done
}

func (b *Buff) getIntervalLock() (ok bool) {
	key := fmt.Sprintf("redisBuff-interval-lock-%s", b.cacheName)
	ok = rdb.SetNX(ctx, key, 1, b.runnerInterval-200*time.Millisecond).Val()
	return
}

// add msg
// sendBuff满时，将堵塞
func (b *Buff) Add(data interface{}) {
	b.send <- data
}

func (b *Buff) lock() func() {
	key := fmt.Sprintf("redisBuff-base-lock-%s", b.cacheName)
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

func (b *Buff) clearMsgWithLock() {
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

func (b *Buff) pushMsgWithLock(msg interface{}) {
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
	if len >= b.msgBatch { // 大于N则讯息则推送
		b.clearMsg()
	}
}
