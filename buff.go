package redisBuff

import (
	"context"
	"fmt"
	"log"
	"strconv"
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
	if rdb == nil {
		panic("redis client was nil, must call InitRedisClient() before New()")
	}

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

	b.readLockName = fmt.Sprintf("redisBuff-read-lock-%s", b.cacheName)
	b.writeLockName = fmt.Sprintf("redisBuff-write-lock-%s", b.cacheName)

	lockDuration := c.LockDuration
	if lockDuration < 1 {
		lockDuration = time.Second * 3
	}
	b.lockDuration = lockDuration

	rlockDuration := c.RLockDuration
	if rlockDuration < 1 {
		rlockDuration = time.Second * 3
	}
	b.rlockDuration = rlockDuration

	clearFunc := c.ClearMsgFunc
	if clearFunc == nil {
		panic(fmt.Sprintf("[New fail] - %s", "c.ClearMsgFunc wail empty"))
	}
	b.clearMsgFunc = clearFunc

	b.debug = c.Debug
	return b
}

type Config struct {
	SendBuff       int                // 决定发送讯息时使用的chan的buff大小
	MsgBatch       int64              // 讯息达到N则时发送
	RunnerInterval time.Duration      // 每N时间排程执行一次讯息处理
	CacheName      string             // 缓存命名
	LockDuration   time.Duration      // 分布式锁上锁时间，根据业务逻辑处理时间调整
	RLockDuration  time.Duration      // 读锁的TTL
	ClearMsgFunc   func(msg []string) // 清除讯息时要执行的
	Debug          bool               // debug model
}

type Buff struct {
	send           chan interface{}
	msgBatch       int64
	runnerInterval time.Duration
	cacheName      string
	lockDuration   time.Duration
	rlockDuration  time.Duration
	clearMsgFunc   func(msg []string)
	debug          bool
	readLockName   string
	writeLockName  string
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
						b.debugMsg("[%s] - chan上还有讯息\n", b.cacheName)
						time.Sleep(200 * time.Second)
					}
					s <- true
					close(s)
				}()
				<-s
				fmt.Printf("redisBuff SendMsgRunner close - %s\n", b.cacheName)
				return
			case <-ticker.C:
				b.debugMsg("[%s] - 循环触发-start\n", b.cacheName)
				if ok := b.getIntervalLock(); !ok {
					b.debugMsg("[%s] - 循环触发-没拿到lock\n", b.cacheName)
					continue
				}
				b.clearMsgWithLock()
				b.debugMsg("[%s] - 循环触发-end\n", b.cacheName)
			}
		}
	}()

	return done
}

func (b *Buff) debugMsg(format string, a ...any) {
	if !b.debug {
		return
	}

	fmt.Printf(format, a...)
}

// 用来控制多server的情况下，保持interval逻辑单例模式运作
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

// 获取目前缓存上的资料
func (b *Buff) List() []string {
	RUnlock := b.RLock()
	defer func() {
		b.debugMsg("[%s] - List end\n", b.cacheName)
		RUnlock()
	}()
	b.debugMsg("[%s] - List start\n", b.cacheName)
	result := rdb.LRange(ctx, b.cacheName, 0, -1).Val()
	b.debugMsg("[%s] - List result: %v\n", b.cacheName, result)
	return result
}

// 读锁
func (b *Buff) RLock() func() { // 怪怪的，打印频率有点怪
	for {
		if rdb.Exists(ctx, b.writeLockName).Val() < 1 {
			break
		}
		b.debugMsg("[%s RLock] - 等待写锁释放\n", b.cacheName)
		time.Sleep(100 * time.Millisecond)
	}

	t := time.Now().Nanosecond()
	rdb.Set(ctx, b.readLockName, t, b.rlockDuration)
	b.debugMsg("[%s RLock] - 获得读锁\n", b.cacheName)
	return func() {
		v, _ := strconv.Atoi(rdb.Get(ctx, b.readLockName).Val())
		if v == t {
			b.debugMsg("[%s RLock] - 释放读锁\n", b.cacheName)
			rdb.Del(ctx, b.readLockName)
		}
	}
}

// 清除/写入讯息要用的
func (b *Buff) lock() func() {
	for {
		if rdb.Exists(ctx, b.readLockName).Val() < 1 {
			break
		}
		b.debugMsg("[%s Lock] - 等待读锁释放\n", b.cacheName)
		time.Sleep(100 * time.Second)
	}

	for {
		ok := rdb.SetNX(ctx, b.writeLockName, 1, b.lockDuration).Val()
		if ok {
			b.debugMsg("[%s Lock] - 获得写锁\n", b.cacheName)
			break
		}
		time.Sleep(time.Millisecond * 50)
	}

	return func() {
		b.debugMsg("[%s Lock] - 释放写锁\n", b.cacheName)
		rdb.Del(ctx, b.writeLockName)
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
		b.debugMsg("[%s] - 大于N则讯息推送 - start\n", b.cacheName)
		b.clearMsg()
		b.debugMsg("[%s] - 大于N则讯息推送 - end\n", b.cacheName)
	}
}
