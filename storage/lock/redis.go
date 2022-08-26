package lock

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
)

type Lock interface {
	Acquire() error
	Expiry() time.Duration
	ExtendLease() (bool, error)
	Release() (bool, error)
}

type RedisLock struct {
	redisCli *redis.Client
	mu       *redsync.Mutex
	expiry   time.Duration
}

func NewRedisLock(redisCli *redis.Client, name string, expiry time.Duration) *RedisLock {
	pool := goredis.NewPool(redisCli)
	rs := redsync.New(pool)
	mu := rs.NewMutex(fmt.Sprintf("pumper-%s.lock", name), redsync.WithExpiry(expiry))
	return &RedisLock{
		redisCli: redisCli,
		expiry:   expiry,
		mu:       mu,
	}
}

func (l *RedisLock) Acquire() error {
	return l.mu.Lock()
}

func (l *RedisLock) Expiry() time.Duration {
	return l.expiry
}

func (l *RedisLock) ExtendLease() (bool, error) {
	return l.mu.Extend()
}

func (l *RedisLock) Release() (bool, error) {
	return l.mu.Unlock()
}
