package pumper

import (
	"testing"
	"time"

	"github.com/bitleak/lmstfy/log"
	"github.com/bitleak/lmstfy/storage/lock"
	goredis "github.com/go-redis/redis/v8"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func init() {
	log.Setup("json", "", "INFO", "ERROR")
}

func TestNewDefault_FailOver(t *testing.T) {
	mockRedis, err := gnomock.Start(redis.Preset())
	require.Empty(t, err)
	defer gnomock.Stop(mockRedis)
	redisCli := goredis.NewClient(&goredis.Options{
		Addr: mockRedis.DefaultAddress(),
	})
	interval := time.Second
	lock := lock.NewRedisLock(redisCli, "test-fail-over", interval)
	p1 := NewDefault(lock, interval/2)
	p2 := NewDefault(lock, interval/2)

	var counter atomic.Int32
	go p1.Loop(func() bool {
		counter.Add(1)
		return false
	})
	go p2.Loop(func() bool {
		counter.Add(1)
		return false
	})
	time.Sleep(2 * time.Second)
	assert.GreaterOrEqual(t, counter.Load(), int32(2))
	assert.LessOrEqual(t, counter.Load(), int32(5))
	p1.Shutdown()
	time.Sleep(2 * time.Second)
	assert.LessOrEqual(t, counter.Load(), int32(8))
	p2.Shutdown()
}
