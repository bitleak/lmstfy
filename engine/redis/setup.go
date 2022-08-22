package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/helper"
)

const MaxRedisConnections = 5000

var (
	logger   *logrus.Logger
	dummyCtx = context.TODO()
)

// SetLogger will set the logger for engine
func SetLogger(l *logrus.Logger) {
	logger = l
}

// Setup set the essential config of redis engine
func Setup(conf *config.Config) error {
	for name, poolConf := range conf.Pool {
		if len(poolConf.Version) != 0 {
			continue
		}

		if poolConf.PoolSize == 0 {
			poolConf.PoolSize = MaxRedisConnections
		}
		opt := &redis.Options{}
		// By Default, the timeout for RW is 3 seconds, we might get few error
		// when redis server is doing AOF rewrite. We prefer data integrity over speed.
		opt.ReadTimeout = 30 * time.Second
		opt.WriteTimeout = 30 * time.Second
		opt.MinIdleConns = 10
		cli := helper.NewRedisClient(&poolConf, opt)
		if cli.Ping(dummyCtx).Err() != nil {
			return fmt.Errorf("redis server %s was not alive", poolConf.Addr)
		}
		e, err := NewEngine(name, cli)
		if err != nil {
			return fmt.Errorf("setup engine error: %s", err)
		}
		engine.Register(engine.KindRedis, name, e)
	}
	return nil
}
