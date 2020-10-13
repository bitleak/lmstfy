package redis_v2

import (
	"errors"
	"fmt"
	"time"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/helper"
	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
)

const MaxRedisConnections = 5000

var logger *logrus.Logger

// Setup set the essential config of redis engine
func Setup(conf *config.Config, l *logrus.Logger) error {
	logger = l
	for name, poolConf := range conf.Pool {
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
		if cli.Ping().Err() != nil {
			return fmt.Errorf("redis server %s was not alive", poolConf.Addr)
		}
		e, err := NewEngine(name, cli)
		if err != nil {
			return fmt.Errorf("setup engine error: %s", err)
		}
		engine.Register("redis", name, e)
	}
	if engine.GetEngineByKind("redis", "") == nil {
		return errors.New("default redis engine not found")
	}
	return nil
}
