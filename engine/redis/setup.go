package redis

import (
	"time"

	"github.com/go-redis/redis"
	"github.com/meitu/lmstfy/config"
	"github.com/meitu/lmstfy/engine"
	"github.com/meitu/lmstfy/helper"
	"github.com/sirupsen/logrus"
)

const MaxRedisConnections = 5000

var logger *logrus.Logger

func Setup(conf *config.Config, l *logrus.Logger) {
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
			panic("Can not connect to redis")
		}
		engine.Register("redis", name, NewEngine(name, cli))
	}
	if engine.GetEngineByKind("redis", "") == nil {
		panic("default redis engine not found")
	}
}
