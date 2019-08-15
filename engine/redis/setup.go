package redis

import (
	"time"

	go_redis "github.com/go-redis/redis"
	"github.com/meitu/lmstfy/config"
	"github.com/meitu/lmstfy/engine"
	"github.com/sirupsen/logrus"
)

const MaxRedisConnections = 5000

var logger *logrus.Logger

func Setup(conf *config.Config, l *logrus.Logger) {
	logger = l
	for redisPool, poolConf := range conf.Pool {
		if poolConf.PoolSize == 0 {
			poolConf.PoolSize = MaxRedisConnections
		}
		cli := go_redis.NewClient(&go_redis.Options{
			Addr:     poolConf.Addr,
			Password: poolConf.Password,
			PoolSize: poolConf.PoolSize,
			// By Default, the timeout for RW is 3 seconds, we might get few error
			// when redis server is doing AOF rewrite. We prefer data integrity over speed.
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,

			MinIdleConns: 10, // Introduced to reduce `dial tcp: i/o timeout` error
		})
		if cli.Ping().Err() != nil {
			panic("Can not connect to redis")
		}
		engine.Register("redis", redisPool, NewEngine(redisPool, cli))
	}
	if engine.GetEngineByKind("redis", "") == nil {
		panic("default redis engine not found")
	}
}
