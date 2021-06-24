package redis

import (
	"strings"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/helper/redis/hooks"

	"github.com/go-redis/redis/v8"
)

// NewClient wrap the standalone and sentinel client
func NewClient(conf *config.RedisConf, opt *redis.Options) (client *redis.Client) {
	if opt == nil {
		opt = &redis.Options{}
	}
	opt.Addr = conf.Addr
	opt.Password = conf.Password
	opt.PoolSize = conf.PoolSize
	opt.DB = conf.DB
	if conf.IsSentinel() {
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    conf.MasterName,
			SentinelAddrs: strings.Split(opt.Addr, ","),
			Password:      opt.Password,
			PoolSize:      opt.PoolSize,
			ReadTimeout:   opt.ReadTimeout,
			WriteTimeout:  opt.WriteTimeout,
			MinIdleConns:  opt.MinIdleConns,
			DB:            opt.DB,
		})
		client.AddHook(hooks.NewMetricsHook(client))
		return client
	}
	client = redis.NewClient(opt)
	client.AddHook(hooks.NewMetricsHook(client))
	return client
}
