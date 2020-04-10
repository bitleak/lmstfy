package helper

import (
	"strings"

	"github.com/bitleak/lmstfy/config"
	"github.com/go-redis/redis"
)

// NewRedisClient wrap the standalone and sentinel client
func NewRedisClient(conf *config.RedisConf, opt *redis.Options) *redis.Client {
	if opt == nil {
		opt = &redis.Options{}
	}
	opt.Addr = conf.Addr
	opt.Password = conf.Password
	opt.PoolSize = conf.PoolSize
	if conf.IsSentinel() {
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    conf.MasterName,
			SentinelAddrs: strings.Split(opt.Addr, ","),
			Password:      opt.Password,
			PoolSize:      opt.PoolSize,
			ReadTimeout:   opt.ReadTimeout,
			WriteTimeout:  opt.WriteTimeout,
			MinIdleConns:  opt.MinIdleConns,
		})
	}
	return redis.NewClient(opt)
}
