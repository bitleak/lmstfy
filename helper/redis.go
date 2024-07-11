package helper

import (
	"context"
	"errors"
	"strings"

	"github.com/go-redis/redis/v8"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine/redis/hooks"
)

// NewRedisClient wrap the standalone and sentinel client
func NewRedisClient(conf *config.RedisConf, opt *redis.Options) (client *redis.Client) {
	if opt == nil {
		opt = &redis.Options{}
	}
	opt.Addr = conf.Addr
	opt.Password = conf.Password
	opt.PoolSize = conf.PoolSize
	opt.DB = conf.DB
	if conf.IsSentinel() {
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       conf.MasterName,
			SentinelAddrs:    strings.Split(opt.Addr, ","),
			SentinelPassword: conf.SentinelPassword,
			Password:         opt.Password,
			PoolSize:         opt.PoolSize,
			ReadTimeout:      opt.ReadTimeout,
			WriteTimeout:     opt.WriteTimeout,
			MinIdleConns:     opt.MinIdleConns,
			DB:               opt.DB,
		})
		client.AddHook(hooks.NewMetricsHook(client))
		return client
	}
	client = redis.NewClient(opt)
	client.AddHook(hooks.NewMetricsHook(client))
	return client
}

// validateRedisPersistConfig will check whether persist config of Redis is good or not
func validateRedisPersistConfig(ctx context.Context, cli *redis.Client, conf *config.RedisConf) error {
	infoStr, err := cli.Info(ctx).Result()
	if err != nil {
		return err
	}
	isNoEvictionPolicy, isAppendOnlyEnabled := false, false
	lines := strings.Split(infoStr, "\r\n")
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "maxmemory_policy":
			isNoEvictionPolicy = fields[1] == "noeviction"
		case "aof_enabled":
			isAppendOnlyEnabled = fields[1] == "1"
		}
	}
	if !isNoEvictionPolicy {
		return errors.New("redis memory_policy MUST be 'noeviction' to prevent data loss")
	}
	if !isAppendOnlyEnabled {
		return errors.New("redis appendonly MUST be 'yes' to prevent data loss")
	}
	return nil
}

// ValidateRedisConfig will check the redis configuration is good or not
func ValidateRedisConfig(ctx context.Context, conf *config.RedisConf) error {
	// For sentinel mode, it will only check the master persist config
	redisCli := NewRedisClient(conf, &redis.Options{PoolSize: 1})
	defer redisCli.Close()

	return validateRedisPersistConfig(ctx, redisCli, conf)
}
