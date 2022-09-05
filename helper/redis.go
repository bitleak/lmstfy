package helper

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine/redis/hooks"
	"github.com/go-redis/redis/v8"
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

// validateRedisPersistConfig will check whether persist config of Redis is good or not
func validateRedisPersistConfig(ctx context.Context, cli *redis.Client, conf *config.RedisConf) error {
	infoStr, err := cli.Info(ctx).Result()
	if err != nil {
		return err
	}
	isNoEvctionPolicy, isAppendOnlyEnabled := false, false
	var maxMem int64
	lines := strings.Split(infoStr, "\r\n")
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "maxmemory_policy":
			isNoEvctionPolicy = fields[1] == "noeviction"
		case "aof_enabled":
			isAppendOnlyEnabled = fields[1] == "1"
		case "maxmemory":
			maxMem, _ = strconv.ParseInt(fields[1], 10, 64)
		}
	}
	if !isNoEvctionPolicy {
		return errors.New("redis memory_policy MUST be 'noeviction' to prevent data loss")
	}
	if !isAppendOnlyEnabled {
		return errors.New("redis appendonly MUST be 'yes' to prevent data loss")
	}
	if conf.EnableSecondaryStorage && maxMem == 0 {
		return errors.New("redis maxmemory MUST be assigned when secondary storage is enabled")
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
