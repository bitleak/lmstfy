package config

import (
	"errors"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
)

const (
	DefaultPoolName = "default"
)

const (
	unsupportedMode = iota + 1
	standaloneMode
	sentinelMode
)

type Config struct {
	Host            string
	Port            int
	AdminHost       string
	AdminPort       int
	LogLevel        string
	LogDir          string
	EnableAccessLog bool
	AdminRedis      RedisConf
	Pool            RedisPool

	// Default publish params
	TTLSecond   int
	DelaySecond int
	TriesNum    int
	// Default consume params
	TTRSecond     int
	TimeoutSecond int
}

type RedisPool map[string]RedisConf

type RedisConf struct {
	Addr      string
	Password  string
	PoolSize  int
	MigrateTo string // If this is not empty, all the PUBLISH will go to that pool

	mode       int
	MasterName string
}

func detectRedisMode(rc *RedisConf) (int, error) {
	// the sentinel addrs would be split with comma
	addrs := strings.Split(rc.Addr, ",")
	cli := redis.NewClient(&redis.Options{
		Addr:     addrs[0],
		Password: rc.Password,
		PoolSize: 1,
	})
	defer cli.Close()
	infoStr, err := cli.Info("server").Result()
	if err != nil {
		return -1, err
	}
	lines := strings.Split(infoStr, "\r\n")
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) == 2 && fields[0] == "redis_mode" {
			switch fields[1] {
			case "standalone":
				return standaloneMode, nil
			case "sentinel":
				return sentinelMode, nil
			default:
				return unsupportedMode, errors.New("unsupported redis mode")
			}
		}
	}
	// redis mode was not found in INFO command, treat it as standalone
	return standaloneMode, nil
}

func (rc *RedisConf) validate() error {
	if rc.Addr == "" {
		return errors.New("the pool addr must not be empty")
	}
	if rc.IsSentinel() && rc.MasterName == "" {
		return errors.New("the master name must not be empty in sentinel mode")
	}
	return nil
}

// IsSentinel return whether the pool was running in sentinel mode
func (rc *RedisConf) IsSentinel() bool {
	return rc.mode == sentinelMode
}

func MustLoad(path string) *Config {
	_, err := os.Stat(path)
	if err != nil {
		panic(err)
	}
	conf := new(Config)
	conf.LogLevel = "info"
	conf.AdminHost = "127.0.0.1"

	conf.TTLSecond = 24 * 60 * 60 // 1 day
	conf.DelaySecond = 0
	conf.TriesNum = 1
	conf.TTRSecond = 2 * 60 // 2 minutes
	conf.TimeoutSecond = 0  // means non-blocking

	if _, err := toml.DecodeFile(path, conf); err != nil {
		panic(err)
	}

	if conf.Host == "" {
		panic("CONF: invalid host")
	}
	if conf.Port == 0 {
		panic("CONF: invalid port")
	}
	if _, ok := conf.Pool[DefaultPoolName]; !ok {
		panic("CONF: default redis pool not found")
	}
	for name, poolConf := range conf.Pool {
		if poolConf.mode, err = detectRedisMode(&poolConf); err != nil {
			panic("CONF: failed to get reedis mode in pool(" + name + "), err: " + err.Error())
		}
		conf.Pool[name] = poolConf
		if err := poolConf.validate(); err != nil {
			panic("CONF: invalid config in pool(" + name + "), err: " + err.Error())
		}
	}
	if conf.AdminRedis.mode, err = detectRedisMode(&conf.AdminRedis); err != nil {
		panic("CONF: failed to get reedis mode in admin pool, err: " + err.Error())
	}
	if err := conf.AdminRedis.validate(); err != nil {
		panic("CONF: invalid config in admin redis, err: " + err.Error())
	}
	if conf.AdminPort == 0 {
		panic("CONF: invalid admin port")
	}

	_, err = logrus.ParseLevel(conf.LogLevel)
	if err != nil {
		panic("CONF: invalid log level")
	}
	return conf
}
