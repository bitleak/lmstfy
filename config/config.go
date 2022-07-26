package config

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/go-redis/redis/v8"
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

const (
	ValidRedisMaxMemPolicyValue = "noeviction"
	ValidRedisAOFEnabledValue   = "1"
)

type Config struct {
	Host            string
	Port            int
	AdminHost       string
	AdminPort       int
	LogLevel        string
	LogDir          string
	LogFormat       string
	Accounts        map[string]string
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
	DB        int
	PoolSize  int
	MigrateTo string // If this is not empty, all the PUBLISH will go to that pool

	mode       int
	MasterName string

	// MaxMemPolicy and AppendOnly shall be checked to make sure that data is persistent
	MaxMemPolicy string
	AofEnabled   string
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
	infoStr, err := cli.Info(context.Background(), "server").Result()
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
	if rc.DB < 0 {
		return errors.New("the pool db must be greater than 0 or equal to 0")
	}
	if rc.IsSentinel() && rc.MasterName == "" {
		return errors.New("the master name must not be empty in sentinel mode")
	}
	if rc.MaxMemPolicy != ValidRedisMaxMemPolicyValue {
		return errors.New("valid maxmempolicy config value must be noeviction, but get " + rc.MaxMemPolicy)
	}
	if rc.AofEnabled != ValidRedisAOFEnabledValue {
		return errors.New("valid aof_enabled config value must be 1, but get " + rc.AofEnabled)
	}
	return nil
}

func processRedisDataPersistConf(rc *RedisConf) {
	addr := rc.Addr
	if rc.IsSentinel() {
		addrs := strings.Split(addr, ",")
		sentinel := redis.NewSentinelClient(&redis.Options{
			Addr: addrs[0],
		})
		val, err := sentinel.GetMasterAddrByName(context.Background(), rc.MasterName).Result()
		if err != nil || len(val) < 2 {
			rc.AofEnabled = "0"
			rc.MaxMemPolicy = ""
			sentinel.Close()
			return
		}
		addr = net.JoinHostPort(val[0], val[1])
		sentinel.Close()
	}
	cli := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: rc.Password,
		PoolSize: 1,
	})
	memPolVal, aofval := getRedisDataPersistInfo(cli)
	rc.MaxMemPolicy = memPolVal
	rc.AofEnabled = aofval
	cli.Close()
	return
}

func getRedisDataPersistInfo(cli *redis.Client) (string, string) {
	ctx := context.Background()
	var memPolVal, aofVal string
	memPolStr, err := cli.Info(ctx, "memory").Result()
	if err == nil {
		lines := strings.Split(memPolStr, "\r\n")
		for _, line := range lines {
			fields := strings.Split(line, ":")
			if len(fields) == 2 && fields[0] == "maxmemory_policy" {
				memPolVal = fields[1]
			}
		}
	}
	aofStr, err := cli.Info(ctx, "persistence").Result()
	if err == nil {
		lines := strings.Split(aofStr, "\r\n")
		for _, line := range lines {
			fields := strings.Split(line, ":")
			if len(fields) == 2 && fields[0] == "aof_enabled" {
				aofVal = fields[1]
			}
		}
	}
	return memPolVal, aofVal
}

// IsSentinel return whether the pool was running in sentinel mode
func (rc *RedisConf) IsSentinel() bool {
	return rc.mode == sentinelMode
}

// MustLoad load config file with specified path, an error returned if any condition not met
func MustLoad(path string) (*Config, error) {
	_, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat config file: %s", err)
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
		return nil, errors.New("invalid host")
	}
	if conf.Port == 0 {
		return nil, errors.New("invalid port")
	}
	if _, ok := conf.Pool[DefaultPoolName]; !ok {
		return nil, errors.New("default redis pool not found")
	}
	for name, poolConf := range conf.Pool {
		if poolConf.mode, err = detectRedisMode(&poolConf); err != nil {
			return nil, fmt.Errorf("failed to get redis mode in pool(%s): %s", name, err)
		}
		processRedisDataPersistConf(&poolConf)
		conf.Pool[name] = poolConf
		if err := poolConf.validate(); err != nil {
			return nil, fmt.Errorf("invalid config in pool(%s): %s", name, err)
		}
	}
	if conf.AdminRedis.mode, err = detectRedisMode(&conf.AdminRedis); err != nil {
		return nil, fmt.Errorf("failed to get reedis mode in admin pool: %s", err)
	}
	processRedisDataPersistConf(&conf.AdminRedis)
	if err := conf.AdminRedis.validate(); err != nil {
		return nil, fmt.Errorf("invalid config in admin redis: %s", err)
	}
	if conf.AdminPort == 0 {
		return nil, errors.New("invalid admin port")
	}

	_, err = logrus.ParseLevel(conf.LogLevel)
	if err != nil {
		return nil, errors.New("invalid log level")
	}
	return conf, nil
}
