package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/sirupsen/logrus"
)

const (
	DefaultPoolName = "default"

	minSecondaryStorageThresholdSeconds = 60 * 60
)

type SpannerConfig struct {
	Project         string
	Instance        string
	Database        string
	CredentialsFile string
	TableName       string
}

func (spanner *SpannerConfig) validate() error {
	if spanner == nil {
		return nil
	}
	if spanner.Instance == "" || spanner.Project == "" || spanner.Database == "" || spanner.TableName == "" {
		return errors.New("'Instance'/'Project'/'Database'/'TableName' should NOT be empty")
	}
	return nil
}

type SecondaryStorage struct {
	Spanner *SpannerConfig
	// max number of jobs that storage pumps per batch
	MaxJobPumpBatchSize int64
	// range from 0 to 1, when the redis memory usage is greater than this value,
	// the storage won't pump jobs to redis anymore until the memory usage is lower than this value.
	//
	// Default is 1, means no limit
	HighRedisMemoryWatermark float64
}

func (storage *SecondaryStorage) validate() error {
	if storage.HighRedisMemoryWatermark < 0 || storage.HighRedisMemoryWatermark > 1 {
		return fmt.Errorf("invalid HighRedisMemoryWatermark: %f, should be between 0 and 1", storage.HighRedisMemoryWatermark)
	}
	return storage.Spanner.validate()
}

type Config struct {
	Host             string
	Port             int
	AdminHost        string
	AdminPort        int
	LogLevel         string
	LogDir           string
	LogFormat        string
	Accounts         map[string]string
	EnableAccessLog  bool
	AdminRedis       RedisConf
	Pool             RedisPool
	SecondaryStorage *SecondaryStorage

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
	Addr             string
	Password         string
	DB               int
	PoolSize         int
	MigrateTo        string // If this is not empty, all the PUBLISH will go to that pool
	MasterName       string
	Version          string
	SentinelPassword string
}

func (rc *RedisConf) validate() error {
	if rc.Addr == "" {
		return errors.New("the pool addr must not be empty")
	}
	if rc.DB < 0 {
		return errors.New("the pool db must be greater than 0 or equal to 0")
	}
	return nil
}

// IsSentinel return whether the pool was running in sentinel mode
func (rc *RedisConf) IsSentinel() bool {
	return rc.MasterName != ""
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
		if err := poolConf.validate(); err != nil {
			return nil, fmt.Errorf("invalid config in pool(%s): %s", name, err)
		}
	}
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

	if conf.SecondaryStorage != nil {
		if err := conf.SecondaryStorage.validate(); err != nil {
			return nil, err
		}
		if conf.SecondaryStorage.HighRedisMemoryWatermark == 0 {
			// default to 1
			conf.SecondaryStorage.HighRedisMemoryWatermark = 1
		}
	}
	return conf, nil
}
