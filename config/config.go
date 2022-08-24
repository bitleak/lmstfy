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
)

type Config struct {
	Host                   string
	Port                   int
	AdminHost              string
	AdminPort              int
	LogLevel               string
	LogDir                 string
	LogFormat              string
	Accounts               map[string]string
	EnableAccessLog        bool
	EnableSecondaryStorage bool
	AdminRedis             RedisConf
	Pool                   RedisPool
	SecondaryStorage       SpannerConfig

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

	MasterName string
	Version    string
}

type SpannerConfig struct {
	Project         string `mapstructure:"project" validate:"required"`
	Instance        string `mapstructure:"instance" validate:"required"`
	Database        string `mapstructure:"db" validate:"required"`
	CredentialsFile string `mapstructure:"credentials_file"`
	TableName       string
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

func verifySecStorageConf(cfg SpannerConfig) error {
	if cfg.Instance == "" || cfg.Project == "" || cfg.Database == "" || cfg.TableName == "" {
		return errors.New("invalid secondary storage config")
	}
	return nil
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

	if conf.EnableSecondaryStorage {
		err = verifySecStorageConf(conf.SecondaryStorage)
		if err != nil {
			return nil, err
		}
	}
	return conf, nil
}
