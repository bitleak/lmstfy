package config

import (
	"os"

	"github.com/BurntSushi/toml"
	"github.com/sirupsen/logrus"
)

const (
	DefaultPoolName = "default"
)

type Config struct {
	Host       string
	Port       int
	AdminHost  string
	AdminPort  int
	LogLevel   string
	LogDir     string
	AdminRedis RedisConf
	Pool       RedisPool

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
	if conf.Pool[DefaultPoolName].Addr == "" {
		panic("CONF: default redis pool not found")
	}
	if conf.AdminRedis.Addr == "" {
		panic("CONF: invalid admin redis addr")
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
