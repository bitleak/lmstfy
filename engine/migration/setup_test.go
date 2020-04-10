package migration

import (
	"fmt"
	"os"
	"testing"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/engine/redis"
	"github.com/bitleak/lmstfy/helper"
	"github.com/sirupsen/logrus"
)

var (
	CONF           *config.Config
	OldRedisEngine engine.Engine
	NewRedisEngine engine.Engine
)

func init() {
	cfg := os.Getenv("LMSTFY_TEST_CONFIG")
	if cfg == "" {
		panic(`
############################################################
PLEASE setup env LMSTFY_TEST_CONFIG to the config file first
############################################################
`)
	}
	var err error
	if CONF, err = config.MustLoad(os.Getenv("LMSTFY_TEST_CONFIG")); err != nil {
		panic(fmt.Sprintf("Failed to load config file: %s", err))
	}
	logger = logrus.New()
	level, _ := logrus.ParseLevel(CONF.LogLevel)
	logger.SetLevel(level)
}

func setup() {
	if err := redis.Setup(CONF, logger); err != nil {
		panic(fmt.Sprintf("Failed to setup redis engine: %s", err))
	}
	for _, poolConf := range CONF.Pool {
		conn := helper.NewRedisClient(&poolConf, nil)
		err := conn.Ping().Err()
		if err != nil {
			panic(fmt.Sprintf("Failed to ping: %s", err))
		}
		err = conn.FlushDB().Err()
		if err != nil {
			panic(fmt.Sprintf("Failed to flush db: %s", err))
		}
	}
	OldRedisEngine = engine.GetEngineByKind("redis", "default")
	NewRedisEngine = engine.GetEngineByKind("redis", "migrate")
}

func teardown() {
	OldRedisEngine.Shutdown()
	NewRedisEngine.Shutdown()
}

func TestMain(m *testing.M) {
	setup()
	ret := m.Run()
	teardown()
	os.Exit(ret)
}
