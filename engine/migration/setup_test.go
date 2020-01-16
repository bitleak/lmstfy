package migration

import (
	"fmt"
	"os"
	"testing"

	"github.com/meitu/lmstfy/config"
	"github.com/meitu/lmstfy/engine"
	"github.com/meitu/lmstfy/engine/redis"
	"github.com/meitu/lmstfy/helper"
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
	CONF = config.MustLoad(os.Getenv("LMSTFY_TEST_CONFIG"))
	logger = logrus.New()
	level, _ := logrus.ParseLevel(CONF.LogLevel)
	logger.SetLevel(level)
}

func setup() {
	redis.Setup(CONF, logger)
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
