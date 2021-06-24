package migration

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	redis_engine "github.com/bitleak/lmstfy/engine/redis"
	"github.com/bitleak/lmstfy/helper/redis"

	"github.com/sirupsen/logrus"
)

var (
	CONF           *config.Config
	OldRedisEngine engine.Engine
	NewRedisEngine engine.Engine
	dummyCtx       = context.TODO()
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
	if err := redis_engine.Setup(CONF, logger); err != nil {
		panic(fmt.Sprintf("Failed to setup redis engine: %s", err))
	}
	for _, poolConf := range CONF.Pool {
		conn := redis.NewClient(&poolConf, nil)
		err := conn.Ping(dummyCtx).Err()
		if err != nil {
			panic(fmt.Sprintf("Failed to ping: %s", err))
		}
		err = conn.FlushDB(dummyCtx).Err()
		if err != nil {
			panic(fmt.Sprintf("Failed to flush db: %s", err))
		}
	}
	OldRedisEngine = engine.GetEngineByKind(engine.KindRedis, "default")
	NewRedisEngine = engine.GetEngineByKind(engine.KindRedis, "migrate")
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
