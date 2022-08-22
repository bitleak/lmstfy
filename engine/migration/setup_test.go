package migration

import (
	"context"
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
	OldRedisEngine engine.Engine
	NewRedisEngine engine.Engine
	dummyCtx       = context.TODO()
)

func setup(Conf *config.Config) {
	logger = logrus.New()
	level, _ := logrus.ParseLevel(Conf.LogLevel)
	logger.SetLevel(level)

	if err := redis.Setup(Conf, logger); err != nil {
		panic(fmt.Sprintf("Failed to setup redis engine: %s", err))
	}
	for _, poolConf := range Conf.Pool {
		conn := helper.NewRedisClient(&poolConf, nil)
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
	presetConfig, err := config.CreatePresetForTest("", "migrate")
	if err != nil {
		panic(fmt.Sprintf("CreatePresetForTest failed with error: %s", err))
	}
	defer presetConfig.Destroy()
	setup(presetConfig.Config)
	ret := m.Run()
	teardown()
	os.Exit(ret)
}
