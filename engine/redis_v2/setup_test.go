package redis_v2

import (
	"fmt"
	"os"
	"testing"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/helper"
	"github.com/bitleak/lmstfy/log"
	"github.com/sirupsen/logrus"
)

var (
	R          *RedisInstance
	testConfig *config.PresetConfigForTest
)

func setup(CONF *config.Config) {
	logger = logrus.New()
	level, _ := logrus.ParseLevel(CONF.LogLevel)
	logger.SetLevel(level)

	log.Setup(CONF.LogFormat, CONF.LogDir, CONF.LogLevel, "ERROR")
	poolConf := CONF.Pool["default"]
	conn := helper.NewRedisClient(&poolConf, nil)
	err := conn.Ping(dummyCtx).Err()
	if err != nil {
		panic(fmt.Sprintf("Failed to ping: %s", err))
	}
	err = conn.FlushDB(dummyCtx).Err()
	if err != nil {
		panic(fmt.Sprintf("Failed to flush db: %s", err))
	}

	R = &RedisInstance{
		Name: "unittest",
		Conn: conn,
	}

	if err = PreloadDeadLetterLuaScript(R); err != nil {
		panic(fmt.Sprintf("Failed to preload deadletter lua script: %s", err))
	}
}

func TestMain(m *testing.M) {
	presetConfig, err := config.CreatePresetForTest(VersionV2)
	if err != nil {
		panic(fmt.Sprintf("CreatePresetForTest failed with error: %s", err))
	}
	defer presetConfig.Destroy()
	setup(presetConfig.Config)
	testConfig = presetConfig
	ret := m.Run()
	os.Exit(ret)
}
