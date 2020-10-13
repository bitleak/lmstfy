package redis_v1

import (
	"fmt"
	"os"
	"testing"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/helper"
	"github.com/sirupsen/logrus"
)

var (
	CONF *config.Config
	R    *RedisInstance
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
	poolConf := CONF.Pool["default"]
	conn := helper.NewRedisClient(&poolConf, nil)
	err := conn.Ping().Err()
	if err != nil {
		panic(fmt.Sprintf("Failed to ping: %s", err))
	}
	err = conn.FlushDB().Err()
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

func teardown() {
}

func TestMain(m *testing.M) {
	setup()
	ret := m.Run()
	teardown()
	os.Exit(ret)
}
