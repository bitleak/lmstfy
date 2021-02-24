package push

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/helper"

	"github.com/sirupsen/logrus"

	"github.com/bitleak/lmstfy/config"
	redis_engine "github.com/bitleak/lmstfy/engine/redis"
)

var (
	CONF   *config.Config
	logger *logrus.Logger
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
	// clear admin redis
	conn := helper.NewRedisClient(&CONF.AdminRedis, nil)
	err := conn.Ping(ctx).Err()
	if err != nil {
		panic(fmt.Sprintf("Failed to ping: %s", err))
	}
	err = conn.FlushDB(ctx).Err()
	if err != nil {
		panic(fmt.Sprintf("Failed to flush db: %s", err))
	}

	if err := Setup(CONF, 100*time.Millisecond, logger); err != nil {
		panic(fmt.Sprintf("Failed to setup push engine: %s", err))
	}
}

func teardown() {
	Shutdown()
}

func TestMain(m *testing.M) {
	setup()
	ret := m.Run()
	teardown()
	os.Exit(ret)
}
