package spanner

import (
	"context"
	"fmt"
	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/helper"
	go_redis "github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"os"
	"testing"
)

type RedisInstance struct {
	Name string
	Conn *go_redis.Client
}

var (
	R        *RedisInstance
	logger   *logrus.Logger
	dummyCtx = context.TODO()
)

func setup(CONF *config.Config) {
	logger = logrus.New()
	level, _ := logrus.ParseLevel(CONF.LogLevel)
	logger.SetLevel(level)

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
}

func TestMain(m *testing.M) {
	presetConfig, err := config.CreatePresetForTest()
	if err != nil {
		panic(fmt.Sprintf("CreatePresetForTest failed with error: %s", err))
	}
	defer presetConfig.Destroy()
	setup(presetConfig.Config)
	ret := m.Run()
	os.Exit(ret)
}
