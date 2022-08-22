package handlers_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/bitleak/lmstfy/auth"
	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	redis_engine "github.com/bitleak/lmstfy/engine/redis"
	"github.com/bitleak/lmstfy/engine/redis_v2"
	"github.com/bitleak/lmstfy/helper"
	"github.com/bitleak/lmstfy/server/handlers"
	"github.com/bitleak/lmstfy/throttler"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func ginTest(req *http.Request) (*gin.Context, *gin.Engine, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	gin.SetMode(gin.ReleaseMode)
	ctx, engine := gin.CreateTestContext(w)
	ctx.Request = req
	return ctx, engine, w
}

func setup(Conf *config.Config) {
	dummyCtx := context.TODO()
	logger := logrus.New()
	level, _ := logrus.ParseLevel(Conf.LogLevel)
	logger.SetLevel(level)

	conn := helper.NewRedisClient(&Conf.AdminRedis, nil)
	err := conn.Ping(dummyCtx).Err()
	if err != nil {
		panic(fmt.Sprintf("Failed to ping: %s", err))
	}
	err = conn.FlushDB(dummyCtx).Err()
	if err != nil {
		panic(fmt.Sprintf("Failed to flush db: %s", err))
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

	if err := redis_engine.Setup(Conf); err != nil {
		panic(fmt.Sprintf("Failed to setup redis engine: %s", err))
	}
	if err := redis_v2.Setup(Conf); err != nil {
		panic(fmt.Sprintf("Failed to setup redis v2 engine: %s", err))
	}
	if engine.GetEngine(config.DefaultPoolName) == nil {
		panic("missing default pool")
	}

	if err := auth.Setup(Conf); err != nil {
		panic(fmt.Sprintf("Failed to setup auth module: %s", err))
	}
	if err := throttler.Setup(&Conf.AdminRedis, logger); err != nil {
		panic(fmt.Sprintf("Failed to setup throttler module: %s", err))
	}
	handlers.Setup(logger)
	handlers.SetupParamDefaults(Conf)
}

func runAllTests(m *testing.M, version string) {
	presetConfig, err := config.CreatePresetForTest(version)
	if err != nil {
		panic(fmt.Sprintf("CreatePresetForTest failed with error: %s", err))
	}

	setup(presetConfig.Config)
	ret := m.Run()
	if ret != 0 {
		os.Exit(ret)
	}
	engine.Shutdown()
	presetConfig.Destroy()
}

func TestMain(m *testing.M) {
	logger := logrus.New()
	redis_engine.SetLogger(logger)
	redis_v2.SetLogger(logger)

	runAllTests(m, "")
	runAllTests(m, redis_v2.VersionV2)
}
