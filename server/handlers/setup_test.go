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
	redis_engine "github.com/bitleak/lmstfy/engine/redis"
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

	if err := redis_engine.Setup(Conf, logger); err != nil {
		panic(fmt.Sprintf("Failed to setup redis engine: %s", err))
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
