package handlers_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/meitu/lmstfy/auth"
	"github.com/meitu/lmstfy/config"
	redis_engine "github.com/meitu/lmstfy/engine/redis"
	"github.com/meitu/lmstfy/helper"
	"github.com/meitu/lmstfy/server/handlers"
	"github.com/sirupsen/logrus"
)

func ginTest(req *http.Request) (*gin.Context, *gin.Engine, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	gin.SetMode(gin.ReleaseMode)
	ctx, engine := gin.CreateTestContext(w)
	ctx.Request = req
	return ctx, engine, w
}

var (
	CONF *config.Config
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
}

func setup() {
	logger := logrus.New()
	level, _ := logrus.ParseLevel(CONF.LogLevel)
	logger.SetLevel(level)
	redis_engine.Setup(CONF, logger)
	auth.Setup(CONF)
	handlers.SetupParamDefaults(CONF)
	handlers.Setup(logger)
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
}

func TestMain(m *testing.M) {
	setup()
	ret := m.Run()
	os.Exit(ret)
}
