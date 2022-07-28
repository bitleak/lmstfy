package helper

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
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
}

func TestMain(m *testing.M) {
	ret := m.Run()
	os.Exit(ret)
}
