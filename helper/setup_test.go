package helper

import (
	"os"
	"testing"

	"github.com/bitleak/lmstfy/config"
)

var (
	CONF *config.Config
)

func TestMain(m *testing.M) {
	presetConfig := config.CreatePresetForTest()
	defer presetConfig.Destroy()
	CONF = presetConfig.Config
	ret := m.Run()
	os.Exit(ret)
}
