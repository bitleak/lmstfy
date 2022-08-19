package helper

import (
	"fmt"
	"os"
	"testing"

	"github.com/bitleak/lmstfy/config"
)

var (
	CONF *config.Config
)

func TestMain(m *testing.M) {
	presetConfig, err := config.CreatePresetForTest()
	if err != nil {
		panic(fmt.Sprintf("CreatePresetForTest failed with error: %s", err))
	}
	defer presetConfig.Destroy()
	CONF = presetConfig.Config
	ret := m.Run()
	os.Exit(ret)
}
