package log

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestFire(t *testing.T) {
	var buf bytes.Buffer
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetOutput(&buf)
	logger.Hooks.Add(NewBackTrackHook(logrus.DebugLevel))
	logger.SetLevel(logrus.DebugLevel)
	logger.Error("test backtrace")
	entry := make(map[string]string)
	json.Unmarshal(buf.Bytes(), &entry)
	if !strings.HasSuffix(entry["bt_func"], "TestFire") {
		t.Fatal("bt_func suffix should be TestFunc was expected")
	}
	if !strings.Contains(entry["bt_line"], "backtrack_test.go") {
		t.Fatal("bt_func suffix contains backtrack_test.go was expected")
	}
}

func TestBackTrackFormatter(t *testing.T) {
	logger := logrus.New()
	logger.Hooks.Add(NewBackTrackHook(logrus.DebugLevel))
	logger.SetLevel(logrus.DebugLevel)

	logger.Debug("debug backtrack")
	logger.Error("error backtrack")

	entry := logger.WithField("ctx", "test")
	entry.Warn("debug backtrack")
}
