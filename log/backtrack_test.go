package log

import (
	"testing"

	"github.com/sirupsen/logrus"
)

func TestBackTrackFormatter(t *testing.T) {
	logger := logrus.New()
	logger.Hooks.Add(NewBackTrackHook(logrus.DebugLevel))
	logger.SetLevel(logrus.DebugLevel)

	logger.Debug("debug backtrack")
	logger.Error("error backtrack")

	entry := logger.WithField("ctx", "test")
	entry.Warn("debug backtrack")
}
