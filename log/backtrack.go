package log

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
)

type BackTrackHook struct {
	level logrus.Level // trigger hook, only log level is severer than or equal to this param
}

func (bt *BackTrackHook) Levels() []logrus.Level {
	levels := make([]logrus.Level, 0)
	for _, l := range logrus.AllLevels {
		if l <= bt.level {
			levels = append(levels, l)
		}
	}
	return levels
}

func (bt *BackTrackHook) Fire(entry *logrus.Entry) error {
	pcs := make([]uintptr, 5)
	n := runtime.Callers(8, pcs)
	if n == 0 {
		return nil
	}
	frames := runtime.CallersFrames(pcs[:n])
	file := "unknown"
	line := 0
	funcName := "unknown"
	for {
		frame, more := frames.Next()
		if strings.Index(frame.Function, "github.com/sirupsen/logrus") == -1 {
			// This if the frame we are looking for
			file = frame.File
			line = frame.Line
			funcName = frame.Function
			break
		}
		if !more {
			// no more frames
			break
		}
	}
	// add backtrack info
	entry.Data["bt_line"] = fmt.Sprintf("%s:%d", file, line)
	entry.Data["bt_func"] = funcName
	return nil
}

func NewBackTrackHook(filteredLevel logrus.Level) logrus.Hook {
	return &BackTrackHook{filteredLevel}
}
