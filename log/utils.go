package log

import (
	"fmt"
	"os"
	"path"

	"github.com/sirupsen/logrus"
)

var (
	globalLogger *logrus.Logger
	accessLogger *logrus.Logger
)

func ReopenLogs(logDir string) error {
	if logDir == "" {
		return nil
	}
	accessLog, err := os.OpenFile(path.Join(logDir, "access.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	errorLog, err := os.OpenFile(path.Join(logDir, "error.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	oldFd := accessLogger.Out.(*os.File)
	accessLogger.Out = accessLog
	oldFd.Close()

	oldFd = globalLogger.Out.(*os.File)
	globalLogger.Out = errorLog
	oldFd.Close()

	return nil
}

func Setup(logFormat, logDir, logLevel, backtrackLevel string) error {
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return fmt.Errorf("failed to parse log level: %s", err)
	}
	btLevel, err := logrus.ParseLevel(backtrackLevel)
	if err != nil {
		return fmt.Errorf("failed to parse backtrack level: %s", err)
	}
	accessLogger = logrus.New()
	globalLogger = logrus.New()

	if logFormat == "json" {
		accessLogger.SetFormatter(&logrus.JSONFormatter{})
		globalLogger.SetFormatter(&logrus.JSONFormatter{})
	}

	globalLogger.Level = level
	globalLogger.Hooks.Add(NewBackTrackHook(btLevel))
	if logDir == "" {
		accessLogger.Out = os.Stdout
		globalLogger.Out = os.Stderr
		return nil
	}
	accessLog, err := os.OpenFile(path.Join(logDir, "access.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to create access.log: %s", err)
	}
	errorLog, err := os.OpenFile(path.Join(logDir, "error.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to create error.log: %s", err)
	}

	accessLogger.Out = accessLog
	globalLogger.Out = errorLog
	return nil
}

func Get() *logrus.Logger {
	return globalLogger
}

func GetAccessLogger() *logrus.Logger {
	return accessLogger
}
