package log

import (
	"fmt"
	"os"
	"path"

	"github.com/sirupsen/logrus"
)

// Reopen log fd handlers when receiving signal syscall.SIGUSR1
func ReopenLogs(logDir string, accessLogger, errorLogger *logrus.Logger) error {
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

	oldFd = errorLogger.Out.(*os.File)
	errorLogger.Out = errorLog
	oldFd.Close()

	return nil
}

// @backtrackLevel: log the backtrack info when logging level is >= backtrackLevel
func SetupLogger(logDir, logLevel, backtrackLevel string) (accessLogger *logrus.Logger, errorLogger *logrus.Logger, err error) {
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse log level: %s", err)
	}
	btLevel, err := logrus.ParseLevel(backtrackLevel)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse backtrack level: %s", err)
	}
	accessLogger = logrus.New()
	errorLogger = logrus.New()
	errorLogger.Level = level
	errorLogger.Hooks.Add(NewBackTrackHook(btLevel))
	if logDir == "" {
		accessLogger.Out = os.Stdout
		errorLogger.Out = os.Stderr
		return
	}
	accessLog, err := os.OpenFile(path.Join(logDir, "access.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create access.log: %s", err)
	}
	errorLog, err := os.OpenFile(path.Join(logDir, "error.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create error.log: %s", err)
	}

	accessLogger.Out = accessLog
	errorLogger.Out = errorLog
	return
}
