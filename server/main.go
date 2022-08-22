package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/bitleak/lmstfy/auth"
	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/engine/migration"
	redis_engine "github.com/bitleak/lmstfy/engine/redis"
	"github.com/bitleak/lmstfy/engine/redis_v2"
	"github.com/bitleak/lmstfy/helper"
	"github.com/bitleak/lmstfy/log"
	"github.com/bitleak/lmstfy/server/handlers"
	"github.com/bitleak/lmstfy/server/middleware"
	"github.com/bitleak/lmstfy/throttler"
	"github.com/bitleak/lmstfy/version"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"go.uber.org/automaxprocs/maxprocs"
)

type optionFlags struct {
	ConfFile         string
	PidFile          string
	ShowVersion      bool
	BackTrackLevel   string
	SkipVerification bool
}

var (
	Flags optionFlags
)

func registerSignal(shutdown chan struct{}, logsReopenCallback func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, []os.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1}...)
	go func() {
		for sig := range c {
			if handleSignals(sig, logsReopenCallback) {
				close(shutdown)
				return
			}
		}
	}()
}

func handleSignals(sig os.Signal, logsReopenCallback func()) (exitNow bool) {
	switch sig {
	case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM:
		return true
	case syscall.SIGUSR1:
		logsReopenCallback()
		return false
	}
	return false
}

func parseFlags() {
	flagSet := flag.NewFlagSet("lmstfy", flag.ExitOnError)
	flagSet.StringVar(&Flags.ConfFile, "c", "conf/config.toml", "config file path")
	flagSet.BoolVar(&Flags.ShowVersion, "v", false, "show current version")
	flagSet.StringVar(&Flags.BackTrackLevel, "bt", "warn", "show backtrack in the log >= {level}")
	flagSet.BoolVar(&Flags.SkipVerification, "sv", false, "dev mode, used to bypass token verification")
	flagSet.StringVar(&Flags.PidFile, "p", "running.pid", "pid file path")
	flagSet.Parse(os.Args[1:])
}

func printVersion() {
	fmt.Printf("version: %s\nbuilt at: %s\ncommit: %s\n", version.Version, version.BuildDate, version.BuildCommit)
}

func apiServer(conf *config.Config, accessLogger, errorLogger *logrus.Logger, devMode bool) *http.Server {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(
		middleware.RequestIDMiddleware,
		middleware.AccessLogMiddleware(accessLogger),
		handlers.CollectMetrics,
		gin.RecoveryWithWriter(errorLogger.Out),
	)
	handlers.SetupParamDefaults(conf)
	err := throttler.Setup(&conf.AdminRedis, errorLogger)
	if err != nil {
		errorLogger.Errorf("Failed to create throttler, err: %s", err.Error())
	}
	SetupRoutes(engine, errorLogger, devMode)
	addr := fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	errorLogger.Infof("Server listening at %s", addr)
	srv := http.Server{
		Addr:    addr,
		Handler: engine,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				return
			}
			panic(fmt.Sprintf("API server failed: %s", err))
		}
	}()
	return &srv
}

func adminServer(conf *config.Config, accessLogger *logrus.Logger, errorLogger *logrus.Logger) *http.Server {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(
		middleware.RequestIDMiddleware,
		middleware.AccessLogMiddleware(accessLogger),
		gin.RecoveryWithWriter(errorLogger.Out))
	SetupAdminRoutes(engine, conf.Accounts)
	errorLogger.Infof("Admin port %d", conf.AdminPort)
	srv := http.Server{
		Addr:    fmt.Sprintf("%s:%d", conf.AdminHost, conf.AdminPort),
		Handler: engine,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				return
			}
			panic(fmt.Sprintf("Admin server failed: %s", err))
		}
	}()
	return &srv
}

func createPidFile(logger *logrus.Logger) {
	f, err := os.OpenFile(Flags.PidFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic("failed to create pid file")
	}
	io.WriteString(f, fmt.Sprintf("%d", os.Getpid()))
	f.Close()
	logger.Infof("Server pid: %d", os.Getpid())
}

func removePidFile() {
	os.Remove(Flags.PidFile)
}

func postValidateConfig(ctx context.Context, conf *config.Config) error {
	for name, poolConf := range conf.Pool {
		if err := helper.ValidateRedisConfig(ctx, &poolConf); err != nil {
			return fmt.Errorf("validate pool[%s] err: %w", name, err)
		}
	}
	if err := helper.ValidateRedisConfig(ctx, &conf.AdminRedis); err != nil {
		return fmt.Errorf("validate admin redis err: %w", err)
	}
	return nil
}

func setupEngines(conf *config.Config, l *logrus.Logger) error {
	redis_engine.SetLogger(l)
	if err := redis_engine.Setup(conf); err != nil {
		return fmt.Errorf("%w in redis engine", err)
	}
	redis_v2.SetLogger(l)
	if err := redis_v2.Setup(conf); err != nil {
		return fmt.Errorf("%w in redis v2 engine", err)
	}
	migration.SetLogger(l)
	if err := migration.Setup(conf); err != nil {
		return fmt.Errorf("%w in migration engine", err)
	}
	if engine.GetEngine(config.DefaultPoolName) == nil {
		return errors.New("missing default pool")
	}
	return nil
}

func main() {
	parseFlags()
	if Flags.ShowVersion {
		printVersion()
		return
	}
	conf, err := config.MustLoad(Flags.ConfFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to load config file: %s", err))
	}
	if err := postValidateConfig(context.Background(), conf); err != nil {
		panic(fmt.Sprintf("Failed to post validate the config file: %s", err))
	}
	shutdown := make(chan struct{})
	accessLogger, errorLogger, err := log.SetupLogger(conf.LogFormat, conf.LogDir, conf.LogLevel, Flags.BackTrackLevel)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup logger: %s", err))
	}
	maxprocs.Logger(func(format string, args ...interface{}) {
		errorLogger.Infof(format, args...)
	})
	registerSignal(shutdown, func() {
		log.ReopenLogs(conf.LogDir, accessLogger, errorLogger)
	})
	if err := setupEngines(conf, errorLogger); err != nil {
		panic(fmt.Sprintf("Failed to setup engines, err: %s", err.Error()))
	}
	if err := auth.Setup(conf); err != nil {
		panic(fmt.Sprintf("Failed to setup auth module: %s", err))
	}
	if conf.EnableAccessLog {
		middleware.EnableAccessLog()
	}
	apiSrv := apiServer(conf, accessLogger, errorLogger, Flags.SkipVerification)
	adminSrv := adminServer(conf, accessLogger, errorLogger)

	createPidFile(errorLogger)

	<-shutdown
	errorLogger.Infof("[%d] Shutting down...", os.Getpid())
	removePidFile()
	adminSrv.Close() // Admin server does not need to be stopped gracefully
	apiSrv.Shutdown(context.Background())

	throttler.GetThrottler().Shutdown()
	errorLogger.Infof("[%d] Bye bye", os.Getpid())
}
