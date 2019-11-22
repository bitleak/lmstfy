package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/meitu/lmstfy/auth"
	"github.com/meitu/lmstfy/config"
	"github.com/meitu/lmstfy/engine/migration"
	redis_engine "github.com/meitu/lmstfy/engine/redis"
	"github.com/meitu/lmstfy/log"
	"github.com/meitu/lmstfy/server/handlers"
	"github.com/meitu/lmstfy/version"
	"github.com/sirupsen/logrus"
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
	engine.Use(RequestIDMiddleware, AccessLogMiddleware(accessLogger), gin.RecoveryWithWriter(errorLogger.Out))
	handlers.SetupParamDefaults(conf)
	SetupRoutes(engine, errorLogger, devMode)
	addr := fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	errorLogger.Infof("Server listening at %s", addr)
	// engine.Run(addr)
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
	engine.Use(RequestIDMiddleware, AccessLogMiddleware(accessLogger), gin.RecoveryWithWriter(errorLogger.Out))

	engine.GET("/info", handlers.EngineMetaInfo)
	engine.GET("/version", handlers.Version)
	engine.GET("/metrics", handlers.PrometheusMetrics)
	engine.GET("/pools", handlers.ListPools)
	engine.GET("/token/:namespace", handlers.ListTokens)
	engine.POST("/token/:namespace", handlers.NewToken)
	engine.DELETE("/token/:namespace/:token", handlers.DeleteToken)
	engine.Any("/debug/pprof/*profile", handlers.PProf)
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

func main() {
	parseFlags()
	if Flags.ShowVersion {
		printVersion()
		return
	}
	conf := config.MustLoad(Flags.ConfFile)
	shutdown := make(chan struct{})
	accessLogger, errorLogger := log.SetupLogger(conf.LogDir, conf.LogLevel, Flags.BackTrackLevel)
	registerSignal(shutdown, func() {
		log.ReopenLogs(conf.LogDir, accessLogger, errorLogger)
	})
	redis_engine.Setup(conf, errorLogger)
	migration.Setup(conf, errorLogger)
	auth.Setup(conf)
	apiSrv := apiServer(conf, accessLogger, errorLogger, Flags.SkipVerification)
	adminSrv := adminServer(conf, accessLogger, errorLogger)

	createPidFile(errorLogger)

	<-shutdown
	errorLogger.Infof("[%d] Shutting down...", os.Getpid())
	removePidFile()
	adminSrv.Close() // Admin server does not need to be stopped gracefully
	apiSrv.Shutdown(context.Background())
	errorLogger.Infof("[%d] Bye bye", os.Getpid())
}
