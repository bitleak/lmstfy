package migration

import (
	"fmt"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	"github.com/sirupsen/logrus"
)

var logger *logrus.Logger

func SetLogger(l *logrus.Logger) {
	logger = l
}

func Setup(conf *config.Config) error {
	for redisPool, poolConf := range conf.Pool {
		if poolConf.MigrateTo != "" {
			oldEngine := engine.GetEngineByKind(engine.KindRedis, redisPool)
			newEngine := engine.GetEngineByKind(engine.KindRedis, poolConf.MigrateTo)
			if newEngine == nil {
				return fmt.Errorf("invalid pool [%s] to migrate to", poolConf.MigrateTo)
			}
			engine.Register(engine.KindMigration, redisPool, NewEngine(oldEngine, newEngine))
		}
	}
	return nil
}
