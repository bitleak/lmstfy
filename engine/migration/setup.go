package migration

import (
	"fmt"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	"github.com/sirupsen/logrus"
)

var logger *logrus.Logger

func Setup(conf *config.Config, l *logrus.Logger) error {
	logger = l
	for redisPool, poolConf := range conf.Pool {
		if poolConf.MigrateTo != "" {
			oldEngine := engine.GetEngineByKind("redis", redisPool)
			newEngine := engine.GetEngineByKind("redis", poolConf.MigrateTo)
			if newEngine == nil {
				return fmt.Errorf("invalid pool [%s] to migrate to", poolConf.MigrateTo)
			}
			engine.Register("migration", redisPool, NewEngine(oldEngine, newEngine))
		}
	}
	return nil
}
