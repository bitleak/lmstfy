package datamanager

import (
	"errors"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/datamanager/pumper"
	"github.com/bitleak/lmstfy/datamanager/storage"
	"github.com/bitleak/lmstfy/datamanager/storage/spanner"
	"github.com/bitleak/lmstfy/engine"
)

type DataManager struct {
	Engine  engine.Engine
	Storage storage.Storage
	Pumper  pumper.Pumper
}

func Setup(cfg *config.Config, poolCfg *config.RedisConf, eng engine.Engine) error {
	if eng == nil {
		return errors.New("failed to set up data manager: engine is nil")
	}
	st, err := spanner.NewStorage(cfg.SecondaryStorage)
	if err != nil {
		return err
	}

	pr, err := pumper.NewPumper(cfg, poolCfg)
	if err != nil {
		return err
	}
	go pr.LoopPump(st, eng)
	return nil
}
