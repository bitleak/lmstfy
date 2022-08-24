package pumper

import (
	"github.com/bitleak/lmstfy/datamanager/storage"
	"github.com/bitleak/lmstfy/engine"
)

type Pumper interface {
	// LoopPump periodically checks job data ready time and pumps jobs to engine
	LoopPump(st storage.Storage, eng engine.Engine)

	Shutdown()
}
