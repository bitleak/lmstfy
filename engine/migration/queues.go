package migration

import "github.com/bitleak/lmstfy/engine"

type Queues struct {
	meta []engine.QueueMeta
	e    *Engine
}

func (qs Queues) Consume(ttrSecond, timeoutSecond uint32) (job engine.Job, err error) {
	job, err = qs.e.oldEngine.Queues(qs.meta).Consume(ttrSecond, 0)
	if job != nil {
		return // During migration, we always prefer the old engine's data as we need to drain it
	}
	return qs.e.newEngine.Queues(qs.meta).Consume(ttrSecond, timeoutSecond)
}

func (qs Queues) BatchConsume(count, ttrSecond, timeoutSecond uint32) (jobs []engine.Job, err error) {
	jobs, err = qs.e.oldEngine.Queues(qs.meta).BatchConsume(count, ttrSecond, 0)
	if jobs != nil {
		return // During migration, we always prefer the old engine's data as we need to drain it
	}
	return qs.e.newEngine.Queues(qs.meta).BatchConsume(count, ttrSecond, timeoutSecond)
}
