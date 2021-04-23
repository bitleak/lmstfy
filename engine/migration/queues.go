package migration

import "github.com/bitleak/lmstfy/engine"

type Queues struct {
	meta []engine.QueueMeta
	e    *Engine
}

func (qs Queues) Consume(ttrSecond, timeoutSecond uint32) (job engine.Job, err error) {
	oldQueues, err := qs.e.oldEngine.Queues(qs.meta)
	if err != nil {
		return job, err
	}

	newQueues, err := qs.e.newEngine.Queues(qs.meta)
	if err != nil {
		return job, err
	}
	job, err = oldQueues.Consume(ttrSecond, 0)
	if job != nil {
		return // During migration, we always prefer the old engine's data as we need to drain it
	}
	return newQueues.Consume(ttrSecond, timeoutSecond)
}

func (qs Queues) BatchConsume(count, ttrSecond, timeoutSecond uint32) (jobs []engine.Job, err error) {
	oldQueues, err := qs.e.oldEngine.Queues(qs.meta)
	if err != nil {
		return jobs, err
	}
	jobs, err = oldQueues.BatchConsume(count, ttrSecond, 0)
	if jobs != nil {
		return // During migration, we always prefer the old engine's data as we need to drain it
	}

	newQueues, err := qs.e.newEngine.Queues(qs.meta)
	if err != nil {
		return jobs, err
	}
	return newQueues.BatchConsume(count, ttrSecond, timeoutSecond)
}
