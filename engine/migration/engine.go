package migration

import (
	"io"

	"github.com/bitleak/lmstfy/engine"
)

type Engine struct {
	oldEngine engine.Engine
	newEngine engine.Engine
}

func NewEngine(old, new engine.Engine) engine.Engine {
	return &Engine{
		oldEngine: old,
		newEngine: new,
	}
}

func (e *Engine) Publish(namespace, queue string, body []byte, ttlSecond, delaySecond uint32, tries uint16) (jobID string, err error) {
	return e.newEngine.Publish(namespace, queue, body, ttlSecond, delaySecond, tries)
}

// BatchConsume consume some jobs of a queue
func (e *Engine) BatchConsume(namespace, queue string, count, ttrSecond, timeoutSecond uint32) (jobs []engine.Job, err error) {
	jobs, err = e.oldEngine.BatchConsume(namespace, queue, count, ttrSecond, 0)
	if len(jobs) != 0 {
		return // During migration, we always prefer the old engine's data as we need to drain it
	}
	return e.newEngine.BatchConsume(namespace, queue, count, ttrSecond, timeoutSecond)
}

func (e *Engine) Consume(namespace, queue string, ttrSecond, timeoutSecond uint32) (job engine.Job, err error) {
	job, err = e.oldEngine.Consume(namespace, queue, ttrSecond, 0)
	if job != nil {
		return // During migration, we always prefer the old engine's data as we need to drain it
	}
	return e.newEngine.Consume(namespace, queue, ttrSecond, timeoutSecond)
}

func (e *Engine) ConsumeMulti(namespace string, queues []string, ttrSecond, timeoutSecond uint32) (job engine.Job, err error) {
	job, err = e.oldEngine.ConsumeMulti(namespace, queues, ttrSecond, 1)
	if job != nil {
		return // During migration, we always prefer the old engine's data as we need to drain it
	}
	return e.newEngine.ConsumeMulti(namespace, queues, ttrSecond, timeoutSecond)
}

func (e *Engine) Delete(namespace, queue, jobID string) error {
	err := e.oldEngine.Delete(namespace, queue, jobID)
	if err != nil {
		return err
	}
	return e.newEngine.Delete(namespace, queue, jobID)
}

func (e *Engine) Peek(namespace, queue, optionalJobID string) (job engine.Job, err error) {
	job, err = e.oldEngine.Peek(namespace, queue, optionalJobID)
	if job != nil {
		return
	}
	return e.newEngine.Peek(namespace, queue, optionalJobID)
}

func (e *Engine) Size(namespace, queue string) (size int64, err error) {
	size1, err := e.oldEngine.Size(namespace, queue)
	if err != nil {
		return 0, err
	}
	size2, err := e.newEngine.Size(namespace, queue)
	return size1 + size2, err
}

func (e *Engine) Destroy(namespace, queue string) (count int64, err error) {
	count1, err := e.oldEngine.Destroy(namespace, queue)
	if err != nil {
		return 0, err
	}
	count2, err := e.newEngine.Destroy(namespace, queue)
	return count1 + count2, err
}

func (e *Engine) PeekDeadLetter(namespace, queue string) (size int64, jobID string, err error) {
	size1, jobID1, err := e.oldEngine.PeekDeadLetter(namespace, queue)
	if err != nil && err != engine.ErrNotFound {
		return 0, "", err
	}
	size2, jobID2, err := e.newEngine.PeekDeadLetter(namespace, queue)
	if err != nil {
		return 0, "", err
	}
	if size1 == 0 {
		return size2, jobID2, nil
	} else {
		return size1 + size2, jobID1, nil // If both engines has deadletter, return the old engine's job ID
	}
}

func (e *Engine) DeleteDeadLetter(namespace, queue string, limit int64) (count int64, err error) {
	count1, err := e.oldEngine.DeleteDeadLetter(namespace, queue, limit)
	if err != nil {
		return 0, err
	}
	count2, err := e.newEngine.DeleteDeadLetter(namespace, queue, limit-count1)
	return count1 + count2, err
}

func (e *Engine) RespawnDeadLetter(namespace, queue string, limit, ttlSecond int64) (count int64, err error) {
	count1, err := e.oldEngine.RespawnDeadLetter(namespace, queue, limit, ttlSecond)
	if err != nil {
		return 0, err
	}
	count2, err := e.newEngine.RespawnDeadLetter(namespace, queue, limit-count1, ttlSecond)
	return count1 + count2, err
}

// SizeOfDeadLetter return the queue size of dead letter
func (e *Engine) SizeOfDeadLetter(namespace, queue string) (size int64, err error) {
	size1, err := e.oldEngine.SizeOfDeadLetter(namespace, queue)
	if err != nil {
		return 0, err
	}
	size2, err := e.newEngine.SizeOfDeadLetter(namespace, queue)
	return size1 + size2, err
}

func (e *Engine) Shutdown() {
	e.oldEngine.Shutdown()
	e.newEngine.Shutdown()
}

func (e *Engine) DumpInfo(output io.Writer) error {
	return e.newEngine.DumpInfo(output)
}
