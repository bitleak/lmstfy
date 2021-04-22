package migration

import "github.com/bitleak/lmstfy/engine"

type Queue struct {
	meta engine.QueueMeta
	e    *Engine
}

func (q Queue) Publish(job engine.Job) (jobID string, err error) {
	return q.e.newEngine.Queue(q.meta).Publish(job)
}

func (q Queue) Consume(ttrSecond, timeoutSecond uint32) (job engine.Job, err error) {
	job, err = q.e.oldEngine.Queue(q.meta).Consume(ttrSecond, 0)
	if job != nil {
		return // During migration, we always prefer the old engine's data as we need to drain it
	}
	return q.e.newEngine.Queue(q.meta).Consume(ttrSecond, timeoutSecond)
}

func (q Queue) BatchConsume(count, ttrSecond, timeoutSecond uint32) (jobs []engine.Job, err error) {
	jobs, err = q.e.oldEngine.Queue(q.meta).BatchConsume(count, ttrSecond, 0)
	if jobs != nil {
		return // During migration, we always prefer the old engine's data as we need to drain it
	}
	return q.e.newEngine.Queue(q.meta).BatchConsume(count, ttrSecond, timeoutSecond)
}

func (q Queue) Delete(jobID string) error {
	err := q.e.oldEngine.Queue(q.meta).Delete(jobID)
	if err != nil {
		return err
	}
	return q.e.newEngine.Queue(q.meta).Delete(jobID)
}

func (q Queue) Peek(optionalJobID string) (job engine.Job, err error) {
	job, err = q.e.oldEngine.Queue(q.meta).Peek(optionalJobID)
	if job != nil {
		return
	}
	return q.e.newEngine.Queue(q.meta).Peek(optionalJobID)
}

func (q Queue) Size() (size int64, err error) {
	size1, err := q.e.oldEngine.Queue(q.meta).Size()
	if err != nil {
		return 0, err
	}
	size2, err := q.e.newEngine.Queue(q.meta).Size()
	return size1 + size2, err
}

func (q Queue) Destroy() (count int64, err error) {
	count1, err := q.e.oldEngine.Queue(q.meta).Destroy()
	if err != nil {
		return 0, err
	}
	count2, err := q.e.newEngine.Queue(q.meta).Destroy()
	return count1 + count2, err
}
