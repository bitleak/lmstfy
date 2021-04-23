package migration

import "github.com/bitleak/lmstfy/engine"

type Queue struct {
	meta engine.QueueMeta
	e    *Engine
}

func (q Queue) Publish(job engine.Job) (jobID string, err error) {
	newQueue, err := q.e.newEngine.Queue(q.meta)
	if err != nil {
		return "", err
	}
	return newQueue.Publish(job)
}

func (q Queue) Consume(ttrSecond, timeoutSecond uint32) (job engine.Job, err error) {
	oldQueue, err := q.e.oldEngine.Queue(q.meta)
	if err != nil {
		return job, err
	}
	job, err = oldQueue.Consume(ttrSecond, 0)
	if job != nil {
		return // During migration, we always prefer the old engine's data as we need to drain it
	}
	newQueue, err := q.e.newEngine.Queue(q.meta)
	if err != nil {
		return job, err
	}
	return newQueue.Consume(ttrSecond, timeoutSecond)
}

func (q Queue) BatchConsume(count, ttrSecond, timeoutSecond uint32) (jobs []engine.Job, err error) {
	oldQueue, err := q.e.oldEngine.Queue(q.meta)
	if err != nil {
		return jobs, err
	}
	jobs, err = oldQueue.BatchConsume(count, ttrSecond, 0)
	if jobs != nil {
		return // During migration, we always prefer the old engine's data as we need to drain it
	}
	newQueue, err := q.e.newEngine.Queue(q.meta)
	if err != nil {
		return jobs, err
	}
	return newQueue.BatchConsume(count, ttrSecond, timeoutSecond)
}

func (q Queue) Delete(jobID string) error {
	oldQueue, err := q.e.oldEngine.Queue(q.meta)
	if err != nil {
		return err
	}
	err = oldQueue.Delete(jobID)
	if err != nil {
		return err
	}
	newQueue, err := q.e.newEngine.Queue(q.meta)
	if err != nil {
		return err
	}
	return newQueue.Delete(jobID)
}

func (q Queue) Peek(optionalJobID string) (job engine.Job, err error) {
	oldQueue, err := q.e.oldEngine.Queue(q.meta)
	if err != nil {
		return job, err
	}
	job, err = oldQueue.Peek(optionalJobID)
	if job != nil {
		return
	}
	newQueue, err := q.e.newEngine.Queue(q.meta)
	if err != nil {
		return job, err
	}
	return newQueue.Peek(optionalJobID)
}

func (q Queue) Size() (size int64, err error) {
	oldQueue, err := q.e.oldEngine.Queue(q.meta)
	if err != nil {
		return 0, err
	}
	size1, err := oldQueue.Size()
	if err != nil {
		return 0, err
	}
	newQueue, err := q.e.newEngine.Queue(q.meta)
	if err != nil {
		return size1, err
	}
	size2, err := newQueue.Size()
	return size1 + size2, err
}

func (q Queue) Destroy() (count int64, err error) {
	oldQueue, err := q.e.oldEngine.Queue(q.meta)
	if err != nil {
		return 0, err
	}
	count1, err := oldQueue.Destroy()
	if err != nil {
		return 0, err
	}
	newQueue, err := q.e.newEngine.Queue(q.meta)
	if err != nil {
		return count1, err
	}
	count2, err := newQueue.Destroy()
	return count1 + count2, err
}
