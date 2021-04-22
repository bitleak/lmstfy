package redis

import "github.com/bitleak/lmstfy/engine"

// Queue is the "ready queue" that has all the jobs that can be consumed right now
type Queues struct {
	meta []engine.QueueMeta
	e    *Engine
}

// Consume multiple queues under the same namespace. the queue order implies priority:
// the first queue in the list is of the highest priority when that queue has job ready to
// be consumed. if none of the queues has any job, then consume wait for any queue that
// has job first.
func (qs Queues) Consume(ttrSecond, timeoutSecond uint32) (job engine.Job, err error) {
	return poll(qs.e, qs.meta, ttrSecond, timeoutSecond)
}

func (qs Queues) BatchConsume(count, ttrSecond, timeoutSecond uint32) (jobs []engine.Job, err error) {
	jobs = make([]engine.Job, 0)
	// timeout is 0 to fast check whether there is any job in the ready queue,
	// if any, we wouldn't be blocked until the new job was published.
	for i := uint32(0); i < count; i++ {
		job, err := qs.Consume(ttrSecond, 0)
		if err != nil {
			return jobs, err
		}
		if job == nil {
			break
		}
		jobs = append(jobs, job)
	}
	// If there is no job and consumed in block mode, wait for a single job and return
	if timeoutSecond > 0 && len(jobs) == 0 {
		job, err := qs.Consume(ttrSecond, timeoutSecond)
		if err != nil {
			return jobs, err
		}
		if job != nil {
			jobs = append(jobs, job)
		}
		return jobs, nil
	}
	return jobs, nil
}
