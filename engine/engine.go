package engine

import (
	"io"
)

type Engine interface {
	Queue(meta QueueMeta) Queue
	Queues(metas []QueueMeta) Queues
	DeadLetter(meta QueueMeta) DeadLetter
	Shutdown()
	DumpInfo(output io.Writer) error
}

type Queue interface {
	Publish(job Job) (jobID string, err error)
	Consumable
	Delete(jobID string) error
	Peek(optionalJobID string) (job Job, err error)
	Size() (size int64, err error)
	Destroy() (count int64, err error)
}

type Queues interface {
	Consumable
}

type Consumable interface {
	Consume(ttrSecond, timeoutSecond uint32) (job Job, err error)
	BatchConsume(count, ttrSecond, timeoutSecond uint32) (jobs []Job, err error)
}

type DeadLetter interface {
	Peek() (size int64, jobID string, err error)
	Delete(limit int64) (count int64, err error)
	Respawn(limit, ttlSecond int64) (count int64, err error)
	Size() (size int64, err error)
}
