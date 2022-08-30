package engine

import (
	"io"
)

type Engine interface {
	Publish(namespace, queue string, body []byte, ttlSecond, delaySecond uint32, tries uint16) (jobID string, err error)
	Consume(namespace string, queues []string, ttrSecond, timeoutSecond uint32) (job Job, err error)
	BatchConsume(namespace string, queues []string, count, ttrSecond, timeoutSecond uint32) (jobs []Job, err error)
	Delete(namespace, queue, jobID string) error
	Peek(namespace, queue, optionalJobID string) (job Job, err error)
	Size(namespace, queue string) (size int64, err error)
	Destroy(namespace, queue string) (count int64, err error)

	// Dead letter
	PeekDeadLetter(namespace, queue string) (size int64, jobID string, err error)
	DeleteDeadLetter(namespace, queue string, limit int64) (count int64, err error)
	RespawnDeadLetter(namespace, queue string, limit, ttlSecond int64) (count int64, err error)
	SizeOfDeadLetter(namespace, queue string) (size int64, err error)

	Shutdown()

	DumpInfo(output io.Writer) error
	PublishWithJobID(namespace, queue, storedJobID string, body []byte, ttlSecond, delaySecond uint32, tries uint16) (jobID string, err error)
}
