package redis

import (
	"errors"

	"github.com/bitleak/lmstfy/engine"
)

type QueueName struct {
	Namespace string
	Queue     string
}

func (k *QueueName) String() string {
	return join(QueuePrefix, k.Namespace, k.Queue)
}

func (k *QueueName) Decode(str string) error {
	splits := splits(3, str)
	if len(splits) != 3 || splits[0] != QueuePrefix {
		return errors.New("invalid format")
	}
	k.Namespace = splits[1]
	k.Queue = splits[2]
	return nil
}

type Queue interface {
	Name() string
	Push(j engine.Job, tries uint16) error
	Poll(timeoutSecond, ttrSecond uint32) (jobID string, tries uint16, err error)
	PollWithFrozenTries(timeoutSecond, ttrSecond uint32) (jobID string, tries uint16, err error)
	Size() (size int64, err error)
	Peek() (jobID string, tries uint16, err error)
	Destroy() (count int64, err error)
}
