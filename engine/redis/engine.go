package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	go_redis "github.com/go-redis/redis/v8"

	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/uuid"
)

type RedisInstance struct {
	Name string
	Conn *go_redis.Client
}

// Engine that connects all the dots including:
// - store jobs to timer set or ready queue
// - deliver jobs to clients
// - manage dead letters
type Engine struct {
	redis   *RedisInstance
	pool    *Pool
	timer   *Timer
	meta    *MetaManager
	monitor *SizeMonitor
	// number of seconds. when job's delay second is greater than pumpStorageThresh,
	//it will be written to storage if enabled
	storageThresh uint32
}

func NewEngine(redisName string, conn *go_redis.Client) (engine.Engine, error) {
	redis := &RedisInstance{
		Name: redisName,
		Conn: conn,
	}
	if err := PreloadDeadLetterLuaScript(redis); err != nil {
		return nil, err
	}
	if err := PreloadQueueLuaScript(redis); err != nil {
		return nil, err
	}
	go RedisInstanceMonitor(redis)
	meta := NewMetaManager(redis)
	timer, err := NewTimer("timer_set", redis, time.Second)
	if err != nil {
		return nil, err
	}
	metadata, err := meta.Dump()
	if err != nil {
		return nil, err
	}
	monitor := NewSizeMonitor(redis, timer, metadata)
	go monitor.Loop()
	eng := &Engine{
		redis:   redis,
		pool:    NewPool(redis),
		timer:   timer,
		meta:    meta,
		monitor: monitor,
	}
	return eng, nil
}

func (e *Engine) Publish(job engine.Job) (jobID string, err error) {
	namespace, queue, delaySecond, tries := job.Namespace(), job.Queue(), job.Delay(), job.Tries()
	defer func() {
		if err == nil {
			metrics.publishJobs.WithLabelValues(e.redis.Name).Inc()
			metrics.publishQueueJobs.WithLabelValues(e.redis.Name, namespace, queue).Inc()
		}
	}()
	e.meta.RecordIfNotExist(namespace, queue)
	e.monitor.MonitorIfNotExist(namespace, queue)
	if tries == 0 {
		return job.ID(), errors.New("invalid job: tries cannot be zero")
	}

	err = e.pool.Add(job)
	if err != nil {
		return job.ID(), fmt.Errorf("pool: %s", err)
	}

	if delaySecond == 0 {
		q := NewQueue(namespace, queue, e.redis, e.timer)
		err = q.Push(job, tries)
		if err != nil {
			err = fmt.Errorf("queue: %s", err)
		}
		return job.ID(), err
	}
	err = e.timer.Add(namespace, queue, job.ID(), delaySecond, tries)
	if err != nil {
		err = fmt.Errorf("timer: %s", err)
	}
	return job.ID(), err
}

// BatchConsume consume some jobs of a queue
func (e *Engine) BatchConsume(namespace string, queues []string, count, ttrSecond, timeoutSecond uint32) (jobs []engine.Job, err error) {
	jobs = make([]engine.Job, 0)
	// timeout is 0 to fast check whether there is any job in the ready queue,
	// if any, we wouldn't be blocked until the new job was published.
	for i := uint32(0); i < count; i++ {
		job, err := e.Consume(namespace, queues, ttrSecond, 0)
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
		job, err := e.Consume(namespace, queues, ttrSecond, timeoutSecond)
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

// Consume multiple queues under the same namespace. the queue order implies priority:
// the first queue in the list is of the highest priority when that queue has job ready to
// be consumed. if none of the queues has any job, then consume wait for any queue that
// has job first.
func (e *Engine) Consume(namespace string, queues []string, ttrSecond, timeoutSecond uint32) (job engine.Job, err error) {
	return e.consumeMulti(namespace, queues, ttrSecond, timeoutSecond)
}

func (e *Engine) consumeMulti(namespace string, queues []string, ttrSecond, timeoutSecond uint32) (job engine.Job, err error) {
	defer func() {
		if job != nil {
			metrics.consumeMultiJobs.WithLabelValues(e.redis.Name).Inc()
			metrics.consumeQueueJobs.WithLabelValues(e.redis.Name, namespace, job.Queue()).Inc()
		}
	}()
	queueNames := make([]QueueName, len(queues))
	for i, q := range queues {
		queueNames[i].Namespace = namespace
		queueNames[i].Queue = q
	}
	for {
		startTime := time.Now().Unix()
		queueName, jobID, tries, err := PollQueues(e.redis, e.timer, queueNames, timeoutSecond, ttrSecond)
		if err != nil {
			return nil, fmt.Errorf("queue: %s", err)
		}
		if jobID == "" {
			return nil, nil
		}
		endTime := time.Now().Unix()
		body, ttl, err := e.pool.Get(namespace, queueName.Queue, jobID)
		switch err {
		case nil:
			// no-op
		case engine.ErrNotFound:
			timeoutSecond = timeoutSecond - uint32(endTime-startTime)
			if timeoutSecond > 0 {
				// This can happen if the job's delay time is larger than job's ttl,
				// so when the timer fires the job ID, the actual job data is long gone.
				// When so, we should use what's left in the timeoutSecond to keep on polling.
				//
				// Other scene is: A consumer DELETE the job _after_ TTR, and B consumer is
				// polling on the queue, and get notified to retry the job, but only to find that
				// job was deleted by A.
				continue
			} else {
				return nil, nil
			}
		default:
			return nil, fmt.Errorf("pool: %s", err)
		}
		job = engine.NewJobWithID(namespace, queueName.Queue, body, ttl, tries, jobID)
		metrics.jobElapsedMS.WithLabelValues(e.redis.Name, namespace, queueName.Queue).Observe(float64(job.ElapsedMS()))
		return job, nil
	}
}

func (e *Engine) Delete(namespace, queue, jobID string) error {
	err := e.pool.Delete(namespace, queue, jobID)
	if err == nil {
		elapsedMS, _ := uuid.ElapsedMilliSecondFromUniqueID(jobID)
		metrics.jobAckElapsedMS.WithLabelValues(e.redis.Name, namespace, queue).Observe(float64(elapsedMS))
	}
	return err
}

func (e *Engine) Peek(namespace, queue, optionalJobID string) (job engine.Job, err error) {
	jobID := optionalJobID
	var tries uint16
	if optionalJobID == "" {
		q := NewQueue(namespace, queue, e.redis, e.timer)
		jobID, tries, err = q.Peek()
		switch err {
		case nil:
			// continue
		case engine.ErrNotFound:
			return nil, engine.ErrEmptyQueue
		default:
			return nil, fmt.Errorf("failed to peek queue: %s", err)
		}
	}
	body, ttl, err := e.pool.Get(namespace, queue, jobID)
	// Tricky: we shouldn't return the not found error when the job was not found,
	// since the job may expired(TTL was reached) and it would confuse the user, so
	// we return the nil job instead of the not found error here. But if the `optionalJobID`
	// was assigned we should return the not fond error.
	if optionalJobID == "" && err == engine.ErrNotFound {
		// return jobID with nil body if the job is expired
		return engine.NewJobWithID(namespace, queue, nil, 0, 0, jobID), nil
	}
	if err != nil {
		return nil, err
	}
	return engine.NewJobWithID(namespace, queue, body, ttl, tries, jobID), err
}

func (e *Engine) Size(namespace, queue string) (size int64, err error) {
	q := NewQueue(namespace, queue, e.redis, e.timer)
	return q.Size()
}

func (e *Engine) Destroy(namespace, queue string) (count int64, err error) {
	e.meta.Remove(namespace, queue)
	e.monitor.Remove(namespace, queue)
	q := NewQueue(namespace, queue, e.redis, e.timer)
	return q.Destroy()
}

func (e *Engine) PeekDeadLetter(namespace, queue string) (size int64, jobID string, err error) {
	dl, err := NewDeadLetter(namespace, queue, e.redis)
	if err != nil {
		return 0, "", err
	}
	return dl.Peek()
}

func (e *Engine) DeleteDeadLetter(namespace, queue string, limit int64) (count int64, err error) {
	dl, err := NewDeadLetter(namespace, queue, e.redis)
	if err != nil {
		return 0, err
	}
	return dl.Delete(limit)
}

func (e *Engine) RespawnDeadLetter(namespace, queue string, limit, ttlSecond int64) (count int64, err error) {
	dl, err := NewDeadLetter(namespace, queue, e.redis)
	if err != nil {
		return 0, err
	}
	return dl.Respawn(limit, ttlSecond)
}

// SizeOfDeadLetter return the queue size of dead letter
func (e *Engine) SizeOfDeadLetter(namespace, queue string) (size int64, err error) {
	dl, err := NewDeadLetter(namespace, queue, e.redis)
	if err != nil {
		return 0, err
	}
	return dl.Size()
}

func (e *Engine) Shutdown() {
	e.timer.Shutdown()
}

func (e *Engine) DumpInfo(out io.Writer) error {
	metadata, err := e.meta.Dump()
	if err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "    ")
	return enc.Encode(metadata)
}
