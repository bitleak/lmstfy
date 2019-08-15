package redis

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	go_redis "github.com/go-redis/redis"
	"github.com/meitu/lmstfy/engine"
	"github.com/meitu/lmstfy/uuid"
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
}

func NewEngine(redisName string, conn *go_redis.Client) engine.Engine {
	redis := &RedisInstance{
		Name: redisName,
		Conn: conn,
	}
	PreloadDeadLetterLuaScript(redis)
	go RedisInstanceMonitor(redis)
	meta := NewMetaManager(redis)
	timer := NewTimer("timer_set", redis, time.Second)
	monitor := NewSizeMonitor(redis, timer, meta.Dump())
	go monitor.Loop()
	return &Engine{
		redis:   redis,
		pool:    NewPool(redis),
		timer:   timer,
		meta:    meta,
		monitor: monitor,
	}
}

func (e *Engine) Publish(namespace, queue string, body []byte, ttlSecond, delaySecond uint32, tries uint16) (jobID string, err error) {
	defer func() {
		if err == nil {
			metrics.publishJobs.WithLabelValues(e.redis.Name).Inc()
			metrics.publishQueueJobs.WithLabelValues(e.redis.Name, namespace, queue).Inc()
		}
	}()
	e.meta.RecordIfNotExist(namespace, queue)
	e.monitor.MonitorIfNotExist(namespace, queue)
	job := engine.NewJob(namespace, queue, body, ttlSecond, delaySecond, tries)
	if tries == 0 {
		return job.ID(), nil
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

func (e *Engine) Consume(namespace, queue string, ttrSecond, timeoutSecond uint32) (job engine.Job, err error) {
	defer func() {
		if job != nil {
			metrics.consumeJobs.WithLabelValues(e.redis.Name).Inc()
			metrics.consumeQueueJobs.WithLabelValues(e.redis.Name, namespace, queue).Inc()
		}
	}()
	q := NewQueue(namespace, queue, e.redis, e.timer)
	for {
		startTime := time.Now().Unix()
		jobID, err := q.Poll(timeoutSecond, ttrSecond)
		if err != nil {
			return nil, fmt.Errorf("queue: %s", err)
		}
		if jobID == "" { // No job available
			return nil, nil
		}
		endTime := time.Now().Unix()
		body, ttl, err := e.pool.Get(namespace, queue, jobID)
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
		// TODO: fix tries
		job = engine.NewJobWithID(namespace, queue, body, ttl, 0, jobID)
		metrics.jobElapsedMS.WithLabelValues(e.redis.Name, namespace, queue).Observe(float64(job.ElapsedMS()))
		return job, err
	}
}

// Consume multiple queues under the same namespace. the queue order implies priority:
// the first queue in the list is of the highest priority when that queue has job ready to
// be consumed. if none of the queues has any job, then consume wait for any queue that
// has job first.
func (e *Engine) ConsumeMulti(namespace string, queues []string, ttrSecond, timeoutSecond uint32) (job engine.Job, err error) {
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
		queueName, jobID, err := PollQueues(e.redis, e.timer, queueNames, timeoutSecond, ttrSecond)
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
		// TODO: fix tries
		job = engine.NewJobWithID(namespace, queueName.Queue, body, ttl, 0, jobID)
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
	if optionalJobID == "" {
		q := NewQueue(namespace, queue, e.redis, e.timer)
		jobID, err = q.Peek()
		switch err {
		case nil:
			// continue
		case engine.ErrNotFound:
			return nil, err
		default:
			return nil, fmt.Errorf("failed to peek queue: %s", err)
		}
	}
	body, ttl, err := e.pool.Get(namespace, queue, jobID)
	if err != nil {
		return nil, err
	}
	// TODO: fix tries. peeking can't get the tries property currently :(.
	return engine.NewJobWithID(namespace, queue, body, ttl, 0, jobID), err
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
	dl := NewDeadLetter(namespace, queue, e.redis)
	return dl.Peek()
}

func (e *Engine) DeleteDeadLetter(namespace, queue string, limit int64) (count int64, err error) {
	dl := NewDeadLetter(namespace, queue, e.redis)
	return dl.Delete(limit)
}

func (e *Engine) RespawnDeadLetter(namespace, queue string, limit, ttlSecond int64) (count int64, err error) {
	dl := NewDeadLetter(namespace, queue, e.redis)
	return dl.Respawn(limit, ttlSecond)
}

func (e *Engine) Shutdown() {
	e.timer.Shutdown()
}

func (e *Engine) DumpInfo(out io.Writer) {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "    ")
	enc.Encode(e.meta.Dump())
}
