package redis

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	go_redis "github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"

	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/uuid"
)

const (
	luaRPOPMultiQueuesScript = `
	for _, queue in ipairs(KEYS) do
		local v = redis.call("RPOP", queue)
		if v ~= false then
			return {queue, v}
		end
	end
	return {"", ""}
`
)

var rpopMultiQueuesSHA string

func queueMetaToReadyQueue(meta engine.QueueMeta) string {
	return join(QueuePrefix, meta.Namespace, meta.Queue)
}

func readyQueueToQueueMeta(queue string) (*engine.QueueMeta, error) {
	splits := splits(3, queue)
	if len(splits) != 3 || splits[0] != QueuePrefix {
		return nil, errors.New("invalid format")
	}
	meta := &engine.QueueMeta{
		Namespace: splits[1],
		Queue:     splits[2],
	}
	return meta, nil
}

// Queue is the "ready queue" that has all the jobs that can be consumed right now
type Queue struct {
	meta engine.QueueMeta
	e    *Engine

	destroySHA string
}

func (q *Queue) Name() string {
	return queueMetaToReadyQueue(q.meta)
}

func (q Queue) Publish(job engine.Job) (jobID string, err error) {
	defer func() {
		if err == nil {
			metrics.publishJobs.WithLabelValues(q.e.redis.Name).Inc()
			metrics.publishQueueJobs.WithLabelValues(q.e.redis.Name, q.meta.Namespace, q.meta.Queue).Inc()
		}
	}()
	q.e.meta.RecordIfNotExist(q.meta)
	q.e.monitor.MonitorIfNotExist(q.meta)

	err = q.e.pool.Add(job)
	if err != nil {
		return job.ID(), fmt.Errorf("pool: %s", err)
	}

	if job.Delay() == 0 {
		err = q.push(job)
		if err != nil {
			err = fmt.Errorf("queue: %s", err)
		}
		return job.ID(), err
	}
	err = q.e.timer.Add(q.meta.Namespace, q.meta.Queue, job.ID(), job.Delay(), job.Tries())
	if err != nil {
		err = fmt.Errorf("timer: %s", err)
	}
	return job.ID(), err
}

// Push a job into the queue, the job data format: {tries}{job id}
func (q *Queue) push(j engine.Job) error {
	if j.Tries() == 0 {
		return nil
	}
	if j.Namespace() != q.meta.Namespace || j.Queue() != q.meta.Queue {
		// Wrong queue for the job
		return engine.ErrWrongQueue
	}
	metrics.queueDirectPushJobs.WithLabelValues(q.e.redis.Name).Inc()
	val := structPack(j.Tries(), j.ID())
	return q.e.redis.Conn.LPush(dummyCtx, q.Name(), val).Err()
}

// Consume consume a job of the queue
func (q Queue) Consume(ttrSecond, timeoutSecond uint32) (job engine.Job, err error) {
	return poll(q.e, []engine.QueueMeta{q.meta}, ttrSecond, timeoutSecond)
}

// BatchConsume consume some jobs of the queue
func (q Queue) BatchConsume(count, ttrSecond, timeoutSecond uint32) (jobs []engine.Job, err error) {
	jobs = make([]engine.Job, 0)
	// timeout is 0 to fast check whether there is any job in the ready queue,
	// if any, we wouldn't be blocked until the new job was published.
	for i := uint32(0); i < count; i++ {
		job, err := q.Consume(ttrSecond, 0)
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
		job, err := q.Consume(ttrSecond, timeoutSecond)
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

func (q Queue) Delete(jobID string) error {
	err := q.e.pool.Delete(q.meta.Namespace, q.meta.Queue, jobID)
	if err == nil {
		elapsedMS, _ := uuid.ElapsedMilliSecondFromUniqueID(jobID)
		metrics.jobAckElapsedMS.WithLabelValues(q.e.redis.Name, q.meta.Namespace, q.meta.Queue).Observe(float64(elapsedMS))
	}
	return err
}

func (q Queue) Peek(optionalJobID string) (job engine.Job, err error) {
	jobID := optionalJobID
	var tries uint16
	if optionalJobID == "" {
		jobID, tries, err = q.peek()
		switch err {
		case nil:
			// continue
		case engine.ErrNotFound:
			return nil, engine.ErrEmptyQueue
		default:
			return nil, fmt.Errorf("failed to peek queue: %s", err)
		}
	}
	body, ttl, err := q.e.pool.Get(q.meta.Namespace, q.meta.Queue, jobID)
	// Tricky: we shouldn't return the not found error when the job was not found,
	// since the job may expired(TTL was reached) and it would confuse the user, so
	// we return the nil job instead of the not found error here. But if the `optionalJobID`
	// was assigned we should return the not fond error.
	if optionalJobID == "" && err == engine.ErrNotFound {
		// return jobID with nil body if the job is expired
		return engine.NewJobWithID(q.meta, nil, 0, 0, jobID), nil
	}
	if err != nil {
		return nil, err
	}
	return engine.NewJobWithID(q.meta, body, ttl, tries, jobID), err
}

// Peek a right-most element in the list without popping it
func (q *Queue) peek() (jobID string, tries uint16, err error) {
	val, err := q.e.redis.Conn.LIndex(dummyCtx, q.Name(), -1).Result()
	switch err {
	case nil:
		// continue
	case go_redis.Nil:
		return "", 0, engine.ErrNotFound
	default:
		return "", 0, err
	}
	tries, jobID, err = structUnpack(val)
	return jobID, tries, err
}

// Return number of the current in-queue jobs
func (q Queue) Size() (size int64, err error) {
	return q.e.redis.Conn.LLen(dummyCtx, q.Name()).Result()
}

func (q Queue) Destroy() (count int64, err error) {
	q.e.meta.Remove(q.meta)
	q.e.monitor.Remove(q.meta)
	poolPrefix := PoolJobKeyPrefix(q.meta)
	var batchSize int64 = 100
	for {
		val, err := q.e.redis.Conn.EvalSha(dummyCtx, q.destroySHA, []string{q.Name(), poolPrefix}, batchSize).Result()
		if err != nil {
			if isLuaScriptGone(err) {
				if err := PreloadDeadLetterLuaScript(q.e.redis); err != nil {
					logger.WithField("err", err).Error("Failed to load deadletter lua script")
				}
				continue
			}
			return count, err
		}
		n, _ := val.(int64)
		count += n
		if n < batchSize { // Dead letter is empty
			break
		}
	}
	return count, nil
}

func poll(e *Engine, metas []engine.QueueMeta, timeoutSecond, ttrSecond uint32) (job engine.Job, err error) {
	defer func() {
		if job != nil {
			metrics.consumeMultiJobs.WithLabelValues(e.redis.Name).Inc()
			metrics.consumeQueueJobs.WithLabelValues(e.redis.Name, job.Namespace(), job.Queue()).Inc()
		}
	}()
	for {
		startTime := time.Now().Unix()
		meta, jobID, tries, err := pollReady(e, metas, timeoutSecond, ttrSecond)
		if err != nil {
			return nil, fmt.Errorf("queue: %s", err)
		}
		if jobID == "" {
			return nil, nil
		}
		endTime := time.Now().Unix()
		body, ttl, err := e.pool.Get(meta.Namespace, meta.Queue, jobID)
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
		job = engine.NewJobWithID(*meta, body, ttl, tries, jobID)
		metrics.jobElapsedMS.WithLabelValues(e.redis.Name, meta.Namespace, meta.Queue).Observe(float64(job.ElapsedMS()))
		return job, nil
	}
}

func pollReady(e *Engine, metas []engine.QueueMeta, timeoutSecond, ttrSecond uint32) (meta *engine.QueueMeta, jobID string, retries uint16, err error) {
	defer func() {
		if jobID != "" {
			metrics.queuePopJobs.WithLabelValues(e.redis.Name).Inc()
		}
	}()

	var val []string
	keys := make([]string, len(metas))
	for i, meta := range metas {
		keys[i] = queueMetaToReadyQueue(meta)
	}
	if timeoutSecond > 0 { // Blocking poll
		val, err = e.redis.Conn.BRPop(dummyCtx, time.Duration(timeoutSecond)*time.Second, keys...).Result()
	} else { // Non-Blocking fetch
		val = make([]string, 2) // Just to be coherent with BRPop return values
		val[0], val[1], err = popMultiQueues(e.redis, keys)
	}
	switch err {
	case nil:
		// continue
	case go_redis.Nil:
		logger.Debug("Job not found")
		return nil, "", 0, nil
	default:
		logger.WithField("err", err).Error("Failed to pop job from queue")
		return nil, "", 0, err
	}
	meta, err = readyQueueToQueueMeta(val[0])
	if err != nil {
		logger.WithField("err", err).Error("Failed to decode queue meta")
		return nil, "", 0, err
	}
	tries, jobID, err := structUnpack(val[1])
	if err != nil {
		logger.WithField("err", err).Error("Failed to unpack lua struct data")
		return nil, "", 0, err
	}

	if tries == 0 {
		logger.WithFields(logrus.Fields{
			"jobID": jobID,
			"ttr":   ttrSecond,
			"meta":  meta,
		}).Error("Job with tries == 0 appeared")
		return nil, "", 0, fmt.Errorf("Job %s with tries == 0 appeared", jobID)
	}
	err = e.timer.Add(meta.Namespace, meta.Queue, jobID, ttrSecond, tries) // NOTE: tries is not decreased
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":   err,
			"jobID": jobID,
			"ttr":   ttrSecond,
			"meta":  meta,
		}).Error("Failed to add job to timer for ttr")
		return meta, jobID, tries, err
	}
	return meta, jobID, tries, nil
}

func PreloadQueueLuaScript(redis *RedisInstance) error {
	sha, err := redis.Conn.ScriptLoad(dummyCtx, luaRPOPMultiQueuesScript).Result()
	if err != nil {
		return fmt.Errorf("preload rpop multi lua script err: %s", err)
	}
	rpopMultiQueuesSHA = sha
	return nil
}

func popMultiQueues(redis *RedisInstance, queueNames []string) (string, string, error) {
	if len(queueNames) == 1 {
		val, err := redis.Conn.RPop(dummyCtx, queueNames[0]).Result()
		return queueNames[0], val, err
	}
	vals, err := redis.Conn.EvalSha(dummyCtx, rpopMultiQueuesSHA, queueNames).Result()
	if err != nil && isLuaScriptGone(err) {
		if err = PreloadQueueLuaScript(redis); err != nil {
			return "", "", err
		}
		vals, err = redis.Conn.EvalSha(dummyCtx, rpopMultiQueuesSHA, queueNames).Result()
	}
	if err != nil {
		return "", "", err
	}
	fields, ok := vals.([]interface{})
	if !ok || len(fields) != 2 {
		return "", "", errors.New("lua return value should be two elements array")
	}
	queueName, ok1 := fields[0].(string)
	value, ok2 := fields[1].(string)
	if !ok1 || !ok2 {
		return "", "", errors.New("invalid lua value type")
	}
	if queueName == "" && value == "" { // queueName and value is empty means rpop without any values
		return "", "", go_redis.Nil
	}
	return queueName, value, nil
}

// Pack (tries, jobID) into lua struct pack of format "HHHc0", in lua this can be done:
//   ```local data = struct.pack("HHc0", tries, #job_id, job_id)```
func structPack(tries uint16, jobID string) (data string) {
	buf := make([]byte, 2+2+len(jobID))
	binary.LittleEndian.PutUint16(buf[0:], tries)
	binary.LittleEndian.PutUint16(buf[2:], uint16(len(jobID)))
	copy(buf[4:], jobID)
	return string(buf)
}

// Unpack the "HHc0" lua struct format, in lua this can be done:
//   ```local tries, job_id = struct.unpack("HHc0", data)```
func structUnpack(data string) (tries uint16, jobID string, err error) {
	buf := []byte(data)
	h1 := binary.LittleEndian.Uint16(buf[0:])
	h2 := binary.LittleEndian.Uint16(buf[2:])
	jobID = string(buf[4:])
	tries = h1
	if len(jobID) != int(h2) {
		err = errors.New("corrupted data")
	}
	return
}
