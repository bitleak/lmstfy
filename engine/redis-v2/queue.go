package redis_v2

import (
	"errors"
	"fmt"
	"time"

	"github.com/bitleak/lmstfy/engine"
	go_redis "github.com/go-redis/redis/v8"
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

type queue struct {
	namespace string
	queue     string
}

func (q *queue) ReadyQueueString() string {
	return join(ReadyQueuePrefix, q.namespace, q.queue)
}

func (q *queue) DeadletterString() string {
	return join(DeadLetterPrefix, q.namespace, q.queue)
}

func (q *queue) PoolPrefixString() string {
	return join(PoolPrefix, q.namespace, q.queue)
}

func (q *queue) Decode(str string) error {
	splits := splits(3, str)
	if len(splits) != 3 || (splits[0] != ReadyQueuePrefix && splits[0] != DeadLetterPrefix && splits[0] != PoolPrefix) {
		return errors.New("invalid format")
	}
	q.namespace = splits[1]
	q.queue = splits[2]
	return nil
}

// Queue is the "ready queue" that has all the jobs that can be consumed right now
type Queue struct {
	queue queue
	redis *RedisInstance

	destroySHA string
}

func NewQueue(ns, q string, redis *RedisInstance) *Queue {
	return &Queue{
		queue: queue{namespace: ns, queue: q},
		redis: redis,

		// NOTE: deadletter and queue are actually the same data structure, we could reuse the lua script
		// to empty the redis list (used as queue here). all we need to do is pass the queue name as the
		// deadletter name.
		destroySHA: deleteDeadletterSHA,
	}
}

func (q *Queue) Name() string {
	return q.queue.ReadyQueueString()
}

// Push a job into the queue, the job data format: {tries}{job id}
func (q *Queue) Push(j engine.Job) error {
	if j.Tries() == 0 {
		return nil
	}
	if j.Namespace() != q.queue.namespace || j.Queue() != q.queue.queue {
		// Wrong queue for the job
		return engine.ErrWrongQueue
	}
	//metrics.queueDirectPushJobs.WithLabelValues(q.redis.Name).Inc()
	return q.redis.Conn.LPush(dummyCtx, q.Name(), j.ID()).Err()
}

// Pop a job. If the tries > 0, add job to the "in-flight" timer with timestamp
// set to `TTR + now()`; Or we might just move the job to "dead-letter".
func (q *Queue) Poll(timeoutSecond uint32) (jobID string, err error) {
	_, jobID, err = PollQueues(q.redis, []queue{q.queue}, timeoutSecond)
	return jobID, err
}

// Return number of the current in-queue jobs
func (q *Queue) Size() (size int64, err error) {
	return q.redis.Conn.LLen(dummyCtx, q.Name()).Result()
}

// Peek a right-most element in the list without popping it
func (q *Queue) Peek() (jobID string, err error) {
	jobID, err = q.redis.Conn.LIndex(dummyCtx, q.Name(), -1).Result()
	switch err {
	case nil:
		// continue
	case go_redis.Nil:
		return "", engine.ErrNotFound
	default:
		return "", err
	}
	return jobID, err
}

func (q *Queue) Destroy() (count int64, err error) {
	poolPrefix := q.queue.PoolPrefixString()
	for {
		val, err := q.redis.Conn.EvalSha(dummyCtx, q.destroySHA, []string{q.Name(), poolPrefix}, BatchSize).Result()
		if err != nil {
			if isLuaScriptGone(err) {
				if err := PreloadDeadLetterLuaScript(q.redis); err != nil {
					logger.WithField("err", err).Error("Failed to load deadletter lua script")
				}
				continue
			}
			return count, err
		}
		n, _ := val.(int64)
		count += n
		if n < BatchSize { // Dead letter is empty
			break
		}
	}
	return count, nil
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

// Poll from multiple queues using blocking method; OR pop a job from one queue using non-blocking method
func PollQueues(redis *RedisInstance, queues []queue, timeoutSecond uint32) (q *queue, jobID string, err error) {
	defer func() {
		if jobID != "" {
			//metrics.queuePopJobs.WithLabelValues(redis.Name).Inc()
		}
	}()

	var val []string
	keys := make([]string, len(queues))
	for i, k := range queues {
		keys[i] = k.ReadyQueueString()
	}
	if timeoutSecond > 0 { // Blocking poll
		val, err = redis.Conn.BRPop(dummyCtx, time.Duration(timeoutSecond)*time.Second, keys...).Result()
	} else { // Non-Blocking fetch
		val = make([]string, 2) // Just to be coherent with BRPop return values
		val[0], val[1], err = popMultiQueues(redis, keys)
	}
	switch err {
	case nil:
		// continue
	case go_redis.Nil:
		logger.Debug("Job not found")
		return nil, "", nil
	default:
		logger.WithField("err", err).Error("Failed to pop job from queue")
		return nil, "", err
	}
	q = &queue{}
	if err := q.Decode(val[0]); err != nil {
		logger.WithField("err", err).Error("Failed to decode queue name")
		return nil, "", err
	}
	jobID = val[1]
	// we have no tries in queues, we need do tries-- and put job into queue in engine
	// Note: if we do tries-- in pump and use rpoplpush and brpoplpush in poll we can do this atomic.
	// But we can't support blocking poll multi queues.
	return q, jobID, nil
}
